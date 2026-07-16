#!/usr/bin/env node

// Devora Master Orchestrator — runs all intelligence sources in parallel
// Outputs structured JSON for Claude to synthesize into actionable briefing

import './utils/env.mjs'; // Load API keys from .env
import { pathToFileURL } from 'node:url';

// === Tier 1: Core OSINT & Geopolitical ===
import { briefing as gdelt } from './sources/gdelt.mjs';
import { briefing as opensky } from './sources/opensky.mjs';
import { briefing as firms } from './sources/firms.mjs';
import { briefing as ships } from './sources/ships.mjs';
import { briefing as safecast } from './sources/safecast.mjs';
import { briefing as acled } from './sources/acled.mjs';
import { briefing as reliefweb } from './sources/reliefweb.mjs';
import { briefing as who } from './sources/who.mjs';
import { briefing as ofac } from './sources/ofac.mjs';
import { briefing as opensanctions } from './sources/opensanctions.mjs';
import { briefing as adsb } from './sources/adsb.mjs';

// === Tier 2: Economic & Financial ===
import { briefing as fred } from './sources/fred.mjs';
import { briefing as treasury } from './sources/treasury.mjs';
import { briefing as bls } from './sources/bls.mjs';
import { briefing as eia } from './sources/eia.mjs';
import { briefing as gscpi } from './sources/gscpi.mjs';
import { briefing as usaspending } from './sources/usaspending.mjs';
import { briefing as comtrade } from './sources/comtrade.mjs';

// === Tier 3: Weather, Environment, Technology, Social ===
import { briefing as noaa } from './sources/noaa.mjs';
import { briefing as epa } from './sources/epa.mjs';
import { briefing as patents } from './sources/patents.mjs';
import { briefing as bluesky } from './sources/bluesky.mjs';
import { briefing as reddit } from './sources/reddit.mjs';
import { briefing as telegram } from './sources/telegram.mjs';
import { briefing as kiwisdr } from './sources/kiwisdr.mjs';

// === Tier 4: Space & Satellites ===
import { briefing as space } from './sources/space.mjs';

// === Tier 5: Live Market Data ===
import { briefing as yfinance } from './sources/yfinance.mjs';

// === Tier 6: Cyber & Infrastructure ===
import { briefing as cisaKev } from './sources/cisa-kev.mjs';
import { briefing as cloudflareRadar } from './sources/cloudflare-radar.mjs';

const SOURCE_TIMEOUT_MS = 30_000; // 30s max per individual source

export async function runSource(name, fn, ...args) {
  const start = Date.now();
  let timer;
  try {
    const dataPromise = fn(...args);
    const timeoutPromise = new Promise((_, reject) => {
      timer = setTimeout(() => reject(new Error(`Source ${name} timed out after ${SOURCE_TIMEOUT_MS / 1000}s`)), SOURCE_TIMEOUT_MS);
    });
    const data = await Promise.race([dataPromise, timeoutPromise]);
    return { name, status: 'ok', durationMs: Date.now() - start, data };
  } catch (e) {
    return { name, status: 'error', durationMs: Date.now() - start, error: e.message };
  } finally {
    clearTimeout(timer);
  }
}

async function runBatches(tasks, batchSize = 4) {
  const results = [];
  for (let i = 0; i < tasks.length; i += batchSize) {
    const batch = tasks.slice(i, i + batchSize);
    console.error(`[Devora] Sweeping batch ${Math.floor(i / batchSize) + 1}/${Math.ceil(tasks.length / batchSize)} (${batch.map(b => b.name).join(', ')})...`);
    const batchPromises = batch.map(t => runSource(t.name, t.fn, ...(t.args || [])));
    const batchResults = await Promise.all(batchPromises);
    results.push(...batchResults);
  }
  return results;
}

export async function fullBriefing() {
  console.error('[Devora] Starting intelligence sweep — 29 sources in parallel batches...');
  const start = Date.now();

  const tasks = [
    // Tier 1: Core OSINT & Geopolitical
    { name: 'GDELT', fn: gdelt },
    { name: 'OpenSky', fn: opensky },
    { name: 'FIRMS', fn: firms },
    { name: 'Maritime', fn: ships },
    { name: 'Safecast', fn: safecast },
    { name: 'ACLED', fn: acled },
    { name: 'ReliefWeb', fn: reliefweb },
    { name: 'WHO', fn: who },
    { name: 'OFAC', fn: ofac },
    { name: 'OpenSanctions', fn: opensanctions },
    { name: 'ADS-B', fn: adsb },

    // Tier 2: Economic & Financial
    { name: 'FRED', fn: fred, args: [process.env.FRED_API_KEY] },
    { name: 'Treasury', fn: treasury },
    { name: 'BLS', fn: bls, args: [process.env.BLS_API_KEY] },
    { name: 'EIA', fn: eia, args: [process.env.EIA_API_KEY] },
    { name: 'GSCPI', fn: gscpi },
    { name: 'USAspending', fn: usaspending },
    { name: 'Comtrade', fn: comtrade },

    // Tier 3: Weather, Environment, Technology, Social
    { name: 'NOAA', fn: noaa },
    { name: 'EPA', fn: epa },
    { name: 'Patents', fn: patents },
    { name: 'Bluesky', fn: bluesky },
    { name: 'Reddit', fn: reddit },
    { name: 'Telegram', fn: telegram },
    { name: 'KiwiSDR', fn: kiwisdr },

    // Tier 4: Space & Satellites
    { name: 'Space', fn: space },

    // Tier 5: Live Market Data
    { name: 'YFinance', fn: yfinance },

    // Tier 6: Cyber & Infrastructure
    { name: 'CISA-KEV', fn: cisaKev },
    { name: 'Cloudflare-Radar', fn: cloudflareRadar },
  ];

  // Run in batches of 4 to stay well under Render's 512MB RAM limit
  const sources = await runBatches(tasks, 4);
  const totalMs = Date.now() - start;

  const output = {
    devora: {
      version: '2.0.0',
      timestamp: new Date().toISOString(),
      totalDurationMs: totalMs,
      sourcesQueried: sources.length,
      sourcesOk: sources.filter(s => s.status === 'ok').length,
      sourcesFailed: sources.filter(s => s.status !== 'ok').length,
    },
    sources: Object.fromEntries(
      sources.filter(s => s.status === 'ok').map(s => [s.name, s.data])
    ),
    errors: sources.filter(s => s.status !== 'ok').map(s => ({ name: s.name, error: s.error })),
    timing: Object.fromEntries(
      sources.map(s => [s.name, { status: s.status, ms: s.durationMs }])
    ),
  };

  console.error(`[Devora] Sweep complete in ${totalMs}ms — ${output.devora.sourcesOk}/${sources.length} sources returned data`);
  return output;
}

// Run and output when executed directly
const entryHref = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;

if (entryHref && import.meta.url === entryHref) {
  const data = await fullBriefing();
  console.log(JSON.stringify(data, null, 2));
}
