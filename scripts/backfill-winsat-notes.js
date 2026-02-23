#!/usr/bin/env node
'use strict';

const { Client } = require('pg');
const { spawnSync } = require('child_process');
const { URL } = require('url');

const DATABASE_URL = process.env.DATABASE_URL;
if (!DATABASE_URL) {
  console.error('DATABASE_URL is required.');
  process.exit(1);
}

const MC_ALIAS = process.env.MC_ALIAS || 'diagobj';
const OBJECT_STORAGE_ENDPOINT = process.env.OBJECT_STORAGE_ENDPOINT || 'http://10.1.10.28:9000';
const OBJECT_STORAGE_ACCESS_KEY = process.env.OBJECT_STORAGE_ACCESS_KEY || 'codexminio';
const OBJECT_STORAGE_SECRET_KEY = process.env.OBJECT_STORAGE_SECRET_KEY || 'semngIYo36sZq27tixYVXeFF';
const OBJECT_STORAGE_PREFIX = process.env.OBJECT_STORAGE_PREFIX || 'run';

function buildMcHost(endpoint, accessKey, secretKey) {
  const url = new URL(endpoint);
  const encodedUser = encodeURIComponent(accessKey);
  const encodedPass = encodeURIComponent(secretKey);
  return `${url.protocol}//${encodedUser}:${encodedPass}@${url.host}`;
}

function normalizeDestination(dest) {
  if (!dest) {
    return null;
  }
  const cleaned = String(dest).replace(/^\/+/, '');
  if (!cleaned) {
    return null;
  }
  if (cleaned.startsWith(`${MC_ALIAS}/`)) {
    return cleaned;
  }
  return `${MC_ALIAS}/${cleaned}`;
}

function buildDestinationCandidates(dest) {
  const normalized = normalizeDestination(dest);
  if (!normalized) {
    return [];
  }
  const parts = normalized.split('/').filter(Boolean);
  if (parts.length < 2) {
    return [normalized];
  }
  const alias = parts[0];
  const bucket = parts[1];
  const rest = parts.slice(2);
  if (rest[0] === OBJECT_STORAGE_PREFIX) {
    return [normalized];
  }
  const withPrefix = [alias, bucket, OBJECT_STORAGE_PREFIX, ...rest].join('/');
  if (withPrefix === normalized) {
    return [normalized];
  }
  return [normalized, withPrefix];
}

function mcCat(path) {
  const result = spawnSync('mc', ['cat', path], {
    encoding: 'utf8',
    env: process.env,
    maxBuffer: 10 * 1024 * 1024
  });
  if (result.status !== 0) {
    const err = (result.stderr || result.stdout || '').trim();
    return { ok: false, error: err || `mc cat failed (${result.status})` };
  }
  return { ok: true, data: result.stdout };
}

function winSatNote(score) {
  if (typeof score !== 'number' || !Number.isFinite(score)) {
    return null;
  }
  if (score < 3.0) return 'Horrible';
  if (score < 4.5) return 'Mauvais';
  if (score < 6.0) return 'Moyen';
  if (score < 7.5) return 'Bon';
  return 'Excellent';
}

function extractWinSatScores(winsat) {
  const winSpr = winsat && winsat.winSPR && typeof winsat.winSPR === 'object' ? winsat.winSPR : null;
  const cpuScore = winSpr && typeof winSpr.CpuScore === 'number' ? winSpr.CpuScore : null;
  const memScore = winSpr && typeof winSpr.MemoryScore === 'number' ? winSpr.MemoryScore : null;
  let graphicsScore = null;
  if (winSpr) {
    if (typeof winSpr.GamingScore === 'number') {
      graphicsScore = winSpr.GamingScore;
    } else if (typeof winSpr.GraphicsScore === 'number') {
      graphicsScore = winSpr.GraphicsScore;
    }
  }
  return { cpuScore, memScore, graphicsScore };
}

function buildMachineKeySql() {
  return `
    CASE
      WHEN serial_number IS NOT NULL AND serial_number <> '' AND mac_address IS NOT NULL AND mac_address <> ''
        THEN 'sn:' || serial_number || '|mac:' || mac_address
      WHEN serial_number IS NOT NULL AND serial_number <> ''
        THEN 'sn:' || serial_number
      WHEN mac_address IS NOT NULL AND mac_address <> ''
        THEN 'mac:' || mac_address
      WHEN hostname IS NOT NULL AND hostname <> ''
        THEN 'host:' || lower(hostname)
      ELSE machine_key
    END
  `;
}

async function updateMachineKeys(client) {
  const expr = buildMachineKeySql();
  await client.query(`UPDATE machines SET machine_key = ${expr}`);
  await client.query(`UPDATE reports SET machine_key = ${expr}`);
}

async function main() {
  const mcHost = buildMcHost(OBJECT_STORAGE_ENDPOINT, OBJECT_STORAGE_ACCESS_KEY, OBJECT_STORAGE_SECRET_KEY);
  process.env[`MC_HOST_${MC_ALIAS}`] = mcHost;

  const client = new Client({ connectionString: DATABASE_URL });
  await client.connect();

  console.log('Updating machine_key to serial+mac when available...');
  try {
    await client.query('BEGIN');
    await updateMachineKeys(client);
    await client.query('COMMIT');
  } catch (error) {
    await client.query('ROLLBACK');
    console.error('Failed to update machine_key values:', error.message || error);
    await client.end();
    process.exit(1);
  }

  const result = await client.query('SELECT id, payload FROM reports WHERE payload IS NOT NULL');
  let scanned = 0;
  let fetched = 0;
  let updated = 0;
  let skipped = 0;

  for (const row of result.rows) {
    scanned += 1;
    let payload;
    try {
      payload = JSON.parse(row.payload);
    } catch (error) {
      skipped += 1;
      continue;
    }
    if (!payload || typeof payload !== 'object') {
      skipped += 1;
      continue;
    }

    const tests = payload.tests && typeof payload.tests === 'object' ? payload.tests : {};
    const needsNotes = !tests.cpuNote || !tests.ramNote || !tests.gpuNote;
    const needsWinSat = !payload.winsat;
    if (!needsNotes && !needsWinSat) {
      skipped += 1;
      continue;
    }

    let didUpdate = false;
    if (payload.winsat && typeof payload.winsat === 'object') {
      const { cpuScore, memScore, graphicsScore } = extractWinSatScores(payload.winsat);
      if (!tests.cpuNote && cpuScore != null) {
        tests.cpuNote = winSatNote(cpuScore);
        didUpdate = true;
      }
      if (!tests.ramNote && memScore != null) {
        tests.ramNote = winSatNote(memScore);
        didUpdate = true;
      }
      const gpuScore = graphicsScore != null ? graphicsScore : tests.gpuScore != null ? tests.gpuScore : null;
      if (!tests.gpuNote && gpuScore != null) {
        tests.gpuNote = winSatNote(gpuScore);
        didUpdate = true;
      }
      if (didUpdate) {
        payload.tests = tests;
      }
    }

    if (needsWinSat || !didUpdate) {
      const destination = payload.rawArtifacts && payload.rawArtifacts.destination;
      const destinationCandidates = buildDestinationCandidates(destination);
      if (!destinationCandidates.length) {
        if (!didUpdate) {
          skipped += 1;
        }
        continue;
      }

      let rawPayload = null;
      for (const candidate of destinationCandidates) {
        const payloadPath = `${candidate.replace(/\/+$/, '')}/payload.json`;
        const mcResult = mcCat(payloadPath);
        if (!mcResult.ok) {
          continue;
        }
        try {
          const cleaned =
            typeof mcResult.data === 'string' ? mcResult.data.replace(/^\uFEFF/, '') : mcResult.data;
          rawPayload = JSON.parse(cleaned);
        } catch (error) {
          rawPayload = null;
        }
        if (rawPayload) {
          fetched += 1;
          break;
        }
      }
      if (!rawPayload || typeof rawPayload !== 'object') {
        if (!didUpdate) {
          skipped += 1;
        }
        continue;
      }

      if (!payload.winsat && rawPayload.winsat) {
        payload.winsat = rawPayload.winsat;
        didUpdate = true;
      }

      const rawTests = rawPayload.tests && typeof rawPayload.tests === 'object' ? rawPayload.tests : null;
      payload.tests = {
        ...(rawTests || {}),
        ...(payload.tests && typeof payload.tests === 'object' ? payload.tests : {})
      };

      const winsat = payload.winsat || rawPayload.winsat;
      const { cpuScore, memScore, graphicsScore } = extractWinSatScores(winsat);

      if (!payload.tests.cpuNote && cpuScore != null) {
        payload.tests.cpuNote = winSatNote(cpuScore);
        didUpdate = true;
      }
      if (!payload.tests.ramNote && memScore != null) {
        payload.tests.ramNote = winSatNote(memScore);
        didUpdate = true;
      }
      const gpuScore =
        graphicsScore != null
          ? graphicsScore
          : payload.tests.gpuScore != null
            ? payload.tests.gpuScore
            : rawTests && rawTests.gpuScore != null
              ? rawTests.gpuScore
              : null;
      if (!payload.tests.gpuNote && gpuScore != null) {
        payload.tests.gpuNote = winSatNote(gpuScore);
        didUpdate = true;
      }
    }

    const newPayload = JSON.stringify(payload);
    if (newPayload !== row.payload) {
      await client.query('UPDATE reports SET payload = $1 WHERE id = $2', [newPayload, row.id]);
      updated += 1;
    } else {
      skipped += 1;
    }
  }

  await client.end();
  console.log(
    `Backfill done. scanned=${scanned} fetched=${fetched} updated=${updated} skipped=${skipped}`
  );
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
