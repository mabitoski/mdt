const crypto = require('crypto');
const fs = require('fs');
const http = require('http');
const https = require('https');
const os = require('os');
const path = require('path');
const { URL } = require('url');
const { spawn } = require('child_process');
const net = require('net');
const tls = require('tls');
const express = require('express');
const helmet = require('helmet');
const rateLimit = require('express-rate-limit');
const session = require('express-session');
const LdapAuth = require('ldapauth-fork');
const PDFDocument = require('pdfkit');
const { Pool } = require('pg');

const app = express();
app.disable('x-powered-by');

const PORT = Number.parseInt(process.env.PORT || '3000', 10);
const PUBLIC_DIR = path.join(__dirname, 'public');
const BRAND_LOGO_PATH = path.join(PUBLIC_DIR, 'logo.png');
const JSON_LIMIT = process.env.JSON_LIMIT || '256kb';
const INGEST_RATE_LIMIT = Number.parseInt(process.env.INGEST_RATE_LIMIT || '180', 10);
const SESSION_SECRET =
  process.env.SESSION_SECRET || crypto.randomBytes(32).toString('hex');
const SESSION_NAME = process.env.SESSION_NAME || 'mdt.sid';
const COOKIE_SECURE = process.env.COOKIE_SECURE === '1';
const ALLOW_LOCAL_ADMIN = process.env.ALLOW_LOCAL_ADMIN !== '0';
const LOCAL_ADMIN_USER = process.env.LOCAL_ADMIN_USER || 'admin';
const LOCAL_ADMIN_PASSWORD = process.env.LOCAL_ADMIN_PASSWORD || '';
const VAULT_URL = process.env.VAULT_URL || '';
const VAULT_AUTH_PATH = process.env.VAULT_AUTH_PATH || '/auth/service';
const VAULT_AUTH_TOKEN = process.env.VAULT_AUTH_TOKEN || '';
const VAULT_AUTH_SCOPES = process.env.VAULT_AUTH_SCOPES || 'read';
const VAULT_BEARER_TOKEN = process.env.VAULT_BEARER_TOKEN || '';
const VAULT_SECRET_PATH = process.env.VAULT_SECRET_PATH || '';
const VAULT_SECRET_USER_FIELD = process.env.VAULT_SECRET_USER_FIELD || 'username';
const VAULT_SECRET_PASSWORD_FIELD = process.env.VAULT_SECRET_PASSWORD_FIELD || 'password';
const VAULT_CACHE_TTL_SEC = Number.parseInt(process.env.VAULT_CACHE_TTL_SEC || '300', 10);
const VAULT_TIMEOUT_MS = Number.parseInt(process.env.VAULT_TIMEOUT_MS || '4000', 10);
const SUGGESTION_EMAIL_ENABLED = process.env.SUGGESTION_EMAIL_ENABLED !== '0';
const SUGGESTION_EMAIL_TO = process.env.SUGGESTION_EMAIL_TO || 'jordan.turck@marl-ds.com';
const SUGGESTION_EMAIL_FROM = process.env.SUGGESTION_EMAIL_FROM || '';
const SUGGESTION_SMTP_HOST = process.env.SUGGESTION_SMTP_HOST || 'smtp.gmail.com';
const SUGGESTION_SMTP_PORT = Number.parseInt(process.env.SUGGESTION_SMTP_PORT || '465', 10);
const SUGGESTION_SMTP_SECURE = process.env.SUGGESTION_SMTP_SECURE !== '0';
const SUGGESTION_SMTP_TIMEOUT_MS = Number.parseInt(
  process.env.SUGGESTION_SMTP_TIMEOUT_MS || '10000',
  10
);
const SUGGESTION_SMTP_USER = process.env.SUGGESTION_SMTP_USER || '';
const SUGGESTION_SMTP_PASS = process.env.SUGGESTION_SMTP_PASS || '';
const SUGGESTION_VAULT_SECRET_PATH =
  process.env.SUGGESTION_VAULT_SECRET_PATH || '';
const SUGGESTION_VAULT_USER_FIELD =
  process.env.SUGGESTION_VAULT_USER_FIELD || 'username';
const SUGGESTION_VAULT_PASSWORD_FIELD =
  process.env.SUGGESTION_VAULT_PASSWORD_FIELD || 'password';
const OBJECT_STORAGE_ENDPOINT =
  process.env.OBJECT_STORAGE_ENDPOINT || process.env.MDT_OBJECT_STORAGE_ENDPOINT || '';
const OBJECT_STORAGE_BUCKET =
  process.env.OBJECT_STORAGE_BUCKET || process.env.MDT_OBJECT_STORAGE_BUCKET || 'alcyone-archive';
const OBJECT_STORAGE_ACCESS_KEY =
  process.env.OBJECT_STORAGE_ACCESS_KEY || process.env.MDT_OBJECT_STORAGE_ACCESS_KEY || '';
const OBJECT_STORAGE_SECRET_KEY =
  process.env.OBJECT_STORAGE_SECRET_KEY || process.env.MDT_OBJECT_STORAGE_SECRET_KEY || '';
const OBJECT_STORAGE_PREFIX =
  process.env.OBJECT_STORAGE_PREFIX || process.env.MDT_OBJECT_STORAGE_PREFIX || 'run';
const OBJECT_STORAGE_ALIAS = process.env.OBJECT_STORAGE_ALIAS || 'alcyone';
const OBJECT_STORAGE_RENAME_ON_TAG =
  ['1', 'true', 'yes', 'on'].includes((process.env.OBJECT_STORAGE_RENAME_ON_TAG || '').toLowerCase());
const DEFAULT_LDAP_SEARCH_FILTER = '(sAMAccountName={{username}})';
const DEFAULT_LDAP_SEARCH_ATTRIBUTES = 'dn,cn,mail';
const LDAP_URL = process.env.LDAP_URL || '';
const GRAFANA_PUBLIC_URL = process.env.GRAFANA_PUBLIC_URL || 'http://10.1.10.27:3002';

function normalizeOrigin(value) {
  if (!value) {
    return '';
  }
  try {
    const parsed = new URL(value);
    return parsed.origin || '';
  } catch (error) {
    return '';
  }
}

const GRAFANA_CSP_SOURCES = Array.from(
  new Set(
    [
      'http://10.1.10.27:3002',
      'http://127.0.0.1:3002',
      'http://localhost:3002',
      'https://10.1.10.27:3002',
      'https://127.0.0.1:3002',
      'https://localhost:3002',
      normalizeOrigin(GRAFANA_PUBLIC_URL)
    ].filter(Boolean)
  )
);
const LDAP_BIND_DN = process.env.LDAP_BIND_DN || '';
const LDAP_BIND_PASSWORD = process.env.LDAP_BIND_PASSWORD || '';
const LDAP_SEARCH_BASE = process.env.LDAP_SEARCH_BASE || '';
const LDAP_SEARCH_FILTER =
  process.env.LDAP_SEARCH_FILTER || DEFAULT_LDAP_SEARCH_FILTER;
const LDAP_SEARCH_ATTRIBUTES_RAW =
  process.env.LDAP_SEARCH_ATTRIBUTES || DEFAULT_LDAP_SEARCH_ATTRIBUTES;
const LDAP_TLS_REJECT_UNAUTHORIZED =
  process.env.LDAP_TLS_REJECT_UNAUTHORIZED !== '0';
const ENV_LDAP_ENABLED = Boolean(LDAP_URL && LDAP_SEARCH_BASE);
const HYDRA_ADMIN_GROUP_DN = (
  process.env.HYDRA_ADMIN_GROUP_DN ||
  'CN=HYDRA_ADMINS,OU=GROUPS,OU=TIER2,DC=nova,DC=local'
).toLowerCase();
const DATABASE_URL = process.env.DATABASE_URL || '';
const PGSSLMODE = (process.env.PGSSLMODE || '').toLowerCase();
const PGSSL =
  process.env.PGSSL === '1' ||
  PGSSLMODE === 'require' ||
  PGSSLMODE === 'verify-full' ||
  PGSSLMODE === 'verify-ca';
const PGSSL_REJECT_UNAUTHORIZED = process.env.PGSSL_REJECT_UNAUTHORIZED !== '0';
const AUDIT_LOG_ENABLED = process.env.AUDIT_LOG_ENABLED !== '0';
const AUDIT_LOG_LIMIT_DEFAULT = Number.parseInt(process.env.AUDIT_LOG_LIMIT || '100', 10);
const AUDIT_LOG_LIMIT_MAX = Number.parseInt(process.env.AUDIT_LOG_LIMIT_MAX || '500', 10);
const REPORT_PAGE_LIMIT_DEFAULT = Number.parseInt(process.env.REPORT_PAGE_LIMIT || '60', 10);
const REPORT_PAGE_LIMIT_MAX = Number.parseInt(process.env.REPORT_PAGE_LIMIT_MAX || '200', 10);
const LOT_TARGET_COUNT_MIN = 1;
const LOT_TARGET_COUNT_MAX = Number.parseInt(process.env.LOT_TARGET_COUNT_MAX || '50000', 10);
const LOT_PRIORITY_DEFAULT = Number.parseInt(process.env.LOT_PRIORITY_DEFAULT || '100', 10);
const LOT_PRIORITY_MIN = 1;
const LOT_PRIORITY_MAX = Number.parseInt(process.env.LOT_PRIORITY_MAX || '9999', 10);

if (!Number.isFinite(PORT)) {
  throw new Error('PORT must be a number');
}

const pool = new Pool({
  connectionString: DATABASE_URL || undefined,
  ssl: PGSSL ? { rejectUnauthorized: PGSSL_REJECT_UNAUTHORIZED } : undefined
});

pool.on('error', (error) => {
  console.error('PostgreSQL pool error', error);
});

function wait(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function generateRequestId() {
  if (typeof crypto.randomUUID === 'function') {
    return crypto.randomUUID();
  }
  return crypto.randomBytes(16).toString('hex');
}

function generateUuid() {
  if (typeof crypto.randomUUID === 'function') {
    return crypto.randomUUID();
  }
  const hex = crypto.randomBytes(16).toString('hex');
  return normalizeUuid(hex);
}

function normalizeObjectStorageSegment(value) {
  if (!value) {
    return '';
  }
  return String(value)
    .trim()
    .replace(/[^A-Za-z0-9._-]+/g, '-')
    .replace(/^-+|-+$/g, '');
}

function hasObjectStorageConfig() {
  return Boolean(
    OBJECT_STORAGE_ENDPOINT &&
      OBJECT_STORAGE_BUCKET &&
      OBJECT_STORAGE_ACCESS_KEY &&
      OBJECT_STORAGE_SECRET_KEY
  );
}

function runMcCommand(args, configDir) {
  return new Promise((resolve, reject) => {
    const env = { ...process.env, MC_CONFIG_DIR: configDir };
    const proc = spawn('mc', args, { env });
    let stdout = '';
    let stderr = '';
    proc.stdout.on('data', (chunk) => {
      stdout += chunk.toString();
    });
    proc.stderr.on('data', (chunk) => {
      stderr += chunk.toString();
    });
    proc.on('error', (error) => {
      reject(error);
    });
    proc.on('close', (code) => {
      if (code === 0) {
        resolve({ stdout, stderr });
        return;
      }
      reject(new Error(`mc ${args.join(' ')} failed (${code}): ${stderr || stdout}`));
    });
  });
}

async function renameObjectStoragePrefix(oldPrefix, newPrefix) {
  const source = normalizeObjectStorageSegment(oldPrefix);
  const destination = normalizeObjectStorageSegment(newPrefix);
  if (!source || !destination || source === destination) {
    return { ok: false, error: 'invalid_prefix', source, destination };
  }
  if (!hasObjectStorageConfig()) {
    return { ok: false, error: 'object_storage_not_configured', source, destination };
  }

  const configDir = await fs.promises.mkdtemp(path.join(os.tmpdir(), 'mc-'));
  try {
    await runMcCommand(
      ['alias', 'set', OBJECT_STORAGE_ALIAS, OBJECT_STORAGE_ENDPOINT, OBJECT_STORAGE_ACCESS_KEY, OBJECT_STORAGE_SECRET_KEY],
      configDir
    );
    const srcPath = `${OBJECT_STORAGE_ALIAS}/${OBJECT_STORAGE_BUCKET}/${source}/`;
    const dstPath = `${OBJECT_STORAGE_ALIAS}/${OBJECT_STORAGE_BUCKET}/${destination}/`;
    await runMcCommand(['mirror', '--overwrite', srcPath, dstPath], configDir);
    await runMcCommand(
      ['rm', '--recursive', '--force', `${OBJECT_STORAGE_ALIAS}/${OBJECT_STORAGE_BUCKET}/${source}`],
      configDir
    );
    return { ok: true, source, destination };
  } catch (error) {
    return { ok: false, error: error.message || String(error), source, destination };
  } finally {
    try {
      await fs.promises.rm(configDir, { recursive: true, force: true });
    } catch (cleanupError) {
      console.warn('Failed to cleanup mc config dir', cleanupError);
    }
  }
}

async function getActiveTag(client) {
  const result = await client.query(
    'SELECT id, name, is_active FROM tags WHERE is_active = true ORDER BY updated_at DESC LIMIT 1'
  );
  const row = result.rows && result.rows[0] ? result.rows[0] : null;
  return row || null;
}

async function ensureActiveTag(client) {
  const existing = await getActiveTag(client);
  if (existing) {
    return existing;
  }
  const fallbackResult = await client.query(
    'SELECT id, name, is_active FROM tags WHERE LOWER(name) = LOWER($1) LIMIT 1',
    [DEFAULT_REPORT_TAG]
  );
  const fallback = fallbackResult.rows && fallbackResult.rows[0] ? fallbackResult.rows[0] : null;
  if (fallback) {
    await client.query('UPDATE tags SET is_active = true, updated_at = NOW() WHERE id = $1', [
      fallback.id
    ]);
    return { ...fallback, is_active: true };
  }
  const id = generateUuid();
  await client.query(
    `
      INSERT INTO tags (id, name, is_active, created_at, updated_at)
      VALUES ($1, $2, true, NOW(), NOW())
    `,
    [id, DEFAULT_REPORT_TAG]
  );
  return { id, name: DEFAULT_REPORT_TAG, is_active: true };
}

async function resolveTagForIngest(client, tagIdRaw, tagNameRaw) {
  const tagId = normalizeUuid(tagIdRaw);
  const tagName = cleanString(tagNameRaw, 64);

  if (tagId) {
    const result = await client.query('SELECT id, name, is_active FROM tags WHERE id = $1', [
      tagId
    ]);
    const row = result.rows && result.rows[0] ? result.rows[0] : null;
    if (row) {
      return row;
    }
  }

  if (tagName) {
    const lookup = await client.query(
      'SELECT id, name, is_active FROM tags WHERE LOWER(name) = LOWER($1) LIMIT 1',
      [tagName]
    );
    if (lookup.rows && lookup.rows[0]) {
      return lookup.rows[0];
    }
    const newId = generateUuid();
    try {
      await client.query(
        `
          INSERT INTO tags (id, name, is_active, created_at, updated_at)
          VALUES ($1, $2, false, NOW(), NOW())
        `,
        [newId, tagName]
      );
      return { id: newId, name: tagName, is_active: false };
    } catch (error) {
      const retry = await client.query(
        'SELECT id, name, is_active FROM tags WHERE LOWER(name) = LOWER($1) LIMIT 1',
        [tagName]
      );
      if (retry.rows && retry.rows[0]) {
        return retry.rows[0];
      }
    }
  }

  return ensureActiveTag(client);
}

async function listTags(client) {
  const result = await client.query(
    'SELECT id, name, is_active FROM tags ORDER BY LOWER(name)'
  );
  return Array.isArray(result.rows) ? result.rows : [];
}

function buildLegacyFilterClause(flag) {
  const legacyFlag = String(flag || '').toLowerCase();
  if (legacyFlag === '1' || legacyFlag === 'true') {
    return `payload IS NOT NULL AND payload <> '' AND payload::jsonb ? 'legacy'`;
  }
  if (legacyFlag === '0' || legacyFlag === 'false') {
    return `(payload IS NULL OR payload = '' OR NOT (payload::jsonb ? 'legacy'))`;
  }
  return '';
}

async function listTagsWithCounts(client, { legacyFlag = null, includeActive = true } = {}) {
  const legacyClause = buildLegacyFilterClause(legacyFlag);
  const reportWhereParts = ['tag_id IS NOT NULL'];
  if (legacyClause) {
    reportWhereParts.push(legacyClause);
  }
  const reportWhere = reportWhereParts.length ? `WHERE ${reportWhereParts.join(' AND ')}` : '';
  const activeWhere = includeActive ? 'tags.is_active = true OR ' : '';
  const result = await client.query(
    `
      SELECT
        tags.id,
        tags.name,
        tags.is_active,
        COALESCE(report_counts.count, 0) AS report_count
      FROM tags
      LEFT JOIN (
        SELECT tag_id, COUNT(*) AS count
        FROM reports
        ${reportWhere}
        GROUP BY tag_id
      ) report_counts ON report_counts.tag_id = tags.id
      WHERE ${activeWhere} COALESCE(report_counts.count, 0) > 0
      ORDER BY LOWER(tags.name)
    `
  );
  return Array.isArray(result.rows) ? result.rows : [];
}

async function getLotById(client, lotId, { forUpdate = false } = {}) {
  const normalized = normalizeUuid(lotId);
  if (!normalized) {
    return null;
  }
  const lockClause = forUpdate ? ' FOR UPDATE' : '';
  const result = await client.query(
    `
      SELECT
        id,
        supplier,
        lot_number,
        target_count,
        produced_count,
        priority,
        is_paused,
        created_by,
        created_at,
        updated_at
      FROM lots
      WHERE id = $1
      ${lockClause}
      LIMIT 1
    `,
    [normalized]
  );
  return result.rows && result.rows[0] ? result.rows[0] : null;
}

async function listLotsWithAssignments(client) {
  const result = await client.query(`
    SELECT
      lots.id,
      lots.supplier,
      lots.lot_number,
      lots.target_count,
      lots.produced_count,
      lots.priority,
      lots.is_paused,
      lots.created_by,
      lots.created_at,
      lots.updated_at,
      COALESCE(
        json_agg(
          json_build_object(
            'technicianKey', lot_assignments.technician_key,
            'technician', lot_assignments.technician_name
          )
          ORDER BY lot_assignments.technician_name
        ) FILTER (WHERE lot_assignments.technician_key IS NOT NULL),
        '[]'::json
      ) AS assignments
    FROM lots
    LEFT JOIN lot_assignments ON lot_assignments.lot_id = lots.id
    GROUP BY lots.id
    ORDER BY lots.is_paused ASC, (lots.produced_count >= lots.target_count) ASC, lots.priority ASC, lots.created_at ASC
  `);
  return Array.isArray(result.rows) ? result.rows : [];
}

async function findAssignedLotForTechnician(client, technician) {
  const techKey = normalizeTechKey(technician);
  if (!techKey) {
    return null;
  }
  const result = await client.query(
    `
      SELECT
        lots.id,
        lots.supplier,
        lots.lot_number,
        lots.target_count,
        lots.produced_count,
        lots.priority,
        lots.is_paused,
        lots.created_by,
        lots.created_at,
        lots.updated_at
      FROM lots
      INNER JOIN lot_assignments ON lot_assignments.lot_id = lots.id
      WHERE lot_assignments.technician_key = $1
        AND lots.is_paused = false
        AND lots.produced_count < lots.target_count
      ORDER BY lots.priority ASC, lots.updated_at ASC, lots.created_at ASC
      LIMIT 1
    `,
    [techKey]
  );
  return result.rows && result.rows[0] ? result.rows[0] : null;
}

async function findPrioritizedLot(client) {
  const result = await client.query(`
    SELECT
      id,
      supplier,
      lot_number,
      target_count,
      produced_count,
      priority,
      is_paused,
      created_by,
      created_at,
      updated_at
    FROM lots
    WHERE is_paused = false
      AND produced_count < target_count
    ORDER BY priority ASC, updated_at ASC, created_at ASC
    LIMIT 1
  `);
  return result.rows && result.rows[0] ? result.rows[0] : null;
}

async function resolveLotForIngest(client, { explicitLotId = null, technician = null } = {}) {
  const explicit = explicitLotId ? await getLotById(client, explicitLotId) : null;
  if (explicit) {
    return { lot: explicit, mode: 'manual' };
  }
  const assigned = await findAssignedLotForTechnician(client, technician);
  if (assigned) {
    return { lot: assigned, mode: 'assigned' };
  }
  const prioritized = await findPrioritizedLot(client);
  if (prioritized) {
    return { lot: prioritized, mode: 'priority' };
  }
  return { lot: null, mode: 'none' };
}

async function registerLotProgress(
  client,
  {
    lot = null,
    machineKey = null,
    reportId = null,
    technician = null,
    source = 'ingest',
    isDoubleCheck = false,
    shouldCount = true
  } = {}
) {
  if (!lot || !lot.id || !machineKey || !shouldCount || isDoubleCheck) {
    return { counted: false, lot: lot || null };
  }
  const insertResult = await client.query(
    `
      INSERT INTO lot_progress (
        lot_id,
        machine_key,
        report_id,
        technician,
        source,
        is_double_check,
        counted_at
      )
      VALUES ($1, $2, $3, $4, $5, $6, NOW())
      ON CONFLICT (lot_id, machine_key) DO NOTHING
      RETURNING lot_id
    `,
    [lot.id, machineKey, normalizeUuid(reportId), technician || null, source || 'ingest', false]
  );
  if (!insertResult.rowCount) {
    return { counted: false, lot };
  }

  const updateResult = await client.query(
    `
      UPDATE lots
      SET produced_count = produced_count + 1,
          updated_at = NOW()
      WHERE id = $1
      RETURNING
        id,
        supplier,
        lot_number,
        target_count,
        produced_count,
        priority,
        is_paused,
        created_by,
        created_at,
        updated_at
    `,
    [lot.id]
  );
  return {
    counted: true,
    lot: updateResult.rows && updateResult.rows[0] ? updateResult.rows[0] : lot
  };
}

async function backfillReportsFromMachines() {
  try {
    const reportCount = await pool.query('SELECT COUNT(*) AS count FROM reports');
    const reportTotal = Number.parseInt(reportCount.rows?.[0]?.count || '0', 10);
    if (reportTotal > 0) {
      return;
    }

    const machines = await pool.query(`
      SELECT
        machine_key,
        hostname,
        mac_address,
        mac_addresses,
        serial_number,
        category,
        tag,
        tag_id,
        lot_id,
        model,
        vendor,
        technician,
        os_version,
        ram_mb,
        ram_slots_total,
        ram_slots_free,
        battery_health,
        camera_status,
        usb_status,
        keyboard_status,
        pad_status,
        badge_reader_status,
        last_seen,
        created_at,
        components,
        payload,
        last_ip
      FROM machines
      ORDER BY last_seen DESC
    `);

    if (!machines.rows || machines.rows.length === 0) {
      return;
    }

    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      for (const row of machines.rows) {
        const reportId = generateUuid();
        await client.query(
          `
            INSERT INTO reports (
              id,
              machine_key,
              hostname,
              mac_address,
              mac_addresses,
              serial_number,
              category,
              tag,
              tag_id,
              lot_id,
              model,
              vendor,
              technician,
              os_version,
              ram_mb,
              ram_slots_total,
              ram_slots_free,
              battery_health,
              camera_status,
              usb_status,
              keyboard_status,
              pad_status,
              badge_reader_status,
              last_seen,
              created_at,
              components,
              payload,
              last_ip
            ) VALUES (
              $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
            $11, $12, $13, $14, $15, $16, $17, $18, $19, $20,
            $21, $22, $23, $24, $25, $26, $27, $28
          )
        `,
        [
          reportId,
          row.machine_key,
          row.hostname,
          row.mac_address,
          row.mac_addresses,
          row.serial_number,
          row.category,
          row.tag,
          row.tag_id,
          row.lot_id,
          row.model,
          row.vendor,
          row.technician,
          row.os_version,
          row.ram_mb,
          row.ram_slots_total,
          row.ram_slots_free,
          row.battery_health,
          row.camera_status,
          row.usb_status,
          row.keyboard_status,
          row.pad_status,
          row.badge_reader_status,
          row.last_seen,
          row.created_at,
          row.components,
          row.payload,
          row.last_ip
        ]
      );
      }
      await client.query('COMMIT');
      console.log(`Backfilled ${machines.rows.length} reports from machines.`);
    } catch (error) {
      try {
        await client.query('ROLLBACK');
      } catch (rollbackError) {
        console.error('Failed to rollback report backfill', rollbackError);
      }
      console.error('Report backfill failed', error);
    } finally {
      client.release();
    }
  } catch (error) {
    console.error('Report backfill check failed', error);
  }
}

async function backfillTags() {
  let client;
  try {
    client = await pool.connect();
    await client.query('BEGIN');

    const distinctTags = await client.query(`
      SELECT DISTINCT tag AS name
      FROM (
        SELECT tag FROM reports WHERE tag IS NOT NULL AND tag <> ''
        UNION ALL
        SELECT tag FROM machines WHERE tag IS NOT NULL AND tag <> ''
      ) AS tags
    `);

    for (const row of distinctTags.rows || []) {
      const name = cleanString(row.name, 64);
      if (!name) {
        continue;
      }
      const existing = await client.query(
        'SELECT id FROM tags WHERE LOWER(name) = LOWER($1) LIMIT 1',
        [name]
      );
      if (existing.rows && existing.rows[0]) {
        continue;
      }
      await client.query(
        `
          INSERT INTO tags (id, name, is_active, created_at, updated_at)
          VALUES ($1, $2, false, NOW(), NOW())
        `,
        [generateUuid(), name]
      );
    }

    const activeTag = await ensureActiveTag(client);
    if (activeTag) {
      await client.query(
        `
          UPDATE reports
          SET tag_id = $1, tag = $2
          WHERE tag_id IS NULL AND (tag IS NULL OR tag = '')
        `,
        [activeTag.id, activeTag.name]
      );
      await client.query(
        `
          UPDATE machines
          SET tag_id = $1, tag = $2
          WHERE tag_id IS NULL AND (tag IS NULL OR tag = '')
        `,
        [activeTag.id, activeTag.name]
      );
    }

    await client.query(`
      UPDATE reports
      SET tag_id = tags.id,
          tag = tags.name
      FROM tags
      WHERE reports.tag_id IS NULL
        AND reports.tag IS NOT NULL
        AND LOWER(reports.tag) = LOWER(tags.name)
    `);

    await client.query(`
      UPDATE machines
      SET tag_id = tags.id,
          tag = tags.name
      FROM tags
      WHERE machines.tag_id IS NULL
        AND machines.tag IS NOT NULL
        AND LOWER(machines.tag) = LOWER(tags.name)
    `);

    await client.query('COMMIT');
  } catch (error) {
    if (client) {
      try {
        await client.query('ROLLBACK');
      } catch (rollbackError) {
        console.error('Failed to rollback tag backfill', rollbackError);
      }
    }
    console.error('Tag backfill failed', error);
  } finally {
    if (client) {
      client.release();
    }
  }
}

async function waitForDatabase() {
  const maxAttempts = Number.parseInt(process.env.DB_CONNECT_RETRIES || '20', 10);
  const delayMs = Number.parseInt(process.env.DB_CONNECT_DELAY_MS || '2000', 10);

  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
    try {
      await pool.query('SELECT 1');
      return;
    } catch (error) {
      const message = error && error.message ? error.message : String(error);
      console.warn(`Database not ready (attempt ${attempt}/${maxAttempts}): ${message}`);
      if (attempt === maxAttempts) {
        throw error;
      }
      await wait(delayMs);
    }
  }
}

async function initDb() {
  await waitForDatabase();
  await pool.query(`
    CREATE TABLE IF NOT EXISTS machines (
      id SERIAL PRIMARY KEY,
      machine_key TEXT NOT NULL UNIQUE,
      hostname TEXT,
      mac_address TEXT,
      mac_addresses TEXT,
      serial_number TEXT,
      category TEXT NOT NULL DEFAULT 'unknown',
      tag TEXT,
      tag_id UUID,
      lot_id UUID,
      model TEXT,
      vendor TEXT,
      technician TEXT,
      os_version TEXT,
      ram_mb INTEGER,
      ram_slots_total INTEGER,
      ram_slots_free INTEGER,
      battery_health INTEGER,
      camera_status TEXT,
      usb_status TEXT,
      keyboard_status TEXT,
      pad_status TEXT,
      badge_reader_status TEXT,
      last_seen TIMESTAMPTZ NOT NULL,
      created_at TIMESTAMPTZ NOT NULL,
      components TEXT,
      payload TEXT,
      last_ip TEXT
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS reports (
      id UUID PRIMARY KEY,
      machine_key TEXT NOT NULL,
      hostname TEXT,
      mac_address TEXT,
      mac_addresses TEXT,
      serial_number TEXT,
      category TEXT NOT NULL DEFAULT 'unknown',
      tag TEXT,
      tag_id UUID,
      lot_id UUID,
      model TEXT,
      vendor TEXT,
      technician TEXT,
      os_version TEXT,
      ram_mb INTEGER,
      ram_slots_total INTEGER,
      ram_slots_free INTEGER,
      battery_health INTEGER,
      camera_status TEXT,
      usb_status TEXT,
      keyboard_status TEXT,
      pad_status TEXT,
      badge_reader_status TEXT,
      last_seen TIMESTAMPTZ NOT NULL,
      created_at TIMESTAMPTZ NOT NULL,
      components TEXT,
      payload TEXT,
      comment TEXT,
      commented_at TIMESTAMPTZ,
      last_ip TEXT
    );
  `);

  // Backfill columns for older schemas that predate tags.
  await pool.query(`ALTER TABLE machines ADD COLUMN IF NOT EXISTS tag TEXT;`);
  await pool.query(`ALTER TABLE machines ADD COLUMN IF NOT EXISTS tag_id UUID;`);
  await pool.query(`ALTER TABLE machines ADD COLUMN IF NOT EXISTS lot_id UUID;`);
  await pool.query(`ALTER TABLE reports ADD COLUMN IF NOT EXISTS tag TEXT;`);
  await pool.query(`ALTER TABLE reports ADD COLUMN IF NOT EXISTS tag_id UUID;`);
  await pool.query(`ALTER TABLE reports ADD COLUMN IF NOT EXISTS lot_id UUID;`);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS tags (
      id UUID PRIMARY KEY,
      name TEXT NOT NULL,
      is_active BOOLEAN NOT NULL DEFAULT false,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS lots (
      id UUID PRIMARY KEY,
      supplier TEXT NOT NULL,
      lot_number TEXT NOT NULL,
      target_count INTEGER NOT NULL,
      produced_count INTEGER NOT NULL DEFAULT 0,
      priority INTEGER NOT NULL DEFAULT ${LOT_PRIORITY_DEFAULT},
      is_paused BOOLEAN NOT NULL DEFAULT false,
      created_by TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS lot_assignments (
      id BIGSERIAL PRIMARY KEY,
      lot_id UUID NOT NULL REFERENCES lots(id) ON DELETE CASCADE,
      technician_key TEXT NOT NULL,
      technician_name TEXT NOT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      UNIQUE(lot_id, technician_key)
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS lot_progress (
      id BIGSERIAL PRIMARY KEY,
      lot_id UUID NOT NULL REFERENCES lots(id) ON DELETE CASCADE,
      machine_key TEXT NOT NULL,
      report_id UUID,
      technician TEXT,
      source TEXT,
      is_double_check BOOLEAN NOT NULL DEFAULT false,
      counted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      UNIQUE(lot_id, machine_key)
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS ldap_settings (
      id SMALLINT PRIMARY KEY,
      enabled BOOLEAN NOT NULL DEFAULT false,
      url TEXT,
      bind_dn TEXT,
      bind_password TEXT,
      search_base TEXT,
      search_filter TEXT,
      search_attributes TEXT,
      tls_reject_unauthorized BOOLEAN NOT NULL DEFAULT true,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS patchnotes (
      id BIGSERIAL PRIMARY KEY,
      version TEXT NOT NULL,
      body TEXT NOT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS patchnote_views (
      id BIGSERIAL PRIMARY KEY,
      patchnote_id BIGINT NOT NULL REFERENCES patchnotes(id) ON DELETE CASCADE,
      username TEXT NOT NULL,
      user_type TEXT NOT NULL,
      seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS suggestions (
      id BIGSERIAL PRIMARY KEY,
      title TEXT NOT NULL,
      body TEXT NOT NULL,
      created_by TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);

  await pool.query(`
    CREATE OR REPLACE FUNCTION safe_jsonb(input_text TEXT)
    RETURNS JSONB
    LANGUAGE plpgsql
    IMMUTABLE
    AS $$
    BEGIN
      IF input_text IS NULL OR btrim(input_text) = '' THEN
        RETURN '{}'::jsonb;
      END IF;
      RETURN input_text::jsonb;
    EXCEPTION
      WHEN others THEN
        RETURN '{}'::jsonb;
    END;
    $$;
  `);

  const columns = [
    ['ram_mb', 'INTEGER'],
    ['ram_slots_total', 'INTEGER'],
    ['ram_slots_free', 'INTEGER'],
    ['battery_health', 'INTEGER'],
    ['mac_addresses', 'TEXT'],
    ['camera_status', 'TEXT'],
    ['usb_status', 'TEXT'],
    ['keyboard_status', 'TEXT'],
    ['pad_status', 'TEXT'],
    ['badge_reader_status', 'TEXT'],
    ['technician', 'TEXT'],
    ['tag', 'TEXT'],
    ['tag_id', 'UUID'],
    ['lot_id', 'UUID'],
    ['last_ip', 'TEXT'],
    ['components', 'TEXT'],
    ['payload', 'TEXT'],
    ['last_seen', 'TIMESTAMPTZ'],
    ['created_at', 'TIMESTAMPTZ']
  ];

  for (const [name, type] of columns) {
    await pool.query(`ALTER TABLE machines ADD COLUMN IF NOT EXISTS ${name} ${type}`);
  }

  const reportColumns = [
    ['machine_key', 'TEXT NOT NULL'],
    ['hostname', 'TEXT'],
    ['mac_address', 'TEXT'],
    ['mac_addresses', 'TEXT'],
    ['serial_number', 'TEXT'],
    ['category', "TEXT NOT NULL DEFAULT 'unknown'"],
    ['tag', 'TEXT'],
    ['tag_id', 'UUID'],
    ['lot_id', 'UUID'],
    ['model', 'TEXT'],
    ['vendor', 'TEXT'],
    ['technician', 'TEXT'],
    ['os_version', 'TEXT'],
    ['ram_mb', 'INTEGER'],
    ['ram_slots_total', 'INTEGER'],
    ['ram_slots_free', 'INTEGER'],
    ['battery_health', 'INTEGER'],
    ['camera_status', 'TEXT'],
    ['usb_status', 'TEXT'],
    ['keyboard_status', 'TEXT'],
    ['pad_status', 'TEXT'],
    ['badge_reader_status', 'TEXT'],
    ['last_seen', 'TIMESTAMPTZ NOT NULL'],
    ['created_at', 'TIMESTAMPTZ NOT NULL'],
    ['components', 'TEXT'],
    ['payload', 'TEXT'],
    ['comment', 'TEXT'],
    ['commented_at', 'TIMESTAMPTZ'],
    ['last_ip', 'TEXT']
  ];

  for (const [name, type] of reportColumns) {
    await pool.query(`ALTER TABLE reports ADD COLUMN IF NOT EXISTS ${name} ${type}`);
  }

  const ldapColumns = [
    ['enabled', 'BOOLEAN NOT NULL DEFAULT false'],
    ['url', 'TEXT'],
    ['bind_dn', 'TEXT'],
    ['bind_password', 'TEXT'],
    ['search_base', 'TEXT'],
    ['search_filter', 'TEXT'],
    ['search_attributes', 'TEXT'],
    ['tls_reject_unauthorized', 'BOOLEAN NOT NULL DEFAULT true'],
    ['updated_at', 'TIMESTAMPTZ NOT NULL DEFAULT NOW()']
  ];

  for (const [name, type] of ldapColumns) {
    await pool.query(`ALTER TABLE ldap_settings ADD COLUMN IF NOT EXISTS ${name} ${type}`);
  }

  await pool.query('CREATE UNIQUE INDEX IF NOT EXISTS idx_machines_machine_key ON machines(machine_key)');
  await pool.query('CREATE INDEX IF NOT EXISTS idx_machines_category ON machines(category)');
  await pool.query('CREATE INDEX IF NOT EXISTS idx_machines_tag ON machines(tag)');
  await pool.query('CREATE INDEX IF NOT EXISTS idx_machines_tag_id ON machines(tag_id)');
  await pool.query('CREATE INDEX IF NOT EXISTS idx_machines_lot_id ON machines(lot_id)');
  await pool.query('CREATE INDEX IF NOT EXISTS idx_machines_last_seen ON machines(last_seen)');
  await pool.query('CREATE INDEX IF NOT EXISTS idx_reports_machine_key ON reports(machine_key)');
  await pool.query('CREATE INDEX IF NOT EXISTS idx_reports_category ON reports(category)');
  await pool.query('CREATE INDEX IF NOT EXISTS idx_reports_tag ON reports(tag)');
  await pool.query('CREATE INDEX IF NOT EXISTS idx_reports_tag_id ON reports(tag_id)');
  await pool.query('CREATE INDEX IF NOT EXISTS idx_reports_lot_id ON reports(lot_id)');
  await pool.query('CREATE INDEX IF NOT EXISTS idx_reports_last_seen ON reports(last_seen)');
  await pool.query('CREATE INDEX IF NOT EXISTS idx_reports_technician ON reports(technician)');
  await pool.query(
    'CREATE INDEX IF NOT EXISTS idx_reports_machine_key_last_seen_id ON reports(machine_key, last_seen DESC, id DESC)'
  );
  await pool.query(
    'CREATE UNIQUE INDEX IF NOT EXISTS idx_lots_supplier_number_unique ON lots (LOWER(supplier), LOWER(lot_number))'
  );
  await pool.query(
    'CREATE INDEX IF NOT EXISTS idx_lots_priority_active ON lots (is_paused, priority, created_at)'
  );
  await pool.query(
    'CREATE INDEX IF NOT EXISTS idx_lot_assignments_tech_key ON lot_assignments (technician_key)'
  );
  await pool.query(
    'CREATE INDEX IF NOT EXISTS idx_lot_progress_lot_counted ON lot_progress (lot_id, counted_at DESC)'
  );
  await pool.query(
    'CREATE UNIQUE INDEX IF NOT EXISTS idx_lot_progress_unique_lot_machine ON lot_progress (lot_id, machine_key)'
  );
  await pool.query('CREATE UNIQUE INDEX IF NOT EXISTS idx_tags_name_unique ON tags (LOWER(name))');
  await pool.query(
    'CREATE UNIQUE INDEX IF NOT EXISTS idx_tags_active_unique ON tags ((is_active)) WHERE is_active'
  );
  await pool.query('CREATE UNIQUE INDEX IF NOT EXISTS idx_patchnotes_version ON patchnotes(version)');
  await pool.query(
    'CREATE UNIQUE INDEX IF NOT EXISTS idx_patchnote_views_unique ON patchnote_views(patchnote_id, username, user_type)'
  );
  await pool.query(
    'CREATE INDEX IF NOT EXISTS idx_patchnote_views_user ON patchnote_views(username, user_type)'
  );

  await pool.query(`
    CREATE OR REPLACE VIEW report_component_transitions AS
    WITH exploded AS (
      SELECT
        reports.id AS report_id,
        reports.machine_key,
        reports.last_seen,
        reports.created_at,
        COALESCE(reports.category, 'unknown') AS category,
        reports.lot_id,
        COALESCE(NULLIF(reports.technician, ''), '--') AS technician,
        e.key AS component_key,
        lower(NULLIF(btrim(COALESCE(e.value, '')), '')) AS status_key
      FROM reports
      LEFT JOIN LATERAL jsonb_each_text(safe_jsonb(reports.components)) AS e(key, value) ON TRUE
      WHERE COALESCE(reports.machine_key, '') <> ''
        AND e.key IS NOT NULL
    ),
    ordered AS (
      SELECT
        exploded.*,
        LAG(exploded.status_key) OVER (
          PARTITION BY exploded.machine_key, exploded.component_key
          ORDER BY exploded.last_seen, exploded.created_at, exploded.report_id
        ) AS previous_status_key
      FROM exploded
    )
    SELECT
      ordered.report_id,
      ordered.machine_key,
      ordered.last_seen,
      ordered.created_at,
      ordered.category,
      ordered.lot_id,
      ordered.technician,
      ordered.component_key,
      CASE ordered.component_key
        WHEN 'diskReadTest' THEN 'Lecture disque'
        WHEN 'diskWriteTest' THEN 'Ecriture disque'
        WHEN 'ramTest' THEN 'RAM (WinSAT)'
        WHEN 'cpuTest' THEN 'CPU (WinSAT)'
        WHEN 'gpuTest' THEN 'GPU (WinSAT)'
        WHEN 'cpuStress' THEN 'CPU (stress)'
        WHEN 'gpuStress' THEN 'GPU (stress)'
        WHEN 'networkPing' THEN 'Ping'
        WHEN 'fsCheck' THEN 'Check disque'
        WHEN 'gpu' THEN 'GPU'
        WHEN 'usb' THEN 'Ports USB'
        WHEN 'keyboard' THEN 'Clavier'
        WHEN 'camera' THEN 'Camera'
        WHEN 'pad' THEN 'Pave tactile'
        WHEN 'badgeReader' THEN 'Lecteur badge'
        WHEN 'biosBattery' THEN 'Pile BIOS'
        WHEN 'biosLanguage' THEN 'Langue BIOS'
        WHEN 'biosPassword' THEN 'Mot de passe BIOS'
        WHEN 'wifiStandard' THEN 'Norme Wi-Fi'
        ELSE ordered.component_key
      END AS component_label,
      ordered.previous_status_key,
      ordered.status_key AS current_status_key,
      (ordered.previous_status_key IN ('nok', 'timeout', 'denied', 'absent')) AS was_incident,
      (ordered.status_key IN ('nok', 'timeout', 'denied', 'absent')) AS is_incident,
      (
        ordered.previous_status_key IN ('nok', 'timeout', 'denied', 'absent')
        AND ordered.status_key = 'ok'
      ) AS is_corrected,
      (
        ordered.previous_status_key = 'ok'
        AND ordered.status_key IN ('nok', 'timeout', 'denied', 'absent')
      ) AS is_regressed
    FROM ordered
    WHERE ordered.previous_status_key IS NOT NULL;
  `);

  // Keep counters resilient even if old data existed before the lot feature.
  await pool.query(`
    UPDATE lots
    SET produced_count = COALESCE(progress.count, 0)
    FROM (
      SELECT lot_id, COUNT(*)::integer AS count
      FROM lot_progress
      GROUP BY lot_id
    ) progress
    WHERE progress.lot_id = lots.id
  `);
  await pool.query(`
    UPDATE lots
    SET produced_count = 0
    WHERE id NOT IN (SELECT DISTINCT lot_id FROM lot_progress)
  `);

  if (AUDIT_LOG_ENABLED) {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS audit_log (
        id BIGSERIAL PRIMARY KEY,
        occurred_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        table_name TEXT NOT NULL,
        action TEXT NOT NULL,
        row_id TEXT,
        machine_key TEXT,
        actor TEXT,
        actor_type TEXT,
        actor_ip TEXT,
        actor_user_agent TEXT,
        request_id TEXT,
        source TEXT,
        old_data JSONB,
        new_data JSONB,
        changed_fields TEXT[]
      );
    `);
    await pool.query('CREATE INDEX IF NOT EXISTS idx_audit_log_table_time ON audit_log(table_name, occurred_at DESC)');
    await pool.query('CREATE INDEX IF NOT EXISTS idx_audit_log_request_id ON audit_log(request_id)');
    await pool.query('CREATE INDEX IF NOT EXISTS idx_audit_log_machine_key ON audit_log(machine_key)');

    await pool.query(`
      CREATE OR REPLACE FUNCTION audit_log_trigger() RETURNS trigger AS $$
      DECLARE
        v_old_raw jsonb;
        v_new_raw jsonb;
        v_old jsonb;
        v_new jsonb;
        v_changed text[];
        v_actor text;
        v_actor_type text;
        v_actor_ip text;
        v_request_id text;
        v_source text;
        v_user_agent text;
        v_row_id text;
        v_machine_key text;
      BEGIN
        v_actor = nullif(current_setting('app.audit_actor', true), '');
        v_actor_type = nullif(current_setting('app.audit_actor_type', true), '');
        v_actor_ip = nullif(current_setting('app.audit_actor_ip', true), '');
        v_request_id = nullif(current_setting('app.audit_request_id', true), '');
        v_source = nullif(current_setting('app.audit_source', true), '');
        v_user_agent = nullif(current_setting('app.audit_user_agent', true), '');

        IF TG_OP = 'INSERT' THEN
          v_new_raw = to_jsonb(NEW);
          v_old_raw = NULL;
        ELSIF TG_OP = 'UPDATE' THEN
          v_new_raw = to_jsonb(NEW);
          v_old_raw = to_jsonb(OLD);
        ELSE
          v_old_raw = to_jsonb(OLD);
          v_new_raw = NULL;
        END IF;

        SELECT array_agg(key) INTO v_changed
        FROM (
          SELECT key FROM jsonb_object_keys(COALESCE(v_new_raw, '{}'::jsonb)) AS key
          UNION
          SELECT key FROM jsonb_object_keys(COALESCE(v_old_raw, '{}'::jsonb)) AS key
        ) keys
        WHERE COALESCE(v_new_raw -> key, 'null'::jsonb)
          IS DISTINCT FROM COALESCE(v_old_raw -> key, 'null'::jsonb);

        v_old = v_old_raw;
        v_new = v_new_raw;
        IF TG_TABLE_NAME = 'ldap_settings' THEN
          v_old = v_old - 'bind_password';
          v_new = v_new - 'bind_password';
        END IF;

        v_row_id = COALESCE(v_new_raw ->> 'id', v_old_raw ->> 'id');
        v_machine_key = COALESCE(v_new_raw ->> 'machine_key', v_old_raw ->> 'machine_key');

        INSERT INTO audit_log (
          table_name,
          action,
          row_id,
          machine_key,
          actor,
          actor_type,
          actor_ip,
          actor_user_agent,
          request_id,
          source,
          old_data,
          new_data,
          changed_fields
        ) VALUES (
          TG_TABLE_NAME,
          TG_OP,
          v_row_id,
          v_machine_key,
          v_actor,
          v_actor_type,
          v_actor_ip,
          v_user_agent,
          v_request_id,
          v_source,
          v_old,
          v_new,
          v_changed
        );

        RETURN NULL;
      END;
      $$ LANGUAGE plpgsql;
    `);

    await pool.query('DROP TRIGGER IF EXISTS audit_log_machines ON machines');
    await pool.query(`
      CREATE TRIGGER audit_log_machines
      AFTER INSERT OR UPDATE OR DELETE ON machines
      FOR EACH ROW EXECUTE FUNCTION audit_log_trigger();
    `);

    await pool.query('DROP TRIGGER IF EXISTS audit_log_ldap_settings ON ldap_settings');
    await pool.query(`
      CREATE TRIGGER audit_log_ldap_settings
      AFTER INSERT OR UPDATE OR DELETE ON ldap_settings
      FOR EACH ROW EXECUTE FUNCTION audit_log_trigger();
    `);
    await pool.query('DROP TRIGGER IF EXISTS audit_log_reports ON reports');
    await pool.query(`
      CREATE TRIGGER audit_log_reports
      AFTER INSERT OR UPDATE OR DELETE ON reports
      FOR EACH ROW EXECUTE FUNCTION audit_log_trigger();
    `);
    await pool.query(`
      CREATE OR REPLACE VIEW report_component_manual_changes AS
      WITH raw_logs AS (
        SELECT
          audit_log.id AS log_id,
          audit_log.occurred_at,
          COALESCE(NULLIF(audit_log.actor, ''), 'systeme') AS actor,
          audit_log.actor_type,
          audit_log.actor_ip,
          audit_log.request_id,
          audit_log.source,
          COALESCE(
            NULLIF(audit_log.machine_key, ''),
            NULLIF(audit_log.new_data ->> 'machine_key', ''),
            NULLIF(audit_log.old_data ->> 'machine_key', '')
          ) AS machine_key,
          COALESCE(
            NULLIF(audit_log.row_id, ''),
            NULLIF(audit_log.new_data ->> 'id', ''),
            NULLIF(audit_log.old_data ->> 'id', '')
          ) AS report_id,
          safe_jsonb(audit_log.old_data ->> 'components') AS old_components,
          safe_jsonb(audit_log.new_data ->> 'components') AS new_components
        FROM audit_log
        WHERE audit_log.table_name = 'reports'
          AND audit_log.action = 'UPDATE'
          AND audit_log.changed_fields @> ARRAY['components']::text[]
          AND COALESCE(NULLIF(audit_log.actor, ''), '') <> ''
      ),
      component_changes AS (
        SELECT
          raw_logs.*,
          keyset.key AS component_key,
          lower(NULLIF(btrim(COALESCE(raw_logs.old_components ->> keyset.key, '')), '')) AS from_status_key,
          lower(NULLIF(btrim(COALESCE(raw_logs.new_components ->> keyset.key, '')), '')) AS to_status_key
        FROM raw_logs
        LEFT JOIN LATERAL (
          SELECT key
          FROM (
            SELECT jsonb_object_keys(COALESCE(raw_logs.old_components, '{}'::jsonb)) AS key
            UNION
            SELECT jsonb_object_keys(COALESCE(raw_logs.new_components, '{}'::jsonb)) AS key
          ) keys
        ) AS keyset ON TRUE
      )
      SELECT
        component_changes.log_id,
        component_changes.occurred_at,
        component_changes.actor,
        component_changes.actor_type,
        component_changes.actor_ip,
        component_changes.request_id,
        component_changes.source,
        component_changes.machine_key,
        component_changes.report_id,
        component_changes.component_key,
        CASE component_changes.component_key
          WHEN 'diskReadTest' THEN 'Lecture disque'
          WHEN 'diskWriteTest' THEN 'Ecriture disque'
          WHEN 'ramTest' THEN 'RAM (WinSAT)'
          WHEN 'cpuTest' THEN 'CPU (WinSAT)'
          WHEN 'gpuTest' THEN 'GPU (WinSAT)'
          WHEN 'cpuStress' THEN 'CPU (stress)'
          WHEN 'gpuStress' THEN 'GPU (stress)'
          WHEN 'networkPing' THEN 'Ping'
          WHEN 'fsCheck' THEN 'Check disque'
          WHEN 'gpu' THEN 'GPU'
          WHEN 'usb' THEN 'Ports USB'
          WHEN 'keyboard' THEN 'Clavier'
          WHEN 'camera' THEN 'Camera'
          WHEN 'pad' THEN 'Pave tactile'
          WHEN 'badgeReader' THEN 'Lecteur badge'
          WHEN 'biosBattery' THEN 'Pile BIOS'
          WHEN 'biosLanguage' THEN 'Langue BIOS'
          WHEN 'biosPassword' THEN 'Mot de passe BIOS'
          WHEN 'wifiStandard' THEN 'Norme Wi-Fi'
          ELSE component_changes.component_key
        END AS component_label,
        component_changes.from_status_key,
        component_changes.to_status_key,
        (component_changes.from_status_key IN ('nok', 'timeout', 'denied', 'absent')) AS from_incident,
        (component_changes.to_status_key IN ('nok', 'timeout', 'denied', 'absent')) AS to_incident,
        (
          component_changes.from_status_key IN ('nok', 'timeout', 'denied', 'absent')
          AND component_changes.to_status_key = 'ok'
        ) AS is_corrected,
        (
          component_changes.from_status_key = 'ok'
          AND component_changes.to_status_key IN ('nok', 'timeout', 'denied', 'absent')
        ) AS is_regressed,
        CASE
          WHEN component_changes.from_status_key IN ('nok', 'timeout', 'denied', 'absent')
            AND component_changes.to_status_key = 'ok'
          THEN 'corrected'
          WHEN component_changes.from_status_key = 'ok'
            AND component_changes.to_status_key IN ('nok', 'timeout', 'denied', 'absent')
          THEN 'regressed'
          ELSE 'updated'
        END AS change_type
      FROM component_changes
      WHERE component_changes.component_key IS NOT NULL
        AND component_changes.component_key NOT IN ('diskSmart', 'networkTest', 'memDiag', 'thermal')
        AND component_changes.from_status_key IS DISTINCT FROM component_changes.to_status_key;
    `);
    await pool.query(`
      CREATE OR REPLACE VIEW report_component_regressions AS
      WITH manual_regressions AS (
        SELECT
          m.log_id::text AS event_id,
          m.occurred_at AS event_time,
          m.report_id::text AS report_id,
          m.machine_key,
          m.component_key,
          m.component_label,
          m.from_status_key AS previous_status_key,
          m.to_status_key AS current_status_key,
          COALESCE(r.category, 'unknown') AS category,
          r.lot_id,
          COALESCE(NULLIF(r.technician, ''), '--') AS technician,
          m.actor,
          m.actor_type,
          m.actor_ip,
          m.source,
          'manual'::text AS regression_origin
        FROM report_component_manual_changes m
        LEFT JOIN reports r ON r.id::text = m.report_id
        WHERE m.is_regressed
          AND COALESCE(m.source, '') !~* '^POST /api/ingest'
      ),
      script_regressions_from_logs AS (
        SELECT
          m.log_id::text AS event_id,
          m.occurred_at AS event_time,
          m.report_id::text AS report_id,
          m.machine_key,
          m.component_key,
          m.component_label,
          m.from_status_key AS previous_status_key,
          m.to_status_key AS current_status_key,
          COALESCE(r.category, 'unknown') AS category,
          r.lot_id,
          COALESCE(NULLIF(r.technician, ''), '--') AS technician,
          COALESCE(NULLIF(m.actor, ''), NULLIF(r.technician, ''), 'systeme') AS actor,
          COALESCE(NULLIF(m.actor_type, ''), 'script') AS actor_type,
          m.actor_ip,
          COALESCE(NULLIF(m.source, ''), 'POST /api/ingest') AS source,
          'script'::text AS regression_origin
        FROM report_component_manual_changes m
        LEFT JOIN reports r ON r.id::text = m.report_id
        WHERE m.is_regressed
          AND COALESCE(m.source, '') ~* '^POST /api/ingest'
      ),
      script_regressions_from_transitions AS (
        SELECT
          t.report_id::text AS event_id,
          t.last_seen AS event_time,
          t.report_id::text AS report_id,
          t.machine_key,
          t.component_key,
          t.component_label,
          t.previous_status_key,
          t.current_status_key,
          t.category,
          t.lot_id,
          COALESCE(NULLIF(t.technician, ''), '--') AS technician,
          COALESCE(NULLIF(insert_log.actor, ''), NULLIF(t.technician, ''), 'systeme') AS actor,
          COALESCE(NULLIF(insert_log.actor_type, ''), 'script') AS actor_type,
          NULLIF(insert_log.actor_ip, '') AS actor_ip,
          COALESCE(NULLIF(insert_log.source, ''), 'POST /api/ingest') AS source,
          'script'::text AS regression_origin
        FROM report_component_transitions t
        LEFT JOIN LATERAL (
          SELECT
            audit_log.actor,
            audit_log.actor_type,
            audit_log.actor_ip,
            audit_log.source
          FROM audit_log
          WHERE audit_log.table_name = 'reports'
            AND audit_log.action = 'INSERT'
            AND (
              audit_log.row_id = t.report_id::text
              OR audit_log.new_data ->> 'id' = t.report_id::text
            )
          ORDER BY audit_log.occurred_at DESC
          LIMIT 1
        ) insert_log ON TRUE
        LEFT JOIN report_component_manual_changes manual
          ON manual.report_id = t.report_id::text
          AND manual.component_key = t.component_key
          AND manual.is_regressed
        WHERE t.is_regressed
          AND manual.log_id IS NULL
      )
      SELECT
        regressions.*,
        CASE
          WHEN COALESCE(regressions.source, '') ~* '^POST /api/ingest'
          THEN 'Script MDT'
          WHEN COALESCE(regressions.source, '') ~* '^PUT /api/reports/.+/component$'
          THEN 'Edition manuelle composant'
          WHEN COALESCE(regressions.source, '') ~* '^PUT /api/machines/.+/(pad|usb)$'
          THEN 'Edition manuelle rapide'
          WHEN COALESCE(regressions.source, '') ~* '^POST /api/reports/report-zero'
          THEN 'Rapport zero manuel'
          WHEN COALESCE(regressions.source, '') ~* '^POST /api/machines/.+/report-zero'
          THEN 'Rapport zero clone'
          ELSE COALESCE(NULLIF(regressions.source, ''), 'inconnu')
        END AS source_group
      FROM (
        SELECT * FROM manual_regressions
        UNION ALL
        SELECT * FROM script_regressions_from_logs
        UNION ALL
        SELECT * FROM script_regressions_from_transitions
      ) regressions;
    `);
  } else {
    await pool.query(`
      CREATE OR REPLACE VIEW report_component_manual_changes AS
      SELECT
        NULL::bigint AS log_id,
        NULL::timestamptz AS occurred_at,
        NULL::text AS actor,
        NULL::text AS actor_type,
        NULL::text AS actor_ip,
        NULL::text AS request_id,
        NULL::text AS source,
        NULL::text AS machine_key,
        NULL::text AS report_id,
        NULL::text AS component_key,
        NULL::text AS component_label,
        NULL::text AS from_status_key,
        NULL::text AS to_status_key,
        NULL::boolean AS from_incident,
        NULL::boolean AS to_incident,
        NULL::boolean AS is_corrected,
        NULL::boolean AS is_regressed,
        NULL::text AS change_type
      WHERE false;
    `);
    await pool.query(`
      CREATE OR REPLACE VIEW report_component_regressions AS
      SELECT
        t.report_id::text AS event_id,
        t.last_seen AS event_time,
        t.report_id::text AS report_id,
        t.machine_key,
        t.component_key,
        t.component_label,
        t.previous_status_key,
        t.current_status_key,
        t.category,
        t.lot_id,
        COALESCE(NULLIF(t.technician, ''), '--') AS technician,
        COALESCE(NULLIF(t.technician, ''), 'systeme') AS actor,
        'script'::text AS actor_type,
        NULL::text AS actor_ip,
        'POST /api/ingest'::text AS source,
        'script'::text AS regression_origin,
        'Script MDT'::text AS source_group
      FROM report_component_transitions t
      WHERE t.is_regressed;
    `);
  }

  await backfillReportsFromMachines();
  await backfillTags();
}

const upsertMachineQuery = `
  INSERT INTO machines (
    machine_key,
    hostname,
    mac_address,
    mac_addresses,
    serial_number,
    category,
    tag,
    tag_id,
    lot_id,
    model,
    vendor,
    technician,
    os_version,
    ram_mb,
    ram_slots_total,
    ram_slots_free,
    battery_health,
    camera_status,
    usb_status,
    keyboard_status,
    pad_status,
    badge_reader_status,
    last_seen,
    created_at,
    components,
    payload,
    last_ip
  ) VALUES (
    $1,
    $2,
    $3,
    $4,
    $5,
    $6,
    $7,
    $8,
    $9,
    $10,
    $11,
    $12,
    $13,
    $14,
    $15,
    $16,
    $17,
    $18,
    $19,
    $20,
    $21,
    $22,
    $23,
    $24,
    $25,
    $26,
    $27
  )
  ON CONFLICT(machine_key) DO UPDATE SET
    hostname = COALESCE(excluded.hostname, machines.hostname),
    mac_address = COALESCE(excluded.mac_address, machines.mac_address),
    mac_addresses = COALESCE(excluded.mac_addresses, machines.mac_addresses),
    serial_number = COALESCE(excluded.serial_number, machines.serial_number),
    category = CASE
      WHEN excluded.category != 'unknown' THEN excluded.category
      ELSE machines.category
    END,
    tag = COALESCE(excluded.tag, machines.tag),
    tag_id = COALESCE(excluded.tag_id, machines.tag_id),
    lot_id = COALESCE(excluded.lot_id, machines.lot_id),
    model = COALESCE(excluded.model, machines.model),
    vendor = COALESCE(excluded.vendor, machines.vendor),
    technician = COALESCE(excluded.technician, machines.technician),
    os_version = COALESCE(excluded.os_version, machines.os_version),
    ram_mb = COALESCE(excluded.ram_mb, machines.ram_mb),
    ram_slots_total = COALESCE(excluded.ram_slots_total, machines.ram_slots_total),
    ram_slots_free = COALESCE(excluded.ram_slots_free, machines.ram_slots_free),
    battery_health = COALESCE(excluded.battery_health, machines.battery_health),
    camera_status = COALESCE(excluded.camera_status, machines.camera_status),
    usb_status = COALESCE(excluded.usb_status, machines.usb_status),
    keyboard_status = COALESCE(excluded.keyboard_status, machines.keyboard_status),
    pad_status = COALESCE(excluded.pad_status, machines.pad_status),
    badge_reader_status = COALESCE(excluded.badge_reader_status, machines.badge_reader_status),
    last_seen = excluded.last_seen,
    components = CASE
      WHEN excluded.components IS NULL THEN machines.components
      ELSE (
        COALESCE(NULLIF(machines.components, ''), '{}')::jsonb ||
        COALESCE(NULLIF(excluded.components, ''), '{}')::jsonb
      )::text
    END,
    payload = COALESCE(excluded.payload, machines.payload),
    last_ip = excluded.last_ip
  RETURNING id
`;

const upsertReportQuery = `
  INSERT INTO reports (
    id,
    machine_key,
    hostname,
    mac_address,
    mac_addresses,
    serial_number,
    category,
    tag,
    tag_id,
    lot_id,
    model,
    vendor,
    technician,
    os_version,
    ram_mb,
    ram_slots_total,
    ram_slots_free,
    battery_health,
    camera_status,
    usb_status,
    keyboard_status,
    pad_status,
    badge_reader_status,
    last_seen,
    created_at,
    components,
    payload,
    last_ip
  ) VALUES (
    $1,
    $2,
    $3,
    $4,
    $5,
    $6,
    $7,
    $8,
    $9,
    $10,
    $11,
    $12,
    $13,
    $14,
    $15,
    $16,
    $17,
    $18,
    $19,
    $20,
    $21,
    $22,
    $23,
    $24,
    $25,
    $26,
    $27,
    $28
  )
  ON CONFLICT(id) DO UPDATE SET
    hostname = COALESCE(excluded.hostname, reports.hostname),
    mac_address = COALESCE(excluded.mac_address, reports.mac_address),
    mac_addresses = COALESCE(excluded.mac_addresses, reports.mac_addresses),
    serial_number = COALESCE(excluded.serial_number, reports.serial_number),
    category = CASE
      WHEN excluded.category != 'unknown' THEN excluded.category
      ELSE reports.category
    END,
    tag = COALESCE(excluded.tag, reports.tag),
    tag_id = COALESCE(excluded.tag_id, reports.tag_id),
    lot_id = COALESCE(excluded.lot_id, reports.lot_id),
    model = COALESCE(excluded.model, reports.model),
    vendor = COALESCE(excluded.vendor, reports.vendor),
    technician = COALESCE(excluded.technician, reports.technician),
    os_version = COALESCE(excluded.os_version, reports.os_version),
    ram_mb = COALESCE(excluded.ram_mb, reports.ram_mb),
    ram_slots_total = COALESCE(excluded.ram_slots_total, reports.ram_slots_total),
    ram_slots_free = COALESCE(excluded.ram_slots_free, reports.ram_slots_free),
    battery_health = COALESCE(excluded.battery_health, reports.battery_health),
    camera_status = COALESCE(excluded.camera_status, reports.camera_status),
    usb_status = COALESCE(excluded.usb_status, reports.usb_status),
    keyboard_status = COALESCE(excluded.keyboard_status, reports.keyboard_status),
    pad_status = COALESCE(excluded.pad_status, reports.pad_status),
    badge_reader_status = COALESCE(excluded.badge_reader_status, reports.badge_reader_status),
    last_seen = excluded.last_seen,
    components = CASE
      WHEN excluded.components IS NULL THEN reports.components
      ELSE (
        COALESCE(NULLIF(reports.components, ''), '{}')::jsonb ||
        COALESCE(NULLIF(excluded.components, ''), '{}')::jsonb
      )::text
    END,
    payload = COALESCE(excluded.payload, reports.payload),
    last_ip = excluded.last_ip
  RETURNING id, machine_key
`;

const listReportsQuery = `
  SELECT
    reports.id AS id,
    machine_key,
    hostname,
    mac_address,
    mac_addresses,
    serial_number,
    category,
    tag,
    tag_id,
    reports.lot_id,
    COALESCE(tags.name, reports.tag) AS tag_name,
    lots.supplier AS lot_supplier,
    lots.lot_number AS lot_number,
    lots.target_count AS lot_target_count,
    lots.produced_count AS lot_produced_count,
    lots.is_paused AS lot_is_paused,
    model,
    vendor,
    technician,
    os_version,
    ram_mb,
    ram_slots_total,
    ram_slots_free,
    battery_health,
    camera_status,
    usb_status,
    keyboard_status,
    pad_status,
    badge_reader_status,
    last_seen,
    last_ip,
    components,
    comment
  FROM reports
  LEFT JOIN tags ON tags.id = reports.tag_id
  LEFT JOIN lots ON lots.id = reports.lot_id
  ORDER BY reports.last_seen DESC
`;

const getReportByIdQuery = `
  SELECT
    reports.id,
    reports.machine_key,
    reports.hostname,
    reports.mac_address,
    reports.mac_addresses,
    reports.serial_number,
    reports.category,
    reports.tag,
    reports.tag_id,
    reports.lot_id AS report_lot_id,
    COALESCE(tags.name, reports.tag) AS report_tag_name,
    report_lot.supplier AS report_lot_supplier,
    report_lot.lot_number AS report_lot_number,
    report_lot.target_count AS report_lot_target_count,
    report_lot.produced_count AS report_lot_produced_count,
    report_lot.is_paused AS report_lot_is_paused,
    reports.model,
    reports.vendor,
    reports.technician,
    reports.os_version,
    reports.ram_mb,
    reports.ram_slots_total,
    reports.ram_slots_free,
    reports.battery_health,
    reports.camera_status,
    reports.usb_status,
    reports.keyboard_status,
    reports.pad_status,
    reports.badge_reader_status,
    reports.last_seen AS report_last_seen,
    reports.created_at AS report_created_at,
    reports.components,
    reports.payload,
    reports.last_ip,
    reports.comment,
    reports.commented_at,
    machines.last_seen AS machine_last_seen,
    machines.created_at AS machine_created_at,
    machines.tag AS machine_tag,
    machines.tag_id AS machine_tag_id,
    machines.lot_id AS machine_lot_id,
    machine_lot.supplier AS machine_lot_supplier,
    machine_lot.lot_number AS machine_lot_number,
    machine_lot.target_count AS machine_lot_target_count,
    machine_lot.produced_count AS machine_lot_produced_count,
    machine_lot.is_paused AS machine_lot_is_paused,
    machines.payload AS machine_payload,
    machines.components AS machine_components
  FROM reports
  LEFT JOIN machines ON machines.machine_key = reports.machine_key
  LEFT JOIN tags ON tags.id = reports.tag_id
  LEFT JOIN lots report_lot ON report_lot.id = reports.lot_id
  LEFT JOIN lots machine_lot ON machine_lot.id = machines.lot_id
  WHERE reports.id = $1
`;

const ingestLimiter = rateLimit({
  windowMs: 60 * 1000,
  max: INGEST_RATE_LIMIT,
  standardHeaders: true,
  legacyHeaders: false
});

const LAPTOP_CHASSIS_CODES = new Set([8, 9, 10, 11, 12, 14, 18, 21, 31]);
const DESKTOP_CHASSIS_CODES = new Set([3, 4, 5, 6, 7, 15, 16]);
const CATEGORY_LABELS = {
  laptop: 'Portable',
  desktop: 'Tour',
  unknown: 'Inconnu'
};
const DEFAULT_REPORT_TAG = 'En cours';
const STATUS_LABELS = {
  ok: 'OK',
  nok: 'NOK',
  fr: 'FR',
  en: 'EN',
  absent: 'Absent',
  not_tested: 'Non testé',
  denied: 'Refuse',
  timeout: 'Timeout',
  scheduled: 'Planifie',
  unknown: '--'
};
const INCIDENT_STATUS_KEYS = new Set(['nok', 'timeout', 'denied', 'absent']);
const RESOLVED_STATUS_KEYS = new Set(['ok']);
const STATUS_STYLES = {
  ok: { background: '#DDF3E6', color: '#1B4C38' },
  nok: { background: '#F9D9D3', color: '#8D1F12' },
  fr: { background: '#E6EEF9', color: '#1F3F8D' },
  en: { background: '#E6EEF9', color: '#1F3F8D' },
  absent: { background: '#EFEFEF', color: '#4B4B4B' },
  not_tested: { background: '#EFEFEF', color: '#4B4B4B' },
  denied: { background: '#EFEFEF', color: '#4B4B4B' },
  timeout: { background: '#FBE2C8', color: '#9B4A16' },
  scheduled: { background: '#DDF3E6', color: '#1B4C38' },
  unknown: { background: '#EFEFEF', color: '#4B4B4B' }
};
const COMPONENT_LABELS = {
  diskReadTest: 'Lecture disque',
  diskWriteTest: 'Ecriture disque',
  ramTest: 'RAM (WinSAT)',
  cpuTest: 'CPU (WinSAT)',
  gpuTest: 'GPU (WinSAT)',
  cpuStress: 'CPU (stress)',
  gpuStress: 'GPU (stress)',
  networkPing: 'Ping',
  fsCheck: 'Check disque',
  gpu: 'GPU',
  usb: 'Ports USB',
  keyboard: 'Clavier',
  camera: 'Camera',
  pad: 'Pave tactile',
  badgeReader: 'Lecteur badge',
  biosBattery: 'Pile BIOS',
  biosLanguage: 'Langue BIOS',
  biosPassword: 'Mot de passe BIOS',
  wifiStandard: 'Norme Wi-Fi'
};
const COMPONENT_ORDER = [
  'diskReadTest',
  'diskWriteTest',
  'ramTest',
  'cpuTest',
  'gpuTest',
  'cpuStress',
  'gpuStress',
  'networkPing',
  'fsCheck',
  'gpu',
  'usb',
  'keyboard',
  'camera',
  'pad',
  'badgeReader',
  'biosBattery',
  'biosLanguage',
  'biosPassword',
  'wifiStandard'
];
const HIDDEN_COMPONENTS = new Set(['diskSmart', 'networkTest', 'memDiag', 'thermal']);
const VALID_PAD_STATUSES = new Set(['ok', 'nok']);
const VALID_USB_STATUSES = new Set(['ok', 'nok']);
const DEFAULT_COMPONENT_STATUSES = new Set(['not_tested', 'ok', 'nok']);
const COMPONENT_ALLOWED_STATUSES = {
  biosLanguage: new Set(['not_tested', 'fr', 'en'])
};
const VALID_COMPONENT_KEYS = new Set(COMPONENT_ORDER);
const COMPONENT_STATUS_COLUMNS = {
  camera: 'camera_status',
  usb: 'usb_status',
  keyboard: 'keyboard_status',
  pad: 'pad_status',
  badgeReader: 'badge_reader_status'
};
const MANUAL_COMPONENT_DEFAULTS = {
  biosBattery: 'not_tested',
  biosLanguage: 'not_tested',
  biosPassword: 'not_tested',
  wifiStandard: 'not_tested'
};

function getAllowedComponentStatuses(key) {
  return COMPONENT_ALLOWED_STATUSES[key] || DEFAULT_COMPONENT_STATUSES;
}

app.set('trust proxy', process.env.TRUST_PROXY === '1');
app.use(express.json({ limit: JSON_LIMIT }));
app.use(express.urlencoded({ extended: false, limit: '16kb' }));
app.use((req, res, next) => {
  const headerId = cleanString(req.get('x-request-id'), 128);
  req.requestId = headerId || generateRequestId();
  res.setHeader('X-Request-Id', req.requestId);
  next();
});
app.use(
  session({
    name: SESSION_NAME,
    secret: SESSION_SECRET,
    resave: false,
    saveUninitialized: false,
    cookie: {
      httpOnly: true,
      sameSite: 'lax',
      secure: COOKIE_SECURE
    }
  })
);
app.use(
  helmet({
    contentSecurityPolicy: {
      useDefaults: true,
      directives: {
        'default-src': ["'self'"],
        'style-src': ["'self'", 'https://fonts.googleapis.com'],
        'font-src': ["'self'", 'https://fonts.gstatic.com'],
        'img-src': ["'self'", 'data:'],
        'script-src': ["'self'"],
        'connect-src': ["'self'", ...GRAFANA_CSP_SOURCES],
        'frame-src': ["'self'", ...GRAFANA_CSP_SOURCES],
        'child-src': ["'self'", ...GRAFANA_CSP_SOURCES],
        'upgrade-insecure-requests': null
      }
    }
  })
);

function requireAuth(req, res, next) {
  if (req.session && req.session.user) {
    return next();
  }
  if (req.path.startsWith('/api')) {
    return res.status(401).json({ ok: false, error: 'unauthorized' });
  }
  if (req.accepts('html')) {
    return res.redirect('/login');
  }
  return res.status(401).json({ ok: false, error: 'unauthorized' });
}

function requireAdmin(req, res, next) {
  if (!req.session || !req.session.user) {
    return res.status(401).json({ ok: false, error: 'unauthorized' });
  }
  if (req.session.user.type !== 'local') {
    return res.status(403).json({ ok: false, error: 'forbidden' });
  }
  return next();
}

function requireAdminPage(req, res, next) {
  if (!req.session || !req.session.user) {
    return res.redirect('/login');
  }
  if (req.session.user.type !== 'local') {
    return res.redirect('/');
  }
  return next();
}

function getPatchnoteUser(req) {
  const user = req.session && req.session.user ? req.session.user : null;
  if (!user || !user.username || !user.type) {
    return null;
  }
  return { username: user.username, type: user.type };
}

function getLdapAttribute(ldapUser, key) {
  if (!ldapUser || typeof ldapUser !== 'object') {
    return null;
  }
  if (Object.prototype.hasOwnProperty.call(ldapUser, key)) {
    return ldapUser[key];
  }
  const match = Object.keys(ldapUser).find(
    (field) => field.toLowerCase() === key.toLowerCase()
  );
  return match ? ldapUser[match] : null;
}

function normalizeLdapValues(value) {
  if (value == null) {
    return [];
  }
  if (Array.isArray(value)) {
    return value
      .map((entry) => (entry == null ? '' : String(entry).trim()))
      .filter(Boolean);
  }
  const stringValue = String(value).trim();
  return stringValue ? [stringValue] : [];
}

function extractLdapGroups(ldapUser) {
  const memberOf = getLdapAttribute(ldapUser, 'memberOf');
  return normalizeLdapValues(memberOf);
}

function isHydraAdminMember(groups) {
  if (!Array.isArray(groups) || !groups.length) {
    return false;
  }
  return groups.some((group) => String(group).toLowerCase() === HYDRA_ADMIN_GROUP_DN);
}

async function fetchLdapUserRecord(username, config) {
  if (!username) {
    return null;
  }
  if (!config || !config.enabled || !config.url || !config.searchBase) {
    return null;
  }

  const options = {
    url: config.url,
    searchBase: config.searchBase,
    searchFilter: normalizeLdapSearchFilter(config.searchFilter).replace(
      '{{username}}',
      escapeLdapFilter(username)
    ),
    searchAttributes: ensureLdapAttributes(config.searchAttributes, ['memberOf']),
    reconnect: true,
    tlsOptions: {
      rejectUnauthorized: config.tlsRejectUnauthorized !== false
    }
  };

  if (config.bindDn && config.bindPassword) {
    options.bindDN = config.bindDn;
    options.bindCredentials = config.bindPassword;
  }

  const ldap = new LdapAuth(options);
  return new Promise((resolve, reject) => {
    ldap._findUser(username, (err, user) => {
      ldap.close(() => {});
      if (err) {
        reject(err);
        return;
      }
      resolve(user || null);
    });
  });
}

async function refreshLdapPermissions(req) {
  if (!req.session || !req.session.user || req.session.user.type !== 'ldap') {
    return req.session?.user || null;
  }
  const user = req.session.user;
  if (user.isHydraAdmin === true && user.permissions && user.permissions.canDeleteReport === true) {
    return user;
  }
  try {
    const ldapConfig = await getEffectiveLdapConfig();
    const ldapUser = await fetchLdapUserRecord(user.username, ldapConfig);
    if (!ldapUser) {
      return user;
    }
    const groups = extractLdapGroups(ldapUser);
    const isHydraAdmin = isHydraAdminMember(groups);
    const updated = {
      ...user,
      groups,
      isHydraAdmin,
      permissions: {
        ...(user.permissions || {}),
        canDeleteReport: isHydraAdmin
      }
    };
    req.session.user = updated;
    req.session.save(() => {});
    return updated;
  } catch (error) {
    return user;
  }
}

function canDeleteReports(user) {
  if (!user) {
    return false;
  }
  if (user.type === 'local') {
    return true;
  }
  if (user.permissions && user.permissions.canDeleteReport) {
    return true;
  }
  return Boolean(user.isHydraAdmin);
}

function canEditTags(user) {
  return canDeleteReports(user);
}

function requireReportDelete(req, res, next) {
  if (!req.session || !req.session.user) {
    return res.status(401).json({ ok: false, error: 'unauthorized' });
  }
  if (canDeleteReports(req.session.user)) {
    return next();
  }
  refreshLdapPermissions(req)
    .then((user) => {
      if (!canDeleteReports(user)) {
        return res.status(403).json({ ok: false, error: 'forbidden' });
      }
      return next();
    })
    .catch(() => res.status(403).json({ ok: false, error: 'forbidden' }));
}

function requireTagEdit(req, res, next) {
  if (!req.session || !req.session.user) {
    return res.status(401).json({ ok: false, error: 'unauthorized' });
  }
  if (canEditTags(req.session.user)) {
    return next();
  }
  refreshLdapPermissions(req)
    .then((user) => {
      if (!canEditTags(user)) {
        return res.status(403).json({ ok: false, error: 'forbidden' });
      }
      return next();
    })
    .catch(() => res.status(403).json({ ok: false, error: 'forbidden' }));
}

function requireTagEditPage(req, res, next) {
  if (!req.session || !req.session.user) {
    return res.redirect('/login');
  }
  if (canEditTags(req.session.user)) {
    return next();
  }
  refreshLdapPermissions(req)
    .then((user) => {
      if (!canEditTags(user)) {
        return res.redirect('/');
      }
      return next();
    })
    .catch(() => res.redirect('/'));
}

function cleanString(value, maxLength) {
  if (typeof value !== 'string') {
    return null;
  }
  const trimmed = value.trim();
  if (!trimmed) {
    return null;
  }
  return trimmed.slice(0, maxLength);
}

function normalizeOptionalString(value, maxLength) {
  if (typeof value !== 'string') {
    return '';
  }
  return value.trim().slice(0, maxLength);
}

function normalizeUuid(value) {
  if (!value) {
    return null;
  }
  const trimmed = String(value).trim();
  if (/^[0-9a-fA-F]{32}$/.test(trimmed)) {
    const normalized = trimmed.replace(
      /^(.{8})(.{4})(.{4})(.{4})(.{12})$/,
      '$1-$2-$3-$4-$5'
    );
    return normalized.toLowerCase();
  }
  if (
    /^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$/.test(
      trimmed
    )
  ) {
    return trimmed.toLowerCase();
  }
  return null;
}

const TECH_TRANSLATE_FROM =
  'àáâäãåçèéêëìíîïñòóôöõùúûüýÿÀÁÂÄÃÅÇÈÉÊËÌÍÎÏÑÒÓÔÖÕÙÚÛÜÝ';
const TECH_TRANSLATE_TO =
  'aaaaaaceeeeiiiinooooouuuuyyAAAAAACEEEEIIIINOOOOOUUUUY';

function normalizeTechKey(value) {
  if (!value) {
    return '';
  }
  return String(value)
    .trim()
    .toLowerCase()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '');
}

function normalizeTextSql(column) {
  return `lower(translate(coalesce(${column}, ''), '${TECH_TRANSLATE_FROM}', '${TECH_TRANSLATE_TO}'))`;
}

function parseTagIds(raw) {
  if (!raw) {
    return [];
  }
  const parts = Array.isArray(raw)
    ? raw
    : String(raw)
        .split(',')
        .map((value) => value.trim())
        .filter(Boolean);
  const ids = [];
  for (const value of parts) {
    const id = normalizeUuid(value);
    if (id) {
      ids.push(id);
    }
  }
  return ids;
}

function parseBooleanFlag(value, fallback = false) {
  if (value == null) {
    return fallback;
  }
  if (typeof value === 'boolean') {
    return value;
  }
  if (typeof value === 'number') {
    return value !== 0;
  }
  const lowered = String(value).trim().toLowerCase();
  if (!lowered) {
    return fallback;
  }
  if (['1', 'true', 'yes', 'on', 'y'].includes(lowered)) {
    return true;
  }
  if (['0', 'false', 'no', 'off', 'n'].includes(lowered)) {
    return false;
  }
  return fallback;
}

function normalizeLotSupplier(value) {
  return cleanString(value, 96);
}

function normalizeLotNumber(value) {
  return cleanString(value, 96);
}

function normalizeLotTargetCount(value) {
  const parsed = Number.parseInt(String(value || '').trim(), 10);
  if (!Number.isFinite(parsed)) {
    return null;
  }
  if (parsed < LOT_TARGET_COUNT_MIN || parsed > LOT_TARGET_COUNT_MAX) {
    return null;
  }
  return parsed;
}

function normalizeLotPriority(value, fallback = LOT_PRIORITY_DEFAULT) {
  const parsed = Number.parseInt(String(value || '').trim(), 10);
  if (!Number.isFinite(parsed)) {
    return Math.min(Math.max(fallback, LOT_PRIORITY_MIN), LOT_PRIORITY_MAX);
  }
  return Math.min(Math.max(parsed, LOT_PRIORITY_MIN), LOT_PRIORITY_MAX);
}

function buildLotLabel(supplier, lotNumber) {
  const supplierText = cleanString(supplier, 96) || 'Sans fournisseur';
  const lotText = cleanString(lotNumber, 96) || 'Sans numero';
  return `${supplierText} - lot ${lotText}`;
}

function normalizeLotFromRow(row) {
  if (!row) {
    return null;
  }
  const supplier = row.supplier || row.lot_supplier || row.report_lot_supplier || row.machine_lot_supplier;
  const lotNumber =
    row.lot_number || row.report_lot_number || row.machine_lot_number;
  if (!row.lot_id && !row.report_lot_id && !row.machine_lot_id && !supplier && !lotNumber) {
    return null;
  }
  const lotId = normalizeUuid(row.lot_id || row.report_lot_id || row.machine_lot_id);
  const targetCountRaw =
    row.target_count != null
      ? row.target_count
      : row.lot_target_count != null
        ? row.lot_target_count
        : row.report_lot_target_count != null
          ? row.report_lot_target_count
          : row.machine_lot_target_count;
  const producedCountRaw =
    row.produced_count != null
      ? row.produced_count
      : row.lot_produced_count != null
        ? row.lot_produced_count
        : row.report_lot_produced_count != null
          ? row.report_lot_produced_count
          : row.machine_lot_produced_count;
  const targetCount = Number.parseInt(targetCountRaw || '0', 10) || 0;
  const producedCount = Number.parseInt(producedCountRaw || '0', 10) || 0;
  const remainingCount = Math.max(targetCount - producedCount, 0);
  const progressPercent = targetCount > 0 ? Math.min(100, Math.round((producedCount * 1000) / targetCount) / 10) : 0;
  return {
    id: lotId || null,
    supplier: supplier || null,
    lotNumber: lotNumber || null,
    label: buildLotLabel(supplier, lotNumber),
    targetCount,
    producedCount,
    remainingCount,
    progressPercent,
    priority: Number.parseInt(row.priority || '0', 10) || LOT_PRIORITY_DEFAULT,
    isPaused: parseBooleanFlag(row.is_paused != null ? row.is_paused : row.lot_is_paused, false)
  };
}

function normalizeLotAssignments(raw) {
  if (!raw) {
    return [];
  }
  let list = raw;
  if (typeof raw === 'string') {
    try {
      list = JSON.parse(raw);
    } catch (error) {
      list = [];
    }
  }
  if (!Array.isArray(list)) {
    return [];
  }
  const dedupe = new Map();
  list.forEach((item) => {
    if (!item || typeof item !== 'object') {
      return;
    }
    const key = normalizeTechKey(item.technicianKey || item.technician || item.key);
    const technician = cleanString(item.technician || item.technicianName || item.label, 64);
    if (!key || !technician) {
      return;
    }
    if (!dedupe.has(key)) {
      dedupe.set(key, { technicianKey: key, technician });
    }
  });
  return Array.from(dedupe.values()).sort((a, b) => a.technician.localeCompare(b.technician, 'fr'));
}

function mapLotRowForResponse(row) {
  if (!row) {
    return null;
  }
  const lot = normalizeLotFromRow({ ...row, lot_id: row.id || row.lot_id });
  if (!lot) {
    return null;
  }
  return {
    ...lot,
    createdBy: row.created_by || null,
    createdAt: row.created_at || null,
    updatedAt: row.updated_at || null,
    assignments: normalizeLotAssignments(row.assignments)
  };
}

function detectDoubleCheck(raw) {
  if (raw == null) {
    return false;
  }
  if (typeof raw === 'boolean') {
    return raw;
  }
  if (typeof raw === 'number') {
    return raw !== 0;
  }
  const lowered = String(raw).trim().toLowerCase();
  if (!lowered) {
    return false;
  }
  if (['double_check', 'double-check', 'doublecheck', 'double check'].includes(lowered)) {
    return true;
  }
  if (['1', 'true', 'yes', 'on'].includes(lowered)) {
    return true;
  }
  return lowered.includes('double') && lowered.includes('check');
}

function isDoubleCheckPayload(body) {
  if (!body || typeof body !== 'object') {
    return false;
  }
  const sources = [body, body.diag, body.legacy, body.payload];
  for (const source of sources) {
    if (!source || typeof source !== 'object') {
      continue;
    }
    if (detectDoubleCheck(source.doubleCheck || source.double_check || source.isDoubleCheck)) {
      return true;
    }
    if (detectDoubleCheck(source.role || source.mode || source.type)) {
      return true;
    }
  }
  return false;
}

function getDateRange(dateFilter) {
  const now = new Date();
  if (dateFilter === 'today') {
    const start = new Date(now.getFullYear(), now.getMonth(), now.getDate());
    const end = new Date(now.getFullYear(), now.getMonth(), now.getDate(), 23, 59, 59, 999);
    return { start: start.toISOString(), end: end.toISOString() };
  }
  if (dateFilter === 'week') {
    const day = now.getDay();
    const diff = (day + 6) % 7;
    const start = new Date(now.getFullYear(), now.getMonth(), now.getDate() - diff);
    start.setHours(0, 0, 0, 0);
    const end = new Date(start);
    end.setDate(start.getDate() + 6);
    end.setHours(23, 59, 59, 999);
    return { start: start.toISOString(), end: end.toISOString() };
  }
  return null;
}

function buildReportFilters(query, { includeCategory = true, activeTagId = null } = {}) {
  const clauses = [];
  const values = [];
  let idx = 1;

  const techRaw = cleanString(query.tech, 64);
  const tech = techRaw ? normalizeTechKey(techRaw) : '';
  if (tech) {
    clauses.push(`${normalizeTextSql('technician')} = $${idx}`);
    values.push(tech);
    idx += 1;
  }

  const tagIds = parseTagIds(query.tags || query.tagIds);
  if (tagIds.length) {
    const activeId = normalizeUuid(activeTagId);
    const includeNull = activeId && tagIds.includes(activeId);
    if (includeNull) {
      clauses.push(`(tag_id = ANY($${idx}::uuid[]) OR tag_id IS NULL)`);
    } else {
      clauses.push(`tag_id = ANY($${idx}::uuid[])`);
    }
    values.push(tagIds);
    idx += 1;
  }

  const legacyFlag = String(query.legacy || '').toLowerCase();
  if (legacyFlag === '1' || legacyFlag === 'true') {
    clauses.push(`payload IS NOT NULL AND payload <> '' AND payload::jsonb ? 'legacy'`);
  } else if (legacyFlag === '0' || legacyFlag === 'false') {
    clauses.push(`(payload IS NULL OR payload = '' OR NOT (payload::jsonb ? 'legacy'))`);
  }

  if (includeCategory) {
    const categoryRaw = cleanString(query.category, 32);
    if (categoryRaw && categoryRaw !== 'all') {
      const category = normalizeCategory(categoryRaw);
      clauses.push(`category = $${idx}`);
      values.push(category);
      idx += 1;
    }
  }

  const commentFilter = query.comment;
  if (commentFilter === 'with') {
    clauses.push(`(comment IS NOT NULL AND comment <> '')`);
  } else if (commentFilter === 'without') {
    clauses.push(`(comment IS NULL OR comment = '')`);
  }

  const component = cleanString(query.component, 64);
  if (component && component !== 'all') {
    clauses.push(
      `lower(COALESCE(NULLIF(components, ''), '{}')::jsonb ->> $${idx}) = 'nok'`
    );
    values.push(component);
    idx += 1;
  }

  const dateFilter = query.date;
  const range = getDateRange(dateFilter);
  if (range) {
    clauses.push(`last_seen >= $${idx} AND last_seen <= $${idx + 1}`);
    values.push(range.start, range.end);
    idx += 2;
  }

  const search = cleanString(query.search, 128);
  if (search) {
    clauses.push(
      `lower(` +
        `coalesce(hostname,'') || ' ' || coalesce(serial_number,'') || ' ' || coalesce(mac_address,'') || ' ' || ` +
        `coalesce(mac_addresses,'') || ' ' || coalesce(machine_key,'') || ' ' || coalesce(technician,'') || ' ' || ` +
        `coalesce(vendor,'') || ' ' || coalesce(model,'') || ' ' || coalesce(comment,'') || ' ' || coalesce(tag,'')` +
        `) LIKE $${idx}`
    );
    values.push(`%${search.toLowerCase()}%`);
    idx += 1;
  }

  return { clauses, values };
}

function shouldUseLatest(query) {
  const raw = String(query.latest || '').toLowerCase();
  return raw === '1' || raw === 'true';
}

function normalizeMac(value) {
  if (value == null) {
    return null;
  }
  const raw = String(value);
  const stripped = raw.replace(/[^a-fA-F0-9]/g, '').toUpperCase();
  if (stripped.length !== 12) {
    return null;
  }
  return stripped.match(/.{2}/g).join(':');
}

function normalizeMacList(value) {
  if (value == null) {
    return null;
  }
  let list = [];
  if (Array.isArray(value)) {
    list = value;
  } else if (typeof value === 'string') {
    const trimmed = value.trim();
    if (!trimmed) {
      return null;
    }
    if (trimmed.startsWith('[') && trimmed.endsWith(']')) {
      try {
        const parsed = JSON.parse(trimmed);
        if (Array.isArray(parsed)) {
          list = parsed;
        } else {
          list = [trimmed];
        }
      } catch (error) {
        list = trimmed.split(/[;,]+/);
      }
    } else {
      list = trimmed.split(/[;,]+/);
    }
  } else {
    return null;
  }

  const normalized = list.map((entry) => normalizeMac(entry)).filter(Boolean);
  const unique = [...new Set(normalized)];
  return unique.length ? unique : null;
}

function normalizeSerial(value) {
  const serial = cleanString(value, 64);
  if (!serial) {
    return null;
  }
  return serial.toUpperCase();
}

function buildMachineKey(serialNumber, macAddress, hostname) {
  const serial = normalizeSerial(serialNumber);
  const mac = normalizeMac(macAddress);
  if (serial && mac) {
    return `sn:${serial}|mac:${mac}`;
  }
  if (serial) {
    return `sn:${serial}`;
  }
  if (mac) {
    return `mac:${mac}`;
  }
  if (hostname) {
    return `host:${String(hostname).toLowerCase()}`;
  }
  return null;
}

function normalizeCategory(value) {
  if (value == null) {
    return 'unknown';
  }
  if (typeof value === 'number') {
    if (LAPTOP_CHASSIS_CODES.has(value)) {
      return 'laptop';
    }
    if (DESKTOP_CHASSIS_CODES.has(value)) {
      return 'desktop';
    }
    return 'unknown';
  }

  const normalized = String(value).trim().toLowerCase();
  if (!normalized) {
    return 'unknown';
  }
  const numeric = Number(normalized);
  if (Number.isFinite(numeric)) {
    if (LAPTOP_CHASSIS_CODES.has(numeric)) {
      return 'laptop';
    }
    if (DESKTOP_CHASSIS_CODES.has(numeric)) {
      return 'desktop';
    }
  }
  if (
    normalized.includes('laptop') ||
    normalized.includes('portable') ||
    normalized.includes('notebook') ||
    normalized.includes('ultrabook')
  ) {
    return 'laptop';
  }
  if (
    normalized.includes('desktop') ||
    normalized.includes('tour') ||
    normalized.includes('tower') ||
    normalized.includes('fixe') ||
    normalized.includes('workstation')
  ) {
    return 'desktop';
  }
  return 'unknown';
}

function pickFirst(obj, keys) {
  if (!obj || typeof obj !== 'object') {
    return undefined;
  }
  for (const key of keys) {
    if (obj[key] !== undefined && obj[key] !== null) {
      return obj[key];
    }
  }
  return undefined;
}

function escapeLdapFilter(value) {
  return String(value).replace(/[\0()*\\]/g, (char) => {
    switch (char) {
      case '\\':
        return '\\5c';
      case '*':
        return '\\2a';
      case '(':
        return '\\28';
      case ')':
        return '\\29';
      case '\0':
        return '\\00';
      default:
        return char;
    }
  });
}

const LDAP_FIELD_LIMIT = 512;

function normalizeLdapSearchFilter(value) {
  const normalized = normalizeOptionalString(value, LDAP_FIELD_LIMIT);
  return normalized || DEFAULT_LDAP_SEARCH_FILTER;
}

function normalizeLdapSearchAttributesString(value) {
  const normalized = normalizeOptionalString(value, LDAP_FIELD_LIMIT);
  const raw = normalized || DEFAULT_LDAP_SEARCH_ATTRIBUTES;
  const list = raw
    .split(',')
    .map((attr) => attr.trim())
    .filter(Boolean);
  return list.length ? list.join(',') : DEFAULT_LDAP_SEARCH_ATTRIBUTES;
}

function parseLdapSearchAttributes(value) {
  const raw = normalizeLdapSearchAttributesString(value);
  return raw
    .split(',')
    .map((attr) => attr.trim())
    .filter(Boolean);
}

function ensureLdapAttributes(value, requiredAttributes = []) {
  const list = parseLdapSearchAttributes(value);
  const normalized = list.map((attr) => attr.toLowerCase());
  requiredAttributes.forEach((attr) => {
    if (!normalized.includes(attr.toLowerCase())) {
      list.push(attr);
    }
  });
  return list;
}

async function loadLdapSettingsFromDb() {
  try {
    const result = await pool.query(`
      SELECT
        id,
        enabled,
        url,
        bind_dn,
        bind_password,
        search_base,
        search_filter,
        search_attributes,
        tls_reject_unauthorized,
        updated_at
      FROM ldap_settings
      WHERE id = 1
      LIMIT 1
    `);
    return result.rows[0] || null;
  } catch (error) {
    return null;
  }
}

function mapLdapSettings(row) {
  if (!row) {
    return null;
  }
  const searchAttributesRaw = normalizeLdapSearchAttributesString(row.search_attributes);
  return {
    source: 'db',
    enabled: Boolean(row.enabled),
    url: row.url || '',
    bindDn: row.bind_dn || '',
    bindPassword: row.bind_password || '',
    searchBase: row.search_base || '',
    searchFilter: normalizeLdapSearchFilter(row.search_filter),
    searchAttributesRaw,
    searchAttributes: parseLdapSearchAttributes(searchAttributesRaw),
    tlsRejectUnauthorized: row.tls_reject_unauthorized !== false,
    updatedAt: row.updated_at || null
  };
}

function getEnvLdapConfig() {
  const searchAttributesRaw = normalizeLdapSearchAttributesString(LDAP_SEARCH_ATTRIBUTES_RAW);
  return {
    source: 'env',
    enabled: ENV_LDAP_ENABLED,
    url: LDAP_URL,
    bindDn: LDAP_BIND_DN,
    bindPassword: LDAP_BIND_PASSWORD,
    searchBase: LDAP_SEARCH_BASE,
    searchFilter: normalizeLdapSearchFilter(LDAP_SEARCH_FILTER),
    searchAttributesRaw,
    searchAttributes: parseLdapSearchAttributes(searchAttributesRaw),
    tlsRejectUnauthorized: LDAP_TLS_REJECT_UNAUTHORIZED,
    updatedAt: null
  };
}

async function getEffectiveLdapConfig() {
  const row = await loadLdapSettingsFromDb();
  const mapped = mapLdapSettings(row);
  if (mapped) {
    return mapped;
  }
  return getEnvLdapConfig();
}

function formatLdapConfigForResponse(config) {
  return {
    enabled: Boolean(config.enabled),
    url: config.url || '',
    bindDn: config.bindDn || '',
    searchBase: config.searchBase || '',
    searchFilter: normalizeLdapSearchFilter(config.searchFilter),
    searchAttributes: normalizeLdapSearchAttributesString(config.searchAttributesRaw),
    tlsRejectUnauthorized: config.tlsRejectUnauthorized !== false,
    bindPasswordSet: Boolean(config.bindPassword),
    source: config.source || 'env',
    updatedAt: config.updatedAt
  };
}

const vaultAdminCache = {
  username: null,
  password: null,
  expiresAt: 0,
  inflight: null
};

const suggestionMailCache = {
  username: null,
  password: null,
  expiresAt: 0,
  inflight: null
};

function parseVaultScopes(value) {
  if (!value) {
    return [];
  }
  return value
    .split(',')
    .map((entry) => entry.trim())
    .filter(Boolean);
}

function buildVaultUrl(pathname) {
  try {
    return new URL(pathname, VAULT_URL).toString();
  } catch (error) {
    return '';
  }
}

function shouldUseVaultAdmin() {
  return Boolean(VAULT_URL && VAULT_SECRET_PATH && (VAULT_AUTH_TOKEN || VAULT_BEARER_TOKEN));
}

function shouldUseSuggestionVault() {
  return Boolean(
    VAULT_URL &&
      SUGGESTION_VAULT_SECRET_PATH &&
      (VAULT_AUTH_TOKEN || VAULT_BEARER_TOKEN)
  );
}

function requestJson(method, urlString, headers, body, timeoutMs) {
  return new Promise((resolve, reject) => {
    if (!urlString) {
      reject(new Error('missing_url'));
      return;
    }
    const urlObj = new URL(urlString);
    const lib = urlObj.protocol === 'https:' ? https : http;
    const payload =
      body == null ? null : typeof body === 'string' ? body : JSON.stringify(body);
    const requestHeaders = { ...(headers || {}) };
    if (payload && !requestHeaders['Content-Type']) {
      requestHeaders['Content-Type'] = 'application/json';
    }
    if (payload && !requestHeaders['Content-Length']) {
      requestHeaders['Content-Length'] = Buffer.byteLength(payload);
    }
    const req = lib.request(
      {
        method,
        hostname: urlObj.hostname,
        port: urlObj.port || (urlObj.protocol === 'https:' ? 443 : 80),
        path: `${urlObj.pathname}${urlObj.search}`,
        headers: requestHeaders
      },
      (res) => {
        let raw = '';
        res.setEncoding('utf8');
        res.on('data', (chunk) => {
          raw += chunk;
        });
        res.on('end', () => {
          let json = null;
          if (raw) {
            try {
              json = JSON.parse(raw);
            } catch (error) {
              json = null;
            }
          }
          resolve({ statusCode: res.statusCode || 0, json, raw });
        });
      }
    );
    req.on('error', reject);
    if (timeoutMs && Number.isFinite(timeoutMs) && timeoutMs > 0) {
      req.setTimeout(timeoutMs, () => {
        req.destroy(new Error('timeout'));
      });
    }
    if (payload) {
      req.write(payload);
    }
    req.end();
  });
}

async function fetchVaultAccessToken() {
  if (VAULT_BEARER_TOKEN) {
    return VAULT_BEARER_TOKEN;
  }
  if (!VAULT_AUTH_TOKEN) {
    return '';
  }
  const authUrl = buildVaultUrl(VAULT_AUTH_PATH);
  if (!authUrl) {
    return '';
  }
  const payload = {
    token: VAULT_AUTH_TOKEN,
    scopes: parseVaultScopes(VAULT_AUTH_SCOPES)
  };
  const response = await requestJson(
    'POST',
    authUrl,
    { 'Content-Type': 'application/json' },
    payload,
    VAULT_TIMEOUT_MS
  );
  if (!response || response.statusCode < 200 || response.statusCode >= 300) {
    return '';
  }
  if (response.json && typeof response.json.access_token === 'string') {
    return response.json.access_token;
  }
  if (response.json && typeof response.json.token === 'string') {
    return response.json.token;
  }
  return '';
}

function extractVaultSecretData(responseJson) {
  if (!responseJson || typeof responseJson !== 'object') {
    return null;
  }
  if (responseJson.data && typeof responseJson.data === 'object') {
    return responseJson.data;
  }
  return responseJson;
}

async function fetchVaultAdminCredentials() {
  if (!ALLOW_LOCAL_ADMIN || !shouldUseVaultAdmin()) {
    return null;
  }
  const now = Date.now();
  if (vaultAdminCache.expiresAt > now && vaultAdminCache.password) {
    return { username: vaultAdminCache.username, password: vaultAdminCache.password };
  }
  if (vaultAdminCache.inflight) {
    return vaultAdminCache.inflight;
  }
  vaultAdminCache.inflight = (async () => {
    try {
      const accessToken = await fetchVaultAccessToken();
      if (!accessToken) {
        return null;
      }
      const secretUrl = buildVaultUrl(VAULT_SECRET_PATH);
      if (!secretUrl) {
        return null;
      }
      const secretResponse = await requestJson(
        'GET',
        secretUrl,
        { Authorization: `Bearer ${accessToken}` },
        null,
        VAULT_TIMEOUT_MS
      );
      if (!secretResponse || secretResponse.statusCode < 200 || secretResponse.statusCode >= 300) {
        return null;
      }
      const secretData = extractVaultSecretData(secretResponse.json);
      if (!secretData || typeof secretData !== 'object') {
        return null;
      }
      const username =
        cleanString(secretData[VAULT_SECRET_USER_FIELD], 128) || LOCAL_ADMIN_USER;
      const password =
        typeof secretData[VAULT_SECRET_PASSWORD_FIELD] === 'string'
          ? secretData[VAULT_SECRET_PASSWORD_FIELD]
          : '';
      if (!password) {
        return null;
      }
      const ttlMs =
        Number.isFinite(VAULT_CACHE_TTL_SEC) && VAULT_CACHE_TTL_SEC > 0
          ? VAULT_CACHE_TTL_SEC * 1000
          : 300000;
      vaultAdminCache.username = username;
      vaultAdminCache.password = password;
      vaultAdminCache.expiresAt = Date.now() + ttlMs;
      return { username, password };
    } catch (error) {
      console.error('Vault admin fetch failed', error);
      return null;
    } finally {
      vaultAdminCache.inflight = null;
    }
  })();
  return vaultAdminCache.inflight;
}

async function fetchSuggestionCredentials() {
  if (!SUGGESTION_EMAIL_ENABLED || !shouldUseSuggestionVault()) {
    return null;
  }
  const now = Date.now();
  if (suggestionMailCache.expiresAt > now && suggestionMailCache.password) {
    return {
      username: suggestionMailCache.username,
      password: suggestionMailCache.password
    };
  }
  if (suggestionMailCache.inflight) {
    return suggestionMailCache.inflight;
  }
  suggestionMailCache.inflight = (async () => {
    try {
      const accessToken = await fetchVaultAccessToken();
      if (!accessToken) {
        return null;
      }
      const secretUrl = buildVaultUrl(SUGGESTION_VAULT_SECRET_PATH);
      if (!secretUrl) {
        return null;
      }
      const secretResponse = await requestJson(
        'GET',
        secretUrl,
        { Authorization: `Bearer ${accessToken}` },
        null,
        VAULT_TIMEOUT_MS
      );
      if (!secretResponse || secretResponse.statusCode < 200 || secretResponse.statusCode >= 300) {
        return null;
      }
      const secretData = extractVaultSecretData(secretResponse.json);
      if (!secretData || typeof secretData !== 'object') {
        return null;
      }
      let username =
        cleanString(secretData[SUGGESTION_VAULT_USER_FIELD], 256) || null;
      if (!username && SUGGESTION_SMTP_USER) {
        username = SUGGESTION_SMTP_USER;
      }
      const password =
        typeof secretData[SUGGESTION_VAULT_PASSWORD_FIELD] === 'string'
          ? secretData[SUGGESTION_VAULT_PASSWORD_FIELD]
          : '';
      const resolvedPassword = password || SUGGESTION_SMTP_PASS || '';
      if (!resolvedPassword) {
        return null;
      }
      const ttlMs =
        Number.isFinite(VAULT_CACHE_TTL_SEC) && VAULT_CACHE_TTL_SEC > 0
          ? VAULT_CACHE_TTL_SEC * 1000
          : 300000;
      suggestionMailCache.username = username;
      suggestionMailCache.password = resolvedPassword;
      suggestionMailCache.expiresAt = Date.now() + ttlMs;
      return { username, password: resolvedPassword };
    } catch (error) {
      console.error('Suggestion mail vault fetch failed', error);
      return null;
    } finally {
      suggestionMailCache.inflight = null;
    }
  })();
  return suggestionMailCache.inflight;
}

async function getSuggestionSmtpCredentials() {
  if (!SUGGESTION_EMAIL_ENABLED) {
    return null;
  }
  if (SUGGESTION_SMTP_USER && SUGGESTION_SMTP_PASS) {
    return { username: SUGGESTION_SMTP_USER, password: SUGGESTION_SMTP_PASS };
  }
  return fetchSuggestionCredentials();
}

function readSmtpResponse(socket) {
  return new Promise((resolve, reject) => {
    let buffer = '';
    function onData(chunk) {
      buffer += chunk.toString('utf8');
      const lines = buffer.split(/\r?\n/).filter(Boolean);
      if (!lines.length) {
        return;
      }
      const last = lines[lines.length - 1];
      const match = last.match(/^(\d{3})\s/);
      if (match) {
        cleanup();
        resolve({ code: match[1], raw: buffer });
      }
    }
    function onError(err) {
      cleanup();
      reject(err);
    }
    function cleanup() {
      socket.off('data', onData);
      socket.off('error', onError);
    }
    socket.on('data', onData);
    socket.on('error', onError);
  });
}

async function sendSmtpCommand(socket, command, expectCodes) {
  if (command) {
    socket.write(`${command}\r\n`);
  }
  const response = await readSmtpResponse(socket);
  const ok = Array.isArray(expectCodes)
    ? expectCodes.includes(response.code)
    : response.code === expectCodes;
  if (!ok) {
    throw new Error(`SMTP ${command || 'read'} failed: ${response.code}`);
  }
  return response;
}

async function sendSmtpMail({ from, to, subject, text, authUser, authPass }) {
  return new Promise((resolve, reject) => {
    let finished = false;
    const timeout = setTimeout(() => {
      if (finished) {
        return;
      }
      finished = true;
      try {
        socket.destroy(new Error('smtp_timeout'));
      } catch (error) {
        // ignore
      }
      reject(new Error('smtp_timeout'));
    }, Math.max(1000, SUGGESTION_SMTP_TIMEOUT_MS));

    const connectOptions = {
      host: SUGGESTION_SMTP_HOST,
      port: SUGGESTION_SMTP_PORT,
      servername: SUGGESTION_SMTP_HOST
    };

    let socket = (SUGGESTION_SMTP_SECURE
      ? tls.connect(connectOptions)
      : net.connect({ host: connectOptions.host, port: connectOptions.port })
    );

    const onError = (err) => {
      if (!finished) {
        finished = true;
        clearTimeout(timeout);
        reject(err);
      }
    };

    socket.on('error', onError);

    socket.on('connect', async () => {
      try {
        await sendSmtpCommand(socket, null, '220');
        await sendSmtpCommand(socket, `EHLO ${os.hostname()}`, ['250']);
        if (!SUGGESTION_SMTP_SECURE) {
          await sendSmtpCommand(socket, 'STARTTLS', '220');
          socket.removeAllListeners('error');
          socket = tls.connect({ socket, servername: SUGGESTION_SMTP_HOST });
          socket.on('error', onError);
          await sendSmtpCommand(socket, `EHLO ${os.hostname()}`, ['250']);
        }
        await sendSmtpCommand(socket, 'AUTH LOGIN', '334');
        await sendSmtpCommand(socket, Buffer.from(authUser).toString('base64'), '334');
        await sendSmtpCommand(socket, Buffer.from(authPass).toString('base64'), '235');
        await sendSmtpCommand(socket, `MAIL FROM:<${from}>`, ['250']);
        await sendSmtpCommand(socket, `RCPT TO:<${to}>`, ['250', '251']);
        await sendSmtpCommand(socket, 'DATA', '354');
        const lines = [
          `From: MDT Live Ops <${from}>`,
          `To: ${to}`,
          `Subject: ${subject}`,
          'Content-Type: text/plain; charset=utf-8',
          '',
          text
        ];
        const message = lines.join('\r\n').replace(/\n\./g, '\n..');
        socket.write(`${message}\r\n.\r\n`);
        await sendSmtpCommand(socket, null, '250');
        await sendSmtpCommand(socket, 'QUIT', ['221', '250']);
        socket.end();
        if (!finished) {
          finished = true;
          clearTimeout(timeout);
          resolve({ ok: true });
        }
      } catch (err) {
        socket.end();
        if (!finished) {
          finished = true;
          clearTimeout(timeout);
          reject(err);
        }
      }
    });
  });
}

async function sendSuggestionEmail({ title, body, createdBy, pageLabel }) {
  if (!SUGGESTION_EMAIL_ENABLED) {
    return { ok: false, error: 'disabled' };
  }
  const creds = await getSuggestionSmtpCredentials();
  if (!creds || !creds.password || !creds.username) {
    return { ok: false, error: 'missing_credentials' };
  }
  const fromAddress = SUGGESTION_EMAIL_FROM || creds.username;
  const subject = `Suggestion MDT: ${title}`;
  const text = [
    `Suggestion: ${title}`,
    '',
    body,
    '',
    `Page: ${pageLabel || '--'}`,
    `Auteur: ${createdBy || 'inconnu'}`
  ].join('\n');
  try {
    await sendSmtpMail({
      from: fromAddress,
      to: SUGGESTION_EMAIL_TO,
      subject,
      text,
      authUser: creds.username,
      authPass: creds.password
    });
    return { ok: true };
  } catch (error) {
    console.error('Failed to send suggestion email', error);
    return { ok: false, error: 'send_failed' };
  }
}

async function getLocalAdminCredentials() {
  if (!ALLOW_LOCAL_ADMIN) {
    return null;
  }
  const vaultCreds = await fetchVaultAdminCredentials();
  if (vaultCreds && vaultCreds.password) {
    return vaultCreds;
  }
  if (!LOCAL_ADMIN_PASSWORD) {
    return null;
  }
  return {
    username: LOCAL_ADMIN_USER,
    password: LOCAL_ADMIN_PASSWORD
  };
}

async function isLocalAdmin(username, password) {
  if (!ALLOW_LOCAL_ADMIN) {
    return false;
  }
  const creds = await getLocalAdminCredentials();
  if (!creds) {
    return false;
  }
  return username === creds.username && password === creds.password;
}

function buildLocalSessionUser(username) {
  return {
    username,
    type: 'local',
    isHydraAdmin: true,
    permissions: {
      canDeleteReport: true
    }
  };
}

function buildLdapSessionUser(username, ldapUser) {
  const groups = extractLdapGroups(ldapUser);
  const isHydraAdmin = isHydraAdminMember(groups);
  return {
    username,
    type: 'ldap',
    displayName: ldapUser.cn || ldapUser.displayName || ldapUser.uid || username,
    dn: ldapUser.dn || null,
    mail: ldapUser.mail || null,
    groups,
    isHydraAdmin,
    permissions: {
      canDeleteReport: isHydraAdmin
    }
  };
}

async function authenticateLdap(username, password, configOverride = null) {
  const config = configOverride || (await getEffectiveLdapConfig());
  if (!config.enabled || !config.url || !config.searchBase) {
    return Promise.reject(new Error('ldap_not_configured'));
  }

  const searchFilter = normalizeLdapSearchFilter(config.searchFilter).replace(
    '{{username}}',
    escapeLdapFilter(username)
  );
  const options = {
    url: config.url,
    searchBase: config.searchBase,
    searchFilter,
    searchAttributes: ensureLdapAttributes(config.searchAttributes, ['memberOf']),
    reconnect: true,
    tlsOptions: {
      rejectUnauthorized: config.tlsRejectUnauthorized !== false
    }
  };

  if (config.bindDn && config.bindPassword) {
    options.bindDN = config.bindDn;
    options.bindCredentials = config.bindPassword;
  }

  return new Promise((resolve, reject) => {
    const ldap = new LdapAuth(options);
    ldap.authenticate(username, password, (err, user) => {
      ldap.close();
      if (err || !user) {
        reject(err || new Error('invalid_credentials'));
        return;
      }
      resolve(user);
    });
  });
}

function pickFirstFromSources(sources, keys) {
  for (const source of sources) {
    const value = pickFirst(source, keys);
    if (value !== undefined && value !== null) {
      return value;
    }
  }
  return undefined;
}

function normalizeStatus(value) {
  if (value == null) {
    return null;
  }
  if (typeof value === 'boolean') {
    return value ? 'ok' : 'nok';
  }
  if (typeof value === 'number') {
    if (value === 1) {
      return 'ok';
    }
    if (value === 0) {
      return 'nok';
    }
  }
  const raw = String(value).trim().toLowerCase();
  if (!raw) {
    return null;
  }
  const cleaned = raw.normalize('NFD').replace(/[\u0300-\u036f]/g, '');
  if (
    cleaned.includes('not tested') ||
    cleaned.includes('not_tested') ||
    cleaned.includes('non teste') ||
    cleaned.includes('non testee') ||
    cleaned.includes('pas teste') ||
    cleaned.includes('not run')
  ) {
    return 'not_tested';
  }
  if (
    cleaned.includes('absent') ||
    cleaned.includes('missing') ||
    cleaned.includes('not present') ||
    cleaned.includes('non present') ||
    cleaned.includes('not detected') ||
    cleaned.includes('indisponible')
  ) {
    return 'absent';
  }
  if (
    cleaned.includes('nok') ||
    cleaned.includes('ko') ||
    cleaned.includes('fail') ||
    cleaned.includes('error') ||
    cleaned.includes('not ok') ||
    cleaned.includes('defaillant') ||
    cleaned.includes('defectueux') ||
    cleaned.includes('not working')
  ) {
    return 'nok';
  }
  if (
    cleaned === 'ok' ||
    cleaned.includes('ok') ||
    cleaned.includes('good') ||
    cleaned.includes('present') ||
    cleaned.includes('working') ||
    cleaned.includes('fonction') ||
    cleaned.includes('disponible')
  ) {
    return 'ok';
  }
  return null;
}

function normalizeBiosLanguage(value) {
  if (value == null) {
    return null;
  }
  const raw = String(value).trim().toLowerCase();
  if (!raw) {
    return null;
  }
  const cleaned = raw.normalize('NFD').replace(/[\u0300-\u036f]/g, '');
  if (
    cleaned.includes('not tested') ||
    cleaned.includes('not_tested') ||
    cleaned.includes('non teste') ||
    cleaned.includes('non testee') ||
    cleaned.includes('pas teste') ||
    cleaned.includes('not run')
  ) {
    return 'not_tested';
  }
  if (
    cleaned === 'fr' ||
    cleaned.startsWith('fr-') ||
    cleaned.includes('francais') ||
    cleaned.includes('france') ||
    cleaned.includes('french')
  ) {
    return 'fr';
  }
  if (
    cleaned === 'en' ||
    cleaned.startsWith('en-') ||
    cleaned.includes('anglais') ||
    cleaned.includes('english')
  ) {
    return 'en';
  }
  return null;
}

function normalizeBiosPasswordStatus(value) {
  if (value == null) {
    return null;
  }
  if (typeof value === 'boolean') {
    return value ? 'nok' : 'ok';
  }
  if (typeof value === 'number') {
    if (value === 1) {
      return 'nok';
    }
    if (value === 0) {
      return 'ok';
    }
  }
  const raw = String(value).trim().toLowerCase();
  if (!raw) {
    return null;
  }
  const cleaned = raw.normalize('NFD').replace(/[\u0300-\u036f]/g, '');
  if (
    cleaned.includes('not tested') ||
    cleaned.includes('not_tested') ||
    cleaned.includes('non teste') ||
    cleaned.includes('non testee') ||
    cleaned.includes('pas teste') ||
    cleaned.includes('not run')
  ) {
    return 'not_tested';
  }
  if (
    cleaned === 'oui' ||
    cleaned === 'yes' ||
    cleaned === 'true' ||
    cleaned === '1' ||
    cleaned.includes('enabled') ||
    cleaned.includes('active') ||
    cleaned.includes('set')
  ) {
    return 'nok';
  }
  if (
    cleaned === 'non' ||
    cleaned === 'no' ||
    cleaned === 'false' ||
    cleaned === '0' ||
    cleaned.includes('disabled') ||
    cleaned.includes('none') ||
    cleaned.includes('unset')
  ) {
    return 'ok';
  }
  const fallback = normalizeStatus(cleaned);
  return fallback === 'ok' || fallback === 'nok' || fallback === 'not_tested' ? fallback : null;
}

function normalizeWifiStandardStatus(value) {
  if (value == null) {
    return null;
  }
  if (typeof value === 'boolean') {
    return value ? 'ok' : 'nok';
  }
  if (typeof value === 'number') {
    if (value === 1) {
      return 'ok';
    }
    if (value === 0) {
      return 'nok';
    }
  }
  const raw = String(value).trim().toLowerCase();
  if (!raw) {
    return null;
  }
  const cleaned = raw.normalize('NFD').replace(/[\u0300-\u036f]/g, '');
  if (
    cleaned.includes('not tested') ||
    cleaned.includes('not_tested') ||
    cleaned.includes('non teste') ||
    cleaned.includes('non testee') ||
    cleaned.includes('pas teste') ||
    cleaned.includes('not run')
  ) {
    return 'not_tested';
  }

  const modernSuffixes = new Set(['be', 'ax', 'ac', 'n']);
  const legacySuffixes = new Set(['g', 'a', 'b']);
  const standards = Array.from(cleaned.matchAll(/(?:802(?:[.\s-])?)?11\s*(be|ax|ac|n|g|a|b)\b/gi)).map(
    (match) => match[1].toLowerCase()
  );
  if (standards.some((suffix) => modernSuffixes.has(suffix))) {
    return 'ok';
  }
  if (standards.some((suffix) => legacySuffixes.has(suffix))) {
    return 'nok';
  }

  if (/\bwi-?fi\s*(7|6e?|5|4)\b/.test(cleaned)) {
    return 'ok';
  }
  if (/\bwi-?fi\s*(3|2|1)\b/.test(cleaned)) {
    return 'nok';
  }
  if (
    cleaned.includes('no wireless interface') ||
    cleaned.includes('there is no wireless interface') ||
    cleaned.includes('aucune interface sans fil') ||
    cleaned.includes('aucune interface reseau sans fil')
  ) {
    return 'nok';
  }

  const fallback = normalizeStatus(cleaned);
  return fallback === 'ok' || fallback === 'nok' || fallback === 'not_tested' ? fallback : null;
}

function normalizeBatteryHealth(value) {
  if (value == null) {
    return null;
  }
  if (typeof value === 'number' && Number.isFinite(value)) {
    if (value < 0 || value > 100) {
      return null;
    }
    return Math.round(value);
  }
  const raw = String(value).replace('%', '').replace(',', '.').trim();
  if (!raw) {
    return null;
  }
  const parsed = Number(raw);
  if (!Number.isFinite(parsed) || parsed < 0 || parsed > 100) {
    return null;
  }
  return Math.round(parsed);
}

function normalizeSlots(value) {
  if (value == null) {
    return null;
  }
  const parsed = Number.parseInt(value, 10);
  if (!Number.isFinite(parsed) || parsed < 0 || parsed > 32) {
    return null;
  }
  return parsed;
}

function normalizeRamMb(value) {
  if (value == null) {
    return null;
  }
  if (typeof value === 'number' && Number.isFinite(value)) {
    if (value <= 0) {
      return null;
    }
    return value < 128 ? Math.round(value * 1024) : Math.round(value);
  }
  const raw = String(value).trim().toLowerCase();
  if (!raw) {
    return null;
  }
  const cleaned = raw.replace(',', '.');
  const match = cleaned.match(/([0-9]+(?:\.[0-9]+)?)/);
  if (!match) {
    return null;
  }
  const amount = Number(match[1]);
  if (!Number.isFinite(amount) || amount <= 0) {
    return null;
  }
  if (
    cleaned.includes('gb') ||
    cleaned.includes('go') ||
    cleaned.includes('gib') ||
    cleaned.includes('gig')
  ) {
    return Math.round(amount * 1024);
  }
  if (cleaned.includes('mb') || cleaned.includes('mo') || cleaned.includes('mib')) {
    return Math.round(amount);
  }
  if (cleaned.endsWith('g')) {
    return Math.round(amount * 1024);
  }
  if (cleaned.endsWith('m')) {
    return Math.round(amount);
  }
  return amount < 128 ? Math.round(amount * 1024) : Math.round(amount);
}

function withManualComponentDefaults(components) {
  const result = { ...MANUAL_COMPONENT_DEFAULTS };
  if (components && typeof components === 'object' && !Array.isArray(components)) {
    Object.entries(components).forEach(([key, value]) => {
      if (value !== undefined && value !== null && value !== '') {
        result[key] = value;
      } else if (!Object.prototype.hasOwnProperty.call(result, key)) {
        result[key] = value;
      }
    });
  }
  return result;
}

function sanitizeComponents(value) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    return null;
  }
  const entries = Object.entries(value);
  if (entries.length === 0) {
    return null;
  }
  const sanitized = {};
  for (const [key, raw] of entries.slice(0, 50)) {
    const cleanKey = cleanString(key, 64);
    if (!cleanKey) {
      continue;
    }
    const cleanValue = cleanString(String(raw), 128);
    if (!cleanValue) {
      sanitized[cleanKey] = '';
      continue;
    }
    if (cleanKey === 'biosBattery') {
      sanitized[cleanKey] = normalizeStatus(cleanValue) || 'not_tested';
      continue;
    }
    if (cleanKey === 'biosLanguage') {
      sanitized[cleanKey] = normalizeBiosLanguage(cleanValue) || 'not_tested';
      continue;
    }
    if (cleanKey === 'biosPassword') {
      sanitized[cleanKey] = normalizeBiosPasswordStatus(cleanValue) || 'not_tested';
      continue;
    }
    if (cleanKey === 'wifiStandard') {
      sanitized[cleanKey] = normalizeWifiStandardStatus(cleanValue) || 'not_tested';
      continue;
    }
    sanitized[cleanKey] = cleanValue;
  }
  return Object.keys(sanitized).length > 0 ? sanitized : null;
}

function mergeComponentSets(base, override) {
  const merged = {};
  if (base && typeof base === 'object' && !Array.isArray(base)) {
    Object.entries(base).forEach(([key, value]) => {
      merged[key] = value;
    });
  }
  if (override && typeof override === 'object' && !Array.isArray(override)) {
    Object.entries(override).forEach(([key, value]) => {
      merged[key] = value;
    });
  }
  return Object.keys(merged).length > 0 ? merged : null;
}

function buildDerivedComponents(body, sources) {
  const derived = {};
  const addStatus = (key, value) => {
    const status = normalizeStatus(value);
    if (status) {
      derived[key] = status;
    }
  };
  const addValue = (key, value, normalizer) => {
    const normalized = normalizer(value);
    if (normalized) {
      derived[key] = normalized;
    }
  };
  addStatus('camera', pickFirstFromSources(sources, ['cameraStatus', 'camera']));
  addStatus('usb', pickFirstFromSources(sources, ['usbStatus', 'usb']));
  addStatus('keyboard', pickFirstFromSources(sources, ['keyboardStatus', 'keyboard']));
  addStatus(
    'pad',
    pickFirstFromSources(sources, [
      'padStatus',
      'touchpadStatus',
      'trackpadStatus',
      'paveTactile',
      'touchpad'
    ])
  );
  addStatus(
    'badgeReader',
    pickFirstFromSources(sources, ['badgeReaderStatus', 'badgeReader', 'badge', 'smartCardReader'])
  );
  addStatus(
    'biosBattery',
    pickFirstFromSources(sources, ['biosBatteryStatus', 'biosBattery', 'cmosBatteryStatus', 'cmosBattery'])
  );
  addValue(
    'biosLanguage',
    pickFirstFromSources(sources, ['biosLanguage', 'biosLanguageStatus', 'languageBios', 'biosLocale']),
    normalizeBiosLanguage
  );
  addValue(
    'biosPassword',
    pickFirstFromSources(sources, [
      'biosPassword',
      'biosPasswordStatus',
      'biosPasswordSet',
      'biosPasswordEnabled',
      'biosPwd'
    ]),
    normalizeBiosPasswordStatus
  );
  addValue(
    'wifiStandard',
    pickFirstFromSources(sources, [
      'wifiStandard',
      'wifiStandardStatus',
      'wifi_standard',
      'wirelessStandard',
      'wlanStandard',
      'radioType',
      'radio_type',
      'typeRadio',
      'type_radio'
    ]),
    normalizeWifiStandardStatus
  );
  addStatus('diskSmart', pickFirstFromSources(sources, ['diskSmart', 'smartDisk', 'smart']));

  const tests =
    body && typeof body.tests === 'object' && !Array.isArray(body.tests) ? body.tests : null;
  if (tests) {
    addStatus('diskReadTest', tests.diskRead);
    addStatus('diskWriteTest', tests.diskWrite);
    addStatus('ramTest', tests.ramTest);
    addStatus('cpuTest', tests.cpuTest);
    addStatus('gpuTest', tests.gpuTest);
    addStatus('cpu', tests.cpuTest || tests.cpu);
    addStatus('gpu', tests.gpuTest || tests.gpu);
    addStatus('cpuStress', tests.cpuStress);
    addStatus('gpuStress', tests.gpuStress);
    addStatus('networkTest', tests.network);
    addStatus('networkPing', tests.networkPing);
    addStatus('fsCheck', tests.fsCheck);
    addStatus('memDiag', tests.memDiag);
  }

  if (body && body.thermal && typeof body.thermal === 'object') {
    addStatus('thermal', body.thermal.status);
  }

  return Object.keys(derived).length > 0 ? derived : null;
}

function safeJsonStringify(value, maxBytes) {
  try {
    const json = JSON.stringify(value);
    if (Buffer.byteLength(json, 'utf8') <= maxBytes) {
      return json;
    }
  } catch (error) {
    return null;
  }
  return JSON.stringify({ truncated: true });
}

function sanitizeAuditData(data, includePayload) {
  if (!data || typeof data !== 'object') {
    return data;
  }
  if (includePayload) {
    return data;
  }
  const sanitized = { ...data };
  if (Object.prototype.hasOwnProperty.call(sanitized, 'payload')) {
    sanitized.payload = '[skipped]';
  }
  return sanitized;
}

function limitString(value, maxLength) {
  if (typeof value !== 'string') {
    return null;
  }
  const trimmed = value.trim();
  if (!trimmed) {
    return null;
  }
  return trimmed.length > maxLength ? trimmed.slice(0, maxLength) : trimmed;
}

function getClientIp(req) {
  if (!req.ip) {
    return null;
  }
  return req.ip.startsWith('::ffff:') ? req.ip.slice(7) : req.ip;
}

function buildAuditContext(req, overrides = {}) {
  const actor = limitString(
    overrides.actor || (req.session && req.session.user ? req.session.user.username : null),
    128
  );
  const actorType = limitString(
    overrides.actorType || (req.session && req.session.user ? req.session.user.type : null),
    32
  );
  const actorIp = limitString(getClientIp(req), 64);
  const userAgent = limitString(req.get('user-agent'), 256);
  const requestId = limitString(req.requestId, 128);
  const source = limitString(overrides.source || `${req.method} ${req.originalUrl}`, 256);
  return {
    actor,
    actorType,
    actorIp,
    userAgent,
    requestId,
    source
  };
}

async function setAuditContext(client, context) {
  if (!AUDIT_LOG_ENABLED || !context) {
    return;
  }
  await client.query(
    `
      SELECT
        set_config('app.audit_actor', $1, true),
        set_config('app.audit_actor_type', $2, true),
        set_config('app.audit_actor_ip', $3, true),
        set_config('app.audit_user_agent', $4, true),
        set_config('app.audit_request_id', $5, true),
        set_config('app.audit_source', $6, true)
    `,
    [
      context.actor || '',
      context.actorType || '',
      context.actorIp || '',
      context.userAgent || '',
      context.requestId || '',
      context.source || ''
    ]
  );
}

function safeString(value, fallback = '--') {
  if (value == null) {
    return fallback;
  }
  const text = String(value).trim();
  return text ? text : fallback;
}

function sanitizeFilename(value) {
  const raw = safeString(value, 'report');
  const normalized = raw.normalize('NFKD').replace(/[^\w.-]+/g, '-');
  const trimmed = normalized.replace(/-+/g, '-').replace(/^[-.]+|[-.]+$/g, '');
  return trimmed ? trimmed.slice(0, 80) : 'report';
}

function normalizeStatusKey(value) {
  if (value == null) {
    return null;
  }
  if (typeof value === 'boolean') {
    return value ? 'ok' : 'nok';
  }
  if (typeof value === 'number') {
    if (value === 1) {
      return 'ok';
    }
    if (value === 0) {
      return 'nok';
    }
  }
  const key = String(value).trim().toLowerCase();
  if (STATUS_LABELS[key]) {
    return key;
  }
  return normalizeStatus(value);
}

function summarizeComponents(components) {
  const summary = { ok: 0, nok: 0, other: 0, total: 0 };
  if (!components || typeof components !== 'object' || Array.isArray(components)) {
    return summary;
  }
  Object.entries(components).forEach(([componentKey, value]) => {
    if (componentKey === 'biosLanguage') {
      return;
    }
    const statusKey =
      componentKey === 'biosPassword'
        ? normalizeBiosPasswordStatus(value)
        : componentKey === 'wifiStandard'
          ? normalizeWifiStandardStatus(value)
          : normalizeStatusKey(value);
    if (!statusKey) {
      return;
    }
    if (statusKey === 'ok') {
      summary.ok += 1;
    } else if (statusKey === 'nok') {
      summary.nok += 1;
    } else {
      summary.other += 1;
    }
    summary.total += 1;
  });
  return summary;
}

function addSummaryStatus(summary, statusKey) {
  if (!summary || !statusKey) {
    return;
  }
  if (statusKey === 'ok') {
    summary.ok += 1;
  } else if (statusKey === 'nok') {
    summary.nok += 1;
  } else {
    summary.other += 1;
  }
  summary.total += 1;
}

function normalizeSummaryStatusForKey(key, value) {
  const resolved = resolveComponentStatusDisplay(key, value);
  const statusKey = resolved && resolved.statusKey ? resolved.statusKey : normalizeStatusKey(value);
  if (!statusKey) {
    return null;
  }
  if (statusKey === 'fr' || statusKey === 'en') {
    return 'ok';
  }
  return statusKey;
}

function summarizePdfDetailForReport(components, payload, commentValue = '') {
  const summary = { ok: 0, nok: 0, other: 0, total: 0 };
  const mergedComponents = withManualComponentDefaults(
    components && typeof components === 'object' && !Array.isArray(components) ? components : {}
  );
  const componentKeys = [
    'usb',
    'keyboard',
    'camera',
    'pad',
    'badgeReader',
    'cpu',
    'gpu',
    'biosBattery',
    'biosLanguage',
    'biosPassword',
    'wifiStandard'
  ];

  componentKeys.forEach((key) => {
    const raw = Object.prototype.hasOwnProperty.call(mergedComponents, key)
      ? mergedComponents[key]
      : 'not_tested';
    const normalized = normalizeSummaryStatusForKey(key, raw || 'not_tested');
    if (normalized) {
      addSummaryStatus(summary, normalized);
    }
  });

  const tests =
    payload && payload.tests && typeof payload.tests === 'object' && !Array.isArray(payload.tests)
      ? payload.tests
      : null;
  const diagnosticCandidates = [];
  if (tests) {
    diagnosticCandidates.push(
      tests.diskRead || mergedComponents.diskReadTest || 'not_tested',
      tests.diskWrite || mergedComponents.diskWriteTest || 'not_tested',
      tests.ram || mergedComponents.ramTest || 'not_tested',
      tests.cpu || mergedComponents.cpuTest || 'not_tested',
      tests.gpu || mergedComponents.gpuTest || 'not_tested',
      tests.networkPing || mergedComponents.networkPing || 'not_tested'
    );
    if (tests.fsCheck || mergedComponents.fsCheck) {
      diagnosticCandidates.push(tests.fsCheck || mergedComponents.fsCheck || 'not_tested');
    }
  } else {
    diagnosticCandidates.push(
      mergedComponents.diskReadTest || 'not_tested',
      mergedComponents.diskWriteTest || 'not_tested',
      mergedComponents.ramTest || 'not_tested',
      mergedComponents.cpuTest || 'not_tested',
      mergedComponents.gpuTest || 'not_tested',
      mergedComponents.networkPing || 'not_tested'
    );
    if (mergedComponents.fsCheck) {
      diagnosticCandidates.push(mergedComponents.fsCheck);
    }
  }
  diagnosticCandidates.forEach((value) => {
    const normalized = normalizeStatusKey(value);
    if (normalized) {
      addSummaryStatus(summary, normalized);
    }
  });

  if (typeof commentValue === 'string' && commentValue.trim()) {
    addSummaryStatus(summary, 'nok');
  }

  return summary;
}

function componentLabelFromKey(componentKey) {
  if (!componentKey) {
    return '--';
  }
  return COMPONENT_LABELS[componentKey] || componentKey;
}

function normalizeComponentStatusForAudit(componentKey, value) {
  if (componentKey === 'biosPassword') {
    return normalizeBiosPasswordStatus(value);
  }
  if (componentKey === 'biosLanguage') {
    return normalizeBiosLanguageStatus(value);
  }
  if (componentKey === 'wifiStandard') {
    return normalizeWifiStandardStatus(value);
  }
  return normalizeStatusKey(value);
}

function parseComponentsSnapshotFromAudit(data) {
  if (!data || typeof data !== 'object' || Array.isArray(data)) {
    return {};
  }
  const raw = data.components;
  if (!raw) {
    return {};
  }
  if (typeof raw === 'string') {
    try {
      const parsed = JSON.parse(raw);
      if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) {
        return parsed;
      }
    } catch (error) {
      return {};
    }
    return {};
  }
  if (typeof raw === 'object' && !Array.isArray(raw)) {
    return raw;
  }
  return {};
}

function classifyComponentTransition(fromStatusKey, toStatusKey) {
  const fromIncident = Boolean(fromStatusKey && INCIDENT_STATUS_KEYS.has(fromStatusKey));
  const toIncident = Boolean(toStatusKey && INCIDENT_STATUS_KEYS.has(toStatusKey));
  const corrected = fromIncident && Boolean(toStatusKey && RESOLVED_STATUS_KEYS.has(toStatusKey));
  const regressed = Boolean(fromStatusKey && RESOLVED_STATUS_KEYS.has(fromStatusKey)) && toIncident;
  if (corrected) {
    return {
      fromIncident,
      toIncident,
      corrected: true,
      regressed: false,
      changeType: 'corrected'
    };
  }
  if (regressed) {
    return {
      fromIncident,
      toIncident,
      corrected: false,
      regressed: true,
      changeType: 'regressed'
    };
  }
  return {
    fromIncident,
    toIncident,
    corrected: false,
    regressed: false,
    changeType: 'updated'
  };
}

function extractComponentChangesFromAudit(oldData, newData) {
  const oldComponents = parseComponentsSnapshotFromAudit(oldData);
  const newComponents = parseComponentsSnapshotFromAudit(newData);
  const keys = new Set([
    ...Object.keys(oldComponents || {}),
    ...Object.keys(newComponents || {})
  ]);
  if (!keys.size) {
    return [];
  }

  const rows = [];
  keys.forEach((componentKey) => {
    if (!componentKey || HIDDEN_COMPONENTS.has(componentKey)) {
      return;
    }
    const oldRaw = Object.prototype.hasOwnProperty.call(oldComponents, componentKey)
      ? oldComponents[componentKey]
      : null;
    const newRaw = Object.prototype.hasOwnProperty.call(newComponents, componentKey)
      ? newComponents[componentKey]
      : null;
    const fromStatusKey = normalizeComponentStatusForAudit(componentKey, oldRaw);
    const toStatusKey = normalizeComponentStatusForAudit(componentKey, newRaw);

    if (fromStatusKey === toStatusKey) {
      return;
    }

    const transition = classifyComponentTransition(fromStatusKey, toStatusKey);
    rows.push({
      componentKey,
      componentLabel: componentLabelFromKey(componentKey),
      fromStatus: fromStatusKey || null,
      toStatus: toStatusKey || null,
      fromLabel: fromStatusKey && STATUS_LABELS[fromStatusKey] ? STATUS_LABELS[fromStatusKey] : '--',
      toLabel: toStatusKey && STATUS_LABELS[toStatusKey] ? STATUS_LABELS[toStatusKey] : '--',
      fromIncident: transition.fromIncident,
      toIncident: transition.toIncident,
      corrected: transition.corrected,
      regressed: transition.regressed,
      changeType: transition.changeType
    });
  });

  rows.sort((a, b) => a.componentLabel.localeCompare(b.componentLabel, 'fr'));
  return rows;
}

function formatDateTime(value) {
  if (!value) {
    return '--';
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return '--';
  }
  return date.toLocaleString('fr-FR', {
    day: '2-digit',
    month: 'short',
    year: 'numeric',
    hour: '2-digit',
    minute: '2-digit'
  });
}

function mergeHardwarePayload(primary, fallback) {
  const base =
    primary && typeof primary === 'object' && !Array.isArray(primary) ? { ...primary } : null;
  const fallbackObj =
    fallback && typeof fallback === 'object' && !Array.isArray(fallback) ? fallback : null;

  if (!base && fallbackObj) {
    return fallbackObj;
  }
  if (!base || !fallbackObj) {
    return base || null;
  }

  const keys = ['cpu', 'gpu', 'disks', 'volumes'];
  keys.forEach((key) => {
    if (base[key] == null && fallbackObj[key] != null) {
      base[key] = fallbackObj[key];
    }
  });

  return base;
}

function formatRam(value) {
  if (typeof value !== 'number' || !Number.isFinite(value)) {
    return '--';
  }
  const gb = value / 1024;
  if (gb >= 1) {
    const rounded = gb % 1 === 0 ? gb.toFixed(0) : gb.toFixed(1);
    return `${rounded} Go`;
  }
  return `${Math.round(value)} Mo`;
}

function formatCpuThreads(cpu) {
  if (!cpu || typeof cpu !== 'object') {
    return '--';
  }
  const cores = Number.isFinite(cpu.cores) ? cpu.cores : null;
  const threads = Number.isFinite(cpu.threads) ? cpu.threads : null;
  if (cores == null && threads == null) {
    return '--';
  }
  if (cores != null && threads != null) {
    return `${cores} / ${threads}`;
  }
  if (cores != null) {
    return `${cores} / --`;
  }
  return `-- / ${threads}`;
}

function formatDiskSize(value) {
  if (!Number.isFinite(value)) {
    return null;
  }
  if (value >= 100) {
    return `${Math.round(value)} Go`;
  }
  return `${Math.round(value * 10) / 10} Go`;
}

function formatTotalStorage(disks, volumes) {
  let total = 0;
  if (Array.isArray(disks) && disks.length > 0) {
    total = disks.reduce((sum, disk) => {
      if (disk && Number.isFinite(disk.sizeGb)) {
        return sum + disk.sizeGb;
      }
      return sum;
    }, 0);
  }
  if ((!Number.isFinite(total) || total <= 0) && Array.isArray(volumes)) {
    total = volumes.reduce((sum, vol) => {
      if (vol && Number.isFinite(vol.sizeGb)) {
        return sum + vol.sizeGb;
      }
      return sum;
    }, 0);
  }
  if (!Number.isFinite(total) || total <= 0) {
    return '--';
  }
  return formatDiskSize(total) || '--';
}

function pickPrimaryDisk(disks) {
  if (!Array.isArray(disks) || disks.length === 0) {
    return null;
  }
  const filtered = disks.filter((disk) => {
    if (!disk || typeof disk !== 'object') {
      return false;
    }
    const media = `${disk.mediaType || ''} ${disk.mediaTypeDetail || ''} ${disk.interface || ''}`.toLowerCase();
    if (media.includes('removable') || media.includes('usb')) {
      return false;
    }
    return true;
  });
  return filtered[0] || disks[0] || null;
}

function pickPrimaryVolume(volumes) {
  if (!Array.isArray(volumes) || volumes.length === 0) {
    return null;
  }
  const system = volumes.find((vol) => String(vol.drive || '').toUpperCase() === 'C');
  if (system) {
    return system;
  }
  const sorted = [...volumes].filter((vol) => Number.isFinite(vol.sizeGb));
  sorted.sort((a, b) => (b.sizeGb || 0) - (a.sizeGb || 0));
  return sorted[0] || volumes[0] || null;
}

function formatPrimaryDisk(disks, volumes) {
  const disk = pickPrimaryDisk(disks);
  if (disk) {
    const nameParts = [disk.model, disk.mediaTypeDetail].filter(Boolean);
    const size = formatDiskSize(disk.sizeGb);
    const name = nameParts.length ? nameParts.join(' ') : '--';
    if (size) {
      return `${name} (${size})`;
    }
    return name;
  }
  const volume = pickPrimaryVolume(volumes);
  if (!volume) {
    return '--';
  }
  const drive = volume.drive ? `${volume.drive}:` : 'Volume';
  const size = formatDiskSize(volume.sizeGb);
  const fs = volume.fileSystem ? ` (${volume.fileSystem})` : '';
  if (size) {
    return `${drive} ${size}${fs}`;
  }
  return `${drive}${fs}`;
}

function formatSlots(free, total) {
  const freeValue = typeof free === 'number' && Number.isFinite(free) ? free : null;
  const totalValue = typeof total === 'number' && Number.isFinite(total) ? total : null;
  if (freeValue === null && totalValue === null) {
    return '--';
  }
  if (freeValue !== null && totalValue !== null) {
    return `${freeValue}/${totalValue} libres`;
  }
  if (freeValue !== null) {
    return `${freeValue} libres`;
  }
  return `${totalValue} total`;
}

function formatBatteryHealth(value) {
  if (typeof value !== 'number' || !Number.isFinite(value)) {
    return '--';
  }
  return `${Math.round(value)}%`;
}

function normalizeWifiStandardCode(value) {
  if (value == null) {
    return null;
  }
  const raw = String(value).trim().toLowerCase();
  if (!raw) {
    return null;
  }
  const cleaned = raw.normalize('NFD').replace(/[\u0300-\u036f]/g, '');
  const match = cleaned.match(/(?:802(?:[.\s-])?)?11\s*(be|ax|ac|n|g|a|b)\b/i);
  if (match && match[1]) {
    return `802.11${match[1].toLowerCase()}`;
  }
  if (/\bwi-?fi\s*7\b/.test(cleaned)) return '802.11be';
  if (/\bwi-?fi\s*6(?:e)?\b/.test(cleaned)) return '802.11ax';
  if (/\bwi-?fi\s*5\b/.test(cleaned)) return '802.11ac';
  if (/\bwi-?fi\s*4\b/.test(cleaned)) return '802.11n';
  if (/\bwi-?fi\s*3\b/.test(cleaned)) return '802.11g';
  if (/\bwi-?fi\s*2\b/.test(cleaned)) return '802.11a';
  if (/\bwi-?fi\s*1\b/.test(cleaned)) return '802.11b';
  return null;
}

function pickBestWifiStandard(standards) {
  if (!Array.isArray(standards) || standards.length === 0) {
    return null;
  }
  const rank = {
    '802.11be': 7,
    '802.11ax': 6,
    '802.11ac': 5,
    '802.11n': 4,
    '802.11g': 3,
    '802.11a': 2,
    '802.11b': 1
  };
  let best = null;
  let bestRank = 0;
  for (const standard of new Set(standards)) {
    const currentRank = rank[standard] || 0;
    if (currentRank > bestRank) {
      best = standard;
      bestRank = currentRank;
    }
  }
  return best;
}

function buildPdfWifiStandardCode(payload) {
  if (!payload || typeof payload !== 'object') {
    return null;
  }
  const payloadWifi =
    payload.wifi && typeof payload.wifi === 'object' && !Array.isArray(payload.wifi)
      ? payload.wifi
      : null;
  const candidates = [];
  if (payloadWifi && Array.isArray(payloadWifi.standards)) {
    candidates.push(...payloadWifi.standards);
  }
  if (payloadWifi && Object.prototype.hasOwnProperty.call(payloadWifi, 'standard')) {
    candidates.push(payloadWifi.standard);
  }
  if (Object.prototype.hasOwnProperty.call(payload, 'wifiStandard')) {
    candidates.push(payload.wifiStandard);
  }
  const normalizedStandards = candidates
    .map((value) => normalizeWifiStandardCode(value))
    .filter(Boolean);
  return pickBestWifiStandard(normalizedStandards);
}

function parseMetricNumber(value) {
  if (typeof value === 'number' && Number.isFinite(value)) {
    return value;
  }
  if (typeof value === 'string') {
    const normalized = value.trim().replace(',', '.');
    if (!normalized) {
      return null;
    }
    const numeric = Number(normalized);
    if (Number.isFinite(numeric)) {
      return numeric;
    }
  }
  return null;
}

function formatMetric(value, unit) {
  const numeric = parseMetricNumber(value);
  if (numeric == null) {
    return null;
  }
  const rounded = numeric % 1 === 0 ? numeric.toFixed(0) : numeric.toFixed(1);
  return `${rounded} ${unit}`;
}

function formatMbps(value) {
  return formatMetric(value, 'MB/s');
}

function formatScore(value) {
  if (typeof value !== 'number' || !Number.isFinite(value)) {
    return null;
  }
  const rounded = value % 1 === 0 ? value.toFixed(0) : value.toFixed(1);
  return `Score ${rounded}`;
}

function formatWinSatNote(score) {
  if (typeof score !== 'number' || !Number.isFinite(score)) {
    return null;
  }
  if (score < 3.0) return 'Horrible';
  if (score < 4.5) return 'Mauvais';
  if (score < 6.0) return 'Moyen';
  if (score < 7.5) return 'Bon';
  return 'Excellent';
}

function buildDiagnosticsRows(payload, components = null) {
  const rows = [];
  const tests =
    payload && payload.tests && typeof payload.tests === 'object' && !Array.isArray(payload.tests)
      ? payload.tests
      : null;
  const componentMap =
    components && typeof components === 'object' && !Array.isArray(components) ? components : {};
  const winSat =
    payload && payload.winsat && typeof payload.winsat === 'object' ? payload.winsat : null;
  const winSpr =
    winSat && winSat.winSPR && typeof winSat.winSPR === 'object' ? winSat.winSPR : null;
  const winSatCpuScore = winSpr && typeof winSpr.CpuScore === 'number' ? winSpr.CpuScore : null;
  const winSatMemScore =
    winSpr && typeof winSpr.MemoryScore === 'number' ? winSpr.MemoryScore : null;
  const winSatGraphicsScore = winSpr
    ? typeof winSpr.GamingScore === 'number'
      ? winSpr.GamingScore
      : typeof winSpr.GraphicsScore === 'number'
        ? winSpr.GraphicsScore
        : null
    : null;
  const pickStatus = (...values) => {
    for (const value of values) {
      if (value !== undefined && value !== null && value !== '') {
        return value;
      }
    }
    return null;
  };

  const addRow = (label, status, extra) => {
    if (status == null && !extra) {
      return;
    }
    rows.push({ label, status, extra });
  };

  const diskReadStatus = pickStatus(tests && tests.diskRead, componentMap.diskReadTest, 'not_tested');
  const diskWriteStatus = pickStatus(tests && tests.diskWrite, componentMap.diskWriteTest, 'not_tested');
  const ramStatus = pickStatus(tests && (tests.ramTest || tests.ram), componentMap.ramTest, 'not_tested');
  const cpuStatus = pickStatus(tests && (tests.cpuTest || tests.cpu), componentMap.cpuTest, 'not_tested');
  const gpuStatus = pickStatus(tests && (tests.gpuTest || tests.gpu), componentMap.gpuTest, 'not_tested');
  const pingStatus = pickStatus(tests && tests.networkPing, componentMap.networkPing, 'not_tested');
  const ramNote = (tests && tests.ramNote) || formatWinSatNote(winSatMemScore);
  const cpuNote = (tests && tests.cpuNote) || formatWinSatNote(winSatCpuScore);
  const gpuScoreSource = tests ? tests.gpuScore : null;
  const gpuNote =
    (tests && tests.gpuNote) ||
    formatWinSatNote(winSatGraphicsScore != null ? winSatGraphicsScore : gpuScoreSource);
  const diskReadMetric = formatMbps(
    tests && tests.diskReadMBps != null
      ? tests.diskReadMBps
      : winSat && winSat.disk && winSat.disk.seqReadMBps != null
        ? winSat.disk.seqReadMBps
        : null
  );
  const diskWriteMetric = formatMbps(
    tests && tests.diskWriteMBps != null
      ? tests.diskWriteMBps
      : winSat && winSat.disk && winSat.disk.seqWriteMBps != null
        ? winSat.disk.seqWriteMBps
        : null
  );
  addRow('Lecture disque', diskReadStatus, diskReadMetric);
  addRow('Ecriture disque', diskWriteStatus, diskWriteMetric);
  addRow('RAM (WinSAT)', ramStatus, ramNote || (tests ? formatMbps(tests.ramMBps) : null));
  addRow('CPU (WinSAT)', cpuStatus, cpuNote || (tests ? formatMbps(tests.cpuMBps) : null));
  addRow('GPU (WinSAT)', gpuStatus, gpuNote || (tests ? formatScore(tests.gpuScore) : null));
  if (tests) {
    addRow('CPU (stress)', tests.cpuStress, null);
    addRow('GPU (stress)', tests.gpuStress, null);
  }
  addRow('Ping', pingStatus, tests ? tests.networkPingTarget || null : null);
  const fsCheckStatus = pickStatus(tests && tests.fsCheck, componentMap.fsCheck, null);
  addRow('Check disque', fsCheckStatus, null);

  return rows;
}

function buildInventoryRows(payload) {
  const inventory =
    payload && payload.inventory && typeof payload.inventory === 'object' ? payload.inventory : null;
  if (!inventory) {
    return [];
  }

  const rows = [];

  const baseboard =
    inventory.baseboard && typeof inventory.baseboard === 'object' ? inventory.baseboard : null;
  const baseboardSerial = baseboard && baseboard.serialNumber ? String(baseboard.serialNumber) : '';
  if (baseboardSerial) {
    rows.push({ label: 'Carte mere', value: baseboardSerial });
  }

  const batteryRaw = inventory.battery;
  const batteryList = Array.isArray(batteryRaw) ? batteryRaw : batteryRaw ? [batteryRaw] : [];
  const batteryValues = batteryList
    .map((item) => {
      if (!item || typeof item !== 'object') {
        return '';
      }
      const serial = item.serialNumber ? String(item.serialNumber).trim() : '';
      const deviceId = item.deviceId ? String(item.deviceId).trim() : '';
      return serial || deviceId || '';
    })
    .filter(Boolean);
  if (batteryValues.length) {
    rows.push({ label: 'Batterie', value: batteryValues.join(' • ') });
  }

  const disksRaw = inventory.disks;
  const diskList = Array.isArray(disksRaw) ? disksRaw : disksRaw ? [disksRaw] : [];
  const diskValues = diskList
    .map((item) => {
      if (!item || typeof item !== 'object') {
        return '';
      }
      const serial = item.serialNumber ? String(item.serialNumber).trim() : '';
      const tag = item.tag ? String(item.tag).trim() : '';
      if (serial && tag) {
        return `${serial} (${tag})`;
      }
      return serial || tag || '';
    })
    .filter(Boolean);
  if (diskValues.length) {
    rows.push({ label: 'Disques', value: diskValues.join(' • ') });
  }

  const memoryRaw = inventory.memory;
  const memoryList = Array.isArray(memoryRaw) ? memoryRaw : memoryRaw ? [memoryRaw] : [];
  const memoryValues = memoryList
    .map((item) => {
      if (!item || typeof item !== 'object') {
        return '';
      }
      const serial = item.serialNumber ? String(item.serialNumber).trim() : '';
      const bank = item.bankLabel ? String(item.bankLabel).trim() : '';
      if (serial && bank) {
        return `${bank}: ${serial}`;
      }
      return serial || '';
    })
    .filter(Boolean);
  if (memoryValues.length) {
    rows.push({ label: 'RAM', value: memoryValues.join(' • ') });
  }

  return rows;
}

function buildComponentRows(components) {
  const source = withManualComponentDefaults(
    components && typeof components === 'object' && !Array.isArray(components) ? components : null
  );
  const entries = Object.entries(source).filter(([key]) => !HIDDEN_COMPONENTS.has(key));
  const orderMap = new Map(COMPONENT_ORDER.map((key, index) => [key, index]));
  return entries
    .sort((a, b) => {
      const orderA = orderMap.has(a[0]) ? orderMap.get(a[0]) : 999;
      const orderB = orderMap.has(b[0]) ? orderMap.get(b[0]) : 999;
      if (orderA !== orderB) {
        return orderA - orderB;
      }
      return a[0].localeCompare(b[0], 'fr');
    })
    .map(([key, value]) => ({
      key,
      label: COMPONENT_LABELS[key] || key,
      status: value
    }));
}

function resolveComponentStatusDisplay(key, value) {
  if (key === 'biosLanguage') {
    const lang = normalizeBiosLanguage(value);
    if (lang === 'fr') {
      return { statusKey: 'fr', statusLabel: 'FR' };
    }
    if (lang === 'en') {
      return { statusKey: 'en', statusLabel: 'EN' };
    }
    return { statusKey: 'not_tested', statusLabel: STATUS_LABELS.not_tested };
  }

  if (key === 'biosPassword') {
    const normalized = normalizeBiosPasswordStatus(value);
    if (normalized === 'ok') {
      return { statusKey: 'ok', statusLabel: 'Non' };
    }
    if (normalized === 'nok') {
      return { statusKey: 'nok', statusLabel: 'Oui' };
    }
    return { statusKey: 'not_tested', statusLabel: STATUS_LABELS.not_tested };
  }

  if (key === 'wifiStandard') {
    const normalized = normalizeWifiStandardStatus(value);
    if (normalized) {
      return {
        statusKey: normalized,
        statusLabel: STATUS_LABELS[normalized] || STATUS_LABELS.unknown
      };
    }
    return { statusKey: 'not_tested', statusLabel: STATUS_LABELS.not_tested };
  }

  const normalized = normalizeStatusKey(value) || 'unknown';
  return { statusKey: normalized, statusLabel: STATUS_LABELS[normalized] || STATUS_LABELS.unknown };
}

function ensureSpace(doc, height) {
  const bottom = doc.page.height - doc.page.margins.bottom;
  if (doc.y + height > bottom) {
    doc.addPage();
  }
}

function ensureNewPage(doc) {
  const top = doc.page.margins.top;
  if (doc.y > top + 6) {
    doc.addPage();
  }
  doc.x = doc.page.margins.left;
}

function drawPill(doc, x, y, text, styles, fontSize = 9) {
  const paddingX = 6;
  const paddingY = 2;
  doc.save();
  doc.fontSize(fontSize);
  const textWidth = doc.widthOfString(text);
  const width = textWidth + paddingX * 2;
  const height = fontSize + paddingY * 2 + 2;
  doc.roundedRect(x, y - paddingY, width, height, height / 2).fill(styles.background);
  doc.fillColor(styles.color).text(text, x + paddingX, y, { lineBreak: false });
  doc.restore();
  return width;
}

function drawSectionTitle(doc, title) {
  const margin = doc.page.margins.left;
  const width = doc.page.width - margin - doc.page.margins.right;
  ensureSpace(doc, 32);
  doc.fontSize(12).fillColor('#1D211F').text(title, margin, doc.y, { width });
  doc.moveDown(0.2);
  doc
    .moveTo(margin, doc.y)
    .lineTo(margin + width, doc.y)
    .strokeColor('#E0E0E0')
    .stroke();
  doc.moveDown(0.9);
  doc.x = margin;
}

function drawKeyValueGrid(doc, rows, columns = 2) {
  const margin = doc.page.margins.left;
  const width = doc.page.width - margin - doc.page.margins.right;
  const colWidth = width / columns;
  const rowHeight = 32;
  const totalRows = Math.ceil(rows.length / columns);
  ensureSpace(doc, totalRows * rowHeight + 8);
  const startY = doc.y;

  rows.forEach((row, index) => {
    const col = index % columns;
    const rowIndex = Math.floor(index / columns);
    const x = margin + col * colWidth;
    const y = startY + rowIndex * rowHeight;
    doc.fontSize(8).fillColor('#6B6F6C').text(row.label.toUpperCase(), x, y);
    doc.fontSize(10).fillColor('#1D211F').text(safeString(row.value), x, y + 10, {
      width: colWidth - 8
    });
  });

  doc.y = startY + totalRows * rowHeight + 10;
  doc.x = margin;
}

function drawStatusRows(doc, rows) {
  const margin = doc.page.margins.left;
  const width = doc.page.width - margin - doc.page.margins.right;
  const rowHeight = 20;
  rows.forEach((row) => {
    ensureSpace(doc, rowHeight + 8);
    const y = doc.y;
    const { statusKey, statusLabel } = resolveComponentStatusDisplay(row.key, row.status);
    const extraText = row.extra ? ` ${row.extra}` : '';
    const pillText = `${statusLabel}${extraText}`.trim();
    doc.fontSize(10).fillColor('#1D211F').text(row.label, margin, y, {
      width: width * 0.62
    });
    doc.fontSize(9);
    const pillWidth = doc.widthOfString(pillText) + 12;
    const pillX = margin + width - pillWidth;
    drawPill(doc, pillX, y, pillText, STATUS_STYLES[statusKey] || STATUS_STYLES.unknown);
    doc.y = y + rowHeight;
    doc.moveDown(0.4);
    doc.x = margin;
  });
}

function truncatePdfText(value, maxLength = 64) {
  const text = safeString(value, '--');
  if (text.length <= maxLength) {
    return text;
  }
  return `${text.slice(0, Math.max(1, maxLength - 1)).trimEnd()}…`;
}

function clampNumber(value, min, max) {
  if (!Number.isFinite(value)) {
    return min;
  }
  return Math.min(max, Math.max(min, value));
}

function drawPdfCard(doc, x, y, width, height, title, options = {}) {
  const radius = Number.isFinite(options.radius) ? options.radius : 12;
  const background = options.background || '#FFFFFF';
  const border = options.border || '#D7E3F0';
  const variant = options.variant || 'card';
  const titleColor = options.titleColor || '#5D7289';
  const titleSize = Number.isFinite(options.titleSize) ? options.titleSize : 7;
  const paddingX = Number.isFinite(options.paddingX) ? options.paddingX : 14;
  const titleOffsetY = Number.isFinite(options.titleOffsetY) ? options.titleOffsetY : 12;
  const bodyTopOffset = Number.isFinite(options.bodyTopOffset) ? options.bodyTopOffset : 27;
  const bodyBottomInset = Number.isFinite(options.bodyBottomInset) ? options.bodyBottomInset : 10;

  doc.save();
  if (variant === 'section') {
    if (background && background !== 'transparent') {
      doc.roundedRect(x, y, width, height, radius).fill(background);
    }
    const lineY = y + titleOffsetY + 10;
    doc
      .lineWidth(1)
      .strokeColor(border)
      .moveTo(x + paddingX, lineY)
      .lineTo(x + width - paddingX, lineY)
      .stroke();
  } else {
    doc.roundedRect(x, y, width, height, radius).fillAndStroke(background, border);
  }
  doc
    .fillColor(titleColor)
    .font('Helvetica-Bold')
    .fontSize(titleSize)
    .text(String(title || '').toUpperCase(), x + paddingX, y + titleOffsetY, { lineBreak: false });
  doc.restore();
  return {
    x: x + paddingX,
    y: y + bodyTopOffset,
    width: Math.max(8, width - paddingX * 2),
    height: Math.max(8, height - bodyTopOffset - bodyBottomInset)
  };
}

function drawCompactKeyValueCard(doc, x, y, width, height, title, rows, columns = 1, options = {}) {
  const body = drawPdfCard(doc, x, y, width, height, title, options.card || {});
  const entries = Array.isArray(rows) ? rows.filter((row) => row && row.label) : [];
  const labelColor = options.labelColor || '#667A90';
  const valueColor = options.valueColor || '#1A2A3A';
  if (!entries.length) {
    doc.font('Helvetica').fontSize(8).fillColor('#7A8590').text('Aucune donnee.', body.x, body.y + 2, {
      width: body.width
    });
    return;
  }

  if (columns <= 1) {
    const rowHeight = Math.max(7.3, body.height / entries.length);
    const labelFontSize = clampNumber(rowHeight * 0.42, 5.8, 7.8);
    const valueFontSize = clampNumber(rowHeight * 0.54, 6.4, 9.4);
    const labelWidth = clampNumber(body.width * 0.3, 74, 126);
    const valueWidth = Math.max(20, body.width - labelWidth - 4);
    const valueMaxLength = Math.max(12, Math.floor((valueWidth / Math.max(valueFontSize - 1.2, 4)) * 1.55));
    entries.forEach((row, index) => {
      const lineY = body.y + index * rowHeight;
      const label = truncatePdfText(row.label, 26).toUpperCase();
      const value = truncatePdfText(row.value, valueMaxLength);
      doc.font('Helvetica-Bold').fontSize(labelFontSize).fillColor(labelColor).text(label, body.x, lineY + 1.5, {
        width: labelWidth,
        lineBreak: false
      });
      doc.font('Helvetica').fontSize(valueFontSize).fillColor(valueColor).text(value, body.x + labelWidth, lineY, {
        width: valueWidth,
        lineBreak: false
      });
    });
    return;
  }

  const gridColumns = Math.max(1, Math.floor(columns));
  const maxRowsPerColumn = Math.max(1, Math.ceil(entries.length / gridColumns));
  const rowHeight = Math.max(7.6, body.height / maxRowsPerColumn);
  const colWidth = body.width / gridColumns;
  const labelFontSize = clampNumber(rowHeight * 0.34, 5.4, 7);
  const valueFontSize = clampNumber(rowHeight * 0.44, 6, 8.2);
  const labelMaxLength = Math.max(12, Math.floor((colWidth - 8) / Math.max(labelFontSize - 1, 4)));
  const valueMaxLength = Math.max(12, Math.floor((colWidth - 8) / Math.max(valueFontSize - 1.2, 4) * 1.35));

  entries.forEach((row, index) => {
    const col = Math.floor(index / maxRowsPerColumn);
    const rowIndex = index % maxRowsPerColumn;
    const cellX = body.x + col * colWidth;
    const cellY = body.y + rowIndex * rowHeight;
    doc.font('Helvetica-Bold').fontSize(labelFontSize).fillColor(labelColor).text(truncatePdfText(row.label, labelMaxLength).toUpperCase(), cellX, cellY + 0.6, {
      width: colWidth - 8,
      lineBreak: false
    });
    doc.font('Helvetica').fontSize(valueFontSize).fillColor(valueColor).text(truncatePdfText(row.value, valueMaxLength), cellX, cellY + rowHeight * 0.46, {
      width: colWidth - 8,
      lineBreak: false
    });
  });
}

function drawCompactStatusCard(doc, x, y, width, height, title, rows, options = {}) {
  const body = drawPdfCard(doc, x, y, width, height, title, options.card || {});
  const entries = Array.isArray(rows) ? rows.filter((row) => row && row.label) : [];
  const textColor = options.textColor || '#1A2A3A';
  if (!entries.length) {
    doc.font('Helvetica').fontSize(8).fillColor('#7A8590').text('Aucune donnee.', body.x, body.y + 2, {
      width: body.width
    });
    return;
  }

  const rowHeight = Math.max(7.8, body.height / entries.length);
  const labelFontSize = clampNumber(rowHeight * 0.5, 6.4, 8.8);
  const pillFontSize = clampNumber(rowHeight * 0.44, 6.1, 8.2);
  const labelWidth = body.width * (rowHeight < 11 ? 0.55 : 0.62);
  const labelMaxLength = Math.max(12, Math.floor((labelWidth / Math.max(labelFontSize - 1.2, 4)) * 1.45));
  entries.forEach((row, index) => {
    const lineY = body.y + index * rowHeight;
    const { statusKey, statusLabel } = resolveComponentStatusDisplay(row.key, row.status);
    const extraText = row.extra ? truncatePdfText(String(row.extra), 16) : '';
    let pillText = truncatePdfText(`${statusLabel}`.trim(), 16);
    doc.font('Helvetica').fontSize(labelFontSize).fillColor(textColor).text(truncatePdfText(row.label, labelMaxLength), body.x, lineY + 1.4, {
      width: labelWidth
    });
    doc.font('Helvetica').fontSize(pillFontSize);
    let pillWidth = doc.widthOfString(pillText) + 12;
    if (pillWidth > body.width * 0.44) {
      pillText = truncatePdfText(pillText, 10);
      pillWidth = doc.widthOfString(pillText) + 12;
    }
    const pillX = body.x + body.width - pillWidth;
    if (extraText) {
      const extraX = body.x + labelWidth + 4;
      const extraWidth = pillX - extraX - 4;
      if (extraWidth > 16) {
        const extraFontSize = clampNumber(pillFontSize - 0.2, 5.8, 7.6);
        doc.font('Helvetica').fontSize(extraFontSize).fillColor('#5D6B78').text(extraText, extraX, lineY + 1.8, {
          width: extraWidth,
          align: 'right',
          lineBreak: false
        });
      }
    }
    drawPill(doc, pillX, lineY + 1, pillText, STATUS_STYLES[statusKey] || STATUS_STYLES.unknown, pillFontSize);
  });
}

function mergeStatusCandidates(values) {
  const normalized = (Array.isArray(values) ? values : [])
    .map((value) => normalizeStatusKey(value))
    .filter(Boolean);
  if (!normalized.length) {
    return 'not_tested';
  }
  if (normalized.includes('nok')) {
    return 'nok';
  }
  if (normalized.includes('ok')) {
    return 'ok';
  }
  if (normalized.includes('fr')) {
    return 'fr';
  }
  if (normalized.includes('en')) {
    return 'en';
  }
  if (normalized.includes('not_tested')) {
    return 'not_tested';
  }
  return normalized[0] || 'unknown';
}

function drawSummaryCards(doc, x, y, width, cards, palette) {
  const items = Array.isArray(cards) ? cards.filter((card) => card && card.label) : [];
  if (!items.length) {
    return 0;
  }
  const gap = 8;
  const cardWidth = (width - gap * (items.length - 1)) / items.length;
  const cardHeight = 36;
  items.forEach((card, index) => {
    const cardX = x + index * (cardWidth + gap);
    const tones = card.tones || {};
    doc
      .roundedRect(cardX, y, cardWidth, cardHeight, 9)
      .fillAndStroke(tones.background || '#EAF1F7', tones.border || '#BFD4E2');
    doc
      .font('Helvetica-Bold')
      .fontSize(7)
      .fillColor(tones.labelColor || '#4D687C')
      .text(String(card.label || '').toUpperCase(), cardX + 9, y + 6, {
        width: cardWidth - 18,
        lineBreak: false
      });
    doc
      .font('Helvetica-Bold')
      .fontSize(11)
      .fillColor(tones.valueColor || palette.cardValue)
      .text(safeString(card.value, '--'), cardX + 9, y + 18, {
        width: cardWidth - 18,
        lineBreak: false
      });
  });
  return cardHeight;
}

function drawSectionTable(doc, x, y, width, height, title, rows, options = {}) {
  const body = drawPdfCard(doc, x, y, width, height, title, options.card || {});
  const entries = Array.isArray(rows) ? rows.filter((row) => row && row.label) : [];
  if (!entries.length) {
    doc.font('Helvetica').fontSize(8).fillColor('#7A8590').text('Aucune donnee.', body.x, body.y + 1, {
      width: body.width
    });
    return;
  }

  const labelWidth = clampNumber(
    Number.isFinite(options.labelWidth) ? options.labelWidth : body.width * 0.34,
    68,
    Math.max(72, body.width * 0.5)
  );
  const valueX = body.x + labelWidth + 6;
  const valueWidth = Math.max(24, body.width - labelWidth - 6);
  const labelSize = Number.isFinite(options.labelSize) ? options.labelSize : 7.2;
  const valueSize = Number.isFinite(options.valueSize) ? options.valueSize : 8.2;
  const labelColor = options.labelColor || '#5A768C';
  const valueColor = options.valueColor || '#10273A';
  const separator = options.separator || '#D8E5EE';
  const minLineHeight = Number.isFinite(options.minLineHeight) ? options.minLineHeight : 11;
  const maxY = body.y + body.height;
  let cursorY = body.y;

  entries.forEach((row) => {
    const label = safeString(row.label, '--').toUpperCase();
    const value = safeString(row.value, '--');
    doc.font('Helvetica').fontSize(valueSize);
    const valueHeight = doc.heightOfString(value, {
      width: valueWidth,
      lineGap: 0.5
    });
    const lineHeight = Math.max(minLineHeight, valueHeight + 1.4);
    if (cursorY + lineHeight > maxY) {
      return;
    }
    doc
      .font('Helvetica-Bold')
      .fontSize(labelSize)
      .fillColor(labelColor)
      .text(label, body.x, cursorY + 1, {
        width: labelWidth,
        lineBreak: false
      });
    doc.font('Helvetica').fontSize(valueSize).fillColor(valueColor).text(value, valueX, cursorY, {
      width: valueWidth,
      lineGap: 0.5
    });
    cursorY += lineHeight + 1.2;
    if (cursorY + 1 <= maxY) {
      doc
        .lineWidth(0.6)
        .strokeColor(separator)
        .moveTo(body.x, cursorY)
        .lineTo(body.x + body.width, cursorY)
        .stroke();
      cursorY += 1.2;
    }
  });
}

function drawProductSheet(doc, x, y, width, height, data, palette) {
  const body = drawPdfCard(doc, x, y, width, height, 'Fiche produit matériel clé', {
    variant: 'section',
    background: 'transparent',
    border: palette.cardBorder,
    titleColor: palette.cardTitle,
    titleOffsetY: 2,
    bodyTopOffset: 17,
    bodyBottomInset: 2,
    paddingX: 12
  });
  const maxY = body.y + body.height;
  const modelText = safeString(data.subtitle, '--');
  doc.font('Helvetica-Bold').fontSize(11).fillColor(palette.cardValue).text(modelText, body.x, body.y, {
    width: body.width
  });
  doc.font('Helvetica').fontSize(8).fillColor('#5E7A8D').text('Configuration principale', body.x, body.y + 14, {
    width: body.width,
    lineBreak: false
  });

  const badgeStyle = { background: '#E3F2F9', color: '#1F4E69' };
  const badgeValues = [
    `RAM ${safeString(data.ramTotal, '--')}`,
    `Slots ${safeString(data.ramSlots, '--')}`,
    `Stockage ${safeString(data.storageTotal, '--')}`,
    `Batterie ${safeString(data.batteryHealth, '--')}`
  ];
  let badgeX = body.x;
  let badgeY = body.y + 24;
  const badgeRight = body.x + body.width;
  badgeValues.forEach((badge) => {
    doc.font('Helvetica').fontSize(7.4);
    const measuredWidth = doc.widthOfString(badge) + 12;
    if (badgeX + measuredWidth > badgeRight) {
      badgeX = body.x;
      badgeY += 13;
    }
    drawPill(doc, badgeX, badgeY, badge, badgeStyle, 7.4);
    badgeX += measuredWidth + 6;
  });

  const points = [
    `CPU: ${safeString(data.cpuName, '--')}`,
    `GPU: ${safeString(data.gpuName, '--')}`,
    `Disque principal: ${safeString(data.storagePrimary, '--')}`,
    `Technicien: ${safeString(data.technician, '--')}`
  ];
  let pointY = badgeY + 15;
  points.forEach((point) => {
    if (pointY + 12 > maxY) {
      return;
    }
    doc.circle(body.x + 2, pointY + 4.5, 1.2).fill('#39BFD0');
    doc
      .font('Helvetica')
      .fontSize(7.8)
      .fillColor('#27475F')
      .text(point, body.x + 8, pointY, {
        width: body.width - 8
      });
    pointY = doc.y + 1.2;
  });
}

function drawReportPdf(doc, data) {
  const margin = doc.page.margins.left;
  const width = doc.page.width - margin - doc.page.margins.right;
  const pageBottom = doc.page.height - doc.page.margins.bottom;
  const pageWidth = doc.page.width;
  const pageHeight = doc.page.height;
  const palette = {
    pageBg: '#F7FBFE',
    headerBg: '#EAF6FC',
    headerBorder: '#B8D9E8',
    headerText: '#0F2A3B',
    headerSubText: '#355C71',
    headerStripe: '#2ADAE6',
    headerGlow: '#3DDCE8',
    accentPillBg: '#155878',
    accentPillText: '#E4F9FF',
    brandText: '#1F708C',
    title: '#102B3C',
    subtitle: '#3C667D',
    cardBorder: '#C9DFEA',
    cardTitle: '#296D89',
    cardLabel: '#5C7890',
    cardValue: '#10273A',
    summaryNeutralBg: '#EAF2F8',
    summaryNeutralBorder: '#C6D9E5',
    summaryNeutralText: '#2C4C61',
    summaryOkBg: '#E1F5E8',
    summaryOkBorder: '#B9DEC8',
    summaryOkText: '#205C43',
    summaryNokBg: '#FBE8E6',
    summaryNokBorder: '#E8C1BC',
    summaryNokText: '#8D2B20',
    summaryNtBg: '#EDF0F4',
    summaryNtBorder: '#CED6DF',
    summaryNtText: '#4F6276',
    footerText: '#5D7B91'
  };

  doc.rect(0, 0, pageWidth, pageHeight).fill(palette.pageBg);

  const headerY = 18;
  const headerHeight = 76;
  const badgeWidth = 124;
  const badgeX = margin + width - badgeWidth - 12;
  const logoExists = fs.existsSync(BRAND_LOGO_PATH);
  const logoSize = 56;
  const logoX = margin + 8;
  const logoY = headerY + 10;
  const titleX = logoExists ? logoX + logoSize + 12 : margin + 12;
  const titleWidth = Math.max(120, badgeX - titleX - 12);

  doc.save();
  const headerWash = doc.linearGradient(margin, headerY, margin + width, headerY + headerHeight);
  headerWash.stop(0, palette.headerBg, 0.88);
  headerWash.stop(1, '#FFFFFF', 0.68);
  doc.roundedRect(margin, headerY, width, headerHeight, 11).fillAndStroke(headerWash, palette.headerBorder);
  doc
    .lineWidth(2.2)
    .strokeColor(palette.headerStripe)
    .moveTo(margin + 6, headerY + headerHeight)
    .lineTo(margin + width - 6, headerY + headerHeight)
    .stroke();
  doc.roundedRect(badgeX, headerY + 10, badgeWidth, 24, 12).fillAndStroke(palette.accentPillBg, '#35B7D0');
  doc.save();
  doc.fillOpacity(0.18);
  doc.circle(logoX + logoSize / 2, logoY + logoSize / 2, logoSize / 2 + 6).fill(palette.headerGlow);
  doc.restore();

  if (logoExists) {
    try {
      doc.image(BRAND_LOGO_PATH, logoX, logoY, { fit: [logoSize, logoSize], align: 'left', valign: 'top' });
    } catch (error) {
      doc.fillColor(palette.headerText).font('Helvetica-Bold').fontSize(8).text('MMA AUTOMATION', margin + 16, headerY + 16, {
        lineBreak: false
      });
    }
  } else {
    doc.fillColor(palette.headerText).font('Helvetica-Bold').fontSize(8).text('MMA AUTOMATION', margin + 16, headerY + 16, {
      lineBreak: false
    });
  }

  doc.fillColor(palette.brandText).font('Helvetica-Bold').fontSize(7.2).text('MMA AUTOMATION', titleX, headerY + 9, {
    width: titleWidth,
    lineBreak: false
  });
  doc.fillColor(palette.title).font('Helvetica-Bold').fontSize(14.6).text(`Nom du poste: ${truncatePdfText(data.title, 46)}`, titleX, headerY + 22, {
    width: titleWidth
  });
  doc.fillColor(palette.accentPillText).font('Helvetica-Bold').fontSize(7.8).text('Rapport machine', badgeX, headerY + 16, {
    width: badgeWidth,
    align: 'center'
  });
  doc
    .fillColor(palette.headerSubText)
    .font('Helvetica-Bold')
    .fontSize(7.1)
    .text('Modèle PC', badgeX, headerY + 40, {
      width: badgeWidth,
      align: 'right',
      lineBreak: false
    });
  doc.fillColor(palette.headerText).font('Helvetica').fontSize(7.8).text(truncatePdfText(data.subtitle, 34), badgeX, headerY + 49, {
    width: badgeWidth,
    align: 'right'
  });
  doc.fillColor(palette.headerSubText).font('Helvetica').fontSize(7.6).text(`Généré: ${truncatePdfText(data.generatedAt, 32)}`, badgeX, headerY + 61, {
    width: badgeWidth,
    align: 'right',
    lineBreak: false
  });
  doc.restore();

  const diagnosticsRaw = Array.isArray(data.diagnostics) ? data.diagnostics : [];
  const normalizeDiagLabel = (label) => {
    const clean = safeString(label, '');
    const lowered = clean.toLowerCase();
    if (lowered === 'check disque' || lowered === 'check disk') {
      return 'Check disk';
    }
    return clean;
  };
  const diagnosticsMap = new Map();
  diagnosticsRaw.forEach((row) => {
    if (!row || !row.label) {
      return;
    }
    const normalizedLabel = normalizeDiagLabel(row.label);
    if (!normalizedLabel || diagnosticsMap.has(normalizedLabel)) {
      return;
    }
    diagnosticsMap.set(normalizedLabel, { ...row, label: normalizedLabel });
  });
  const diagnosticsOrder = [
    'Lecture disque',
    'Ecriture disque',
    'RAM (WinSAT)',
    'CPU (WinSAT)',
    'GPU (WinSAT)',
    'Ping',
    'Check disk'
  ];
  const diagnosticsRows = diagnosticsOrder
    .map((label) => diagnosticsMap.get(label))
    .filter(Boolean);

  const componentRawRows = Array.isArray(data.components) ? data.components : [];
  const componentMap = new Map();
  componentRawRows.forEach((row) => {
    if (!row || !row.key || componentMap.has(row.key)) {
      return;
    }
    componentMap.set(row.key, row.status);
  });
  const diagnosticStatus = (label) => {
    const row = diagnosticsMap.get(label);
    return row ? row.status : null;
  };
  const componentStatus = (key) => (componentMap.has(key) ? componentMap.get(key) : null);
  const diskStatus = mergeStatusCandidates([
    componentStatus('fsCheck'),
    componentStatus('diskReadTest'),
    componentStatus('diskWriteTest'),
    diagnosticStatus('Check disk'),
    diagnosticStatus('Lecture disque'),
    diagnosticStatus('Ecriture disque')
  ]);
  const cpuStatusUnified = mergeStatusCandidates([
    componentStatus('cpu'),
    componentStatus('cpuTest'),
    diagnosticStatus('CPU (WinSAT)')
  ]);
  const gpuStatusUnified = mergeStatusCandidates([
    componentStatus('gpu'),
    componentStatus('gpuTest'),
    diagnosticStatus('GPU (WinSAT)')
  ]);
  const componentRows = [
    { key: 'cpuUnified', label: 'CPU OK', status: cpuStatusUnified },
    { key: 'gpuUnified', label: 'GPU OK', status: gpuStatusUnified },
    { key: 'diskUnified', label: 'Disque OK', status: diskStatus },
    { key: 'usb', label: 'Ports USB', status: componentStatus('usb') },
    { key: 'keyboard', label: 'Clavier', status: componentStatus('keyboard') },
    { key: 'camera', label: 'Camera', status: componentStatus('camera') },
    { key: 'pad', label: 'Pave tactile', status: componentStatus('pad') },
    { key: 'badgeReader', label: 'Lecteur badge', status: componentStatus('badgeReader') },
    { key: 'biosBattery', label: 'Pile BIOS', status: componentStatus('biosBattery') },
    { key: 'biosLanguage', label: 'Langue BIOS', status: componentStatus('biosLanguage') },
    { key: 'biosPassword', label: 'Mot de passe BIOS', status: componentStatus('biosPassword') },
    {
      key: 'wifiStandard',
      label: 'Norme Wi-Fi',
      status: componentStatus('wifiStandard'),
      extra: data.wifiStandardCode || null
    }
  ].filter((row) => row.status != null);

  const summarySource =
    data && data.summaryForPdf && typeof data.summaryForPdf === 'object' && !Array.isArray(data.summaryForPdf)
      ? data.summaryForPdf
      : null;
  const parseSummaryValue = (value) => {
    const numeric = Number(value);
    if (!Number.isFinite(numeric) || numeric < 0) {
      return 0;
    }
    return Math.round(numeric);
  };
  const displayedSummary = {
    ok: summarySource ? parseSummaryValue(summarySource.ok) : 0,
    nok: summarySource ? parseSummaryValue(summarySource.nok) : 0,
    other: summarySource ? parseSummaryValue(summarySource.other) : 0,
    total: summarySource ? parseSummaryValue(summarySource.total) : 0
  };

  if (!summarySource) {
    diagnosticsRows.forEach((row) => {
      addSummaryStatus(displayedSummary, normalizeStatusKey(row && row.status));
    });
    componentRows.forEach((row) => {
      if (!row) {
        return;
      }
      const resolved = resolveComponentStatusDisplay(row.key, row.status);
      addSummaryStatus(
        displayedSummary,
        resolved && resolved.statusKey ? resolved.statusKey : normalizeStatusKey(row.status)
      );
    });
  }

  const identRows = [
    { label: 'Serial', value: data.serialNumber },
    { label: 'MAC', value: data.macPrimary },
    { label: 'OS', value: data.osVersion },
    { label: 'Dernier passage', value: data.lastSeen },
    { label: 'Premier passage', value: data.createdAt }
  ];
  const detailedConfigRows = [
    { label: 'RAM totale', value: data.ramTotal },
    { label: 'Slots RAM', value: data.ramSlots },
    { label: 'CPU', value: data.cpuName },
    { label: 'Coeurs / Threads', value: data.cpuThreads },
    { label: 'GPU', value: data.gpuName },
    { label: 'Stockage total', value: data.storageTotal },
    { label: 'Disque principal', value: data.storagePrimary },
    { label: 'Batterie', value: data.batteryHealth }
  ];

  const summaryY = headerY + headerHeight + 7;
  const summaryCards = [
    {
      label: 'OK total',
      value: `${displayedSummary.ok || 0}`,
      tones: {
        background: palette.summaryOkBg,
        border: palette.summaryOkBorder,
        labelColor: palette.summaryOkText,
        valueColor: palette.summaryOkText
      }
    },
    {
      label: 'NOK total',
      value: `${displayedSummary.nok || 0}`,
      tones: {
        background: palette.summaryNokBg,
        border: palette.summaryNokBorder,
        labelColor: palette.summaryNokText,
        valueColor: palette.summaryNokText
      }
    },
    {
      label: 'Non testé',
      value: `${displayedSummary.other || 0}`,
      tones: {
        background: palette.summaryNtBg,
        border: palette.summaryNtBorder,
        labelColor: palette.summaryNtText,
        valueColor: palette.summaryNtText
      }
    },
    {
      label: 'Dernier passage',
      value: data.lastSeen,
      tones: {
        background: palette.summaryNeutralBg,
        border: palette.summaryNeutralBorder,
        labelColor: palette.summaryNeutralText,
        valueColor: palette.summaryNeutralText
      }
    }
  ];
  const summaryHeight = drawSummaryCards(doc, margin, summaryY, width, summaryCards, palette);

  const footerTop = pageBottom - 22;
  const contentTop = summaryY + summaryHeight + 7;
  const contentBottom = footerTop - 8;
  const contentHeight = Math.max(80, contentBottom - contentTop);
  const columnGap = 12;
  const leftWidth = Math.floor(width * 0.56);
  const rightWidth = width - leftWidth - columnGap;
  const leftX = margin;
  const rightX = leftX + leftWidth + columnGap;
  const cardGap = 10;

  const leftProductHeight = Math.max(118, Math.floor(contentHeight * 0.2));
  const leftIdHeight = Math.max(120, Math.floor(contentHeight * 0.2));
  const leftDetailHeight = Math.max(148, Math.floor(contentHeight * 0.26));
  let leftInventoryHeight =
    contentHeight - leftProductHeight - leftIdHeight - leftDetailHeight - cardGap * 3;
  if (leftInventoryHeight < 78) {
    const deficit = 78 - leftInventoryHeight;
    const reduceFromDetail = Math.min(deficit, 18);
    leftInventoryHeight += reduceFromDetail;
  }

  const rightDiagHeight = Math.max(168, Math.floor(contentHeight * 0.3));
  const rightCompHeight = contentHeight - cardGap - rightDiagHeight;

  drawProductSheet(doc, leftX, contentTop, leftWidth, leftProductHeight, data, palette);

  const sectionTableStyle = {
    card: {
      variant: 'section',
      background: 'transparent',
      border: palette.cardBorder,
      titleColor: palette.cardTitle,
      titleOffsetY: 2,
      bodyTopOffset: 17,
      bodyBottomInset: 2
    },
    labelColor: palette.cardLabel,
    valueColor: palette.cardValue
  };
  drawSectionTable(
    doc,
    leftX,
    contentTop + leftProductHeight + cardGap,
    leftWidth,
    leftIdHeight,
    'Identifiants',
    identRows,
    sectionTableStyle
  );
  drawSectionTable(
    doc,
    leftX,
    contentTop + leftProductHeight + leftIdHeight + cardGap * 2,
    leftWidth,
    leftDetailHeight,
    'Configuration détaillée',
    detailedConfigRows,
    sectionTableStyle
  );
  drawSectionTable(
    doc,
    leftX,
    contentTop + leftProductHeight + leftIdHeight + leftDetailHeight + cardGap * 3,
    leftWidth,
    Math.max(78, leftInventoryHeight),
    'Identifiants matériel détaillés',
    data.inventoryRows || [],
    sectionTableStyle
  );

  const statusCardStyle = {
    card: {
      variant: 'section',
      background: 'transparent',
      border: palette.cardBorder,
      titleColor: palette.cardTitle,
      titleOffsetY: 2,
      bodyTopOffset: 17,
      bodyBottomInset: 2
    },
    textColor: palette.cardValue
  };

  drawCompactStatusCard(
    doc,
    rightX,
    contentTop,
    rightWidth,
    rightDiagHeight,
    'Diagnostics',
    diagnosticsRows,
    statusCardStyle
  );
  drawCompactStatusCard(
    doc,
    rightX,
    contentTop + rightDiagHeight + cardGap,
    rightWidth,
    rightCompHeight,
    'État des composants',
    componentRows,
    statusCardStyle
  );

  doc
    .lineWidth(0.8)
    .strokeColor('#BFD7E4')
    .moveTo(margin, footerTop)
    .lineTo(margin + width, footerTop)
    .stroke();
  const footerY = footerTop + 4;
  const footerLeftWidth = Math.floor(width * 0.5);
  const footerMiddleWidth = Math.floor(width * 0.32);
  const footerRightWidth = width - footerLeftWidth - footerMiddleWidth;
  doc.font('Helvetica').fontSize(7).fillColor(palette.footerText).text(
    `Rapport ID: ${truncatePdfText(safeString(data.id, '--'), 36)}`,
    margin,
    footerY,
    {
      width: footerLeftWidth,
      lineBreak: false
    }
  );
  doc.font('Helvetica').fontSize(7).fillColor(palette.footerText).text(
    `Généré: ${truncatePdfText(safeString(data.generatedAt, '--'), 26)}`,
    margin + footerLeftWidth,
    footerY,
    {
      width: footerMiddleWidth,
      align: 'center',
      lineBreak: false
    }
  );
  doc.font('Helvetica').fontSize(7).fillColor(palette.footerText).text(
    'Page 1/1',
    margin + footerLeftWidth + footerMiddleWidth,
    footerY,
    {
      width: footerRightWidth,
      align: 'right',
      lineBreak: false
    }
  );
}
app.get('/', requireAuth, (req, res) => {
  res.sendFile(path.join(PUBLIC_DIR, 'index.html'));
});

app.get('/index.html', requireAuth, (req, res) => {
  res.sendFile(path.join(PUBLIC_DIR, 'index.html'));
});

app.get('/admin', requireAuth, requireAdminPage, (req, res) => {
  res.sendFile(path.join(PUBLIC_DIR, 'admin.html'));
});

app.get('/admin.html', requireAuth, requireAdminPage, (req, res) => {
  res.sendFile(path.join(PUBLIC_DIR, 'admin.html'));
});

app.get('/lots', requireAuth, requireTagEditPage, (req, res) => {
  res.sendFile(path.join(PUBLIC_DIR, 'lots.html'));
});

app.get('/lots.html', requireAuth, requireTagEditPage, (req, res) => {
  res.sendFile(path.join(PUBLIC_DIR, 'lots.html'));
});

app.get('/journal', requireAuth, (req, res) => {
  res.sendFile(path.join(PUBLIC_DIR, 'journal.html'));
});

app.get('/journal.html', requireAuth, (req, res) => {
  res.sendFile(path.join(PUBLIC_DIR, 'journal.html'));
});

app.get('/logs', requireAuth, (req, res) => {
  res.redirect('/journal');
});

app.get('/login', (req, res) => {
  if (req.session && req.session.user) {
    return res.redirect('/');
  }
  return res.sendFile(path.join(PUBLIC_DIR, 'login.html'));
});

app.post('/login', async (req, res) => {
  const username = cleanString(req.body?.username, 128);
  const password = typeof req.body?.password === 'string' ? req.body.password : '';

  if (!username || !password) {
    return res.redirect('/login?error=1');
  }

  let user = null;
  if (await isLocalAdmin(username, password)) {
    user = buildLocalSessionUser(username);
  } else {
    try {
      const ldapConfig = await getEffectiveLdapConfig();
      if (ldapConfig.enabled && ldapConfig.url && ldapConfig.searchBase) {
        const ldapUser = await authenticateLdap(username, password, ldapConfig);
        user = buildLdapSessionUser(username, ldapUser);
      }
    } catch (error) {
      user = null;
    }
  }

  if (!user) {
    return res.redirect('/login?error=1');
  }

  return req.session.regenerate((err) => {
    if (err) {
      return res.redirect('/login?error=1');
    }
    req.session.user = user;
    return req.session.save(() => res.redirect('/'));
  });
});

app.post('/api/login', async (req, res) => {
  const username = cleanString(req.body?.username, 128);
  const password = typeof req.body?.password === 'string' ? req.body.password : '';

  if (!username || !password) {
    return res.status(400).json({ ok: false, error: 'invalid_credentials' });
  }

  let user = null;
  if (await isLocalAdmin(username, password)) {
    user = buildLocalSessionUser(username);
  } else {
    try {
      const ldapConfig = await getEffectiveLdapConfig();
      if (ldapConfig.enabled && ldapConfig.url && ldapConfig.searchBase) {
        const ldapUser = await authenticateLdap(username, password, ldapConfig);
        user = buildLdapSessionUser(username, ldapUser);
      }
    } catch (error) {
      user = null;
    }
  }

  if (!user) {
    return res.status(401).json({ ok: false, error: 'invalid_credentials' });
  }

  return req.session.regenerate((err) => {
    if (err) {
      return res.status(500).json({ ok: false, error: 'session_error' });
    }
    req.session.user = user;
    return req.session.save(() => res.json({ ok: true, user }));
  });
});

app.get('/logout', (req, res) => {
  if (!req.session) {
    return res.redirect('/login');
  }
  return req.session.destroy(() => res.redirect('/login'));
});

app.get('/patchnotes', requireAuth, (req, res) => {
  res.sendFile(path.join(PUBLIC_DIR, 'patchnotes.html'));
});

app.get('/legacy-imports', requireAuth, (req, res) => {
  res.sendFile(path.join(PUBLIC_DIR, 'legacy-imports.html'));
});

app.get('/legacy-imports.html', requireAuth, (req, res) => {
  res.sendFile(path.join(PUBLIC_DIR, 'legacy-imports.html'));
});

app.get('/metrics', requireAuth, (req, res) => {
  res.sendFile(path.join(PUBLIC_DIR, 'metrics.html'));
});

app.get('/metrics.html', requireAuth, (req, res) => {
  res.sendFile(path.join(PUBLIC_DIR, 'metrics.html'));
});

app.use(express.static(PUBLIC_DIR, { extensions: ['html'], index: false }));

app.get('/api/health', (req, res) => {
  res.json({ ok: true });
});

app.get('/api/me', requireAuth, (req, res) => {
  refreshLdapPermissions(req)
    .then((user) => {
      res.json({ ok: true, user: user || req.session.user });
    })
    .catch(() => {
      res.json({ ok: true, user: req.session.user });
    });
});

app.get('/api/patchnotes/latest', requireAuth, async (req, res) => {
  const user = getPatchnoteUser(req);
  if (!user) {
    return res.status(401).json({ ok: false, error: 'unauthorized' });
  }

  try {
    const latest = await pool.query(
      `
        SELECT id, version, body
        FROM patchnotes
        ORDER BY created_at DESC, id DESC
        LIMIT 1
      `
    );
    if (!latest.rows.length) {
      return res.json({ ok: true, patchnote: null });
    }

    const patchnote = latest.rows[0];
    const seen = await pool.query(
      `
        SELECT 1
        FROM patchnote_views
        WHERE patchnote_id = $1 AND username = $2 AND user_type = $3
        LIMIT 1
      `,
      [patchnote.id, user.username, user.type]
    );
    if (seen.rows.length) {
      return res.json({ ok: true, patchnote: null });
    }
    return res.json({
      ok: true,
      patchnote: {
        id: patchnote.id,
        version: patchnote.version,
        body: patchnote.body
      }
    });
  } catch (error) {
    console.error('Failed to load patchnote', error);
    return res.status(500).json({ ok: false, error: 'patchnote_error' });
  }
});

app.post('/api/patchnotes/ack', requireAuth, async (req, res) => {
  const user = getPatchnoteUser(req);
  if (!user) {
    return res.status(401).json({ ok: false, error: 'unauthorized' });
  }
  const patchnoteId = Number.parseInt(req.body?.patchnoteId, 10);
  if (!Number.isFinite(patchnoteId)) {
    return res.status(400).json({ ok: false, error: 'invalid_patchnote' });
  }
  try {
    await pool.query(
      `
        INSERT INTO patchnote_views (patchnote_id, username, user_type)
        VALUES ($1, $2, $3)
        ON CONFLICT DO NOTHING
      `,
      [patchnoteId, user.username, user.type]
    );
    return res.json({ ok: true });
  } catch (error) {
    console.error('Failed to ack patchnote', error);
    return res.status(500).json({ ok: false, error: 'patchnote_error' });
  }
});

app.get('/api/patchnotes', requireAuth, async (req, res) => {
  try {
    const result = await pool.query(
      `
        SELECT id, version, body, created_at
        FROM patchnotes
        ORDER BY created_at DESC, id DESC
      `
    );
    return res.json({
      ok: true,
      patchnotes: result.rows.map((row) => ({
        id: row.id,
        version: row.version,
        body: row.body,
        createdAt: row.created_at
      }))
    });
  } catch (error) {
    console.error('Failed to load patchnotes list', error);
    return res.status(500).json({ ok: false, error: 'patchnote_error' });
  }
});

app.get('/api/suggestions', requireAuth, async (req, res) => {
  try {
    const result = await pool.query(
      `
        SELECT id, title, body, created_by, created_at
        FROM suggestions
        ORDER BY created_at DESC
      `
    );
    const suggestions = (result.rows || []).map((row) => ({
      id: row.id,
      title: row.title || '',
      body: row.body || '',
      createdBy: row.created_by || '',
      createdAt: row.created_at
    }));
    return res.json({ ok: true, suggestions });
  } catch (error) {
    console.error('Failed to load suggestions', error);
    return res.status(500).json({ ok: false, error: 'db_error' });
  }
});

app.post('/api/suggestions', requireAuth, async (req, res) => {
  const title = cleanString(req.body?.title, 120);
  const bodyRaw = typeof req.body?.body === 'string' ? req.body.body : '';
  const body = bodyRaw.trim();
  if (!title || !body) {
    return res.status(400).json({ ok: false, error: 'invalid_payload' });
  }
  if (body.length > 5000) {
    return res.status(400).json({ ok: false, error: 'payload_too_large' });
  }
  const createdBy = req.session?.user?.username || '';
  try {
    const insertResult = await pool.query(
      `
        INSERT INTO suggestions (title, body, created_by)
        VALUES ($1, $2, $3)
        RETURNING id, created_at
      `,
      [title, body, createdBy]
    );
    const row = insertResult.rows && insertResult.rows[0] ? insertResult.rows[0] : null;
    const pageLabel = cleanString(req.body?.page, 64) || '';
    setImmediate(() => {
      sendSuggestionEmail({
        title,
        body,
        createdBy,
        pageLabel
      }).catch(() => null);
    });
    return res.json({
      ok: true,
      suggestion: {
        id: row?.id,
        title,
        body,
        createdBy,
        createdAt: row?.created_at
      },
      mailQueued: true
    });
  } catch (error) {
    console.error('Failed to create suggestion', error);
    return res.status(500).json({ ok: false, error: 'db_error' });
  }
});

app.get('/api/logs', requireAuth, async (req, res) => {
  if (!AUDIT_LOG_ENABLED) {
    return res.json({ ok: true, logs: [], enabled: false });
  }

  const tableFilter = cleanString(req.query?.table, 64);
  const actionRaw = cleanString(req.query?.action, 16);
  const actionFilter = actionRaw ? actionRaw.toUpperCase() : null;
  const machineKeyFilter = cleanString(req.query?.machineKey, 128);
  const actorFilter = cleanString(req.query?.actor, 128);
  const requestFilter = cleanString(req.query?.requestId, 128);
  const hostnameFilter = cleanString(req.query?.hostname, 128);
  const searchRaw = cleanString(req.query?.q, 256);
  const sinceRaw = cleanString(req.query?.since, 64);
  const limitRaw = Number.parseInt(req.query?.limit, 10);

  const limitDefault = Number.isFinite(AUDIT_LOG_LIMIT_DEFAULT)
    ? AUDIT_LOG_LIMIT_DEFAULT
    : 100;
  const limitMax = Number.isFinite(AUDIT_LOG_LIMIT_MAX) ? AUDIT_LOG_LIMIT_MAX : 500;
  let limit = Number.isFinite(limitRaw) ? limitRaw : limitDefault;
  if (limit < 1) {
    limit = limitDefault;
  }
  if (limitMax > 0) {
    limit = Math.min(limit, limitMax);
  }

  const allowedTables = new Set(['machines', 'reports', 'ldap_settings']);
  const allowedActions = new Set(['INSERT', 'UPDATE', 'DELETE']);

  const filters = [];
  const values = [];

  if (tableFilter && allowedTables.has(tableFilter)) {
    values.push(tableFilter);
    filters.push(`audit_log.table_name = $${values.length}`);
  }
  if (actionFilter && allowedActions.has(actionFilter)) {
    values.push(actionFilter);
    filters.push(`audit_log.action = $${values.length}`);
  }
  if (machineKeyFilter) {
    values.push(machineKeyFilter);
    filters.push(`audit_log.machine_key = $${values.length}`);
  }
  if (hostnameFilter) {
    values.push(`%${hostnameFilter}%`);
    filters.push(`machines.hostname ILIKE $${values.length}`);
  }
  if (actorFilter) {
    values.push(`%${actorFilter}%`);
    filters.push(`audit_log.actor ILIKE $${values.length}`);
  }
  if (requestFilter) {
    values.push(`%${requestFilter}%`);
    filters.push(`audit_log.request_id ILIKE $${values.length}`);
  }
  if (sinceRaw) {
    const sinceDate = new Date(sinceRaw);
    if (!Number.isNaN(sinceDate.getTime())) {
      values.push(sinceDate.toISOString());
      filters.push(`audit_log.occurred_at >= $${values.length}`);
    }
  }
  if (searchRaw) {
    const idx = values.push(`%${searchRaw}%`);
    filters.push(
      `(audit_log.machine_key ILIKE $${idx} OR audit_log.actor ILIKE $${idx} ` +
        `OR audit_log.request_id ILIKE $${idx} OR audit_log.source ILIKE $${idx} ` +
        `OR machines.hostname ILIKE $${idx})`
    );
  }

  const whereClause = filters.length ? `WHERE ${filters.join(' AND ')}` : '';
  values.push(limit);
  const limitParam = `$${values.length}`;

  try {
    const result = await pool.query(
      `
        SELECT
          audit_log.id,
          audit_log.occurred_at,
          audit_log.table_name,
          audit_log.action,
          audit_log.row_id,
          audit_log.machine_key,
          machines.hostname,
          audit_log.actor,
          audit_log.actor_type,
          audit_log.actor_ip,
          audit_log.request_id,
          audit_log.source,
          audit_log.changed_fields,
          audit_log.old_data,
          audit_log.new_data
        FROM audit_log
        LEFT JOIN machines ON machines.machine_key = audit_log.machine_key
        ${whereClause}
        ORDER BY audit_log.occurred_at DESC
        LIMIT ${limitParam}
      `,
      values
    );

    const logs = result.rows.map((row) => {
      const oldData = sanitizeAuditData(row.old_data, false);
      const newData = sanitizeAuditData(row.new_data, false);
      const changedFields = Array.isArray(row.changed_fields) ? row.changed_fields : [];
      const componentChanges = extractComponentChangesFromAudit(oldData, newData);
      const changes = changedFields.map((field) => ({
        field,
        before: oldData ? oldData[field] : null,
        after: newData ? newData[field] : null
      }));
      return {
        id: row.id,
        occurredAt: row.occurred_at,
        table: row.table_name,
        action: row.action,
        rowId: row.row_id,
        machineKey: row.machine_key,
        hostname: row.hostname,
        actor: row.actor,
        actorType: row.actor_type,
        actorIp: row.actor_ip,
        requestId: row.request_id,
        source: row.source,
        changedFields,
        changes,
        componentChanges
      };
    });

    return res.json({ ok: true, logs });
  } catch (error) {
    console.error('Failed to fetch audit logs', error);
    return res.status(500).json({ ok: false, error: 'db_error' });
  }
});

app.get('/api/logs/:id', requireAuth, async (req, res) => {
  if (!AUDIT_LOG_ENABLED) {
    return res.status(404).json({ ok: false, error: 'not_found' });
  }

  const id = Number.parseInt(req.params.id, 10);
  if (!Number.isFinite(id)) {
    return res.status(400).json({ ok: false, error: 'invalid_id' });
  }

  const includePayload = req.query?.includePayload === '1';

  try {
    const result = await pool.query(
      `
        SELECT
          audit_log.id,
          audit_log.occurred_at,
          audit_log.table_name,
          audit_log.action,
          audit_log.row_id,
          audit_log.machine_key,
          machines.hostname,
          audit_log.actor,
          audit_log.actor_type,
          audit_log.actor_ip,
          audit_log.request_id,
          audit_log.source,
          audit_log.changed_fields,
          audit_log.old_data,
          audit_log.new_data
        FROM audit_log
        LEFT JOIN machines ON machines.machine_key = audit_log.machine_key
        WHERE audit_log.id = $1
        LIMIT 1
      `,
      [id]
    );

    const row = result.rows && result.rows[0] ? result.rows[0] : null;
    if (!row) {
      return res.status(404).json({ ok: false, error: 'not_found' });
    }

    const log = {
      id: row.id,
      occurredAt: row.occurred_at,
      table: row.table_name,
      action: row.action,
      rowId: row.row_id,
      machineKey: row.machine_key,
      hostname: row.hostname,
      actor: row.actor,
      actorType: row.actor_type,
      actorIp: row.actor_ip,
      requestId: row.request_id,
      source: row.source,
      changedFields: row.changed_fields || [],
      componentChanges: extractComponentChangesFromAudit(row.old_data, row.new_data),
      oldData: sanitizeAuditData(row.old_data, includePayload),
      newData: sanitizeAuditData(row.new_data, includePayload)
    };

    return res.json({ ok: true, log });
  } catch (error) {
    console.error('Failed to fetch audit log detail', error);
    return res.status(500).json({ ok: false, error: 'db_error' });
  }
});

app.get('/api/admin/ldap', requireAuth, requireAdmin, async (req, res) => {
  const config = await getEffectiveLdapConfig();
  res.json({ ok: true, config: formatLdapConfigForResponse(config) });
});

app.put('/api/admin/ldap', requireAuth, requireAdmin, async (req, res) => {
  const body = req.body && typeof req.body === 'object' ? req.body : {};
  const url = normalizeOptionalString(body.url, LDAP_FIELD_LIMIT);
  const bindDn = normalizeOptionalString(body.bindDn, LDAP_FIELD_LIMIT);
  const searchBase = normalizeOptionalString(body.searchBase, LDAP_FIELD_LIMIT);
  const searchFilter = normalizeLdapSearchFilter(body.searchFilter);
  const searchAttributes = normalizeLdapSearchAttributesString(body.searchAttributes);
  const tlsRejectUnauthorized =
    typeof body.tlsRejectUnauthorized === 'boolean' ? body.tlsRejectUnauthorized : true;
  const enabled =
    typeof body.enabled === 'boolean' ? body.enabled : Boolean(url && searchBase);
  const clearBindPassword = body.clearBindPassword === true;
  const currentConfig = await getEffectiveLdapConfig();
  let bindPassword = currentConfig.bindPassword || '';

  if (typeof body.bindPassword === 'string' && body.bindPassword.trim() !== '') {
    bindPassword = body.bindPassword;
  } else if (clearBindPassword) {
    bindPassword = '';
  }

  if (enabled && (!url || !searchBase)) {
    return res.status(400).json({ ok: false, error: 'missing_required' });
  }

  let client;
  try {
    client = await pool.connect();
    await client.query('BEGIN');
    await setAuditContext(client, buildAuditContext(req));
    await client.query(
      `
        INSERT INTO ldap_settings (
          id,
          enabled,
          url,
          bind_dn,
          bind_password,
          search_base,
          search_filter,
          search_attributes,
          tls_reject_unauthorized,
          updated_at
        ) VALUES (
          1,
          $1,
          $2,
          $3,
          $4,
          $5,
          $6,
          $7,
          $8,
          NOW()
        )
        ON CONFLICT (id) DO UPDATE SET
          enabled = EXCLUDED.enabled,
          url = EXCLUDED.url,
          bind_dn = EXCLUDED.bind_dn,
          bind_password = EXCLUDED.bind_password,
          search_base = EXCLUDED.search_base,
          search_filter = EXCLUDED.search_filter,
          search_attributes = EXCLUDED.search_attributes,
          tls_reject_unauthorized = EXCLUDED.tls_reject_unauthorized,
          updated_at = NOW()
      `,
      [
        enabled,
        url,
        bindDn,
        bindPassword,
        searchBase,
        searchFilter,
        searchAttributes,
        tlsRejectUnauthorized
      ]
    );
    await client.query('COMMIT');
  } catch (error) {
    if (client) {
      try {
        await client.query('ROLLBACK');
      } catch (rollbackError) {
        console.error('Failed to rollback LDAP settings update', rollbackError);
      }
    }
    console.error('Failed to update LDAP settings', error);
    return res.status(500).json({ ok: false, error: 'db_error' });
  } finally {
    if (client) {
      client.release();
    }
  }

  const updated = await getEffectiveLdapConfig();
  return res.json({ ok: true, config: formatLdapConfigForResponse(updated) });
});

app.post('/api/ingest', ingestLimiter, async (req, res) => {
  const body = req.body;
  if (!body || typeof body !== 'object') {
    return res.status(400).json({ ok: false, error: 'invalid_payload' });
  }

  const hostname = cleanString(pickFirst(body, ['hostname', 'computerName', 'name']), 64);
  let macAddress = normalizeMac(pickFirst(body, ['macAddress', 'mac', 'mac_address']));
  const serialNumber = normalizeSerial(pickFirst(body, ['serialNumber', 'serial', 'serial_number']));
  const category = normalizeCategory(
    pickFirst(body, ['category', 'type', 'formFactor', 'chassis', 'chassisType'])
  );
  const tagIdRaw = pickFirst(body, [
    'tagId',
    'tag_id',
    'prodTagId',
    'productionTagId',
    'batchId',
    'lotId'
  ]);
  const lotIdRaw = pickFirst(body, [
    'lotId',
    'lot_id',
    'batchId',
    'batch_id',
    'productionLotId',
    'production_lot_id'
  ]);
  const tag = cleanString(
    pickFirst(body, ['tag', 'prodTag', 'productionTag', 'production', 'batch', 'lot', 'prod']),
    64
  );
  const model = cleanString(pickFirst(body, ['model', 'computerModel', 'product', 'productName']), 64);
  const vendor = cleanString(pickFirst(body, ['vendor', 'manufacturer', 'make']), 64);
  const technician = cleanString(
    pickFirst(body, ['technician', 'technicianName', 'tech', 'techName', 'operator']),
    64
  );
  const osVersion = cleanString(pickFirst(body, ['osVersion', 'os', 'os_version']), 64);
  const components = sanitizeComponents(
    pickFirst(body, ['components', 'componentStatus', 'composants', 'etatComposants'])
  );
  const bodyComponents =
    body && body.components && typeof body.components === 'object' && !Array.isArray(body.components)
      ? body.components
      : null;
  const bodyHardware =
    body && body.hardware && typeof body.hardware === 'object' && !Array.isArray(body.hardware)
      ? body.hardware
      : null;
  const bodyWifi =
    body && body.wifi && typeof body.wifi === 'object' && !Array.isArray(body.wifi) ? body.wifi : null;
  const bodyNetwork =
    body && body.network && typeof body.network === 'object' && !Array.isArray(body.network)
      ? body.network
      : null;
  const sources = [body, bodyComponents, bodyHardware, bodyWifi, bodyNetwork];
  const derivedComponents = buildDerivedComponents(body, sources);
  const mergedComponents = mergeComponentSets(derivedComponents, components);
  let macAddresses = normalizeMacList(
    pickFirstFromSources(sources, ['macAddresses', 'macs', 'mac_addresses', 'macList', 'maclist'])
  );
  const ramMb = normalizeRamMb(
    pickFirstFromSources(sources, [
      'ramMb',
      'ramMB',
      'ramGb',
      'ramGB',
      'ram',
      'memory',
      'memoryMb',
      'memoryMB',
      'memoryGb',
      'memoryGB',
      'totalMemory',
      'totalRam'
    ])
  );
  const ramSlotsTotal = normalizeSlots(
    pickFirstFromSources(sources, [
      'ramSlotsTotal',
      'ramSlots',
      'memorySlots',
      'slotsTotal',
      'ramSlotCount',
      'ramSlotsCount'
    ])
  );
  const ramSlotsFree = normalizeSlots(
    pickFirstFromSources(sources, [
      'ramSlotsFree',
      'ramSlotsAvailable',
      'slotsFree',
      'slotsAvailable',
      'ramSlotsOpen',
      'ramSlotsEmpty'
    ])
  );
  const batteryHealth = normalizeBatteryHealth(
    pickFirstFromSources(sources, [
      'batteryHealth',
      'batteryHealthPercent',
      'batteryLife',
      'batteryLifePercent',
      'batteryPercent',
      'battery'
    ])
  );
  const cameraStatus = normalizeStatus(
    pickFirstFromSources(sources, ['cameraStatus', 'camera', 'webcam', 'webcamStatus'])
  );
  const usbStatus = normalizeStatus(
    pickFirstFromSources(sources, ['usbStatus', 'usbPorts', 'portsUsb', 'portsUSB', 'usb'])
  );
  const keyboardStatus = normalizeStatus(
    pickFirstFromSources(sources, ['keyboardStatus', 'keyboard', 'clavier'])
  );
  const padStatus = normalizeStatus(
    pickFirstFromSources(sources, [
      'padStatus',
      'touchpadStatus',
      'trackpadStatus',
      'touchPadStatus',
      'touchpad',
      'trackpad',
      'pad',
      'touchPad',
      'paveTactile',
      'pave_tactile'
    ])
  );
  const badgeReaderStatus = normalizeStatus(
    pickFirstFromSources(sources, [
      'badgeReaderStatus',
      'badgeReader',
      'badge',
      'badgeReaderState',
      'smartCardReader',
      'rfid'
    ])
  );

  if (macAddress && (!macAddresses || !macAddresses.includes(macAddress))) {
    macAddresses = [macAddress, ...(macAddresses || [])];
  }
  if (!macAddress && macAddresses && macAddresses.length > 0) {
    macAddress = macAddresses[0];
  }

  if (!hostname && !macAddress && !serialNumber) {
    return res.status(400).json({
      ok: false,
      error: 'missing_identifier',
      message: 'Provide at least hostname, macAddress, or serialNumber.'
    });
  }

  const machineKey = buildMachineKey(serialNumber, macAddress, hostname);

  const now = new Date().toISOString();
  const reportIdRaw = pickFirstFromSources([body, body.diag], [
    'reportId',
    'report_id',
    'clientRunId',
    'client_run_id',
    'runId',
    'run_id'
  ]);
  const reportId = normalizeUuid(reportIdRaw) || generateUuid();
  const isDoubleCheck = isDoubleCheckPayload(body);
  const payloadMode = cleanString(
    pickFirst(body, ['payloadMode', 'payload_mode', 'updateMode', 'ingestMode']),
    16
  );
  const skipPayloadRaw = pickFirst(body, ['skipPayload', 'payloadSkip', 'partialUpdate', 'partial']);
  const skipPayload =
    (payloadMode && ['skip', 'partial', 'patch'].includes(payloadMode.toLowerCase())) ||
    skipPayloadRaw === true ||
    skipPayloadRaw === 'true';
  const payload = skipPayload ? null : safeJsonStringify(body, 64 * 1024);
  const ipAddress = getClientIp(req);

  let client;
  try {
    client = await pool.connect();
    await client.query('BEGIN');
    const resolvedTag = await resolveTagForIngest(client, tagIdRaw, tag);
    const lotResolution = await resolveLotForIngest(client, {
      explicitLotId: lotIdRaw,
      technician
    });
    const resolvedLot = lotResolution.lot;
    const resolvedLotId = resolvedLot && resolvedLot.id ? resolvedLot.id : null;
    const shouldCountLot = Boolean(
      resolvedLotId &&
      machineKey &&
      !isDoubleCheck &&
      !parseBooleanFlag(resolvedLot ? resolvedLot.is_paused : false, false)
    );
    await setAuditContext(
      client,
      buildAuditContext(req, {
        actor: technician || machineKey,
        actorType: technician ? 'technician' : 'ingest'
      })
    );
    const reportValues = [
      reportId,
      machineKey,
      hostname,
      macAddress,
      macAddresses ? JSON.stringify(macAddresses) : null,
      serialNumber,
      category,
      resolvedTag.name || DEFAULT_REPORT_TAG,
      resolvedTag.id || null,
      resolvedLotId,
      model,
      vendor,
      technician,
      osVersion,
      ramMb,
      ramSlotsTotal,
      ramSlotsFree,
      batteryHealth,
      cameraStatus,
      usbStatus,
      keyboardStatus,
      padStatus,
      badgeReaderStatus,
      now,
      now,
      mergedComponents ? JSON.stringify(mergedComponents) : null,
      payload,
      ipAddress
    ];

    const values = [
      machineKey,
      hostname,
      macAddress,
      macAddresses ? JSON.stringify(macAddresses) : null,
      serialNumber,
      category,
      resolvedTag.name || DEFAULT_REPORT_TAG,
      resolvedTag.id || null,
      resolvedLotId,
      model,
      vendor,
      technician,
      osVersion,
      ramMb,
      ramSlotsTotal,
      ramSlotsFree,
      batteryHealth,
      cameraStatus,
      usbStatus,
      keyboardStatus,
      padStatus,
      badgeReaderStatus,
      now,
      now,
      mergedComponents ? JSON.stringify(mergedComponents) : null,
      payload,
      ipAddress
    ];
    const reportResult = await client.query(upsertReportQuery, reportValues);
    const result = await client.query(upsertMachineQuery, values);
    const lotProgress = await registerLotProgress(client, {
      lot: resolvedLot,
      machineKey,
      reportId,
      technician,
      source: 'ingest',
      isDoubleCheck,
      shouldCount: shouldCountLot
    });
    await client.query('COMMIT');
    const reportRow = reportResult.rows && reportResult.rows[0] ? reportResult.rows[0] : null;
    const reportIdValue = reportRow && reportRow.id ? reportRow.id : reportId;
    return res.status(200).json({
      ok: true,
      id: reportIdValue,
      reportId: reportIdValue,
      machineKey,
      lot: normalizeLotFromRow(lotProgress && lotProgress.lot ? lotProgress.lot : resolvedLot),
      lotCounted: Boolean(lotProgress && lotProgress.counted)
    });
  } catch (error) {
    if (client) {
      try {
        await client.query('ROLLBACK');
      } catch (rollbackError) {
        console.error('Failed to rollback ingest', rollbackError);
      }
    }
    console.error('Failed to ingest payload', error);
    return res.status(500).json({ ok: false, error: 'db_error' });
  } finally {
    if (client) {
      client.release();
    }
  }
});

app.get('/api/reports', requireAuth, async (req, res) => {
  const limitRaw = Number.parseInt(req.query.limit || '', 10);
  const offsetRaw = Number.parseInt(req.query.offset || '', 10);
  const limit = Number.isFinite(limitRaw) && limitRaw > 0
    ? Math.min(limitRaw, REPORT_PAGE_LIMIT_MAX)
    : REPORT_PAGE_LIMIT_DEFAULT;
  const offset = Number.isFinite(offsetRaw) && offsetRaw >= 0 ? offsetRaw : 0;
  const includeTotal = req.query.includeTotal === '1' || req.query.includeTotal === 'true';
  let activeTagId = null;
  const hasTagFilter = Boolean(req.query.tags || req.query.tagIds);
  if (hasTagFilter) {
    try {
      const activeTag = await getActiveTag(pool);
      activeTagId = activeTag ? activeTag.id : null;
    } catch (error) {
      activeTagId = null;
    }
  }
  const { clauses, values } = buildReportFilters(req.query, {
    includeCategory: true,
    activeTagId
  });
  const where = clauses.length ? `WHERE ${clauses.join(' AND ')}` : '';
  const useLatest = shouldUseLatest(req.query);

  try {
    let total = null;
    if (includeTotal) {
      if (useLatest) {
        const countResult = await pool.query(
          `
            SELECT COUNT(DISTINCT machine_key) AS total
            FROM reports
            ${where}
          `,
          values
        );
        total = Number.parseInt(countResult.rows?.[0]?.total || '0', 10);
      } else {
        const countResult = await pool.query(
          `
            SELECT COUNT(*) AS total
            FROM reports
            ${where}
          `,
          values
        );
        total = Number.parseInt(countResult.rows?.[0]?.total || '0', 10);
      }
    }
    const result = useLatest
      ? await pool.query(
        `
          WITH latest AS (
            SELECT DISTINCT ON (reports.machine_key)
              reports.id,
              reports.machine_key,
              reports.hostname,
              reports.mac_address,
              reports.mac_addresses,
              reports.serial_number,
              reports.category,
              reports.tag,
              reports.tag_id,
              reports.lot_id,
              reports.model,
              reports.vendor,
              reports.technician,
              reports.os_version,
              reports.ram_mb,
              reports.ram_slots_total,
              reports.ram_slots_free,
              reports.battery_health,
              reports.camera_status,
              reports.usb_status,
              reports.keyboard_status,
              reports.pad_status,
              reports.badge_reader_status,
              reports.last_seen,
              reports.last_ip,
              reports.components,
              reports.comment
            FROM reports
            ${where}
            ORDER BY reports.machine_key, reports.last_seen DESC, reports.id DESC
          )
          SELECT
            latest.id,
            latest.machine_key,
            latest.hostname,
            latest.mac_address,
            latest.mac_addresses,
            latest.serial_number,
            latest.category,
            latest.tag,
            latest.tag_id,
            latest.lot_id,
            COALESCE(tags.name, latest.tag) AS tag_name,
            lots.supplier AS lot_supplier,
            lots.lot_number AS lot_number,
            lots.target_count AS lot_target_count,
            lots.produced_count AS lot_produced_count,
            lots.is_paused AS lot_is_paused,
            latest.model,
            latest.vendor,
            latest.technician,
            latest.os_version,
            latest.ram_mb,
            latest.ram_slots_total,
            latest.ram_slots_free,
            latest.battery_health,
            latest.camera_status,
            latest.usb_status,
            latest.keyboard_status,
            latest.pad_status,
            latest.badge_reader_status,
            latest.last_seen,
            latest.last_ip,
            latest.components,
            latest.comment
          FROM latest
          LEFT JOIN tags ON tags.id = latest.tag_id
          LEFT JOIN lots ON lots.id = latest.lot_id
          ORDER BY latest.last_seen DESC
          LIMIT $${values.length + 1} OFFSET $${values.length + 2}
        `,
        [...values, limit, offset]
      )
      : await pool.query(
        `
          SELECT
            reports.id,
            reports.machine_key,
            reports.hostname,
            reports.mac_address,
            reports.mac_addresses,
            reports.serial_number,
            reports.category,
            reports.tag,
            reports.tag_id,
            reports.lot_id,
            COALESCE(tags.name, reports.tag) AS tag_name,
            lots.supplier AS lot_supplier,
            lots.lot_number AS lot_number,
            lots.target_count AS lot_target_count,
            lots.produced_count AS lot_produced_count,
            lots.is_paused AS lot_is_paused,
            reports.model,
            reports.vendor,
            reports.technician,
            reports.os_version,
            reports.ram_mb,
            reports.ram_slots_total,
            reports.ram_slots_free,
            reports.battery_health,
            reports.camera_status,
            reports.usb_status,
            reports.keyboard_status,
            reports.pad_status,
            reports.badge_reader_status,
            reports.last_seen,
            reports.last_ip,
            reports.components,
            reports.comment
          FROM reports
          LEFT JOIN tags ON tags.id = reports.tag_id
          LEFT JOIN lots ON lots.id = reports.lot_id
          ${where}
          ORDER BY reports.last_seen DESC
          LIMIT $${values.length + 1} OFFSET $${values.length + 2}
        `,
        [...values, limit, offset]
      );

    const machines = result.rows.map((row) => {
      let components = null;
      try {
        components = row.components ? JSON.parse(row.components) : null;
      } catch (error) {
        components = null;
      }
      components = withManualComponentDefaults(components);
      return {
        id: row.id,
        machineKey: row.machine_key,
        hostname: row.hostname,
        macAddress: row.mac_address,
        macAddresses: normalizeMacList(row.mac_addresses),
        serialNumber: row.serial_number,
        category: row.category,
        tag: row.tag || null,
        tagId: row.tag_id || null,
        tagName: row.tag_name || row.tag || null,
        lot: normalizeLotFromRow(row),
        model: row.model,
        vendor: row.vendor,
        technician: row.technician,
        osVersion: row.os_version,
        ramMb: row.ram_mb,
        ramSlotsTotal: row.ram_slots_total,
        ramSlotsFree: row.ram_slots_free,
        batteryHealth: row.battery_health,
        cameraStatus: row.camera_status,
        usbStatus: row.usb_status,
        keyboardStatus: row.keyboard_status,
        padStatus: row.pad_status,
        badgeReaderStatus: row.badge_reader_status,
        lastSeen: row.last_seen,
        lastIp: row.last_ip,
        comment: row.comment,
        components
      };
    });

    return res.json({
      ok: true,
      machines,
      limit,
      offset,
      hasMore: machines.length === limit,
      total
    });
  } catch (error) {
    console.error('Failed to list reports', error);
    return res.status(500).json({ ok: false, error: 'db_error' });
  }
});

app.get('/api/stats', requireAuth, async (req, res) => {
  let activeTagId = null;
  const hasTagFilter = Boolean(req.query.tags || req.query.tagIds);
  if (hasTagFilter) {
    try {
      const activeTag = await getActiveTag(pool);
      activeTagId = activeTag ? activeTag.id : null;
    } catch (error) {
      activeTagId = null;
    }
  }
  const { clauses, values } = buildReportFilters(req.query, {
    includeCategory: false,
    activeTagId
  });
  const queryWithoutTech = { ...req.query };
  delete queryWithoutTech.tech;
  const { clauses: techClauses, values: techValues } = buildReportFilters(queryWithoutTech, {
    includeCategory: false,
    activeTagId
  });
  const where = clauses.length ? `WHERE ${clauses.join(' AND ')}` : '';
  const techWhere = techClauses.length ? `WHERE ${techClauses.join(' AND ')}` : '';
  const useLatest = shouldUseLatest(req.query);

  try {
    const result = useLatest
      ? await pool.query(
        `
          WITH latest AS (
            SELECT DISTINCT ON (reports.machine_key)
              reports.machine_key,
              reports.category,
              reports.technician,
              reports.last_seen
            FROM reports
            ${where}
            ORDER BY reports.machine_key, reports.last_seen DESC, reports.id DESC
          )
          SELECT
            COUNT(*) AS total,
            COUNT(*) FILTER (WHERE category = 'laptop') AS laptop,
            COUNT(*) FILTER (WHERE category = 'desktop') AS desktop,
            COUNT(*) FILTER (WHERE category = 'unknown') AS unknown
          FROM latest
        `,
        values
      )
      : await pool.query(
        `
          SELECT
            COUNT(*) AS total,
            COUNT(*) FILTER (WHERE category = 'laptop') AS laptop,
            COUNT(*) FILTER (WHERE category = 'desktop') AS desktop,
            COUNT(*) FILTER (WHERE category = 'unknown') AS unknown
          FROM reports
          ${where}
        `,
        values
      );
    const row = result.rows && result.rows[0] ? result.rows[0] : null;
    const techResult = useLatest
      ? await pool.query(
        `
          WITH latest AS (
            SELECT DISTINCT ON (reports.machine_key)
              reports.machine_key,
              reports.technician,
              reports.last_seen
            FROM reports
            ${techWhere}
            ORDER BY reports.machine_key, reports.last_seen DESC, reports.id DESC
          )
          SELECT DISTINCT technician
          FROM latest
          WHERE technician IS NOT NULL AND technician <> ''
          ORDER BY technician
        `,
        techValues
      )
      : await pool.query(
        `
          SELECT DISTINCT technician
          FROM (
            SELECT technician
            FROM reports
            ${techWhere}
          ) techs
          WHERE technician IS NOT NULL AND technician <> ''
          ORDER BY technician
        `,
        techValues
      );
    const techs = (techResult.rows || [])
      .map((item) => item.technician)
      .filter(Boolean);
    return res.json({
      ok: true,
      total: Number.parseInt(row?.total || '0', 10),
      laptop: Number.parseInt(row?.laptop || '0', 10),
      desktop: Number.parseInt(row?.desktop || '0', 10),
      unknown: Number.parseInt(row?.unknown || '0', 10),
      techs
    });
  } catch (error) {
    console.error('Failed to fetch stats', error);
    return res.status(500).json({ ok: false, error: 'db_error' });
  }
});

app.get('/api/machines', requireAuth, async (req, res) => {
  const metaOnly = req.query.meta === '1';
  const legacyFlag = req.query.legacy;
  let permissions = null;
  try {
    const user = await refreshLdapPermissions(req);
    permissions = {
      canDeleteReport: canDeleteReports(user),
      canEditTags: canEditTags(user)
    };
  } catch (error) {
    permissions = null;
  }
  try {
    if (metaOnly) {
      const includeActive = legacyFlag == null;
      const tags = await listTagsWithCounts(pool, { legacyFlag, includeActive });
      const lotRows = await listLotsWithAssignments(pool);
      const lots = lotRows.map((row) => mapLotRowForResponse(row)).filter(Boolean);
      const activeLot = lots.find(
        (lot) => lot && !lot.isPaused && Number.isFinite(lot.targetCount) && lot.producedCount < lot.targetCount
      ) || null;
      const activeTag = tags.find((tag) => tag.is_active) || null;
      return res.json({
        ok: true,
        machines: [],
        permissions,
        tags,
        activeTagId: activeTag ? activeTag.id : null,
        lots,
        activeLotId: activeLot ? activeLot.id : null
      });
    }
    const result = await pool.query(listReportsQuery);
    const tags = await listTagsWithCounts(pool);
    const lotRows = await listLotsWithAssignments(pool);
    const lots = lotRows.map((row) => mapLotRowForResponse(row)).filter(Boolean);
    const activeLot = lots.find(
      (lot) => lot && !lot.isPaused && Number.isFinite(lot.targetCount) && lot.producedCount < lot.targetCount
    ) || null;
    const activeTag = tags.find((tag) => tag.is_active) || null;
    const machines = result.rows.map((row) => {
      let components = null;
      try {
        components = row.components ? JSON.parse(row.components) : null;
      } catch (error) {
        components = null;
      }

      return {
        id: row.id,
        machineKey: row.machine_key,
        hostname: row.hostname,
        macAddress: row.mac_address,
        macAddresses: normalizeMacList(row.mac_addresses),
        serialNumber: row.serial_number,
        category: row.category,
        tag: row.tag || null,
        tagId: row.tag_id || null,
        tagName: row.tag_name || row.tag || null,
        lot: normalizeLotFromRow(row),
        model: row.model,
        vendor: row.vendor,
        technician: row.technician,
        osVersion: row.os_version,
        ramMb: row.ram_mb,
        ramSlotsTotal: row.ram_slots_total,
        ramSlotsFree: row.ram_slots_free,
        batteryHealth: row.battery_health,
        cameraStatus: row.camera_status,
        usbStatus: row.usb_status,
        keyboardStatus: row.keyboard_status,
        padStatus: row.pad_status,
        badgeReaderStatus: row.badge_reader_status,
        lastSeen: row.last_seen,
        lastIp: row.last_ip,
        comment: row.comment,
        components
      };
    });

    res.json({
      machines,
      permissions,
      tags,
      activeTagId: activeTag ? activeTag.id : null,
      lots,
      activeLotId: activeLot ? activeLot.id : null
    });
  } catch (error) {
    console.error('Failed to list machines', error);
    return res.status(500).json({ ok: false, error: 'db_error' });
  }
});

app.get('/api/lots', requireAuth, async (req, res) => {
  try {
    const lotRows = await listLotsWithAssignments(pool);
    const lots = lotRows.map((row) => mapLotRowForResponse(row)).filter(Boolean);
    const activeLot = lots.find(
      (lot) => lot && !lot.isPaused && Number.isFinite(lot.targetCount) && lot.producedCount < lot.targetCount
    ) || null;
    return res.json({
      ok: true,
      lots,
      activeLotId: activeLot ? activeLot.id : null
    });
  } catch (error) {
    console.error('Failed to list lots', error);
    return res.status(500).json({ ok: false, error: 'db_error' });
  }
});

app.post('/api/lots', requireTagEdit, async (req, res) => {
  const body = req.body && typeof req.body === 'object' ? req.body : {};
  const supplier = normalizeLotSupplier(body.supplier);
  const lotNumber = normalizeLotNumber(body.lotNumber || body.lot || body.number);
  const targetCount = normalizeLotTargetCount(body.targetCount || body.pieceCount || body.count);
  const priority = normalizeLotPriority(body.priority, LOT_PRIORITY_DEFAULT);

  if (!supplier || !lotNumber || !targetCount) {
    return res.status(400).json({ ok: false, error: 'invalid_payload' });
  }

  let client;
  try {
    client = await pool.connect();
    await client.query('BEGIN');
    await setAuditContext(client, buildAuditContext(req));
    const id = generateUuid();
    const insertResult = await client.query(
      `
        INSERT INTO lots (
          id,
          supplier,
          lot_number,
          target_count,
          produced_count,
          priority,
          is_paused,
          created_by,
          created_at,
          updated_at
        )
        VALUES ($1, $2, $3, $4, 0, $5, false, $6, NOW(), NOW())
        RETURNING
          id,
          supplier,
          lot_number,
          target_count,
          produced_count,
          priority,
          is_paused,
          created_by,
          created_at,
          updated_at
      `,
      [id, supplier, lotNumber, targetCount, priority, req.session?.user?.username || null]
    );
    await client.query('COMMIT');
    const lot = mapLotRowForResponse(insertResult.rows && insertResult.rows[0] ? insertResult.rows[0] : null);
    return res.status(201).json({ ok: true, lot });
  } catch (error) {
    if (client) {
      try {
        await client.query('ROLLBACK');
      } catch (rollbackError) {
        console.error('Failed to rollback lot create', rollbackError);
      }
    }
    if (error && error.code === '23505') {
      return res.status(409).json({ ok: false, error: 'lot_exists' });
    }
    console.error('Failed to create lot', error);
    return res.status(500).json({ ok: false, error: 'db_error' });
  } finally {
    if (client) {
      client.release();
    }
  }
});

app.put('/api/lots/:id', requireTagEdit, async (req, res) => {
  const lotId = normalizeUuid(req.params.id);
  if (!lotId) {
    return res.status(400).json({ ok: false, error: 'invalid_id' });
  }

  const body = req.body && typeof req.body === 'object' ? req.body : {};

  let client;
  try {
    client = await pool.connect();
    await client.query('BEGIN');
    await setAuditContext(client, buildAuditContext(req));
    const existing = await getLotById(client, lotId, { forUpdate: true });
    if (!existing) {
      await client.query('ROLLBACK');
      return res.status(404).json({ ok: false, error: 'not_found' });
    }

    let supplier = existing.supplier;
    if (Object.prototype.hasOwnProperty.call(body, 'supplier')) {
      supplier = normalizeLotSupplier(body.supplier);
      if (!supplier) {
        await client.query('ROLLBACK');
        return res.status(400).json({ ok: false, error: 'invalid_supplier' });
      }
    }

    let lotNumber = existing.lot_number;
    if (
      Object.prototype.hasOwnProperty.call(body, 'lotNumber') ||
      Object.prototype.hasOwnProperty.call(body, 'lot')
    ) {
      lotNumber = normalizeLotNumber(body.lotNumber || body.lot);
      if (!lotNumber) {
        await client.query('ROLLBACK');
        return res.status(400).json({ ok: false, error: 'invalid_lot_number' });
      }
    }

    let targetCount = Number.parseInt(existing.target_count || '0', 10) || 0;
    if (
      Object.prototype.hasOwnProperty.call(body, 'targetCount') ||
      Object.prototype.hasOwnProperty.call(body, 'pieceCount') ||
      Object.prototype.hasOwnProperty.call(body, 'count')
    ) {
      targetCount = normalizeLotTargetCount(body.targetCount || body.pieceCount || body.count);
      if (!targetCount) {
        await client.query('ROLLBACK');
        return res.status(400).json({ ok: false, error: 'invalid_target_count' });
      }
    }

    const priority = Object.prototype.hasOwnProperty.call(body, 'priority')
      ? normalizeLotPriority(body.priority, existing.priority)
      : Number.parseInt(existing.priority || '0', 10) || LOT_PRIORITY_DEFAULT;
    const isPaused = Object.prototype.hasOwnProperty.call(body, 'isPaused')
      ? parseBooleanFlag(body.isPaused, false)
      : Object.prototype.hasOwnProperty.call(body, 'paused')
        ? parseBooleanFlag(body.paused, false)
        : parseBooleanFlag(existing.is_paused, false);

    const updateResult = await client.query(
      `
        UPDATE lots
        SET supplier = $2,
            lot_number = $3,
            target_count = $4,
            priority = $5,
            is_paused = $6,
            updated_at = NOW()
        WHERE id = $1
        RETURNING
          id,
          supplier,
          lot_number,
          target_count,
          produced_count,
          priority,
          is_paused,
          created_by,
          created_at,
          updated_at
      `,
      [lotId, supplier, lotNumber, targetCount, priority, isPaused]
    );
    await client.query('COMMIT');
    const lot = mapLotRowForResponse(updateResult.rows && updateResult.rows[0] ? updateResult.rows[0] : null);
    return res.json({ ok: true, lot });
  } catch (error) {
    if (client) {
      try {
        await client.query('ROLLBACK');
      } catch (rollbackError) {
        console.error('Failed to rollback lot update', rollbackError);
      }
    }
    if (error && error.code === '23505') {
      return res.status(409).json({ ok: false, error: 'lot_exists' });
    }
    console.error('Failed to update lot', error);
    return res.status(500).json({ ok: false, error: 'db_error' });
  } finally {
    if (client) {
      client.release();
    }
  }
});

app.post('/api/lots/:id/assignments', requireTagEdit, async (req, res) => {
  const lotId = normalizeUuid(req.params.id);
  if (!lotId) {
    return res.status(400).json({ ok: false, error: 'invalid_id' });
  }
  const body = req.body && typeof req.body === 'object' ? req.body : {};
  const technician = cleanString(body.technician || body.username || body.user, 64);
  const technicianKey = normalizeTechKey(technician);

  if (!technician || !technicianKey) {
    return res.status(400).json({ ok: false, error: 'invalid_technician' });
  }

  let client;
  try {
    client = await pool.connect();
    await client.query('BEGIN');
    await setAuditContext(client, buildAuditContext(req));
    const lot = await getLotById(client, lotId, { forUpdate: true });
    if (!lot) {
      await client.query('ROLLBACK');
      return res.status(404).json({ ok: false, error: 'not_found' });
    }

    await client.query(
      `
        INSERT INTO lot_assignments (lot_id, technician_key, technician_name, created_at)
        VALUES ($1, $2, $3, NOW())
        ON CONFLICT (lot_id, technician_key) DO UPDATE
        SET technician_name = EXCLUDED.technician_name
      `,
      [lotId, technicianKey, technician]
    );

    const rows = await listLotsWithAssignments(client);
    await client.query('COMMIT');
    const updated = rows.map((row) => mapLotRowForResponse(row)).find((item) => item && item.id === lotId) || null;
    return res.json({ ok: true, lot: updated });
  } catch (error) {
    if (client) {
      try {
        await client.query('ROLLBACK');
      } catch (rollbackError) {
        console.error('Failed to rollback lot assignment add', rollbackError);
      }
    }
    console.error('Failed to assign technician to lot', error);
    return res.status(500).json({ ok: false, error: 'db_error' });
  } finally {
    if (client) {
      client.release();
    }
  }
});

app.delete('/api/lots/:id/assignments/:techKey', requireTagEdit, async (req, res) => {
  const lotId = normalizeUuid(req.params.id);
  const technicianKey = normalizeTechKey(req.params.techKey || '');
  if (!lotId || !technicianKey) {
    return res.status(400).json({ ok: false, error: 'invalid_params' });
  }

  let client;
  try {
    client = await pool.connect();
    await client.query('BEGIN');
    await setAuditContext(client, buildAuditContext(req));
    const lot = await getLotById(client, lotId, { forUpdate: true });
    if (!lot) {
      await client.query('ROLLBACK');
      return res.status(404).json({ ok: false, error: 'not_found' });
    }
    await client.query(
      `
        DELETE FROM lot_assignments
        WHERE lot_id = $1
          AND technician_key = $2
      `,
      [lotId, technicianKey]
    );
    const rows = await listLotsWithAssignments(client);
    await client.query('COMMIT');
    const updated = rows.map((row) => mapLotRowForResponse(row)).find((item) => item && item.id === lotId) || null;
    return res.json({ ok: true, lot: updated });
  } catch (error) {
    if (client) {
      try {
        await client.query('ROLLBACK');
      } catch (rollbackError) {
        console.error('Failed to rollback lot assignment delete', rollbackError);
      }
    }
    console.error('Failed to delete lot assignment', error);
    return res.status(500).json({ ok: false, error: 'db_error' });
  } finally {
    if (client) {
      client.release();
    }
  }
});

app.get('/api/machines/:id', requireAuth, async (req, res) => {
  const id = normalizeUuid(req.params.id);
  if (!id) {
    return res.status(400).json({ ok: false, error: 'invalid_id' });
  }

  let row;
  try {
    const result = await pool.query(getReportByIdQuery, [id]);
    row = result.rows && result.rows[0] ? result.rows[0] : null;
  } catch (error) {
    console.error('Failed to fetch machine detail', error);
    return res.status(500).json({ ok: false, error: 'db_error' });
  }

  if (!row) {
    return res.status(404).json({ ok: false, error: 'not_found' });
  }

  let components = null;
  let machineComponents = null;
  let payload = null;
  let machinePayload = null;
  let relatedReports = [];

  try {
    components = row.components ? JSON.parse(row.components) : null;
  } catch (error) {
    components = null;
  }
  try {
    machineComponents = row.machine_components ? JSON.parse(row.machine_components) : null;
  } catch (error) {
    machineComponents = null;
  }
  if (!components && machineComponents) {
    components = machineComponents;
  }
  components = withManualComponentDefaults(components);

  try {
    payload = row.payload ? JSON.parse(row.payload) : null;
  } catch (error) {
    payload = null;
  }
  try {
    machinePayload = row.machine_payload ? JSON.parse(row.machine_payload) : null;
  } catch (error) {
    machinePayload = null;
  }
  payload = mergeHardwarePayload(payload, machinePayload);

  const relatedSerial = normalizeSerial(row.serial_number);
  let relatedMac = normalizeMac(row.mac_address);
  if (!relatedMac) {
    const macList = normalizeMacList(row.mac_addresses);
    if (Array.isArray(macList) && macList.length) {
      relatedMac = macList[0];
    }
  }
  if (relatedSerial && relatedMac) {
    try {
      const reportResult = await pool.query(
        `
          SELECT id, last_seen, created_at
          FROM reports
          WHERE serial_number = $1
            AND (
              mac_address = $2
              OR mac_addresses ILIKE $3
            )
          ORDER BY last_seen DESC
          LIMIT 10
        `,
        [relatedSerial, relatedMac, `%${relatedMac}%`]
      );
      relatedReports = reportResult.rows.map((item) => ({
        id: item.id,
        lastSeen: item.last_seen,
        createdAt: item.created_at
      }));
    } catch (error) {
      relatedReports = [];
    }
  } else if (row.machine_key) {
    try {
      const reportResult = await pool.query(
        `
          SELECT id, last_seen, created_at
          FROM reports
          WHERE machine_key = $1
          ORDER BY last_seen DESC
          LIMIT 10
        `,
        [row.machine_key]
      );
      relatedReports = reportResult.rows.map((item) => ({
        id: item.id,
        lastSeen: item.last_seen,
        createdAt: item.created_at
      }));
    } catch (error) {
      relatedReports = [];
    }
  }

  const lot = normalizeLotFromRow({
    report_lot_id: row.report_lot_id,
    report_lot_supplier: row.report_lot_supplier,
    report_lot_number: row.report_lot_number,
    report_lot_target_count: row.report_lot_target_count,
    report_lot_produced_count: row.report_lot_produced_count,
    report_lot_is_paused: row.report_lot_is_paused,
    machine_lot_id: row.machine_lot_id,
    machine_lot_supplier: row.machine_lot_supplier,
    machine_lot_number: row.machine_lot_number,
    machine_lot_target_count: row.machine_lot_target_count,
    machine_lot_produced_count: row.machine_lot_produced_count,
    machine_lot_is_paused: row.machine_lot_is_paused
  });

  res.json({
    machine: {
      id: row.id,
      machineKey: row.machine_key,
      hostname: row.hostname,
      macAddress: row.mac_address,
      macAddresses: normalizeMacList(row.mac_addresses),
      serialNumber: row.serial_number,
      category: row.category,
      tag: row.tag || row.machine_tag || null,
      tagId: row.tag_id || row.machine_tag_id || null,
      tagName: row.report_tag_name || row.tag || row.machine_tag || null,
      lot,
      model: row.model,
      vendor: row.vendor,
      technician: row.technician,
      osVersion: row.os_version,
      ramMb: row.ram_mb,
      ramSlotsTotal: row.ram_slots_total,
      ramSlotsFree: row.ram_slots_free,
      batteryHealth: row.battery_health,
      cameraStatus: row.camera_status,
      usbStatus: row.usb_status,
      keyboardStatus: row.keyboard_status,
      padStatus: row.pad_status,
      badgeReaderStatus: row.badge_reader_status,
      lastSeen: row.machine_last_seen || row.report_last_seen,
      createdAt: row.machine_created_at || row.report_created_at,
      reportLastSeen: row.report_last_seen,
      reportCreatedAt: row.report_created_at,
      lastIp: row.last_ip,
      comment: row.comment,
      commentedAt: row.commented_at,
      components,
      payload,
      relatedReports
    }
  });
});

app.put('/api/machines/:id/pad', requireAuth, async (req, res) => {
  const id = normalizeUuid(req.params.id);
  if (!id) {
    return res.status(400).json({ ok: false, error: 'invalid_id' });
  }

  const rawStatus = typeof req.body?.status === 'string' ? req.body.status.trim().toLowerCase() : '';
  if (!VALID_PAD_STATUSES.has(rawStatus)) {
    return res.status(400).json({ ok: false, error: 'invalid_status' });
  }

  let client;
  try {
    client = await pool.connect();
    await client.query('BEGIN');
    await setAuditContext(client, buildAuditContext(req));
    const result = await client.query('SELECT components, machine_key FROM reports WHERE id = $1', [id]);
    const row = result.rows && result.rows[0] ? result.rows[0] : null;
    if (!row) {
      await client.query('ROLLBACK');
      return res.status(404).json({ ok: false, error: 'not_found' });
    }

    let components = {};
    try {
      components = row.components ? JSON.parse(row.components) : {};
    } catch (error) {
      components = {};
    }
    components.pad = rawStatus;

    await client.query('UPDATE reports SET pad_status = $1, components = $2 WHERE id = $3', [
      rawStatus,
      JSON.stringify(components),
      id
    ]);
    if (row.machine_key) {
      await client.query('UPDATE machines SET pad_status = $1, components = $2 WHERE machine_key = $3', [
        rawStatus,
        JSON.stringify(components),
        row.machine_key
      ]);
    }
    await client.query('COMMIT');
    return res.json({ ok: true, status: rawStatus, components });
  } catch (error) {
    if (client) {
      try {
        await client.query('ROLLBACK');
      } catch (rollbackError) {
        console.error('Failed to rollback pad status update', rollbackError);
      }
    }
    console.error('Failed to update pad status', error);
    return res.status(500).json({ ok: false, error: 'db_error' });
  } finally {
    if (client) {
      client.release();
    }
  }
});

app.put('/api/machines/:id/usb', requireAuth, async (req, res) => {
  const id = normalizeUuid(req.params.id);
  if (!id) {
    return res.status(400).json({ ok: false, error: 'invalid_id' });
  }

  const rawStatus = typeof req.body?.status === 'string' ? req.body.status.trim().toLowerCase() : '';
  if (!VALID_USB_STATUSES.has(rawStatus)) {
    return res.status(400).json({ ok: false, error: 'invalid_status' });
  }

  let client;
  try {
    client = await pool.connect();
    await client.query('BEGIN');
    await setAuditContext(client, buildAuditContext(req));
    const result = await client.query('SELECT components, machine_key FROM reports WHERE id = $1', [id]);
    const row = result.rows && result.rows[0] ? result.rows[0] : null;
    if (!row) {
      await client.query('ROLLBACK');
      return res.status(404).json({ ok: false, error: 'not_found' });
    }

    let components = {};
    try {
      components = row.components ? JSON.parse(row.components) : {};
    } catch (error) {
      components = {};
    }
    components.usb = rawStatus;

    await client.query('UPDATE reports SET usb_status = $1, components = $2 WHERE id = $3', [
      rawStatus,
      JSON.stringify(components),
      id
    ]);
    if (row.machine_key) {
      await client.query('UPDATE machines SET usb_status = $1, components = $2 WHERE machine_key = $3', [
        rawStatus,
        JSON.stringify(components),
        row.machine_key
      ]);
    }
    await client.query('COMMIT');
    return res.json({ ok: true, status: rawStatus, components });
  } catch (error) {
    if (client) {
      try {
        await client.query('ROLLBACK');
      } catch (rollbackError) {
        console.error('Failed to rollback usb status update', rollbackError);
      }
    }
    console.error('Failed to update usb status', error);
    return res.status(500).json({ ok: false, error: 'db_error' });
  } finally {
    if (client) {
      client.release();
    }
  }
});

app.put('/api/reports/:id/component', requireAuth, async (req, res) => {
  const id = normalizeUuid(req.params.id);
  if (!id) {
    return res.status(400).json({ ok: false, error: 'invalid_id' });
  }

  const rawKey = typeof req.body?.key === 'string' ? req.body.key.trim() : '';
  if (!VALID_COMPONENT_KEYS.has(rawKey)) {
    return res.status(400).json({ ok: false, error: 'invalid_component' });
  }

  const rawStatus = typeof req.body?.status === 'string' ? req.body.status.trim().toLowerCase() : '';
  const allowedStatuses = getAllowedComponentStatuses(rawKey);
  if (!allowedStatuses.has(rawStatus)) {
    return res.status(400).json({ ok: false, error: 'invalid_status' });
  }

  let client;
  try {
    client = await pool.connect();
    await client.query('BEGIN');
    await setAuditContext(client, buildAuditContext(req));
    const result = await client.query('SELECT components, machine_key FROM reports WHERE id = $1', [id]);
    const row = result.rows && result.rows[0] ? result.rows[0] : null;
    if (!row) {
      await client.query('ROLLBACK');
      return res.status(404).json({ ok: false, error: 'not_found' });
    }

    let components = {};
    try {
      components = row.components ? JSON.parse(row.components) : {};
    } catch (error) {
      components = {};
    }
    components[rawKey] = rawStatus;

    const column = COMPONENT_STATUS_COLUMNS[rawKey];
    if (column) {
      await client.query(
        `UPDATE reports SET ${column} = $1, components = $2 WHERE id = $3`,
        [rawStatus, JSON.stringify(components), id]
      );
      if (row.machine_key) {
        await client.query(
          `UPDATE machines SET ${column} = $1, components = $2 WHERE machine_key = $3`,
          [rawStatus, JSON.stringify(components), row.machine_key]
        );
      }
    } else {
      await client.query('UPDATE reports SET components = $1 WHERE id = $2', [
        JSON.stringify(components),
        id
      ]);
      if (row.machine_key) {
        await client.query('UPDATE machines SET components = $1 WHERE machine_key = $2', [
          JSON.stringify(components),
          row.machine_key
        ]);
      }
    }

    await client.query('COMMIT');
    return res.json({ ok: true, key: rawKey, status: rawStatus, components });
  } catch (error) {
    if (client) {
      try {
        await client.query('ROLLBACK');
      } catch (rollbackError) {
        console.error('Failed to rollback component update', rollbackError);
      }
    }
    console.error('Failed to update component status', error);
    return res.status(500).json({ ok: false, error: 'db_error' });
  } finally {
    if (client) {
      client.release();
    }
  }
});

app.put('/api/reports/:id/category', requireAuth, async (req, res) => {
  const id = normalizeUuid(req.params.id);
  if (!id) {
    return res.status(400).json({ ok: false, error: 'invalid_id' });
  }

  const rawValue = typeof req.body?.category === 'string' ? req.body.category.trim() : '';
  const category = normalizeCategory(rawValue);
  if (!['unknown', 'laptop', 'desktop'].includes(category)) {
    return res.status(400).json({ ok: false, error: 'invalid_category' });
  }

  let client;
  try {
    client = await pool.connect();
    await client.query('BEGIN');
    await setAuditContext(client, buildAuditContext(req));
    const result = await client.query('SELECT machine_key FROM reports WHERE id = $1', [id]);
    const row = result.rows && result.rows[0] ? result.rows[0] : null;
    if (!row) {
      await client.query('ROLLBACK');
      return res.status(404).json({ ok: false, error: 'not_found' });
    }

    await client.query('UPDATE reports SET category = $1 WHERE id = $2', [category, id]);
    if (row.machine_key) {
      await client.query('UPDATE machines SET category = $1 WHERE machine_key = $2', [
        category,
        row.machine_key
      ]);
    }

    await client.query('COMMIT');
    return res.json({ ok: true, category });
  } catch (error) {
    if (client) {
      try {
        await client.query('ROLLBACK');
      } catch (rollbackError) {
        console.error('Failed to rollback category update', rollbackError);
      }
    }
    console.error('Failed to update category', error);
    return res.status(500).json({ ok: false, error: 'db_error' });
  } finally {
    if (client) {
      client.release();
    }
  }
});

app.post('/api/machines/:id/report-zero', requireAuth, async (req, res) => {
  const id = normalizeUuid(req.params.id);
  if (!id) {
    return res.status(400).json({ ok: false, error: 'invalid_id' });
  }
  const body = req.body && typeof req.body === 'object' ? req.body : {};

  let client;
  try {
    client = await pool.connect();
    await client.query('BEGIN');
    await setAuditContext(client, buildAuditContext(req));
    const result = await client.query('SELECT * FROM reports WHERE id = $1', [id]);
    const row = result.rows && result.rows[0] ? result.rows[0] : null;
    if (!row) {
      await client.query('ROLLBACK');
      return res.status(404).json({ ok: false, error: 'not_found' });
    }

    const now = new Date().toISOString();
    const reportId = generateUuid();
    const zeroStatus = 'not_tested';
    const explicitLotId = normalizeUuid(body.lotId || body.lot_id || body.batchId || body.batch_id);
    const isDoubleCheck = true;
    const components = {
      diskReadTest: zeroStatus,
      diskWriteTest: zeroStatus,
      ramTest: zeroStatus,
      cpuTest: zeroStatus,
      gpuTest: zeroStatus,
      networkPing: zeroStatus,
      fsCheck: zeroStatus,
      gpu: zeroStatus,
      usb: zeroStatus,
      keyboard: zeroStatus,
      camera: zeroStatus,
      pad: zeroStatus,
      badgeReader: zeroStatus,
      biosBattery: zeroStatus,
      biosLanguage: zeroStatus,
      biosPassword: zeroStatus,
      wifiStandard: zeroStatus
    };
    const tests = {
      diskRead: zeroStatus,
      diskWrite: zeroStatus,
      ramTest: zeroStatus,
      cpuTest: zeroStatus,
      gpuTest: zeroStatus,
      networkPing: zeroStatus,
      networkPingTarget: '1.1.1.1',
      fsCheck: zeroStatus
    };

    let macAddresses = null;
    if (row.mac_addresses) {
      try {
        const parsed = JSON.parse(row.mac_addresses);
        if (Array.isArray(parsed)) {
          macAddresses = parsed;
        }
      } catch (error) {
        macAddresses = null;
      }
    }

    const payload = safeJsonStringify(
      {
        reportId,
        hostname: row.hostname || null,
        macAddress: row.mac_address || null,
        macAddresses: macAddresses || undefined,
        serialNumber: row.serial_number || null,
        category: row.category || null,
        technician: row.technician || null,
        vendor: row.vendor || null,
        model: row.model || null,
        osVersion: row.os_version || null,
        diag: {
          type: 'double_check',
          diagnosticsPerformed: 0,
          appVersion: 'report-zero'
        },
        tests
      },
      64 * 1024
    );

    const resolvedTag = await resolveTagForIngest(client, row.tag_id, row.tag);
    const lotResolution = await resolveLotForIngest(client, {
      explicitLotId: explicitLotId || row.lot_id || null,
      technician: row.technician || null
    });
    const resolvedLot = lotResolution.lot;
    const resolvedLotId = resolvedLot && resolvedLot.id ? resolvedLot.id : null;
    const reportValues = [
      reportId,
      row.machine_key,
      row.hostname,
      row.mac_address,
      row.mac_addresses,
      row.serial_number,
      row.category || 'unknown',
      resolvedTag.name || DEFAULT_REPORT_TAG,
      resolvedTag.id || null,
      resolvedLotId,
      row.model,
      row.vendor,
      row.technician,
      row.os_version,
      row.ram_mb,
      row.ram_slots_total,
      row.ram_slots_free,
      row.battery_health,
      zeroStatus,
      zeroStatus,
      zeroStatus,
      zeroStatus,
      zeroStatus,
      now,
      now,
      JSON.stringify(components),
      payload,
      row.last_ip
    ];

    await client.query(upsertReportQuery, reportValues);
    if (row.machine_key) {
      const machineValues = [
        row.machine_key,
        row.hostname,
        row.mac_address,
        row.mac_addresses,
        row.serial_number,
        row.category || 'unknown',
        resolvedTag.name || DEFAULT_REPORT_TAG,
        resolvedTag.id || null,
        resolvedLotId,
        row.model,
        row.vendor,
        row.technician,
        row.os_version,
        row.ram_mb,
        row.ram_slots_total,
        row.ram_slots_free,
        row.battery_health,
        zeroStatus,
        zeroStatus,
        zeroStatus,
        zeroStatus,
        zeroStatus,
        now,
        row.created_at || now,
        JSON.stringify(components),
        payload,
        row.last_ip
      ];
      await client.query(upsertMachineQuery, machineValues);
    }

    const lotProgress = await registerLotProgress(client, {
      lot: resolvedLot,
      machineKey: row.machine_key,
      reportId,
      technician: row.technician,
      source: 'report-zero-copy',
      isDoubleCheck,
      shouldCount: false
    });

    await client.query('COMMIT');
    return res.json({
      ok: true,
      reportId,
      lot: normalizeLotFromRow(lotProgress && lotProgress.lot ? lotProgress.lot : resolvedLot),
      lotCounted: false
    });
  } catch (error) {
    if (client) {
      try {
        await client.query('ROLLBACK');
      } catch (rollbackError) {
        console.error('Failed to rollback report zero', rollbackError);
      }
    }
    console.error('Failed to create report zero', error);
    return res.status(500).json({ ok: false, error: 'db_error' });
  } finally {
    if (client) {
      client.release();
    }
  }
});

app.post('/api/reports/report-zero', requireAuth, async (req, res) => {
  const body = req.body;
  if (!body || typeof body !== 'object') {
    return res.status(400).json({ ok: false, error: 'invalid_payload' });
  }

  const hostname = cleanString(body.hostname, 64);
  let macAddress = normalizeMac(body.macAddress);
  const serialNumber = normalizeSerial(body.serialNumber);
  const category = normalizeCategory(body.category);
  const tagIdRaw = body.tagId || body.tag_id || null;
  const lotIdRaw = body.lotId || body.lot_id || body.batchId || body.batch_id || null;
  const tag = cleanString(body.tag, 64);
  const model = cleanString(body.model, 64);
  const vendor = cleanString(body.vendor, 64);
  const technician = cleanString(body.technician, 64);
  const osVersion = cleanString(body.osVersion, 64);
  const isDoubleCheck = isDoubleCheckPayload(body);
  let macAddresses = normalizeMacList(body.macAddresses);

  if (macAddress && (!macAddresses || !macAddresses.includes(macAddress))) {
    macAddresses = [macAddress, ...(macAddresses || [])];
  }
  if (!macAddress && macAddresses && macAddresses.length > 0) {
    macAddress = macAddresses[0];
  }

  if (!hostname && !macAddress && !serialNumber) {
    return res.status(400).json({
      ok: false,
      error: 'missing_identifier',
      message: 'Provide at least hostname, macAddress, or serialNumber.'
    });
  }

  const machineKey = buildMachineKey(serialNumber, macAddress, hostname);

  const now = new Date().toISOString();
  const reportId = generateUuid();
  const zeroStatus = 'not_tested';
  const components = {
    diskReadTest: zeroStatus,
    diskWriteTest: zeroStatus,
    ramTest: zeroStatus,
    cpuTest: zeroStatus,
    gpuTest: zeroStatus,
    networkPing: zeroStatus,
    fsCheck: zeroStatus,
    gpu: zeroStatus,
    usb: zeroStatus,
    keyboard: zeroStatus,
    camera: zeroStatus,
    pad: zeroStatus,
    badgeReader: zeroStatus,
    biosBattery: zeroStatus,
    biosLanguage: zeroStatus,
    biosPassword: zeroStatus,
    wifiStandard: zeroStatus
  };
  const tests = {
    diskRead: zeroStatus,
    diskWrite: zeroStatus,
    ramTest: zeroStatus,
    cpuTest: zeroStatus,
    gpuTest: zeroStatus,
    networkPing: zeroStatus,
    networkPingTarget: '1.1.1.1',
    fsCheck: zeroStatus
  };

  const payload = safeJsonStringify(
    {
      reportId,
      hostname: hostname || null,
      macAddress: macAddress || null,
      macAddresses: macAddresses || undefined,
      serialNumber: serialNumber || null,
      category: category || null,
      technician: technician || null,
      vendor: vendor || null,
      model: model || null,
      osVersion: osVersion || null,
      diag: {
        type: isDoubleCheck ? 'double_check' : 'manual',
        diagnosticsPerformed: 0,
        appVersion: 'report-zero'
      },
      tests
    },
    64 * 1024
  );

  let client;
  try {
    client = await pool.connect();
    await client.query('BEGIN');
    await setAuditContext(client, buildAuditContext(req));
    const resolvedTag = await resolveTagForIngest(client, tagIdRaw, tag);
    const lotResolution = await resolveLotForIngest(client, {
      explicitLotId: lotIdRaw,
      technician
    });
    const resolvedLot = lotResolution.lot;
    const resolvedLotId = resolvedLot && resolvedLot.id ? resolvedLot.id : null;
    const shouldCountLot = Boolean(
      resolvedLotId &&
      machineKey &&
      !isDoubleCheck &&
      !parseBooleanFlag(resolvedLot ? resolvedLot.is_paused : false, false)
    );
    const reportValues = [
      reportId,
      machineKey,
      hostname,
      macAddress,
      macAddresses ? JSON.stringify(macAddresses) : null,
      serialNumber,
      category || 'unknown',
      resolvedTag.name || DEFAULT_REPORT_TAG,
      resolvedTag.id || null,
      resolvedLotId,
      model,
      vendor,
      technician,
      osVersion,
      null,
      null,
      null,
      null,
      zeroStatus,
      zeroStatus,
      zeroStatus,
      zeroStatus,
      zeroStatus,
      now,
      now,
      JSON.stringify(components),
      payload,
      getClientIp(req)
    ];

    const machineValues = [
      machineKey,
      hostname,
      macAddress,
      macAddresses ? JSON.stringify(macAddresses) : null,
      serialNumber,
      category || 'unknown',
      resolvedTag.name || DEFAULT_REPORT_TAG,
      resolvedTag.id || null,
      resolvedLotId,
      model,
      vendor,
      technician,
      osVersion,
      null,
      null,
      null,
      null,
      zeroStatus,
      zeroStatus,
      zeroStatus,
      zeroStatus,
      zeroStatus,
      now,
      now,
      JSON.stringify(components),
      payload,
      getClientIp(req)
    ];
    await client.query(upsertReportQuery, reportValues);
    await client.query(upsertMachineQuery, machineValues);
    const lotProgress = await registerLotProgress(client, {
      lot: resolvedLot,
      machineKey,
      reportId,
      technician,
      source: 'report-zero',
      isDoubleCheck,
      shouldCount: shouldCountLot
    });
    await client.query('COMMIT');
    return res.json({
      ok: true,
      reportId,
      machineKey,
      lot: normalizeLotFromRow(lotProgress && lotProgress.lot ? lotProgress.lot : resolvedLot),
      lotCounted: Boolean(lotProgress && lotProgress.counted)
    });
  } catch (error) {
    if (client) {
      try {
        await client.query('ROLLBACK');
      } catch (rollbackError) {
        console.error('Failed to rollback report zero', rollbackError);
      }
    }
    console.error('Failed to create report zero', error);
    return res.status(500).json({ ok: false, error: 'db_error' });
  } finally {
    if (client) {
      client.release();
    }
  }
});

app.delete('/api/reports/imports/legacy', requireAuth, requireReportDelete, async (req, res) => {
  let client;
  try {
    client = await pool.connect();
    await client.query('BEGIN');
    await setAuditContext(client, buildAuditContext(req, { source: 'DELETE /api/reports/imports/legacy' }));

    const result = await client.query(`
      WITH deleted_reports AS (
        DELETE FROM reports
        WHERE payload IS NOT NULL
          AND payload <> ''
          AND safe_jsonb(payload) ? 'legacy'
        RETURNING id, machine_key
      ),
      deleted_progress AS (
        DELETE FROM lot_progress lp
        USING deleted_reports dr
        WHERE lp.report_id = dr.id
        RETURNING lp.id
      )
      SELECT
        COALESCE((SELECT COUNT(*)::integer FROM deleted_reports), 0) AS deleted_reports,
        COALESCE((SELECT COUNT(DISTINCT machine_key)::integer FROM deleted_reports), 0) AS impacted_machines,
        COALESCE((SELECT COUNT(*)::integer FROM deleted_progress), 0) AS deleted_progress
    `);
    const row = result.rows && result.rows[0] ? result.rows[0] : null;

    await client.query(`
      UPDATE lots
      SET produced_count = COALESCE(progress.count, 0)
      FROM (
        SELECT lot_id, COUNT(*)::integer AS count
        FROM lot_progress
        GROUP BY lot_id
      ) progress
      WHERE progress.lot_id = lots.id
    `);
    await client.query(`
      UPDATE lots
      SET produced_count = 0
      WHERE id NOT IN (SELECT DISTINCT lot_id FROM lot_progress)
    `);

    await client.query('COMMIT');
    return res.json({
      ok: true,
      deletedReports: Number.parseInt(row?.deleted_reports || '0', 10),
      impactedMachines: Number.parseInt(row?.impacted_machines || '0', 10),
      deletedLotProgress: Number.parseInt(row?.deleted_progress || '0', 10)
    });
  } catch (error) {
    if (client) {
      try {
        await client.query('ROLLBACK');
      } catch (rollbackError) {
        console.error('Failed to rollback legacy import purge', rollbackError);
      }
    }
    console.error('Failed to purge legacy imports', error);
    return res.status(500).json({ ok: false, error: 'db_error' });
  } finally {
    if (client) {
      client.release();
    }
  }
});

app.delete('/api/reports/:id', requireAuth, requireReportDelete, async (req, res) => {
  const id = normalizeUuid(req.params.id);
  if (!id) {
    return res.status(400).json({ ok: false, error: 'invalid_id' });
  }

  let client;
  try {
    client = await pool.connect();
    await client.query('BEGIN');
    await setAuditContext(client, buildAuditContext(req));
    const result = await client.query('DELETE FROM reports WHERE id = $1 RETURNING machine_key', [
      id
    ]);
    const row = result.rows && result.rows[0] ? result.rows[0] : null;
    if (!row) {
      await client.query('ROLLBACK');
      return res.status(404).json({ ok: false, error: 'not_found' });
    }
    await client.query('COMMIT');
    return res.json({ ok: true, machineKey: row.machine_key || null });
  } catch (error) {
    if (client) {
      try {
        await client.query('ROLLBACK');
      } catch (rollbackError) {
        console.error('Failed to rollback report delete', rollbackError);
      }
    }
    console.error('Failed to delete report', error);
    return res.status(500).json({ ok: false, error: 'db_error' });
  } finally {
    if (client) {
      client.release();
    }
  }
});

app.put('/api/machines/:id/comment', requireAuth, async (req, res) => {
  const id = normalizeUuid(req.params.id);
  if (!id) {
    return res.status(400).json({ ok: false, error: 'invalid_id' });
  }

  const rawComment = typeof req.body?.comment === 'string' ? req.body.comment : '';
  const trimmed = rawComment.trim();
  const comment = trimmed ? trimmed.slice(0, 800) : null;

  let client;
  try {
    client = await pool.connect();
    await client.query('BEGIN');
    await setAuditContext(client, buildAuditContext(req));
    const result = await client.query(
      `
        UPDATE reports
        SET comment = NULLIF($1::text, ''),
            commented_at = CASE WHEN NULLIF($1::text, '') IS NULL THEN NULL ELSE NOW() END
        WHERE id = $2
        RETURNING comment, commented_at
      `,
      [comment, id]
    );
    const row = result.rows && result.rows[0] ? result.rows[0] : null;
    if (!row) {
      await client.query('ROLLBACK');
      return res.status(404).json({ ok: false, error: 'not_found' });
    }
    await client.query('COMMIT');
    return res.json({ ok: true, comment: row.comment, commentedAt: row.commented_at });
  } catch (error) {
    if (client) {
      try {
        await client.query('ROLLBACK');
      } catch (rollbackError) {
        console.error('Failed to rollback comment update', rollbackError);
      }
    }
    console.error('Failed to update comment', error);
    return res.status(500).json({ ok: false, error: 'db_error' });
  } finally {
    if (client) {
      client.release();
    }
  }
});

app.put('/api/tags/rename', requireTagEdit, async (req, res) => {
  const tagIdRaw = req.body?.tagId || req.body?.tag_id || null;
  const oldTagRaw = cleanString(req.body?.oldTag, 64);
  const newTagRaw = cleanString(req.body?.newTag || req.body?.newName, 64);
  const newTag = newTagRaw ? newTagRaw.trim() : '';

  if (!newTag) {
    return res.status(400).json({ ok: false, error: 'invalid_tag' });
  }
  if (!tagIdRaw && !oldTagRaw) {
    return res.status(400).json({ ok: false, error: 'missing_tag_id' });
  }

  let client;
  try {
    client = await pool.connect();
    await client.query('BEGIN');
    await setAuditContext(client, buildAuditContext(req));
    let tagRow = null;
    if (tagIdRaw) {
      const tagId = normalizeUuid(tagIdRaw);
      if (!tagId) {
        await client.query('ROLLBACK');
        return res.status(400).json({ ok: false, error: 'invalid_tag_id' });
      }
      const tagResult = await client.query(
        'SELECT id, name, is_active FROM tags WHERE id = $1 FOR UPDATE',
        [tagId]
      );
      tagRow = tagResult.rows && tagResult.rows[0] ? tagResult.rows[0] : null;
    } else if (oldTagRaw) {
      const tagResult = await client.query(
        'SELECT id, name, is_active FROM tags WHERE LOWER(name) = LOWER($1) LIMIT 1 FOR UPDATE',
        [oldTagRaw]
      );
      tagRow = tagResult.rows && tagResult.rows[0] ? tagResult.rows[0] : null;
    }

    if (!tagRow) {
      await client.query('ROLLBACK');
      return res.status(404).json({ ok: false, error: 'not_found' });
    }

    const conflict = await client.query(
      'SELECT id FROM tags WHERE LOWER(name) = LOWER($1) AND id <> $2 LIMIT 1',
      [newTag, tagRow.id]
    );
    if (conflict.rows && conflict.rows[0]) {
      await client.query('ROLLBACK');
      return res.status(409).json({ ok: false, error: 'tag_conflict' });
    }

    await client.query('UPDATE tags SET name = $2, updated_at = NOW() WHERE id = $1', [
      tagRow.id,
      newTag
    ]);
    const reportResult = await client.query(
      'UPDATE reports SET tag = $2 WHERE tag_id = $1',
      [tagRow.id, newTag]
    );
    const machineResult = await client.query(
      'UPDATE machines SET tag = $2 WHERE tag_id = $1',
      [tagRow.id, newTag]
    );

    let activeTag = null;
    if (tagRow.is_active) {
      await client.query('UPDATE tags SET is_active = false WHERE id = $1', [tagRow.id]);
      const existingActive = await client.query(
        'SELECT id, name FROM tags WHERE LOWER(name) = LOWER($1) LIMIT 1 FOR UPDATE',
        [DEFAULT_REPORT_TAG]
      );
      if (existingActive.rows && existingActive.rows[0]) {
        await client.query('UPDATE tags SET is_active = true, updated_at = NOW() WHERE id = $1', [
          existingActive.rows[0].id
        ]);
        activeTag = {
          id: existingActive.rows[0].id,
          name: existingActive.rows[0].name,
          is_active: true
        };
      } else {
        const newActiveId = generateUuid();
        await client.query(
          `
            INSERT INTO tags (id, name, is_active, created_at, updated_at)
            VALUES ($1, $2, true, NOW(), NOW())
          `,
          [newActiveId, DEFAULT_REPORT_TAG]
        );
        activeTag = { id: newActiveId, name: DEFAULT_REPORT_TAG, is_active: true };
      }
    }
    await client.query('COMMIT');

    const shouldRenameStorage =
      tagRow.is_active &&
      OBJECT_STORAGE_RENAME_ON_TAG &&
      newTag &&
      newTag.toLowerCase() !== DEFAULT_REPORT_TAG.toLowerCase();
    let storageRename = null;
    if (shouldRenameStorage) {
      const targetPrefix = normalizeObjectStorageSegment(newTag);
      if (targetPrefix && normalizeObjectStorageSegment(OBJECT_STORAGE_PREFIX) !== targetPrefix) {
        storageRename = await renameObjectStoragePrefix(OBJECT_STORAGE_PREFIX, targetPrefix);
      } else {
        storageRename = { ok: false, error: 'invalid_target', source: OBJECT_STORAGE_PREFIX };
      }
    }
    return res.json({
      ok: true,
      tag: { id: tagRow.id, name: newTag },
      updatedReports: reportResult.rowCount || 0,
      updatedMachines: machineResult.rowCount || 0,
      activeTag,
      storageRename
    });
  } catch (error) {
    if (client) {
      try {
        await client.query('ROLLBACK');
      } catch (rollbackError) {
        console.error('Failed to rollback tag rename', rollbackError);
      }
    }
    console.error('Failed to rename tag', error);
    return res.status(500).json({ ok: false, error: 'db_error' });
  } finally {
    if (client) {
      client.release();
    }
  }
});

app.get('/api/machines/:id/report.pdf', requireAuth, async (req, res) => {
  const id = normalizeUuid(req.params.id);
  if (!id) {
    return res.status(400).json({ ok: false, error: 'invalid_id' });
  }

  let row;
  try {
    const result = await pool.query(getReportByIdQuery, [id]);
    row = result.rows && result.rows[0] ? result.rows[0] : null;
  } catch (error) {
    console.error('Failed to fetch machine detail for PDF', error);
    return res.status(500).json({ ok: false, error: 'db_error' });
  }

  if (!row) {
    return res.status(404).json({ ok: false, error: 'not_found' });
  }

  let components = null;
  let machineComponents = null;
  let payload = null;
  let machinePayload = null;
  try {
    components = row.components ? JSON.parse(row.components) : null;
  } catch (error) {
    components = null;
  }
  try {
    machineComponents = row.machine_components ? JSON.parse(row.machine_components) : null;
  } catch (error) {
    machineComponents = null;
  }
  if (!components && machineComponents) {
    components = machineComponents;
  }

  try {
    payload = row.payload ? JSON.parse(row.payload) : null;
  } catch (error) {
    payload = null;
  }
  try {
    machinePayload = row.machine_payload ? JSON.parse(row.machine_payload) : null;
  } catch (error) {
    machinePayload = null;
  }
  payload = mergeHardwarePayload(payload, machinePayload);

  const macAddresses = normalizeMacList(row.mac_addresses);
  const macList = Array.isArray(macAddresses) ? macAddresses.filter(Boolean) : [];
  const macPrimary = row.mac_address || macList[0] || '--';
  const category = normalizeCategory(row.category);
  const title = safeString(row.hostname || row.serial_number || row.mac_address || macList[0], `Machine ${row.id}`);
  const subtitle = [row.vendor, row.model].filter(Boolean).join(' ') || 'Modele non renseigne';
  const payloadCpu = payload && payload.cpu && typeof payload.cpu === 'object' ? payload.cpu : null;
  const payloadGpu = payload && payload.gpu && typeof payload.gpu === 'object' ? payload.gpu : null;
  const diskInfoRaw = payload ? payload.disks : null;
  const diskInfo = Array.isArray(diskInfoRaw) ? diskInfoRaw : diskInfoRaw ? [diskInfoRaw] : [];
  const volumeInfoRaw = payload ? payload.volumes : null;
  const volumeInfo = Array.isArray(volumeInfoRaw) ? volumeInfoRaw : volumeInfoRaw ? [volumeInfoRaw] : [];

  const reportData = {
    id: row.id,
    title,
    subtitle,
    category,
    serialNumber: row.serial_number || '--',
    macPrimary,
    macList: macList.length ? macList.join(', ') : '--',
    osVersion: row.os_version || '--',
    lastIp: row.last_ip || '--',
    lastSeen: formatDateTime(row.machine_last_seen || row.report_last_seen),
    createdAt: formatDateTime(row.machine_created_at || row.report_created_at),
    technician: row.technician || '--',
    ramTotal: formatRam(row.ram_mb),
    ramSlots: formatSlots(row.ram_slots_free, row.ram_slots_total),
    cpuName: (payloadCpu && payloadCpu.name) || '--',
    cpuThreads: formatCpuThreads(payloadCpu),
    gpuName: (payloadGpu && payloadGpu.name) || '--',
    storageTotal: formatTotalStorage(diskInfo, volumeInfo),
    storagePrimary: formatPrimaryDisk(diskInfo, volumeInfo),
    batteryHealth: formatBatteryHealth(row.battery_health),
    wifiStandardCode: buildPdfWifiStandardCode(payload),
    cameraStatus: row.camera_status,
    usbStatus: row.usb_status,
    keyboardStatus: row.keyboard_status,
    padStatus: row.pad_status,
    badgeReaderStatus: row.badge_reader_status,
    diagnostics: buildDiagnosticsRows(payload, components),
    inventoryRows: buildInventoryRows(payload),
    components: buildComponentRows(components),
    summary: summarizeComponents(components),
    summaryForPdf: summarizePdfDetailForReport(components, payload, row.comment || ''),
    generatedAt: formatDateTime(new Date())
  };

  const filename = `rapport-atelier-${sanitizeFilename(title)}.pdf`;
  res.setHeader('Content-Type', 'application/pdf');
  res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
  res.setHeader('Cache-Control', 'no-store');

  const doc = new PDFDocument({ size: 'A4', margin: 40 });
  doc.info.Title = `Rapport atelier - ${title}`;
  doc.info.Author = 'Atelier Ops';
  doc.on('error', (error) => {
    console.error('PDF generation error', error);
  });
  doc.pipe(res);
  drawReportPdf(doc, reportData);
  doc.end();
});

app.use((err, req, res, next) => {
  if (err && err.type === 'entity.parse.failed') {
    return res.status(400).json({ ok: false, error: 'invalid_json' });
  }
  if (err && err.type === 'entity.too.large') {
    return res.status(413).json({ ok: false, error: 'payload_too_large' });
  }
  return next(err);
});

app.use((req, res) => {
  res.status(404).json({ ok: false, error: 'not_found' });
});

async function startServer() {
  try {
    await initDb();
    app.listen(PORT, () => {
      console.log(`MDT web listening on http://localhost:${PORT}`);
    });
  } catch (error) {
    console.error('Failed to initialize database', error);
    process.exit(1);
  }
}

startServer();
