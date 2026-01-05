const crypto = require('crypto');
const path = require('path');
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
const JSON_LIMIT = process.env.JSON_LIMIT || '256kb';
const INGEST_RATE_LIMIT = Number.parseInt(process.env.INGEST_RATE_LIMIT || '180', 10);
const SESSION_SECRET =
  process.env.SESSION_SECRET || crypto.randomBytes(32).toString('hex');
const SESSION_NAME = process.env.SESSION_NAME || 'mdt.sid';
const COOKIE_SECURE = process.env.COOKIE_SECURE === '1';
const ALLOW_LOCAL_ADMIN = process.env.ALLOW_LOCAL_ADMIN !== '0';
const LOCAL_ADMIN_USER = process.env.LOCAL_ADMIN_USER || 'admin';
const LOCAL_ADMIN_PASSWORD = process.env.LOCAL_ADMIN_PASSWORD || 'admin';
const DEFAULT_LDAP_SEARCH_FILTER = '(sAMAccountName={{username}})';
const DEFAULT_LDAP_SEARCH_ATTRIBUTES = 'dn,cn,mail';
const LDAP_URL = process.env.LDAP_URL || '';
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
const DATABASE_URL = process.env.DATABASE_URL || '';
const PGSSLMODE = (process.env.PGSSLMODE || '').toLowerCase();
const PGSSL =
  process.env.PGSSL === '1' ||
  PGSSLMODE === 'require' ||
  PGSSLMODE === 'verify-full' ||
  PGSSLMODE === 'verify-ca';
const PGSSL_REJECT_UNAUTHORIZED = process.env.PGSSL_REJECT_UNAUTHORIZED !== '0';

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
    ['last_ip', 'TEXT'],
    ['components', 'TEXT'],
    ['payload', 'TEXT'],
    ['last_seen', 'TIMESTAMPTZ'],
    ['created_at', 'TIMESTAMPTZ']
  ];

  for (const [name, type] of columns) {
    await pool.query(`ALTER TABLE machines ADD COLUMN IF NOT EXISTS ${name} ${type}`);
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
  await pool.query('CREATE INDEX IF NOT EXISTS idx_machines_last_seen ON machines(last_seen)');
}

const upsertMachineQuery = `
  INSERT INTO machines (
    machine_key,
    hostname,
    mac_address,
    mac_addresses,
    serial_number,
    category,
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
    $24
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

const listMachinesQuery = `
  SELECT
    id,
    hostname,
    mac_address,
    mac_addresses,
    serial_number,
    category,
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
    components
  FROM machines
  ORDER BY last_seen DESC
`;

const getMachineByIdQuery = `
  SELECT
    id,
    hostname,
    mac_address,
    mac_addresses,
    serial_number,
    category,
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
  WHERE id = $1
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
const STATUS_LABELS = {
  ok: 'OK',
  nok: 'NOK',
  absent: 'Absent',
  not_tested: 'Non teste',
  denied: 'Refuse',
  timeout: 'Timeout',
  scheduled: 'Planifie',
  unknown: '--'
};
const STATUS_STYLES = {
  ok: { background: '#DDF3E6', color: '#1B4C38' },
  nok: { background: '#F9D9D3', color: '#8D1F12' },
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
  badgeReader: 'Lecteur badge'
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
  'badgeReader'
];
const HIDDEN_COMPONENTS = new Set(['diskSmart', 'networkTest', 'memDiag', 'thermal']);

app.set('trust proxy', process.env.TRUST_PROXY === '1');
app.use(express.json({ limit: JSON_LIMIT }));
app.use(express.urlencoded({ extended: false, limit: '16kb' }));
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
        'connect-src': ["'self'"],
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

function isLocalAdmin(username, password) {
  if (!ALLOW_LOCAL_ADMIN) {
    return false;
  }
  return username === LOCAL_ADMIN_USER && password === LOCAL_ADMIN_PASSWORD;
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
    searchAttributes: config.searchAttributes,
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
    sanitized[cleanKey] = cleanValue || '';
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
  addStatus('diskSmart', pickFirstFromSources(sources, ['diskSmart', 'smartDisk', 'smart']));

  const tests =
    body && typeof body.tests === 'object' && !Array.isArray(body.tests) ? body.tests : null;
  if (tests) {
    addStatus('diskReadTest', tests.diskRead);
    addStatus('diskWriteTest', tests.diskWrite);
    addStatus('ramTest', tests.ramTest);
    addStatus('cpuTest', tests.cpuTest);
    addStatus('gpuTest', tests.gpuTest);
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

function getClientIp(req) {
  if (!req.ip) {
    return null;
  }
  return req.ip.startsWith('::ffff:') ? req.ip.slice(7) : req.ip;
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
  return STATUS_LABELS[key] ? key : null;
}

function summarizeComponents(components) {
  const summary = { ok: 0, nok: 0, other: 0, total: 0 };
  if (!components || typeof components !== 'object' || Array.isArray(components)) {
    return summary;
  }
  Object.values(components).forEach((value) => {
    const key = normalizeStatusKey(value);
    if (!key) {
      return;
    }
    if (key === 'ok') {
      summary.ok += 1;
    } else if (key === 'nok') {
      summary.nok += 1;
    } else {
      summary.other += 1;
    }
    summary.total += 1;
  });
  return summary;
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

function formatMetric(value, unit) {
  if (typeof value !== 'number' || !Number.isFinite(value)) {
    return null;
  }
  const rounded = value % 1 === 0 ? value.toFixed(0) : value.toFixed(1);
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

function buildDiagnosticsRows(payload) {
  const rows = [];
  const tests =
    payload && payload.tests && typeof payload.tests === 'object' && !Array.isArray(payload.tests)
      ? payload.tests
      : null;
  if (!tests) {
    return rows;
  }

  const addRow = (label, status, extra) => {
    if (status == null && !extra) {
      return;
    }
    rows.push({ label, status, extra });
  };

  addRow('Lecture disque', tests.diskRead, formatMbps(tests.diskReadMBps));
  addRow('Ecriture disque', tests.diskWrite, formatMbps(tests.diskWriteMBps));
  addRow('RAM (WinSAT)', tests.ramTest, formatMbps(tests.ramMBps));
  addRow('CPU (WinSAT)', tests.cpuTest, formatMbps(tests.cpuMBps));
  addRow('GPU (WinSAT)', tests.gpuTest, formatScore(tests.gpuScore));
  addRow('CPU (stress)', tests.cpuStress, null);
  addRow('GPU (stress)', tests.gpuStress, null);
  addRow('Ping', tests.networkPing, tests.networkPingTarget || null);
  addRow('Check disque', tests.fsCheck, null);

  return rows;
}

function buildComponentRows(components) {
  if (!components || typeof components !== 'object' || Array.isArray(components)) {
    return [];
  }
  const entries = Object.entries(components).filter(([key]) => !HIDDEN_COMPONENTS.has(key));
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
      label: COMPONENT_LABELS[key] || key,
      status: value
    }));
}

function ensureSpace(doc, height) {
  const bottom = doc.page.height - doc.page.margins.bottom;
  if (doc.y + height > bottom) {
    doc.addPage();
  }
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
  ensureSpace(doc, 28);
  doc.fontSize(12).fillColor('#1D211F').text(title, margin, doc.y, { width });
  doc.moveDown(0.2);
  doc
    .moveTo(margin, doc.y)
    .lineTo(margin + width, doc.y)
    .strokeColor('#E0E0E0')
    .stroke();
  doc.moveDown(0.6);
  doc.x = margin;
}

function drawKeyValueGrid(doc, rows, columns = 2) {
  const margin = doc.page.margins.left;
  const width = doc.page.width - margin - doc.page.margins.right;
  const colWidth = width / columns;
  const rowHeight = 26;
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

  doc.y = startY + totalRows * rowHeight + 6;
  doc.x = margin;
}

function drawStatusRows(doc, rows) {
  const margin = doc.page.margins.left;
  const width = doc.page.width - margin - doc.page.margins.right;
  rows.forEach((row) => {
    ensureSpace(doc, 20);
    const y = doc.y;
    const statusKey = normalizeStatusKey(row.status) || 'unknown';
    const statusLabel = STATUS_LABELS[statusKey] || STATUS_LABELS.unknown;
    const extraText = row.extra ? ` ${row.extra}` : '';
    const pillText = `${statusLabel}${extraText}`.trim();
    doc.fontSize(10).fillColor('#1D211F').text(row.label, margin, y, {
      width: width * 0.62
    });
    doc.fontSize(9);
    const pillWidth = doc.widthOfString(pillText) + 12;
    const pillX = margin + width - pillWidth;
    drawPill(doc, pillX, y, pillText, STATUS_STYLES[statusKey] || STATUS_STYLES.unknown);
    doc.moveDown(1);
    doc.x = margin;
  });
}

function drawReportPdf(doc, data) {
  const margin = doc.page.margins.left;
  const width = doc.page.width - margin - doc.page.margins.right;

  doc.rect(0, 0, doc.page.width, 72).fill('#1D211F');
  doc.fillColor('#FFFFFF');
  doc.fontSize(9).text('MDT Live Ops', margin, 16);
  doc.fontSize(18).text(data.title, margin, 30, { width });
  doc.fontSize(10).text(data.subtitle, margin, 52, { width });
  doc.fontSize(9).text(`Genere: ${data.generatedAt}`, margin, 16, { width, align: 'right' });

  doc.fillColor('#1D211F');
  doc.y = 86;

  let x = margin;
  x += drawPill(doc, x, doc.y, CATEGORY_LABELS[data.category] || CATEGORY_LABELS.unknown, {
    background: '#E7F2EA',
    color: '#1B4C38'
  });
  x += 8;
  if (data.summary.total > 0) {
    x += drawPill(doc, x, doc.y, `OK ${data.summary.ok}`, STATUS_STYLES.ok);
    x += 6;
    x += drawPill(doc, x, doc.y, `NOK ${data.summary.nok}`, STATUS_STYLES.nok);
    x += 6;
    x += drawPill(doc, x, doc.y, `NT ${data.summary.other}`, STATUS_STYLES.unknown);
  }

  doc.moveDown(1.4);

  drawSectionTitle(doc, 'Identifiants');
  drawKeyValueGrid(doc, [
    { label: 'Serial', value: data.serialNumber },
    { label: 'MAC', value: data.macPrimary },
    { label: 'MACs', value: data.macList },
    { label: 'OS', value: data.osVersion },
    { label: 'IP', value: data.lastIp },
    { label: 'Dernier passage', value: data.lastSeen },
    { label: 'Premier passage', value: data.createdAt }
  ]);

  drawSectionTitle(doc, 'Materiel clef');
  drawKeyValueGrid(doc, [
    { label: 'Categorie', value: CATEGORY_LABELS[data.category] || CATEGORY_LABELS.unknown },
    { label: 'Technicien', value: data.technician },
    { label: 'RAM totale', value: data.ramTotal },
    { label: 'Slots RAM', value: data.ramSlots },
    { label: 'CPU', value: data.cpuName },
    { label: 'Coeurs / Threads', value: data.cpuThreads },
    { label: 'GPU', value: data.gpuName },
    { label: 'Stockage total', value: data.storageTotal },
    { label: 'Disque principal', value: data.storagePrimary },
    { label: 'Batterie', value: data.batteryHealth },
    { label: 'Camera', value: STATUS_LABELS[normalizeStatusKey(data.cameraStatus) || 'unknown'] },
    { label: 'USB', value: STATUS_LABELS[normalizeStatusKey(data.usbStatus) || 'unknown'] },
    { label: 'Clavier', value: STATUS_LABELS[normalizeStatusKey(data.keyboardStatus) || 'unknown'] },
    { label: 'Pave tactile', value: STATUS_LABELS[normalizeStatusKey(data.padStatus) || 'unknown'] },
    { label: 'Lecteur badge', value: STATUS_LABELS[normalizeStatusKey(data.badgeReaderStatus) || 'unknown'] }
  ], 2);

  drawSectionTitle(doc, 'Diagnostics');
  if (data.diagnostics.length) {
    drawStatusRows(doc, data.diagnostics);
  } else {
    doc.fontSize(10).fillColor('#6B6F6C').text('Aucun test disponible.');
    doc.moveDown(0.6);
  }

  drawSectionTitle(doc, 'Etat des composants');
  if (data.components.length) {
    drawStatusRows(doc, data.components);
  } else {
    doc.fontSize(10).fillColor('#6B6F6C').text('Aucun statut de composant.');
  }
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
  if (isLocalAdmin(username, password)) {
    user = { username, type: 'local' };
  } else {
    try {
      const ldapConfig = await getEffectiveLdapConfig();
      if (ldapConfig.enabled && ldapConfig.url && ldapConfig.searchBase) {
        const ldapUser = await authenticateLdap(username, password, ldapConfig);
        user = {
          username,
          type: 'ldap',
          displayName: ldapUser.cn || ldapUser.displayName || ldapUser.uid || username,
          dn: ldapUser.dn || null,
          mail: ldapUser.mail || null
        };
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
  if (isLocalAdmin(username, password)) {
    user = { username, type: 'local' };
  } else {
    try {
      const ldapConfig = await getEffectiveLdapConfig();
      if (ldapConfig.enabled && ldapConfig.url && ldapConfig.searchBase) {
        const ldapUser = await authenticateLdap(username, password, ldapConfig);
        user = {
          username,
          type: 'ldap',
          displayName: ldapUser.cn || ldapUser.displayName || ldapUser.uid || username,
          dn: ldapUser.dn || null,
          mail: ldapUser.mail || null
        };
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

app.use(express.static(PUBLIC_DIR, { extensions: ['html'], index: false }));

app.get('/api/health', (req, res) => {
  res.json({ ok: true });
});

app.get('/api/me', requireAuth, (req, res) => {
  res.json({ ok: true, user: req.session.user });
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

  await pool.query(
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
  const sources = [body, body.components, body.hardware];
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

  const machineKey = serialNumber
    ? `sn:${serialNumber}`
    : macAddress
      ? `mac:${macAddress}`
      : `host:${hostname.toLowerCase()}`;

  const now = new Date().toISOString();
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

  const values = [
    machineKey,
    hostname,
    macAddress,
    macAddresses ? JSON.stringify(macAddresses) : null,
    serialNumber,
    category,
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

  try {
    const result = await pool.query(upsertMachineQuery, values);
    const id = result.rows && result.rows[0] ? result.rows[0].id : null;
    return res.status(200).json({
      ok: true,
      id,
      machineKey
    });
  } catch (error) {
    console.error('Failed to ingest payload', error);
    return res.status(500).json({ ok: false, error: 'db_error' });
  }
});

app.get('/api/machines', requireAuth, async (req, res) => {
  try {
    const result = await pool.query(listMachinesQuery);
    const machines = result.rows.map((row) => {
      let components = null;
      try {
        components = row.components ? JSON.parse(row.components) : null;
      } catch (error) {
        components = null;
      }

      return {
      id: row.id,
      hostname: row.hostname,
      macAddress: row.mac_address,
      macAddresses: normalizeMacList(row.mac_addresses),
      serialNumber: row.serial_number,
      category: row.category,
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
      components
      };
    });

    res.json({ machines });
  } catch (error) {
    console.error('Failed to list machines', error);
    return res.status(500).json({ ok: false, error: 'db_error' });
  }
});

app.get('/api/machines/:id', requireAuth, async (req, res) => {
  const id = Number.parseInt(req.params.id, 10);
  if (!Number.isFinite(id)) {
    return res.status(400).json({ ok: false, error: 'invalid_id' });
  }

  let row;
  try {
    const result = await pool.query(getMachineByIdQuery, [id]);
    row = result.rows && result.rows[0] ? result.rows[0] : null;
  } catch (error) {
    console.error('Failed to fetch machine detail', error);
    return res.status(500).json({ ok: false, error: 'db_error' });
  }

  if (!row) {
    return res.status(404).json({ ok: false, error: 'not_found' });
  }

  let components = null;
  let payload = null;

  try {
    components = row.components ? JSON.parse(row.components) : null;
  } catch (error) {
    components = null;
  }

  try {
    payload = row.payload ? JSON.parse(row.payload) : null;
  } catch (error) {
    payload = null;
  }

  res.json({
    machine: {
      id: row.id,
      hostname: row.hostname,
      macAddress: row.mac_address,
      macAddresses: normalizeMacList(row.mac_addresses),
      serialNumber: row.serial_number,
      category: row.category,
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
      createdAt: row.created_at,
      lastIp: row.last_ip,
      components,
      payload
    }
  });
});

app.get('/api/machines/:id/report.pdf', requireAuth, async (req, res) => {
  const id = Number.parseInt(req.params.id, 10);
  if (!Number.isFinite(id)) {
    return res.status(400).json({ ok: false, error: 'invalid_id' });
  }

  let row;
  try {
    const result = await pool.query(getMachineByIdQuery, [id]);
    row = result.rows && result.rows[0] ? result.rows[0] : null;
  } catch (error) {
    console.error('Failed to fetch machine detail for PDF', error);
    return res.status(500).json({ ok: false, error: 'db_error' });
  }

  if (!row) {
    return res.status(404).json({ ok: false, error: 'not_found' });
  }

  let components = null;
  let payload = null;
  try {
    components = row.components ? JSON.parse(row.components) : null;
  } catch (error) {
    components = null;
  }
  try {
    payload = row.payload ? JSON.parse(row.payload) : null;
  } catch (error) {
    payload = null;
  }

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
    lastSeen: formatDateTime(row.last_seen),
    createdAt: formatDateTime(row.created_at),
    technician: row.technician || '--',
    ramTotal: formatRam(row.ram_mb),
    ramSlots: formatSlots(row.ram_slots_free, row.ram_slots_total),
    cpuName: (payloadCpu && payloadCpu.name) || '--',
    cpuThreads: formatCpuThreads(payloadCpu),
    gpuName: (payloadGpu && payloadGpu.name) || '--',
    storageTotal: formatTotalStorage(diskInfo, volumeInfo),
    storagePrimary: formatPrimaryDisk(diskInfo, volumeInfo),
    batteryHealth: formatBatteryHealth(row.battery_health),
    cameraStatus: row.camera_status,
    usbStatus: row.usb_status,
    keyboardStatus: row.keyboard_status,
    padStatus: row.pad_status,
    badgeReaderStatus: row.badge_reader_status,
    diagnostics: buildDiagnosticsRows(payload),
    components: buildComponentRows(components),
    summary: summarizeComponents(components),
    generatedAt: formatDateTime(new Date())
  };

  const filename = `mdt-report-${sanitizeFilename(title)}.pdf`;
  res.setHeader('Content-Type', 'application/pdf');
  res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
  res.setHeader('Cache-Control', 'no-store');

  const doc = new PDFDocument({ size: 'A4', margin: 40 });
  doc.info.Title = `Rapport MDT - ${title}`;
  doc.info.Author = 'MDT Web';
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
