const crypto = require('crypto');
const fs = require('fs');
const path = require('path');
const express = require('express');
const helmet = require('helmet');
const rateLimit = require('express-rate-limit');
const session = require('express-session');
const LdapAuth = require('ldapauth-fork');
const Database = require('better-sqlite3');

const app = express();
app.disable('x-powered-by');

const PORT = Number.parseInt(process.env.PORT || '3000', 10);
const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, 'data');
const PUBLIC_DIR = path.join(__dirname, 'public');
const DB_PATH = process.env.DATABASE_PATH || path.join(DATA_DIR, 'mdt.db');
const JSON_LIMIT = process.env.JSON_LIMIT || '256kb';
const INGEST_RATE_LIMIT = Number.parseInt(process.env.INGEST_RATE_LIMIT || '180', 10);
const SESSION_SECRET =
  process.env.SESSION_SECRET || crypto.randomBytes(32).toString('hex');
const SESSION_NAME = process.env.SESSION_NAME || 'mdt.sid';
const COOKIE_SECURE = process.env.COOKIE_SECURE === '1';
const ALLOW_LOCAL_ADMIN = process.env.ALLOW_LOCAL_ADMIN !== '0';
const LOCAL_ADMIN_USER = process.env.LOCAL_ADMIN_USER || 'admin';
const LOCAL_ADMIN_PASSWORD = process.env.LOCAL_ADMIN_PASSWORD || 'admin';
const LDAP_URL = process.env.LDAP_URL || '';
const LDAP_BIND_DN = process.env.LDAP_BIND_DN || '';
const LDAP_BIND_PASSWORD = process.env.LDAP_BIND_PASSWORD || '';
const LDAP_SEARCH_BASE = process.env.LDAP_SEARCH_BASE || '';
const LDAP_SEARCH_FILTER =
  process.env.LDAP_SEARCH_FILTER || '(sAMAccountName={{username}})';
const LDAP_SEARCH_ATTRIBUTES = (process.env.LDAP_SEARCH_ATTRIBUTES || 'dn,cn,mail')
  .split(',')
  .map((attr) => attr.trim())
  .filter(Boolean);
const LDAP_TLS_REJECT_UNAUTHORIZED =
  process.env.LDAP_TLS_REJECT_UNAUTHORIZED !== '0';
const LDAP_ENABLED = Boolean(LDAP_URL && LDAP_SEARCH_BASE);

if (!Number.isFinite(PORT)) {
  throw new Error('PORT must be a number');
}

fs.mkdirSync(DATA_DIR, { recursive: true });

const db = new Database(DB_PATH);

// Basic tuning for safe concurrent reads/writes.
db.exec(`
  PRAGMA journal_mode = WAL;
  PRAGMA synchronous = NORMAL;

  CREATE TABLE IF NOT EXISTS machines (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    machine_key TEXT NOT NULL UNIQUE,
    hostname TEXT,
    mac_address TEXT,
    serial_number TEXT,
    category TEXT NOT NULL DEFAULT 'unknown',
    model TEXT,
    vendor TEXT,
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
    last_seen TEXT NOT NULL,
    created_at TEXT NOT NULL,
    components TEXT,
    payload TEXT,
    last_ip TEXT
  );

  CREATE INDEX IF NOT EXISTS idx_machines_category ON machines(category);
  CREATE INDEX IF NOT EXISTS idx_machines_last_seen ON machines(last_seen);
`);

const existingColumns = new Set(
  db.prepare("PRAGMA table_info(machines)").all().map((column) => column.name)
);

function ensureColumn(name, type) {
  if (!existingColumns.has(name)) {
    db.exec(`ALTER TABLE machines ADD COLUMN ${name} ${type}`);
    existingColumns.add(name);
  }
}

ensureColumn('ram_mb', 'INTEGER');
ensureColumn('ram_slots_total', 'INTEGER');
ensureColumn('ram_slots_free', 'INTEGER');
ensureColumn('battery_health', 'INTEGER');
ensureColumn('camera_status', 'TEXT');
ensureColumn('usb_status', 'TEXT');
ensureColumn('keyboard_status', 'TEXT');
ensureColumn('pad_status', 'TEXT');
ensureColumn('badge_reader_status', 'TEXT');

const upsertMachine = db.prepare(`
  INSERT INTO machines (
    machine_key,
    hostname,
    mac_address,
    serial_number,
    category,
    model,
    vendor,
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
    @machine_key,
    @hostname,
    @mac_address,
    @serial_number,
    @category,
    @model,
    @vendor,
    @os_version,
    @ram_mb,
    @ram_slots_total,
    @ram_slots_free,
    @battery_health,
    @camera_status,
    @usb_status,
    @keyboard_status,
    @pad_status,
    @badge_reader_status,
    @last_seen,
    @created_at,
    @components,
    @payload,
    @last_ip
  )
  ON CONFLICT(machine_key) DO UPDATE SET
    hostname = COALESCE(excluded.hostname, machines.hostname),
    mac_address = COALESCE(excluded.mac_address, machines.mac_address),
    serial_number = COALESCE(excluded.serial_number, machines.serial_number),
    category = CASE
      WHEN excluded.category != 'unknown' THEN excluded.category
      ELSE machines.category
    END,
    model = COALESCE(excluded.model, machines.model),
    vendor = COALESCE(excluded.vendor, machines.vendor),
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
    components = COALESCE(excluded.components, machines.components),
    payload = COALESCE(excluded.payload, machines.payload),
    last_ip = excluded.last_ip
`);

const listMachines = db.prepare(`
  SELECT
    id,
    hostname,
    mac_address,
    serial_number,
    category,
    model,
    vendor,
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
    last_ip
  FROM machines
  ORDER BY last_seen DESC
`);

const getMachineById = db.prepare(`
  SELECT
    id,
    hostname,
    mac_address,
    serial_number,
    category,
    model,
    vendor,
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
  WHERE id = ?
`);

const getMachineIdByKey = db.prepare('SELECT id FROM machines WHERE machine_key = ?');

const ingestLimiter = rateLimit({
  windowMs: 60 * 1000,
  max: INGEST_RATE_LIMIT,
  standardHeaders: true,
  legacyHeaders: false
});

const LAPTOP_CHASSIS_CODES = new Set([8, 9, 10, 11, 12, 14, 18, 21, 31]);
const DESKTOP_CHASSIS_CODES = new Set([3, 4, 5, 6, 7, 15, 16]);

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

function isLocalAdmin(username, password) {
  if (!ALLOW_LOCAL_ADMIN) {
    return false;
  }
  return username === LOCAL_ADMIN_USER && password === LOCAL_ADMIN_PASSWORD;
}

function authenticateLdap(username, password) {
  if (!LDAP_ENABLED) {
    return Promise.reject(new Error('ldap_not_configured'));
  }

  const searchFilter = LDAP_SEARCH_FILTER.replace(
    '{{username}}',
    escapeLdapFilter(username)
  );
  const options = {
    url: LDAP_URL,
    searchBase: LDAP_SEARCH_BASE,
    searchFilter,
    searchAttributes: LDAP_SEARCH_ATTRIBUTES,
    reconnect: true,
    tlsOptions: {
      rejectUnauthorized: LDAP_TLS_REJECT_UNAUTHORIZED
    }
  };

  if (LDAP_BIND_DN && LDAP_BIND_PASSWORD) {
    options.bindDN = LDAP_BIND_DN;
    options.bindCredentials = LDAP_BIND_PASSWORD;
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

app.get('/', requireAuth, (req, res) => {
  res.sendFile(path.join(PUBLIC_DIR, 'index.html'));
});

app.get('/index.html', requireAuth, (req, res) => {
  res.sendFile(path.join(PUBLIC_DIR, 'index.html'));
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
  } else if (LDAP_ENABLED) {
    try {
      const ldapUser = await authenticateLdap(username, password);
      user = {
        username,
        type: 'ldap',
        displayName: ldapUser.cn || ldapUser.displayName || ldapUser.uid || username,
        dn: ldapUser.dn || null,
        mail: ldapUser.mail || null
      };
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
  } else if (LDAP_ENABLED) {
    try {
      const ldapUser = await authenticateLdap(username, password);
      user = {
        username,
        type: 'ldap',
        displayName: ldapUser.cn || ldapUser.displayName || ldapUser.uid || username,
        dn: ldapUser.dn || null,
        mail: ldapUser.mail || null
      };
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

app.post('/api/ingest', ingestLimiter, (req, res) => {
  const body = req.body;
  if (!body || typeof body !== 'object') {
    return res.status(400).json({ ok: false, error: 'invalid_payload' });
  }

  const hostname = cleanString(pickFirst(body, ['hostname', 'computerName', 'name']), 64);
  const macAddress = normalizeMac(pickFirst(body, ['macAddress', 'mac', 'mac_address']));
  const serialNumber = normalizeSerial(pickFirst(body, ['serialNumber', 'serial', 'serial_number']));
  const category = normalizeCategory(
    pickFirst(body, ['category', 'type', 'formFactor', 'chassis', 'chassisType'])
  );
  const model = cleanString(pickFirst(body, ['model', 'computerModel', 'product', 'productName']), 64);
  const vendor = cleanString(pickFirst(body, ['vendor', 'manufacturer', 'make']), 64);
  const osVersion = cleanString(pickFirst(body, ['osVersion', 'os', 'os_version']), 64);
  const components = sanitizeComponents(
    pickFirst(body, ['components', 'componentStatus', 'composants', 'etatComposants'])
  );
  const sources = [body, body.components, body.hardware];
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
  const payload = safeJsonStringify(body, 64 * 1024);
  const ipAddress = getClientIp(req);

  upsertMachine.run({
    machine_key: machineKey,
    hostname,
    mac_address: macAddress,
    serial_number: serialNumber,
    category,
    model,
    vendor,
    os_version: osVersion,
    ram_mb: ramMb,
    ram_slots_total: ramSlotsTotal,
    ram_slots_free: ramSlotsFree,
    battery_health: batteryHealth,
    camera_status: cameraStatus,
    usb_status: usbStatus,
    keyboard_status: keyboardStatus,
    pad_status: padStatus,
    badge_reader_status: badgeReaderStatus,
    last_seen: now,
    created_at: now,
    components: components ? JSON.stringify(components) : null,
    payload,
    last_ip: ipAddress
  });

  const row = getMachineIdByKey.get(machineKey);

  return res.status(200).json({
    ok: true,
    id: row ? row.id : null,
    machineKey
  });
});

app.get('/api/machines', requireAuth, (req, res) => {
  const machines = listMachines.all().map((row) => ({
    id: row.id,
    hostname: row.hostname,
    macAddress: row.mac_address,
    serialNumber: row.serial_number,
    category: row.category,
    model: row.model,
    vendor: row.vendor,
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
    lastIp: row.last_ip
  }));

  res.json({ machines });
});

app.get('/api/machines/:id', requireAuth, (req, res) => {
  const id = Number.parseInt(req.params.id, 10);
  if (!Number.isFinite(id)) {
    return res.status(400).json({ ok: false, error: 'invalid_id' });
  }

  const row = getMachineById.get(id);
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
      serialNumber: row.serial_number,
      category: row.category,
      model: row.model,
      vendor: row.vendor,
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

app.listen(PORT, () => {
  console.log(`MDT web listening on http://localhost:${PORT}`);
});
