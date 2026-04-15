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
const archiver = require('archiver');
const helmet = require('helmet');
const rateLimit = require('express-rate-limit');
const session = require('express-session');
const LdapAuth = require('ldapauth-fork');
const { ConfidentialClientApplication } = require('@azure/msal-node');
const PDFDocument = require('pdfkit');
const { Pool } = require('pg');
const bwipjs = require('bwip-js');

const app = express();
app.disable('x-powered-by');

const PORT = Number.parseInt(process.env.PORT || '3000', 10);
const PUBLIC_DIR = path.join(__dirname, 'public');
const BRAND_LOGO_PATH = path.join(PUBLIC_DIR, 'logo.png');
const JSON_LIMIT = process.env.JSON_LIMIT || '2mb';
const INGEST_RATE_LIMIT = Number.parseInt(process.env.INGEST_RATE_LIMIT || '180', 10);
const SESSION_SECRET =
  process.env.SESSION_SECRET || crypto.randomBytes(32).toString('hex');
const SESSION_NAME = process.env.SESSION_NAME || 'mdt.sid';
const COOKIE_SECURE = process.env.COOKIE_SECURE === '1';
const FORCE_HTTPS = process.env.FORCE_HTTPS === '1';
const FORCE_HTTPS_HEALTHCHECK_BYPASS = process.env.FORCE_HTTPS_HEALTHCHECK_BYPASS !== '0';
const FORCE_HTTPS_ALLOW_HTTP_INGEST = process.env.FORCE_HTTPS_ALLOW_HTTP_INGEST !== '0';
const FORCE_HTTPS_REDIRECT_CODE = Number.parseInt(process.env.FORCE_HTTPS_REDIRECT_CODE || '308', 10);
const HTTPS_PUBLIC_ORIGIN = (process.env.HTTPS_PUBLIC_ORIGIN || '').trim();
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
const WEEKLY_RECAP_ENABLED = process.env.WEEKLY_RECAP_ENABLED !== '0';
const WEEKLY_RECAP_RECIPIENTS_RAW =
  process.env.WEEKLY_RECAP_RECIPIENTS || process.env.SUGGESTION_EMAIL_TO || '';
const WEEKLY_RECAP_FROM = process.env.WEEKLY_RECAP_FROM || '';
const WEEKLY_RECAP_DAY_RAW = (process.env.WEEKLY_RECAP_DAY || 'monday').trim().toLowerCase();
const WEEKLY_RECAP_HOUR_RAW = Number.parseInt(process.env.WEEKLY_RECAP_HOUR || '7', 10);
const WEEKLY_RECAP_MINUTE_RAW = Number.parseInt(process.env.WEEKLY_RECAP_MINUTE || '30', 10);
const WEEKLY_RECAP_TIMEZONE = (process.env.WEEKLY_RECAP_TIMEZONE || 'Europe/Paris').trim() || 'Europe/Paris';
const APP_TIMEZONE = (process.env.APP_TIMEZONE || WEEKLY_RECAP_TIMEZONE || 'Europe/Paris').trim() || 'Europe/Paris';
const WEEKLY_RECAP_BATTERY_THRESHOLD_RAW = Number.parseInt(
  process.env.WEEKLY_RECAP_BATTERY_THRESHOLD || '75',
  10
);
const WEEKLY_RECAP_CHECK_INTERVAL_MS = Number.parseInt(
  process.env.WEEKLY_RECAP_CHECK_INTERVAL_MS || '300000',
  10
);
const OPERATOR_TECHNICIAN_SCOPE_ENABLED =
  String(process.env.OPERATOR_TECHNICIAN_SCOPE_ENABLED || '0').trim() === '1';
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
const ARTIFACT_UPLOAD_LIMIT = process.env.ARTIFACT_UPLOAD_LIMIT || '64mb';
const ARTIFACT_RELAY_ROOT =
  process.env.ARTIFACT_RELAY_ROOT || path.join(os.tmpdir(), 'mdt-web-artifact-relay');
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
const MICROSOFT_ENTRA_TENANT_ID = (process.env.MICROSOFT_ENTRA_TENANT_ID || '').trim();
const MICROSOFT_ENTRA_CLIENT_ID = (process.env.MICROSOFT_ENTRA_CLIENT_ID || '').trim();
const MICROSOFT_ENTRA_CLIENT_SECRET = process.env.MICROSOFT_ENTRA_CLIENT_SECRET || '';
const MICROSOFT_ENTRA_REDIRECT_URI = (process.env.MICROSOFT_ENTRA_REDIRECT_URI || '').trim();
const MICROSOFT_ADMIN_EMAILS_RAW = process.env.MICROSOFT_ADMIN_EMAILS || '';
const MICROSOFT_ADMIN_ROLE = (process.env.MICROSOFT_ADMIN_ROLE || '').trim();
const MICROSOFT_READER_GROUP_IDS_RAW = process.env.MICROSOFT_READER_GROUP_IDS || '';
const MICROSOFT_OPERATOR_GROUP_IDS_RAW = process.env.MICROSOFT_OPERATOR_GROUP_IDS || '';
const MICROSOFT_LOGISTICS_GROUP_IDS_RAW = process.env.MICROSOFT_LOGISTICS_GROUP_IDS || '';
const MICROSOFT_ADMIN_GROUP_IDS_RAW = process.env.MICROSOFT_ADMIN_GROUP_IDS || '';
const MICROSOFT_PLATFORM_ADMIN_GROUP_IDS_RAW = process.env.MICROSOFT_PLATFORM_ADMIN_GROUP_IDS || '';
const MDT_BETA_AGENT_TOKEN = String(process.env.MDT_BETA_AGENT_TOKEN || '').trim();
const MDT_BETA_AUTOMATION_ENABLED = Boolean(MDT_BETA_AGENT_TOKEN);
const MDT_BETA_DEFAULT_SOURCE_TASK_SEQUENCE_ID = String(
  process.env.MDT_BETA_DEFAULT_SOURCE_TASK_SEQUENCE_ID || 'MDT-AUTO'
).trim();
const MDT_BETA_GROUP_NAME = String(process.env.MDT_BETA_GROUP_NAME || 'MMA Beta').trim() || 'MMA Beta';
const MDT_BETA_SCRIPTS_FOLDER = String(process.env.MDT_BETA_SCRIPTS_FOLDER || 'beta').trim() || 'beta';
const MDT_BETA_JOB_RUNNING_TIMEOUT_MS = Math.max(
  60000,
  Number.parseInt(process.env.MDT_BETA_JOB_RUNNING_TIMEOUT_MS || '1800000', 10) || 1800000
);
const MICROSOFT_SSO_ENABLED = Boolean(
  MICROSOFT_ENTRA_TENANT_ID && MICROSOFT_ENTRA_CLIENT_ID && MICROSOFT_ENTRA_CLIENT_SECRET
);
const MICROSOFT_AUTH_SCOPES = Object.freeze(['openid', 'profile', 'email', 'User.Read']);
const MICROSOFT_GRAPH_ME_CHECK_MEMBER_OBJECTS_URL =
  'https://graph.microsoft.com/v1.0/me/checkMemberObjects';
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
const PDF_BATCH_EXPORT_LIMIT = Number.parseInt(process.env.PDF_BATCH_EXPORT_LIMIT || '120', 10);
const LOT_TARGET_COUNT_MIN = 1;
const LOT_TARGET_COUNT_MAX = Number.parseInt(process.env.LOT_TARGET_COUNT_MAX || '50000', 10);
const LOT_PRIORITY_DEFAULT = Number.parseInt(process.env.LOT_PRIORITY_DEFAULT || '100', 10);
const LOT_PRIORITY_MIN = 1;
const LOT_PRIORITY_MAX = Number.parseInt(process.env.LOT_PRIORITY_MAX || '9999', 10);
const ALERT_BATTERY_THRESHOLD = Math.max(
  1,
  Math.min(
    100,
    Number.parseInt(
      process.env.BATTERY_ALERT_THRESHOLD || process.env.WEEKLY_RECAP_BATTERY_THRESHOLD || '75',
      10
    ) || 75
  )
);
const BIOS_CLOCK_DRIFT_ALERT_THRESHOLD_SECONDS = Math.max(
  60,
  Number.parseInt(process.env.BIOS_CLOCK_DRIFT_ALERT_THRESHOLD_SECONDS || '300', 10) || 300
);
const BIOS_CLOCK_DELTA_ALERT_THRESHOLD_SECONDS = Math.max(
  60,
  Number.parseInt(process.env.BIOS_CLOCK_DELTA_ALERT_THRESHOLD_SECONDS || '300', 10) || 300
);
const BIOS_CLOCK_BACKWARD_GRACE_SECONDS = 60;
const CLOCK_ALERT_REASON_ORDER = Object.freeze([
  'clock_backwards',
  'clock_drift',
  'delta_mismatch'
]);

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

const ACCESS_LEVELS = Object.freeze({
  reader: 'reader',
  operator: 'operator',
  logistics: 'logistics',
  admin: 'admin',
  platformAdmin: 'platform_admin'
});

const ACCESS_LEVEL_RANKS = Object.freeze({
  [ACCESS_LEVELS.reader]: 0,
  [ACCESS_LEVELS.operator]: 10,
  [ACCESS_LEVELS.logistics]: 20,
  [ACCESS_LEVELS.admin]: 30,
  [ACCESS_LEVELS.platformAdmin]: 40
});

function normalizeAccessLevel(value) {
  const raw = String(value || '')
    .trim()
    .toLowerCase();
  if (raw === ACCESS_LEVELS.operator) {
    return ACCESS_LEVELS.operator;
  }
  if (raw === ACCESS_LEVELS.logistics) {
    return ACCESS_LEVELS.logistics;
  }
  if (raw === ACCESS_LEVELS.admin) {
    return ACCESS_LEVELS.admin;
  }
  if (raw === ACCESS_LEVELS.platformAdmin) {
    return ACCESS_LEVELS.platformAdmin;
  }
  return ACCESS_LEVELS.reader;
}

function accessLevelRank(value) {
  return ACCESS_LEVEL_RANKS[normalizeAccessLevel(value)] || 0;
}

function maxAccessLevel(currentLevel, nextLevel) {
  return accessLevelRank(nextLevel) > accessLevelRank(currentLevel)
    ? normalizeAccessLevel(nextLevel)
    : normalizeAccessLevel(currentLevel);
}

function normalizeDirectoryObjectId(value) {
  const normalized = String(value || '')
    .trim()
    .toLowerCase();
  return /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(normalized)
    ? normalized
    : '';
}

function parseDirectoryObjectIdList(raw) {
  return String(raw || '')
    .split(/[\s,;]+/)
    .map((entry) => normalizeDirectoryObjectId(entry))
    .filter(Boolean);
}

const MICROSOFT_GROUP_IDS = Object.freeze({
  [ACCESS_LEVELS.reader]: parseDirectoryObjectIdList(MICROSOFT_READER_GROUP_IDS_RAW),
  [ACCESS_LEVELS.operator]: parseDirectoryObjectIdList(MICROSOFT_OPERATOR_GROUP_IDS_RAW),
  [ACCESS_LEVELS.logistics]: parseDirectoryObjectIdList(MICROSOFT_LOGISTICS_GROUP_IDS_RAW),
  [ACCESS_LEVELS.admin]: parseDirectoryObjectIdList(MICROSOFT_ADMIN_GROUP_IDS_RAW),
  [ACCESS_LEVELS.platformAdmin]: parseDirectoryObjectIdList(MICROSOFT_PLATFORM_ADMIN_GROUP_IDS_RAW)
});

function getConfiguredMicrosoftGroupIds() {
  return Array.from(
    new Set(
      Object.values(MICROSOFT_GROUP_IDS)
        .flat()
        .map((value) => normalizeDirectoryObjectId(value))
        .filter(Boolean)
    )
  );
}

function buildPermissionSet(overrides = {}) {
  const canCreateReportZero = overrides.canCreateReportZero === true;
  const canEditReports = overrides.canEditReports === true;
  const canEditBatteryHealth = overrides.canEditBatteryHealth === true;
  const canEditTechnician = overrides.canEditTechnician === true;
  const canImportManualCsv = overrides.canImportManualCsv === true;
  const canManageLots = overrides.canManageLots === true;
  const canManagePallets = overrides.canManagePallets === true;
  const canExportBatchReports = overrides.canExportBatchReports === true;
  const canRenameTags = overrides.canRenameTags === true;
  const canDeleteReport = overrides.canDeleteReport === true;
  const canPurgeLegacyImports = overrides.canPurgeLegacyImports === true;
  const canManageLdap = overrides.canManageLdap === true;
  const canManageLogistics = canManageLots || canManagePallets || canExportBatchReports;
  return {
    canCreateReportZero,
    canEditReports,
    canEditBatteryHealth,
    canEditTechnician,
    canImportManualCsv,
    canManageLots,
    canManagePallets,
    canManageLogistics,
    canExportBatchReports,
    canRenameTags,
    canDeleteReport,
    canPurgeLegacyImports,
    canManageLdap,
    canAccessAdminPage: canManageLdap,
    canEditTags: canManageLots || canManagePallets || canRenameTags
  };
}

function buildPermissionsForAccessLevel(accessLevel) {
  switch (normalizeAccessLevel(accessLevel)) {
    case ACCESS_LEVELS.platformAdmin:
      return buildPermissionSet({
        canCreateReportZero: true,
        canEditReports: true,
        canEditBatteryHealth: true,
        canEditTechnician: true,
        canImportManualCsv: true,
        canManageLots: true,
        canManagePallets: true,
        canExportBatchReports: true,
        canRenameTags: true,
        canDeleteReport: true,
        canPurgeLegacyImports: true,
        canManageLdap: true
      });
    case ACCESS_LEVELS.admin:
      return buildPermissionSet({
        canCreateReportZero: true,
        canEditReports: true,
        canEditBatteryHealth: true,
        canEditTechnician: true,
        canImportManualCsv: true,
        canManageLots: true,
        canManagePallets: true,
        canExportBatchReports: true,
        canRenameTags: true,
        canDeleteReport: true,
        canPurgeLegacyImports: true
      });
    case ACCESS_LEVELS.logistics:
      return buildPermissionSet({
        canCreateReportZero: true,
        canEditReports: true,
        canImportManualCsv: true,
        canManageLots: true,
        canManagePallets: true,
        canExportBatchReports: true
      });
    case ACCESS_LEVELS.operator:
      return buildPermissionSet({
        canCreateReportZero: true,
        canEditReports: true,
        canImportManualCsv: true
      });
    default:
      return buildPermissionSet();
  }
}

function normalizePermissionSet(raw) {
  if (!raw || typeof raw !== 'object') {
    return buildPermissionSet();
  }
  return buildPermissionSet(raw);
}

function getUserPermissions(user) {
  if (!user || typeof user !== 'object') {
    return buildPermissionSet();
  }
  if (user.type === 'local') {
    return buildPermissionsForAccessLevel(ACCESS_LEVELS.platformAdmin);
  }
  return normalizePermissionSet(user.permissions);
}

function hasPermission(user, permissionKey) {
  if (!permissionKey) {
    return false;
  }
  return getUserPermissions(user)[permissionKey] === true;
}

function normalizeIdentityLabel(value, maxLength = 128) {
  const cleaned = cleanString(value, maxLength);
  if (!cleaned) {
    return null;
  }
  const normalized = cleaned.replace(/[._-]+/g, ' ').replace(/\s+/g, ' ').trim();
  return normalized ? normalized.slice(0, maxLength) : null;
}

function extractPrimaryTechnicianLabel(value) {
  const normalized = normalizeIdentityLabel(value, 128);
  if (!normalized) {
    return null;
  }
  const primary = normalized.split(' ').find(Boolean) || normalized;
  return cleanString(primary, 64);
}

function getUserTechnicianScope(user) {
  if (!user || typeof user !== 'object') {
    return null;
  }
  if (!OPERATOR_TECHNICIAN_SCOPE_ENABLED) {
    return null;
  }
  if (normalizeAccessLevel(user.accessLevel) !== ACCESS_LEVELS.operator) {
    return null;
  }

  const displayLabel = normalizeIdentityLabel(user.displayName, 128);
  const email = normalizeEmailAddress(user.mail || user.username);
  const emailLocal = email ? normalizeIdentityLabel(email.split('@')[0], 128) : null;
  const usernameLabel = normalizeIdentityLabel(user.username, 128);
  const preferredPrimary =
    extractPrimaryTechnicianLabel(displayLabel) ||
    extractPrimaryTechnicianLabel(emailLocal) ||
    extractPrimaryTechnicianLabel(usernameLabel) ||
    cleanString(user.displayName || user.username || user.mail, 64) ||
    'Operateur';

  const candidates = [
    preferredPrimary,
    displayLabel,
    emailLocal,
    usernameLabel
  ].filter(Boolean);

  const dedupe = new Map();
  candidates.forEach((candidate) => {
    const key = normalizeTechKey(candidate);
    if (!key || dedupe.has(key)) {
      return;
    }
    dedupe.set(key, cleanString(candidate, 128) || candidate);
  });

  const technicianLabels = Array.from(dedupe.values());
  const technicianKeys = technicianLabels
    .map((label) => normalizeTechKey(label))
    .filter(Boolean);
  const primaryLabel = technicianLabels[0] || preferredPrimary;
  const primaryKey = normalizeTechKey(primaryLabel);

  return {
    restricted: true,
    primaryLabel,
    primaryKey: primaryKey || '',
    technicianLabels,
    technicianKeys
  };
}

function getForcedReportTechKeys(user) {
  const scope = getUserTechnicianScope(user);
  return scope ? scope.technicianKeys : null;
}

function canUserAccessReportTechnician(user, technician) {
  const scope = getUserTechnicianScope(user);
  if (!scope) {
    return true;
  }
  if (!scope.technicianKeys.length) {
    return false;
  }
  const technicianKey = normalizeTechKey(technician || '');
  return Boolean(technicianKey && scope.technicianKeys.includes(technicianKey));
}

function buildClientUserPayload(user) {
  if (!user || typeof user !== 'object') {
    return user || null;
  }
  return {
    ...user,
    permissions: getUserPermissions(user),
    operatorScope: getUserTechnicianScope(user)
  };
}

async function getScopedReportRowById(
  client,
  reportId,
  user,
  {
    columns = 'id, machine_key, technician',
    forUpdate = false
  } = {}
) {
  const scope = getUserTechnicianScope(user);
  const params = [reportId];
  const clauses = ['id = $1'];

  if (scope) {
    if (!scope.technicianKeys.length) {
      return null;
    }
    clauses.push(`${normalizeTextSql('technician')} = ANY($2::text[])`);
    params.push(scope.technicianKeys);
  }

  const lockClause = forUpdate ? ' FOR UPDATE' : '';
  const result = await client.query(
    `SELECT ${columns} FROM reports WHERE ${clauses.join(' AND ')}${lockClause}`,
    params
  );
  return result.rows && result.rows[0] ? result.rows[0] : null;
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

function normalizeObjectStoragePrefix(value) {
  const segments = String(value || '')
    .split(/[\\/]+/)
    .map((segment) => normalizeObjectStorageSegment(segment))
    .filter(Boolean);
  return segments.length ? segments.join('/') : 'run';
}

function normalizeArtifactArchiveName(value) {
  const raw = String(value || 'run-artifacts.zip').trim();
  const basename = path.posix.basename(raw.replace(/\\/g, '/'));
  const normalized = basename
    .replace(/[^A-Za-z0-9._-]+/g, '-')
    .replace(/^-+|-+$/g, '');
  return normalized || 'run-artifacts.zip';
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

async function writeRelayedArtifactToLocal({ buffer, rootPrefix, safeKey, clientRunId, archiveName }) {
  const prefixSegments = String(rootPrefix || 'run')
    .split('/')
    .filter(Boolean);
  const targetDir = path.join(
    ARTIFACT_RELAY_ROOT,
    ...prefixSegments,
    safeKey || 'unknown',
    clientRunId || 'unknown'
  );
  await fs.promises.mkdir(targetDir, { recursive: true });
  const destination = path.join(targetDir, archiveName);
  await fs.promises.writeFile(destination, buffer);
  return {
    ok: true,
    storage: 'local_spool',
    destination,
    archiveName
  };
}

async function storeRelayedArtifactArchive({
  buffer,
  rootPrefix,
  safeKey,
  clientRunId,
  archiveName
}) {
  if (!Buffer.isBuffer(buffer) || !buffer.length) {
    return { ok: false, error: 'empty_body' };
  }

  if (!hasObjectStorageConfig()) {
    return writeRelayedArtifactToLocal({
      buffer,
      rootPrefix,
      safeKey,
      clientRunId,
      archiveName
    });
  }

  const configDir = await fs.promises.mkdtemp(path.join(os.tmpdir(), 'mc-'));
  const tempDir = await fs.promises.mkdtemp(path.join(os.tmpdir(), 'artifact-relay-'));
  try {
    const tempArchivePath = path.join(tempDir, archiveName);
    await fs.promises.writeFile(tempArchivePath, buffer);
    await runMcCommand(
      ['alias', 'set', OBJECT_STORAGE_ALIAS, OBJECT_STORAGE_ENDPOINT, OBJECT_STORAGE_ACCESS_KEY, OBJECT_STORAGE_SECRET_KEY],
      configDir
    );
    const destination = `${OBJECT_STORAGE_ALIAS}/${OBJECT_STORAGE_BUCKET}/${rootPrefix}/${safeKey}/${clientRunId}/${archiveName}`;
    await runMcCommand(['cp', tempArchivePath, destination], configDir);
    return {
      ok: true,
      storage: 'object_storage',
      destination,
      archiveName
    };
  } catch (error) {
    const localFallback = await writeRelayedArtifactToLocal({
      buffer,
      rootPrefix,
      safeKey,
      clientRunId,
      archiveName
    });
    return {
      ...localFallback,
      warning: `object_storage_upload_failed: ${error.message || String(error)}`
    };
  } finally {
    try {
      await fs.promises.rm(configDir, { recursive: true, force: true });
    } catch (cleanupError) {
      console.warn('Failed to cleanup mc config dir', cleanupError);
    }
    try {
      await fs.promises.rm(tempDir, { recursive: true, force: true });
    } catch (cleanupError) {
      console.warn('Failed to cleanup artifact relay temp dir', cleanupError);
    }
  }
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

async function syncLotProducedCounts(client) {
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
}

async function replaceMachineLotProgress(
  client,
  {
    lot = null,
    machineKey = null,
    reportId = null,
    technician = null,
    source = 'manual-lot-update'
  } = {}
) {
  if (!machineKey) {
    return { counted: false, lot: lot || null };
  }

  await client.query('DELETE FROM lot_progress WHERE machine_key = $1', [machineKey]);

  let counted = false;
  if (lot && lot.id && !parseBooleanFlag(lot.is_paused, false)) {
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
        VALUES ($1, $2, $3, $4, $5, false, NOW())
        ON CONFLICT (lot_id, machine_key) DO NOTHING
        RETURNING lot_id
      `,
      [lot.id, machineKey, normalizeUuid(reportId), technician || null, source]
    );
    counted = Boolean(insertResult.rowCount);
  }

  await syncLotProducedCounts(client);

  if (lot && lot.id) {
    const refreshedLot = await getLotById(client, lot.id);
    return { counted, lot: refreshedLot || lot };
  }

  return { counted, lot: null };
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
        client_generated_at,
        first_client_generated_at,
        last_seen,
        created_at,
        clock_drift_seconds,
        clock_delta_seconds,
        bios_clock_alert,
        bios_clock_alert_code,
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
              client_generated_at,
              last_seen,
              created_at,
              clock_drift_seconds,
              clock_delta_seconds,
              bios_clock_alert,
              bios_clock_alert_code,
              components,
              payload,
              last_ip
            ) VALUES (
              $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
            $11, $12, $13, $14, $15, $16, $17, $18, $19, $20,
            $21, $22, $23, $24, $25, $26, $27, $28, $29, $30,
            $31, $32, $33
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
          row.client_generated_at,
          row.last_seen,
          row.created_at,
          row.clock_drift_seconds,
          row.clock_delta_seconds,
          row.bios_clock_alert,
          row.bios_clock_alert_code,
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

async function backfillClockSignals() {
  try {
    await pool.query(
      `
        UPDATE reports
        SET client_generated_at = COALESCE(
          client_generated_at,
          safe_timestamptz(safe_jsonb(payload) ->> 'generated_at'),
          safe_timestamptz(safe_jsonb(payload) ->> 'generatedAt'),
          safe_timestamptz(safe_jsonb(payload) -> 'diag' ->> 'completedAt'),
          safe_timestamptz(safe_jsonb(payload) -> 'diag' ->> 'generatedAt'),
          safe_timestamptz(safe_jsonb(payload) -> 'diag' ->> 'generated_at'),
          safe_timestamptz(safe_jsonb(payload) -> 'rawArtifacts' ->> 'generated_at'),
          safe_timestamptz(safe_jsonb(payload) -> 'rawArtifacts' ->> 'generatedAt')
        )
        WHERE payload IS NOT NULL
          AND payload <> ''
      `
    );

    await pool.query(
      `
        WITH ordered AS (
          SELECT
            reports.id,
            reports.machine_key,
            reports.last_seen,
            reports.client_generated_at,
            LAG(reports.last_seen) OVER (
              PARTITION BY reports.machine_key
              ORDER BY reports.last_seen ASC, reports.id ASC
            ) AS previous_server_seen_at,
            LAG(reports.client_generated_at) OVER (
              PARTITION BY reports.machine_key
              ORDER BY reports.last_seen ASC, reports.id ASC
            ) AS previous_client_generated_at
          FROM reports
        ),
        metrics AS (
          SELECT
            ordered.id,
            CASE
              WHEN ordered.client_generated_at IS NOT NULL
                THEN ROUND(ABS(EXTRACT(EPOCH FROM (ordered.last_seen - ordered.client_generated_at))))::integer
              ELSE NULL
            END AS clock_drift_seconds,
            CASE
              WHEN ordered.client_generated_at IS NOT NULL
                AND ordered.previous_server_seen_at IS NOT NULL
                AND ordered.previous_client_generated_at IS NOT NULL
                THEN ROUND(
                  ABS(
                    EXTRACT(
                      EPOCH FROM (
                        (ordered.last_seen - ordered.previous_server_seen_at) -
                        (ordered.client_generated_at - ordered.previous_client_generated_at)
                      )
                    )
                  )
                )::integer
              ELSE NULL
            END AS clock_delta_seconds,
            CASE
              WHEN ordered.client_generated_at IS NOT NULL
                AND ordered.previous_client_generated_at IS NOT NULL
                THEN ROUND(EXTRACT(EPOCH FROM (ordered.client_generated_at - ordered.previous_client_generated_at)))::integer
              ELSE NULL
            END AS client_delta_seconds
          FROM ordered
        ),
        flags AS (
          SELECT
            metrics.id,
            metrics.clock_drift_seconds,
            metrics.clock_delta_seconds,
            array_to_string(
              array_remove(
                ARRAY[
                  CASE
                    WHEN metrics.client_delta_seconds IS NOT NULL
                      AND metrics.client_delta_seconds < -${BIOS_CLOCK_BACKWARD_GRACE_SECONDS}
                      THEN 'clock_backwards'
                    ELSE NULL
                  END,
                  CASE
                    WHEN metrics.clock_drift_seconds IS NOT NULL
                      AND metrics.clock_drift_seconds >= ${BIOS_CLOCK_DRIFT_ALERT_THRESHOLD_SECONDS}
                      THEN 'clock_drift'
                    ELSE NULL
                  END,
                  CASE
                    WHEN metrics.clock_delta_seconds IS NOT NULL
                      AND metrics.clock_delta_seconds >= ${BIOS_CLOCK_DELTA_ALERT_THRESHOLD_SECONDS}
                      THEN 'delta_mismatch'
                    ELSE NULL
                  END
                ],
                NULL
              ),
              ','
            ) AS bios_clock_alert_code
          FROM metrics
        )
        UPDATE reports
        SET
          clock_drift_seconds = flags.clock_drift_seconds,
          clock_delta_seconds = flags.clock_delta_seconds,
          bios_clock_alert = COALESCE(flags.bios_clock_alert_code, '') <> '',
          bios_clock_alert_code = NULLIF(flags.bios_clock_alert_code, '')
        FROM flags
        WHERE reports.id = flags.id
      `
    );

    await pool.query(
      `
        WITH latest AS (
          SELECT DISTINCT ON (reports.machine_key)
            reports.machine_key,
            reports.client_generated_at,
            reports.clock_drift_seconds,
            reports.clock_delta_seconds,
            reports.bios_clock_alert,
            reports.bios_clock_alert_code
          FROM reports
          ORDER BY reports.machine_key, reports.last_seen DESC, reports.id DESC
        ),
        firsts AS (
          SELECT DISTINCT ON (reports.machine_key)
            reports.machine_key,
            reports.client_generated_at AS first_client_generated_at
          FROM reports
          WHERE reports.client_generated_at IS NOT NULL
          ORDER BY reports.machine_key, reports.client_generated_at ASC, reports.last_seen ASC, reports.id ASC
        )
        UPDATE machines
        SET
          client_generated_at = COALESCE(latest.client_generated_at, machines.client_generated_at),
          first_client_generated_at = COALESCE(
            machines.first_client_generated_at,
            firsts.first_client_generated_at,
            latest.client_generated_at
          ),
          clock_drift_seconds = latest.clock_drift_seconds,
          clock_delta_seconds = latest.clock_delta_seconds,
          bios_clock_alert = COALESCE(latest.bios_clock_alert, false),
          bios_clock_alert_code = latest.bios_clock_alert_code
        FROM latest
        LEFT JOIN firsts ON firsts.machine_key = latest.machine_key
        WHERE machines.machine_key = latest.machine_key
      `
    );
  } catch (error) {
    console.error('Clock signal backfill failed', error);
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
      pallet_id UUID,
      pallet_status TEXT,
      shipment_date DATE,
      shipment_client TEXT,
      shipment_order_number TEXT,
      shipment_pallet_code TEXT,
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
      client_generated_at TIMESTAMPTZ,
      first_client_generated_at TIMESTAMPTZ,
      last_seen TIMESTAMPTZ NOT NULL,
      created_at TIMESTAMPTZ NOT NULL,
      clock_drift_seconds INTEGER,
      clock_delta_seconds INTEGER,
      bios_clock_alert BOOLEAN NOT NULL DEFAULT false,
      bios_clock_alert_code TEXT,
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
      pallet_id UUID,
      pallet_status TEXT,
      shipment_date DATE,
      shipment_client TEXT,
      shipment_order_number TEXT,
      shipment_pallet_code TEXT,
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
      client_generated_at TIMESTAMPTZ,
      last_seen TIMESTAMPTZ NOT NULL,
      created_at TIMESTAMPTZ NOT NULL,
      clock_drift_seconds INTEGER,
      clock_delta_seconds INTEGER,
      bios_clock_alert BOOLEAN NOT NULL DEFAULT false,
      bios_clock_alert_code TEXT,
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
  await pool.query(`ALTER TABLE machines ADD COLUMN IF NOT EXISTS pallet_id UUID;`);
  await pool.query(`ALTER TABLE machines ADD COLUMN IF NOT EXISTS pallet_status TEXT;`);
  await pool.query(`ALTER TABLE machines ADD COLUMN IF NOT EXISTS shipment_date DATE;`);
  await pool.query(`ALTER TABLE machines ADD COLUMN IF NOT EXISTS shipment_client TEXT;`);
  await pool.query(`ALTER TABLE machines ADD COLUMN IF NOT EXISTS shipment_order_number TEXT;`);
  await pool.query(`ALTER TABLE machines ADD COLUMN IF NOT EXISTS shipment_pallet_code TEXT;`);
  await pool.query(`ALTER TABLE reports ADD COLUMN IF NOT EXISTS tag TEXT;`);
  await pool.query(`ALTER TABLE reports ADD COLUMN IF NOT EXISTS tag_id UUID;`);
  await pool.query(`ALTER TABLE reports ADD COLUMN IF NOT EXISTS lot_id UUID;`);
  await pool.query(`ALTER TABLE reports ADD COLUMN IF NOT EXISTS pallet_id UUID;`);
  await pool.query(`ALTER TABLE reports ADD COLUMN IF NOT EXISTS pallet_status TEXT;`);
  await pool.query(`ALTER TABLE reports ADD COLUMN IF NOT EXISTS shipment_date DATE;`);
  await pool.query(`ALTER TABLE reports ADD COLUMN IF NOT EXISTS shipment_client TEXT;`);
  await pool.query(`ALTER TABLE reports ADD COLUMN IF NOT EXISTS shipment_order_number TEXT;`);
  await pool.query(`ALTER TABLE reports ADD COLUMN IF NOT EXISTS shipment_pallet_code TEXT;`);

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
    CREATE TABLE IF NOT EXISTS pallets (
      id UUID PRIMARY KEY,
      code TEXT NOT NULL,
      code_key TEXT NOT NULL UNIQUE,
      last_movement_type TEXT CHECK (last_movement_type IS NULL OR last_movement_type IN ('entry', 'exit')),
      last_movement_at TIMESTAMPTZ,
      created_by TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS pallet_imports (
      id UUID PRIMARY KEY,
      import_type TEXT NOT NULL CHECK (import_type IN ('entry', 'exit')),
      file_name TEXT,
      row_count INTEGER NOT NULL DEFAULT 0,
      applied_count INTEGER NOT NULL DEFAULT 0,
      skipped_count INTEGER NOT NULL DEFAULT 0,
      created_by TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      summary TEXT
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS pallet_serials (
      serial_number TEXT PRIMARY KEY,
      pallet_id UUID NOT NULL REFERENCES pallets(id) ON DELETE CASCADE,
      movement_type TEXT NOT NULL CHECK (movement_type IN ('entry', 'exit')),
      shipment_date DATE,
      shipment_client TEXT,
      shipment_order_number TEXT,
      shipment_pallet_code TEXT,
      machine_key TEXT,
      last_import_id UUID REFERENCES pallet_imports(id) ON DELETE SET NULL,
      updated_by TEXT,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);
  await pool.query(`ALTER TABLE pallet_serials ADD COLUMN IF NOT EXISTS shipment_date DATE;`);
  await pool.query(`ALTER TABLE pallet_serials ADD COLUMN IF NOT EXISTS shipment_client TEXT;`);
  await pool.query(`ALTER TABLE pallet_serials ADD COLUMN IF NOT EXISTS shipment_order_number TEXT;`);
  await pool.query(`ALTER TABLE pallet_serials ADD COLUMN IF NOT EXISTS shipment_pallet_code TEXT;`);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS pallet_movements (
      id BIGSERIAL PRIMARY KEY,
      pallet_id UUID NOT NULL REFERENCES pallets(id) ON DELETE CASCADE,
      serial_number TEXT NOT NULL,
      machine_key TEXT,
      import_id UUID REFERENCES pallet_imports(id) ON DELETE SET NULL,
      movement_type TEXT NOT NULL CHECK (movement_type IN ('entry', 'exit')),
      created_by TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
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
    CREATE TABLE IF NOT EXISTS weekly_recap_runs (
      id UUID PRIMARY KEY,
      period_key TEXT NOT NULL,
      period_start TIMESTAMPTZ NOT NULL,
      period_end TIMESTAMPTZ NOT NULL,
      trigger_source TEXT NOT NULL,
      created_by TEXT,
      recipients TEXT NOT NULL,
      status TEXT NOT NULL,
      sent_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      summary TEXT,
      error TEXT
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS mdt_beta_technicians (
      id UUID PRIMARY KEY,
      display_name TEXT NOT NULL,
      slug TEXT NOT NULL UNIQUE,
      source_task_sequence_id TEXT NOT NULL,
      beta_task_sequence_id TEXT NOT NULL UNIQUE,
      beta_task_sequence_name TEXT NOT NULL,
      task_sequence_group_name TEXT NOT NULL DEFAULT 'MMA Beta',
      scripts_folder TEXT NOT NULL DEFAULT 'beta',
      status TEXT NOT NULL DEFAULT 'queued' CHECK (status IN ('queued', 'provisioning', 'ready', 'failed', 'disabled')),
      created_by TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      last_job_id UUID,
      last_error TEXT,
      last_result JSONB NOT NULL DEFAULT '{}'::jsonb
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS mdt_beta_jobs (
      id UUID PRIMARY KEY,
      technician_id UUID NOT NULL REFERENCES mdt_beta_technicians(id) ON DELETE CASCADE,
      job_type TEXT NOT NULL DEFAULT 'provision' CHECK (job_type IN ('provision', 'delete')),
      status TEXT NOT NULL DEFAULT 'queued' CHECK (status IN ('queued', 'running', 'succeeded', 'failed')),
      payload JSONB NOT NULL DEFAULT '{}'::jsonb,
      result JSONB NOT NULL DEFAULT '{}'::jsonb,
      error TEXT,
      requested_by TEXT,
      agent_id TEXT,
      claimed_at TIMESTAMPTZ,
      heartbeat_at TIMESTAMPTZ,
      started_at TIMESTAMPTZ,
      finished_at TIMESTAMPTZ,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);

  await pool.query('ALTER TABLE mdt_beta_jobs DROP CONSTRAINT IF EXISTS mdt_beta_jobs_job_type_check');
  await pool.query(`
    ALTER TABLE mdt_beta_jobs
    ADD CONSTRAINT mdt_beta_jobs_job_type_check
    CHECK (job_type IN ('provision', 'delete'))
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS mdt_beta_agents (
      agent_id TEXT PRIMARY KEY,
      hostname TEXT,
      deployment_share_root TEXT,
      task_sequence_group_name TEXT,
      scripts_folder TEXT,
      status TEXT NOT NULL DEFAULT 'idle',
      last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      last_job_id UUID,
      last_error TEXT,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
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

  await pool.query(`
    CREATE OR REPLACE FUNCTION safe_timestamptz(input_text TEXT)
    RETURNS TIMESTAMPTZ
    LANGUAGE plpgsql
    IMMUTABLE
    AS $$
    BEGIN
      IF input_text IS NULL OR btrim(input_text) = '' THEN
        RETURN NULL;
      END IF;
      RETURN input_text::timestamptz;
    EXCEPTION
      WHEN others THEN
        RETURN NULL;
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
    ['client_generated_at', 'TIMESTAMPTZ'],
    ['first_client_generated_at', 'TIMESTAMPTZ'],
    ['technician', 'TEXT'],
    ['tag', 'TEXT'],
    ['tag_id', 'UUID'],
    ['lot_id', 'UUID'],
    ['shipment_date', 'DATE'],
    ['shipment_client', 'TEXT'],
    ['shipment_order_number', 'TEXT'],
    ['shipment_pallet_code', 'TEXT'],
    ['clock_drift_seconds', 'INTEGER'],
    ['clock_delta_seconds', 'INTEGER'],
    ['bios_clock_alert', 'BOOLEAN NOT NULL DEFAULT false'],
    ['bios_clock_alert_code', 'TEXT'],
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
    ['shipment_date', 'DATE'],
    ['shipment_client', 'TEXT'],
    ['shipment_order_number', 'TEXT'],
    ['shipment_pallet_code', 'TEXT'],
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
    ['client_generated_at', 'TIMESTAMPTZ'],
    ['last_seen', 'TIMESTAMPTZ NOT NULL'],
    ['created_at', 'TIMESTAMPTZ NOT NULL'],
    ['clock_drift_seconds', 'INTEGER'],
    ['clock_delta_seconds', 'INTEGER'],
    ['bios_clock_alert', 'BOOLEAN NOT NULL DEFAULT false'],
    ['bios_clock_alert_code', 'TEXT'],
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
  await pool.query('CREATE INDEX IF NOT EXISTS idx_machines_pallet_id ON machines(pallet_id)');
  await pool.query('CREATE INDEX IF NOT EXISTS idx_machines_shipment_date ON machines(shipment_date)');
  await pool.query('CREATE INDEX IF NOT EXISTS idx_machines_shipment_client ON machines(shipment_client)');
  await pool.query('CREATE INDEX IF NOT EXISTS idx_machines_shipment_order_number ON machines(shipment_order_number)');
  await pool.query('CREATE INDEX IF NOT EXISTS idx_machines_shipment_pallet_code ON machines(shipment_pallet_code)');
  await pool.query('CREATE INDEX IF NOT EXISTS idx_machines_client_generated_at ON machines(client_generated_at)');
  await pool.query('CREATE INDEX IF NOT EXISTS idx_machines_bios_clock_alert ON machines(bios_clock_alert)');
  await pool.query('CREATE INDEX IF NOT EXISTS idx_machines_last_seen ON machines(last_seen)');
  await pool.query('CREATE INDEX IF NOT EXISTS idx_reports_machine_key ON reports(machine_key)');
  await pool.query('CREATE INDEX IF NOT EXISTS idx_reports_category ON reports(category)');
  await pool.query('CREATE INDEX IF NOT EXISTS idx_reports_tag ON reports(tag)');
  await pool.query('CREATE INDEX IF NOT EXISTS idx_reports_tag_id ON reports(tag_id)');
  await pool.query('CREATE INDEX IF NOT EXISTS idx_reports_lot_id ON reports(lot_id)');
  await pool.query('CREATE INDEX IF NOT EXISTS idx_reports_pallet_id ON reports(pallet_id)');
  await pool.query('CREATE INDEX IF NOT EXISTS idx_reports_shipment_date ON reports(shipment_date)');
  await pool.query('CREATE INDEX IF NOT EXISTS idx_reports_shipment_client ON reports(shipment_client)');
  await pool.query('CREATE INDEX IF NOT EXISTS idx_reports_shipment_order_number ON reports(shipment_order_number)');
  await pool.query('CREATE INDEX IF NOT EXISTS idx_reports_shipment_pallet_code ON reports(shipment_pallet_code)');
  await pool.query('CREATE INDEX IF NOT EXISTS idx_reports_client_generated_at ON reports(client_generated_at)');
  await pool.query('CREATE INDEX IF NOT EXISTS idx_reports_bios_clock_alert ON reports(bios_clock_alert)');
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
  await pool.query('CREATE INDEX IF NOT EXISTS idx_pallets_last_movement_at ON pallets(last_movement_at DESC)');
  await pool.query('CREATE INDEX IF NOT EXISTS idx_pallet_serials_pallet_id ON pallet_serials(pallet_id)');
  await pool.query('CREATE INDEX IF NOT EXISTS idx_pallet_serials_machine_key ON pallet_serials(machine_key)');
  await pool.query(
    'CREATE INDEX IF NOT EXISTS idx_pallet_movements_pallet_created_at ON pallet_movements(pallet_id, created_at DESC)'
  );
  await pool.query(
    'CREATE INDEX IF NOT EXISTS idx_pallet_movements_serial_created_at ON pallet_movements(serial_number, created_at DESC)'
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
  await pool.query(
    'CREATE INDEX IF NOT EXISTS idx_mdt_beta_technicians_status_created ON mdt_beta_technicians(status, created_at DESC)'
  );
  await pool.query(
    'CREATE INDEX IF NOT EXISTS idx_mdt_beta_jobs_status_created ON mdt_beta_jobs(status, created_at ASC)'
  );
  await pool.query(
    'CREATE INDEX IF NOT EXISTS idx_mdt_beta_jobs_technician_created ON mdt_beta_jobs(technician_id, created_at DESC)'
  );
  await pool.query(
    'CREATE INDEX IF NOT EXISTS idx_mdt_beta_agents_last_seen ON mdt_beta_agents(last_seen_at DESC)'
  );
  await pool.query(
    'CREATE INDEX IF NOT EXISTS idx_weekly_recap_runs_period_sent ON weekly_recap_runs(period_key, sent_at DESC)'
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
    await pool.query('DROP TRIGGER IF EXISTS audit_log_weekly_recap_runs ON weekly_recap_runs');
    await pool.query(`
      CREATE TRIGGER audit_log_weekly_recap_runs
      AFTER INSERT OR UPDATE OR DELETE ON weekly_recap_runs
      FOR EACH ROW EXECUTE FUNCTION audit_log_trigger();
    `);
    await pool.query('DROP TRIGGER IF EXISTS audit_log_mdt_beta_technicians ON mdt_beta_technicians');
    await pool.query(`
      CREATE TRIGGER audit_log_mdt_beta_technicians
      AFTER INSERT OR UPDATE OR DELETE ON mdt_beta_technicians
      FOR EACH ROW EXECUTE FUNCTION audit_log_trigger();
    `);
    await pool.query('DROP TRIGGER IF EXISTS audit_log_mdt_beta_jobs ON mdt_beta_jobs');
    await pool.query(`
      CREATE TRIGGER audit_log_mdt_beta_jobs
      AFTER INSERT OR UPDATE OR DELETE ON mdt_beta_jobs
      FOR EACH ROW EXECUTE FUNCTION audit_log_trigger();
    `);
    await pool.query('DROP TRIGGER IF EXISTS audit_log_mdt_beta_agents ON mdt_beta_agents');
    await pool.query(`
      CREATE TRIGGER audit_log_mdt_beta_agents
      AFTER INSERT OR UPDATE OR DELETE ON mdt_beta_agents
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
  await backfillClockSignals();
}

async function backfillUnknownTechniciansToLuka() {
  const unknownValues = Array.from(UNKNOWN_TECHNICIAN_KEYS).filter(Boolean);
  const reportsResult = await pool.query(
    `
      UPDATE reports
      SET technician = $1
      WHERE technician IS NOT NULL
        AND btrim(technician) <> ''
        AND lower(btrim(technician)) = ANY($2::text[])
    `,
    [DEFAULT_FALLBACK_TECHNICIAN, unknownValues]
  );
  const machinesResult = await pool.query(
    `
      UPDATE machines
      SET technician = $1
      WHERE technician IS NOT NULL
        AND btrim(technician) <> ''
        AND lower(btrim(technician)) = ANY($2::text[])
    `,
    [DEFAULT_FALLBACK_TECHNICIAN, unknownValues]
  );
  const lotProgressResult = await pool.query(
    `
      UPDATE lot_progress
      SET technician = $1
      WHERE technician IS NOT NULL
        AND btrim(technician) <> ''
        AND lower(btrim(technician)) = ANY($2::text[])
    `,
    [DEFAULT_FALLBACK_TECHNICIAN, unknownValues]
  );
  const changedCount =
    Number(reportsResult.rowCount || 0) +
    Number(machinesResult.rowCount || 0) +
    Number(lotProgressResult.rowCount || 0);

  if (changedCount > 0) {
    console.log(
      `Backfilled technician '${DEFAULT_FALLBACK_TECHNICIAN}' for unknown rows: ` +
        `reports=${reportsResult.rowCount || 0}, machines=${machinesResult.rowCount || 0}, ` +
        `lot_progress=${lotProgressResult.rowCount || 0}`
    );
  }
}

async function repairTechniciansFromPayload() {
  const unknownValues = Array.from(UNKNOWN_TECHNICIAN_KEYS).filter(Boolean);
  const reportsResult = await pool.query(
    `
      WITH candidates AS (
        SELECT
          reports.id,
          NULLIF(btrim(safe_jsonb(reports.payload) ->> 'technician'), '') AS payload_technician
        FROM reports
        WHERE reports.payload IS NOT NULL
          AND reports.payload <> ''
      )
      UPDATE reports
      SET technician = candidates.payload_technician
      FROM candidates
      WHERE reports.id = candidates.id
        AND candidates.payload_technician IS NOT NULL
        AND lower(candidates.payload_technician) <> ALL($1::text[])
        AND (
          reports.technician IS NULL
          OR btrim(reports.technician) = ''
          OR lower(btrim(reports.technician)) = ANY($1::text[])
          OR (
            lower(btrim(reports.technician)) = lower($2)
            AND lower(candidates.payload_technician) <> lower($2)
          )
        )
        AND reports.technician IS DISTINCT FROM candidates.payload_technician
    `,
    [unknownValues, DEFAULT_FALLBACK_TECHNICIAN]
  );
  const machinesResult = await pool.query(
    `
      WITH candidates AS (
        SELECT
          machines.machine_key,
          NULLIF(btrim(safe_jsonb(machines.payload) ->> 'technician'), '') AS payload_technician
        FROM machines
        WHERE machines.payload IS NOT NULL
          AND machines.payload <> ''
      )
      UPDATE machines
      SET technician = candidates.payload_technician
      FROM candidates
      WHERE machines.machine_key = candidates.machine_key
        AND candidates.payload_technician IS NOT NULL
        AND lower(candidates.payload_technician) <> ALL($1::text[])
        AND (
          machines.technician IS NULL
          OR btrim(machines.technician) = ''
          OR lower(btrim(machines.technician)) = ANY($1::text[])
          OR (
            lower(btrim(machines.technician)) = lower($2)
            AND lower(candidates.payload_technician) <> lower($2)
          )
        )
        AND machines.technician IS DISTINCT FROM candidates.payload_technician
    `,
    [unknownValues, DEFAULT_FALLBACK_TECHNICIAN]
  );
  const lotProgressResult = await pool.query(
    `
      WITH latest AS (
        SELECT DISTINCT ON (reports.machine_key)
          reports.machine_key,
          reports.technician
        FROM reports
        WHERE reports.machine_key IS NOT NULL
          AND reports.technician IS NOT NULL
          AND btrim(reports.technician) <> ''
        ORDER BY reports.machine_key, reports.last_seen DESC, reports.id DESC
      )
      UPDATE lot_progress
      SET technician = latest.technician
      FROM latest
      WHERE lot_progress.machine_key = latest.machine_key
        AND latest.technician IS NOT NULL
        AND (
          lot_progress.technician IS NULL
          OR btrim(lot_progress.technician) = ''
          OR lower(btrim(lot_progress.technician)) = ANY($1::text[])
          OR (
            lower(btrim(lot_progress.technician)) = lower($2)
            AND lower(latest.technician) <> lower($2)
          )
        )
        AND lot_progress.technician IS DISTINCT FROM latest.technician
    `,
    [unknownValues, DEFAULT_FALLBACK_TECHNICIAN]
  );
  const changedCount =
    Number(reportsResult.rowCount || 0) +
    Number(machinesResult.rowCount || 0) +
    Number(lotProgressResult.rowCount || 0);

  if (changedCount > 0) {
    console.log(
      'Repaired technician labels from payload: ' +
        `reports=${reportsResult.rowCount || 0}, machines=${machinesResult.rowCount || 0}, ` +
        `lot_progress=${lotProgressResult.rowCount || 0}`
    );
  }
}

async function repairStoredTruncatedPayloads() {
  const truncatedPayloadJson = '{"truncated": true}';
  const reportsResult = await pool.query(
    `
      WITH fallback_payloads AS (
        SELECT
          reports.id,
          COALESCE(
            (
              SELECT candidate.payload
              FROM reports AS candidate
              WHERE candidate.machine_key = reports.machine_key
                AND candidate.id <> reports.id
                AND candidate.payload IS NOT NULL
                AND btrim(candidate.payload) <> ''
                AND safe_jsonb(candidate.payload) <> $1::jsonb
              ORDER BY candidate.last_seen DESC, candidate.id DESC
              LIMIT 1
            ),
            (
              SELECT machines.payload
              FROM machines
              WHERE machines.machine_key = reports.machine_key
                AND machines.payload IS NOT NULL
                AND btrim(machines.payload) <> ''
                AND safe_jsonb(machines.payload) <> $1::jsonb
              LIMIT 1
            )
          ) AS fallback_payload
        FROM reports
        WHERE reports.payload IS NOT NULL
          AND btrim(reports.payload) <> ''
          AND safe_jsonb(reports.payload) = $1::jsonb
      )
      UPDATE reports
      SET payload = fallback_payloads.fallback_payload
      FROM fallback_payloads
      WHERE reports.id = fallback_payloads.id
        AND fallback_payloads.fallback_payload IS NOT NULL
    `,
    [truncatedPayloadJson]
  );
  const machinesResult = await pool.query(
    `
      WITH fallback_payloads AS (
        SELECT
          machines.machine_key,
          (
            SELECT candidate.payload
            FROM reports AS candidate
            WHERE candidate.machine_key = machines.machine_key
              AND candidate.payload IS NOT NULL
              AND btrim(candidate.payload) <> ''
              AND safe_jsonb(candidate.payload) <> $1::jsonb
            ORDER BY candidate.last_seen DESC, candidate.id DESC
            LIMIT 1
          ) AS fallback_payload
        FROM machines
        WHERE machines.payload IS NOT NULL
          AND btrim(machines.payload) <> ''
          AND safe_jsonb(machines.payload) = $1::jsonb
      )
      UPDATE machines
      SET payload = fallback_payloads.fallback_payload
      FROM fallback_payloads
      WHERE machines.machine_key = fallback_payloads.machine_key
        AND fallback_payloads.fallback_payload IS NOT NULL
    `,
    [truncatedPayloadJson]
  );
  const remainingResult = await pool.query(
    `
      SELECT
        (SELECT count(*)
         FROM reports
         WHERE payload IS NOT NULL
           AND btrim(payload) <> ''
           AND safe_jsonb(payload) = $1::jsonb) AS report_count,
        (SELECT count(*)
         FROM machines
         WHERE payload IS NOT NULL
           AND btrim(payload) <> ''
           AND safe_jsonb(payload) = $1::jsonb) AS machine_count
    `,
    [truncatedPayloadJson]
  );
  const remainingRow =
    remainingResult.rows && remainingResult.rows[0] ? remainingResult.rows[0] : null;
  const remainingReports = Number.parseInt(remainingRow?.report_count || '0', 10) || 0;
  const remainingMachines = Number.parseInt(remainingRow?.machine_count || '0', 10) || 0;
  const repairedReports = Number(reportsResult.rowCount || 0);
  const repairedMachines = Number(machinesResult.rowCount || 0);

  if (repairedReports > 0 || repairedMachines > 0) {
    console.log(
      'Repaired truncated payloads: ' +
        `reports=${repairedReports}, machines=${repairedMachines}, ` +
        `remaining_reports=${remainingReports}, remaining_machines=${remainingMachines}`
    );
  } else if (remainingReports > 0 || remainingMachines > 0) {
    console.warn(
      'Truncated payloads remain after repair attempt: ' +
        `reports=${remainingReports}, machines=${remainingMachines}`
    );
  }
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
    pallet_id,
    pallet_status,
    shipment_date,
    shipment_client,
    shipment_order_number,
    shipment_pallet_code,
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
    client_generated_at,
    first_client_generated_at,
    last_seen,
    created_at,
    clock_drift_seconds,
    clock_delta_seconds,
    bios_clock_alert,
    bios_clock_alert_code,
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
    $28,
    $29,
    $30,
    $31,
    $32,
    $33,
    $34,
    $35,
    $36,
    $37,
    $38,
    $39
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
    pallet_id = COALESCE(excluded.pallet_id, machines.pallet_id),
    pallet_status = COALESCE(excluded.pallet_status, machines.pallet_status),
    shipment_date = COALESCE(excluded.shipment_date, machines.shipment_date),
    shipment_client = COALESCE(excluded.shipment_client, machines.shipment_client),
    shipment_order_number = COALESCE(excluded.shipment_order_number, machines.shipment_order_number),
    shipment_pallet_code = COALESCE(excluded.shipment_pallet_code, machines.shipment_pallet_code),
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
    client_generated_at = excluded.client_generated_at,
    first_client_generated_at = COALESCE(machines.first_client_generated_at, excluded.first_client_generated_at),
    last_seen = excluded.last_seen,
    clock_drift_seconds = excluded.clock_drift_seconds,
    clock_delta_seconds = excluded.clock_delta_seconds,
    bios_clock_alert = COALESCE(excluded.bios_clock_alert, false),
    bios_clock_alert_code = excluded.bios_clock_alert_code,
    components = CASE
      WHEN excluded.components IS NULL THEN machines.components
      ELSE (
        COALESCE(NULLIF(machines.components, ''), '{}')::jsonb ||
        COALESCE(NULLIF(excluded.components, ''), '{}')::jsonb
      )::text
    END,
    payload = CASE
      WHEN excluded.payload IS NULL OR btrim(excluded.payload) = '' THEN machines.payload
      WHEN safe_jsonb(excluded.payload) = '{"truncated": true}'::jsonb THEN machines.payload
      ELSE excluded.payload
    END,
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
    pallet_id,
    pallet_status,
    shipment_date,
    shipment_client,
    shipment_order_number,
    shipment_pallet_code,
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
    client_generated_at,
    last_seen,
    created_at,
    clock_drift_seconds,
    clock_delta_seconds,
    bios_clock_alert,
    bios_clock_alert_code,
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
    $28,
    $29,
    $30,
    $31,
    $32,
    $33,
    $34,
    $35,
    $36,
    $37,
    $38,
    $39
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
    pallet_id = COALESCE(excluded.pallet_id, reports.pallet_id),
    pallet_status = COALESCE(excluded.pallet_status, reports.pallet_status),
    shipment_date = COALESCE(excluded.shipment_date, reports.shipment_date),
    shipment_client = COALESCE(excluded.shipment_client, reports.shipment_client),
    shipment_order_number = COALESCE(excluded.shipment_order_number, reports.shipment_order_number),
    shipment_pallet_code = COALESCE(excluded.shipment_pallet_code, reports.shipment_pallet_code),
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
    client_generated_at = excluded.client_generated_at,
    last_seen = excluded.last_seen,
    clock_drift_seconds = excluded.clock_drift_seconds,
    clock_delta_seconds = excluded.clock_delta_seconds,
    bios_clock_alert = COALESCE(excluded.bios_clock_alert, false),
    bios_clock_alert_code = excluded.bios_clock_alert_code,
    components = CASE
      WHEN excluded.components IS NULL THEN reports.components
      ELSE (
        COALESCE(NULLIF(reports.components, ''), '{}')::jsonb ||
        COALESCE(NULLIF(excluded.components, ''), '{}')::jsonb
      )::text
    END,
    payload = CASE
      WHEN excluded.payload IS NULL OR btrim(excluded.payload) = '' THEN reports.payload
      WHEN safe_jsonb(excluded.payload) = '{"truncated": true}'::jsonb THEN reports.payload
      ELSE excluded.payload
    END,
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
    reports.pallet_id,
    reports.pallet_status,
    reports.shipment_date,
    reports.shipment_client,
    reports.shipment_order_number,
    reports.shipment_pallet_code,
    COALESCE(tags.name, reports.tag) AS tag_name,
    lots.supplier AS lot_supplier,
    lots.lot_number AS lot_number,
    lots.target_count AS lot_target_count,
    lots.produced_count AS lot_produced_count,
    lots.is_paused AS lot_is_paused,
    pallets.code AS pallet_code,
    pallets.last_movement_at AS pallet_last_movement_at,
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
    client_generated_at,
    last_seen,
    clock_drift_seconds,
    clock_delta_seconds,
    bios_clock_alert,
    bios_clock_alert_code,
    last_ip,
    components,
    comment
  FROM reports
  LEFT JOIN tags ON tags.id = reports.tag_id
  LEFT JOIN lots ON lots.id = reports.lot_id
  LEFT JOIN pallets ON pallets.id = reports.pallet_id
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
    reports.pallet_id AS report_pallet_id,
    reports.pallet_status AS report_pallet_status,
    reports.shipment_date AS report_shipment_date,
    reports.shipment_client AS report_shipment_client,
    reports.shipment_order_number AS report_shipment_order_number,
    reports.shipment_pallet_code AS report_shipment_pallet_code,
    COALESCE(tags.name, reports.tag) AS report_tag_name,
    report_lot.supplier AS report_lot_supplier,
    report_lot.lot_number AS report_lot_number,
    report_lot.target_count AS report_lot_target_count,
    report_lot.produced_count AS report_lot_produced_count,
    report_lot.is_paused AS report_lot_is_paused,
    report_pallet.code AS report_pallet_code,
    report_pallet.last_movement_at AS report_pallet_last_movement_at,
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
    reports.client_generated_at AS report_client_generated_at,
    reports.last_seen AS report_last_seen,
    reports.created_at AS report_created_at,
    reports.clock_drift_seconds AS report_clock_drift_seconds,
    reports.clock_delta_seconds AS report_clock_delta_seconds,
    reports.bios_clock_alert AS report_bios_clock_alert,
    reports.bios_clock_alert_code AS report_bios_clock_alert_code,
    reports.components,
    reports.payload,
    reports.last_ip,
    reports.comment,
    reports.commented_at,
    machines.client_generated_at AS machine_client_generated_at,
    machines.first_client_generated_at AS machine_first_client_generated_at,
    machines.last_seen AS machine_last_seen,
    machines.created_at AS machine_created_at,
    machines.clock_drift_seconds AS machine_clock_drift_seconds,
    machines.clock_delta_seconds AS machine_clock_delta_seconds,
    machines.bios_clock_alert AS machine_bios_clock_alert,
    machines.bios_clock_alert_code AS machine_bios_clock_alert_code,
    machines.tag AS machine_tag,
    machines.tag_id AS machine_tag_id,
    machines.lot_id AS machine_lot_id,
    machines.pallet_id AS machine_pallet_id,
    machines.pallet_status AS machine_pallet_status,
    machines.shipment_date AS machine_shipment_date,
    machines.shipment_client AS machine_shipment_client,
    machines.shipment_order_number AS machine_shipment_order_number,
    machines.shipment_pallet_code AS machine_shipment_pallet_code,
    machine_lot.supplier AS machine_lot_supplier,
    machine_lot.lot_number AS machine_lot_number,
    machine_lot.target_count AS machine_lot_target_count,
    machine_lot.produced_count AS machine_lot_produced_count,
    machine_lot.is_paused AS machine_lot_is_paused,
    machine_pallet.code AS machine_pallet_code,
    machine_pallet.last_movement_at AS machine_pallet_last_movement_at,
    machines.payload AS machine_payload,
    machines.components AS machine_components
  FROM reports
  LEFT JOIN machines ON machines.machine_key = reports.machine_key
  LEFT JOIN tags ON tags.id = reports.tag_id
  LEFT JOIN lots report_lot ON report_lot.id = reports.lot_id
  LEFT JOIN lots machine_lot ON machine_lot.id = machines.lot_id
  LEFT JOIN pallets report_pallet ON report_pallet.id = reports.pallet_id
  LEFT JOIN pallets machine_pallet ON machine_pallet.id = machines.pallet_id
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
const SERVER_CHASSIS_CODES = new Set([17, 23, 28, 29]);
const CATEGORY_LABELS = {
  laptop: 'Portable',
  desktop: 'Tour',
  server: 'Serveur',
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
  diskSmart: 'SMART disques',
  serverRaid: 'RAID',
  powerSupply: 'Alimentations',
  serverFans: 'Ventilos',
  serverBmc: 'BMC',
  serverServices: 'Services critiques',
  thermal: 'Thermique',
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
  'diskSmart',
  'serverRaid',
  'powerSupply',
  'serverFans',
  'serverBmc',
  'serverServices',
  'thermal',
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
const HIDDEN_COMPONENTS = new Set(['networkTest', 'memDiag']);
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
const MACHINE_PRIMARY_COMPONENT_KEYS = Object.freeze([
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
]);
const MACHINE_PRIMARY_DIAGNOSTIC_KEYS = Object.freeze([
  'diskReadTest',
  'diskWriteTest',
  'ramTest',
  'cpuTest',
  'gpuTest',
  'networkPing'
]);
const SERVER_PRIMARY_KEYS = Object.freeze([
  'diskSmart',
  'serverRaid',
  'powerSupply',
  'serverFans',
  'serverBmc',
  'thermal'
]);
const MANUAL_REPORT_IMPORT_MAX_ROWS = 500;
const MANUAL_REPORT_TEMPLATE_COLUMNS = [
  'hostname',
  'serial_number',
  'mac_address',
  'category',
  'technician',
  'lot_id',
  'vendor',
  'model',
  'os_version',
  'ram_gb',
  'ram_slots_total',
  'ram_slots_free',
  'battery_health',
  'camera_status',
  'usb_status',
  'keyboard_status',
  'pad_status',
  'badge_reader_status',
  'bios_battery',
  'bios_language',
  'bios_password',
  'wifi_standard',
  'disk_read_status',
  'disk_write_status',
  'ram_test_status',
  'cpu_test_status',
  'gpu_test_status',
  'network_ping_status',
  'fs_check_status',
  'cpu_name',
  'cpu_cores',
  'cpu_threads',
  'gpu_name',
  'primary_disk_model',
  'primary_disk_size_gb',
  'storage_total_gb',
  'autopilot_hash',
  'shipment_date',
  'shipment_client',
  'shipment_order_number',
  'shipment_pallet_code',
  'double_check'
];
const MANUAL_REPORT_TEMPLATE_SAMPLE = {
  hostname: 'PC-MDT-001',
  serial_number: 'ABC123456',
  mac_address: 'AA:BB:CC:DD:EE:FF',
  category: 'laptop',
  technician: 'Mateo',
  lot_id: '',
  vendor: 'Dell',
  model: 'Latitude 5420',
  os_version: 'Windows 11 Pro',
  ram_gb: '16',
  ram_slots_total: '2',
  ram_slots_free: '0',
  battery_health: '88',
  camera_status: 'ok',
  usb_status: 'ok',
  keyboard_status: 'ok',
  pad_status: 'ok',
  badge_reader_status: 'absent',
  bios_battery: 'ok',
  bios_language: 'fr',
  bios_password: 'ok',
  wifi_standard: '802.11ax',
  disk_read_status: 'ok',
  disk_write_status: 'ok',
  ram_test_status: 'ok',
  cpu_test_status: 'ok',
  gpu_test_status: 'ok',
  network_ping_status: 'ok',
  fs_check_status: 'ok',
  cpu_name: 'Intel Core i5-1145G7',
  cpu_cores: '4',
  cpu_threads: '8',
  gpu_name: 'Intel Iris Xe',
  primary_disk_model: 'NVMe 512Go',
  primary_disk_size_gb: '476',
  storage_total_gb: '476',
  autopilot_hash: 'DEVICE-HARDWARE-HASH-EXEMPLE',
  shipment_date: '2026-03-26',
  shipment_client: 'Client Demo',
  shipment_order_number: 'CMD-2026-001',
  shipment_pallet_code: 'PAL-0001',
  double_check: '0'
};
const MANUAL_REPORT_CSV_ALIASES = {
  hostname: ['hostname', 'computer_name', 'name', 'nom_poste'],
  serialNumber: ['serial_number', 'serial', 'serialnumber', 'numero_serie', 'numero_de_serie', 'sn'],
  macAddress: ['mac_address', 'mac', 'adresse_mac'],
  macAddresses: ['mac_addresses', 'macs', 'mac_list', 'liste_mac'],
  category: ['category', 'categorie', 'type'],
  technician: ['technician', 'technicien', 'tech', 'operator'],
  lotId: ['lot_id', 'lotid', 'batch_id', 'batchid'],
  tag: ['tag', 'production_tag', 'prod_tag'],
  tagId: ['tag_id', 'tagid'],
  vendor: ['vendor', 'manufacturer', 'fabricant', 'marque'],
  model: ['model', 'modele'],
  osVersion: ['os_version', 'os', 'systeme', 'systeme_exploitation'],
  ramMb: ['ram_mb', 'memory_mb'],
  ramGb: ['ram_gb', 'ram_go', 'memory_gb'],
  ramSlotsTotal: ['ram_slots_total', 'slots_total', 'memory_slots_total'],
  ramSlotsFree: ['ram_slots_free', 'slots_free', 'memory_slots_free'],
  batteryHealth: ['battery_health', 'battery_health_percent', 'etat_batterie'],
  cameraStatus: ['camera_status', 'camera', 'webcam_status'],
  usbStatus: ['usb_status', 'usb'],
  keyboardStatus: ['keyboard_status', 'keyboard', 'clavier_status', 'clavier'],
  padStatus: ['pad_status', 'touchpad_status', 'trackpad_status', 'pave_tactile', 'pad'],
  badgeReaderStatus: ['badge_reader_status', 'badge_reader', 'lecteur_badge', 'badge_status'],
  biosBattery: ['bios_battery', 'bios_battery_status', 'cmos_battery'],
  biosLanguage: ['bios_language', 'langue_bios'],
  biosPassword: ['bios_password', 'mot_de_passe_bios'],
  wifiStandard: ['wifi_standard', 'norme_wifi'],
  diskReadTest: ['disk_read_status', 'lecture_disque', 'disk_read_test'],
  diskWriteTest: ['disk_write_status', 'ecriture_disque', 'disk_write_test'],
  ramTest: ['ram_test_status', 'ram_test'],
  cpuTest: ['cpu_test_status', 'cpu_test'],
  gpuTest: ['gpu_test_status', 'gpu_test'],
  networkPing: ['network_ping_status', 'ping_status', 'network_ping'],
  fsCheck: ['fs_check_status', 'check_disque', 'fs_check'],
  cpuName: ['cpu_name', 'processeur'],
  cpuCores: ['cpu_cores', 'coeurs_cpu'],
  cpuThreads: ['cpu_threads', 'threads_cpu'],
  gpuName: ['gpu_name', 'carte_graphique'],
  primaryDiskModel: ['primary_disk_model', 'disk_model', 'modele_disque'],
  primaryDiskSizeGb: ['primary_disk_size_gb', 'disk_size_gb', 'taille_disque_gb'],
  storageTotalGb: ['storage_total_gb', 'total_storage_gb', 'stockage_total_gb'],
  autopilotHash: ['autopilot_hash', 'hardware_hash', 'device_hardware_hash'],
  shipmentDate: ['shipment_date', 'date_expedition', 'date_d_expedition', 'expedition_date'],
  shipmentClient: ['shipment_client', 'client'],
  shipmentOrderNumber: ['shipment_order_number', 'order_number', 'commande', 'numero_commande'],
  shipmentPalletCode: ['shipment_pallet_code', 'pallet_code', 'palette', 'numero_palette'],
  doubleCheck: ['double_check', 'doublecheck', 'controle_double', 'double_check_flag']
};

function getAllowedComponentStatuses(key) {
  return COMPONENT_ALLOWED_STATUSES[key] || DEFAULT_COMPONENT_STATUSES;
}

app.set('trust proxy', process.env.TRUST_PROXY === '1' || FORCE_HTTPS);
app.use(express.json({ limit: JSON_LIMIT }));
app.use(express.urlencoded({ extended: false, limit: '16kb' }));
app.use((req, res, next) => {
  const headerId = cleanString(req.get('x-request-id'), 128);
  req.requestId = headerId || generateRequestId();
  res.setHeader('X-Request-Id', req.requestId);
  next();
});
app.use((req, res, next) => {
  if (!FORCE_HTTPS) {
    return next();
  }
  if (FORCE_HTTPS_ALLOW_HTTP_INGEST && req.path.startsWith('/api/ingest')) {
    return next();
  }
  if (
    FORCE_HTTPS_HEALTHCHECK_BYPASS &&
    req.method === 'GET' &&
    (req.path === '/api/health' || req.path === '/health')
  ) {
    return next();
  }
  const forwardedProto = cleanString(req.get('x-forwarded-proto'), 32);
  const requestIsSecure =
    req.secure ||
    req.protocol === 'https' ||
    (forwardedProto ? forwardedProto.split(',')[0].trim().toLowerCase() === 'https' : false);
  if (requestIsSecure) {
    return next();
  }

  const redirectPath = req.originalUrl || req.url || '/';
  const safePath = redirectPath.startsWith('/') ? redirectPath : '/';
  let redirectTarget = '';
  if (HTTPS_PUBLIC_ORIGIN) {
    try {
      const parsed = new URL(HTTPS_PUBLIC_ORIGIN);
      redirectTarget = `${parsed.origin}${safePath}`;
    } catch (error) {
      redirectTarget = '';
    }
  }
  if (!redirectTarget) {
    const host = cleanString(req.get('host'), 255);
    if (!host) {
      return res.status(400).json({ ok: false, error: 'invalid_host' });
    }
    redirectTarget = `https://${host}${safePath}`;
  }
  return res.redirect(
    [301, 302, 307, 308].includes(FORCE_HTTPS_REDIRECT_CODE) ? FORCE_HTTPS_REDIRECT_CODE : 308,
    redirectTarget
  );
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
      secure: COOKIE_SECURE || FORCE_HTTPS
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
  if (isSupportedSessionUser(req.session?.user)) {
    return next();
  }
  if (req.session && req.session.user) {
    return clearSessionUser(req, () => {
      if (req.path.startsWith('/api')) {
        return res.status(401).json({ ok: false, error: 'sso_only' });
      }
      if (req.accepts('html')) {
        return res.redirect('/login?error=sso_only');
      }
      return res.status(401).json({ ok: false, error: 'sso_only' });
    });
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
  return requirePermission('canManageLdap')(req, res, next);
}

function requireAdminPage(req, res, next) {
  return requirePermissionPage('canAccessAdminPage')(req, res, next);
}

function getPatchnoteUser(req) {
  const user = req.session && req.session.user ? req.session.user : null;
  if (!user || !user.username || !user.type) {
    return null;
  }
  return { username: user.username, type: user.type };
}

function isSupportedSessionUser(user) {
  if (!user || typeof user !== 'object') {
    return false;
  }
  const username = Boolean(cleanString(user.username, 128));
  if (!username) {
    return false;
  }
  return user.type === 'microsoft';
}

function clearSessionUser(req, callback) {
  if (!req.session) {
    callback();
    return;
  }
  delete req.session.user;
  delete req.session.microsoftAuthState;
  delete req.session.microsoftAuthStartedAt;
  req.session.save(() => callback());
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
  const currentPermissions = normalizePermissionSet(user.permissions);
  const accessLevel = normalizeAccessLevel(user.accessLevel);
  if (
    (user.isHydraAdmin === true && currentPermissions.canDeleteReport === true) ||
    (accessLevel === ACCESS_LEVELS.operator && currentPermissions.canEditReports === true)
  ) {
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
    const accessLevel = isHydraAdmin ? ACCESS_LEVELS.admin : ACCESS_LEVELS.operator;
    const updated = {
      ...user,
      groups,
      accessLevel,
      isHydraAdmin,
      permissions: buildPermissionsForAccessLevel(accessLevel)
    };
    req.session.user = updated;
    req.session.save(() => {});
    return updated;
  } catch (error) {
    return user;
  }
}

function canDeleteReports(user) {
  return hasPermission(user, 'canDeleteReport');
}

function canEditTags(user) {
  return hasPermission(user, 'canEditTags');
}

function canManageLogistics(user) {
  return hasPermission(user, 'canManageLogistics');
}

function canOperateReports(user) {
  return hasPermission(user, 'canEditReports');
}

function canManageLdap(user) {
  return hasPermission(user, 'canManageLdap');
}

function requirePermission(permissionKey) {
  return (req, res, next) => {
    if (!req.session || !req.session.user) {
      return res.status(401).json({ ok: false, error: 'unauthorized' });
    }
    if (!isSupportedSessionUser(req.session.user)) {
      return clearSessionUser(req, () => res.status(401).json({ ok: false, error: 'sso_only' }));
    }
    if (hasPermission(req.session.user, permissionKey)) {
      return next();
    }
    return refreshLdapPermissions(req)
      .then((user) => {
        if (!hasPermission(user, permissionKey)) {
          return res.status(403).json({ ok: false, error: 'forbidden' });
        }
        return next();
      })
      .catch(() => res.status(403).json({ ok: false, error: 'forbidden' }));
  };
}

function requirePermissionPage(permissionKey) {
  return (req, res, next) => {
    if (!req.session || !req.session.user) {
      return res.redirect('/login');
    }
    if (!isSupportedSessionUser(req.session.user)) {
      return clearSessionUser(req, () => res.redirect('/login?error=sso_only'));
    }
    if (hasPermission(req.session.user, permissionKey)) {
      return next();
    }
    return refreshLdapPermissions(req)
      .then((user) => {
        if (!hasPermission(user, permissionKey)) {
          return res.redirect('/');
        }
        return next();
      })
      .catch(() => res.redirect('/'));
  };
}

function requireReportDelete(req, res, next) {
  return requirePermission('canDeleteReport')(req, res, next);
}

function requireOperator(req, res, next) {
  return requirePermission('canEditReports')(req, res, next);
}

function requireBatteryHealthEdit(req, res, next) {
  return requirePermission('canEditBatteryHealth')(req, res, next);
}

function requireTechnicianEdit(req, res, next) {
  return requirePermission('canEditTechnician')(req, res, next);
}

function requireLogistics(req, res, next) {
  return requirePermission('canManageLogistics')(req, res, next);
}

function requireLogisticsPage(req, res, next) {
  return requirePermissionPage('canManageLogistics')(req, res, next);
}

function requireTagEdit(req, res, next) {
  return requireLogistics(req, res, next);
}

function requireTagEditPage(req, res, next) {
  return requireLogisticsPage(req, res, next);
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
const DEFAULT_FALLBACK_TECHNICIAN = 'Luka';
const UNKNOWN_TECHNICIAN_KEYS = new Set([
  'unknown',
  'inconnu',
  '--',
  'none',
  'null',
  'n/a',
  'na',
  'non renseigne',
  'non-renseigne',
  'non renseignee',
  'non-renseignee'
]);

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

function normalizeReportTechnician(value, { fallback = null } = {}) {
  const cleaned = cleanString(value, 64);
  if (!cleaned) {
    return null;
  }
  const technicianKey = normalizeTechKey(cleaned);
  if (!technicianKey) {
    return null;
  }
  if (UNKNOWN_TECHNICIAN_KEYS.has(technicianKey)) {
    return fallback;
  }
  return cleaned;
}

function normalizeMdtBetaSlug(value) {
  return String(value || '')
    .trim()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .slice(0, 48);
}

function normalizeMdtTaskSequenceId(value) {
  return String(value || '')
    .trim()
    .toUpperCase()
    .replace(/[^A-Z0-9-]+/g, '-')
    .replace(/-+/g, '-')
    .replace(/^-+|-+$/g, '')
    .slice(0, 64);
}

function buildMdtBetaTaskSequenceId(slug) {
  return normalizeMdtTaskSequenceId(`MDT-AUTO-${String(slug || '').toUpperCase()}`);
}

function buildMdtBetaTaskSequenceName(displayName) {
  return cleanString(`MDT-AUTO-${String(displayName || '').trim()}`, 128) || 'MDT-AUTO';
}

function getSessionActorName(req) {
  return (
    cleanString(
      req.session?.user?.displayName ||
        req.session?.user?.username ||
        req.session?.user?.mail ||
        'systeme',
      128
    ) || 'systeme'
  );
}

function buildMdtBetaAgentAuditContext(req, agentId, overrides = {}) {
  const safeAgentId = cleanString(agentId, 128) || 'mdt-beta-agent';
  return buildAuditContext(req, {
    actor: `mdt-beta-agent:${safeAgentId}`,
    actorType: 'system',
    source: overrides.source || `${req.method} ${req.originalUrl}`
  });
}

function buildMdtBetaJobPayload(technician) {
  return {
    technicianId: technician.id,
    displayName: technician.displayName,
    slug: technician.slug,
    sourceTaskSequenceId: technician.sourceTaskSequenceId,
    destinationTaskSequenceId: technician.betaTaskSequenceId,
    destinationTaskSequenceName: technician.betaTaskSequenceName,
    taskSequenceGroupName: technician.taskSequenceGroupName || MDT_BETA_GROUP_NAME,
    betaScriptsFolder: technician.scriptsFolder || MDT_BETA_SCRIPTS_FOLDER
  };
}

function mapMdtBetaTechnicianRow(row) {
  if (!row) {
    return null;
  }
  return {
    id: row.id,
    displayName: row.display_name,
    slug: row.slug,
    sourceTaskSequenceId: row.source_task_sequence_id,
    betaTaskSequenceId: row.beta_task_sequence_id,
    betaTaskSequenceName: row.beta_task_sequence_name,
    taskSequenceGroupName: row.task_sequence_group_name,
    scriptsFolder: row.scripts_folder,
    status: row.status,
    createdBy: row.created_by || null,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
    lastJobId: row.last_job_id || null,
    lastError: row.last_error || null,
    lastResult: row.last_result || null,
    latestJob: row.job_id
      ? {
          id: row.job_id,
          status: row.job_status,
          agentId: row.job_agent_id || null,
          createdAt: row.job_created_at,
          startedAt: row.job_started_at,
          finishedAt: row.job_finished_at,
          error: row.job_error || null,
          result: row.job_result || null
        }
      : null
  };
}

async function listMdtBetaTechnicians(client = pool) {
  const result = await client.query(
    `
      SELECT
        t.*,
        j.id AS job_id,
        j.status AS job_status,
        j.agent_id AS job_agent_id,
        j.created_at AS job_created_at,
        j.started_at AS job_started_at,
        j.finished_at AS job_finished_at,
        j.error AS job_error,
        j.result AS job_result
      FROM mdt_beta_technicians t
      LEFT JOIN LATERAL (
        SELECT id, status, agent_id, created_at, started_at, finished_at, error, result
        FROM mdt_beta_jobs
        WHERE technician_id = t.id
        ORDER BY created_at DESC, id DESC
        LIMIT 1
      ) j ON TRUE
      ORDER BY t.created_at DESC, t.display_name ASC
    `
  );
  return result.rows.map((row) => mapMdtBetaTechnicianRow(row));
}

async function getLatestMdtBetaAgent(client = pool) {
  const result = await client.query(
    `
      SELECT
        agent_id,
        hostname,
        deployment_share_root,
        task_sequence_group_name,
        scripts_folder,
        status,
        last_seen_at,
        last_job_id,
        last_error,
        updated_at
      FROM mdt_beta_agents
      ORDER BY last_seen_at DESC, agent_id ASC
      LIMIT 1
    `
  );
  const row = result.rows && result.rows[0] ? result.rows[0] : null;
  if (!row) {
    return null;
  }
  return {
    agentId: row.agent_id,
    hostname: row.hostname || null,
    deploymentShareRoot: row.deployment_share_root || null,
    taskSequenceGroupName: row.task_sequence_group_name || null,
    scriptsFolder: row.scripts_folder || null,
    status: row.status || 'unknown',
    lastSeenAt: row.last_seen_at,
    lastJobId: row.last_job_id || null,
    lastError: row.last_error || null,
    updatedAt: row.updated_at
  };
}

async function buildMdtBetaAdminPayload() {
  const [technicians, latestAgent, queueResult] = await Promise.all([
    listMdtBetaTechnicians(),
    getLatestMdtBetaAgent(),
    pool.query(
      `
        SELECT
          COUNT(*) FILTER (WHERE status = 'queued')::integer AS queued_count,
          COUNT(*) FILTER (WHERE status = 'running')::integer AS running_count,
          COUNT(*) FILTER (WHERE status = 'failed')::integer AS failed_count,
          COUNT(*) FILTER (WHERE status = 'succeeded')::integer AS succeeded_count
        FROM mdt_beta_jobs
      `
    )
  ]);
  const queue = queueResult.rows && queueResult.rows[0] ? queueResult.rows[0] : {};
  return {
    enabled: MDT_BETA_AUTOMATION_ENABLED,
    defaults: {
      sourceTaskSequenceId: MDT_BETA_DEFAULT_SOURCE_TASK_SEQUENCE_ID,
      taskSequenceGroupName: MDT_BETA_GROUP_NAME,
      scriptsFolder: MDT_BETA_SCRIPTS_FOLDER
    },
    agent: latestAgent,
    queue: {
      queuedCount: Number(queue.queued_count) || 0,
      runningCount: Number(queue.running_count) || 0,
      failedCount: Number(queue.failed_count) || 0,
      succeededCount: Number(queue.succeeded_count) || 0
    },
    technicians
  };
}

async function upsertMdtBetaAgentState(
  client,
  {
    agentId,
    hostname = null,
    deploymentShareRoot = null,
    taskSequenceGroupName = null,
    scriptsFolder = null,
    status = 'idle',
    lastJobId = null,
    lastError = null
  }
) {
  await client.query(
    `
      INSERT INTO mdt_beta_agents (
        agent_id,
        hostname,
        deployment_share_root,
        task_sequence_group_name,
        scripts_folder,
        status,
        last_seen_at,
        last_job_id,
        last_error,
        updated_at
      )
      VALUES ($1, $2, $3, $4, $5, $6, NOW(), $7, $8, NOW())
      ON CONFLICT (agent_id) DO UPDATE SET
        hostname = EXCLUDED.hostname,
        deployment_share_root = EXCLUDED.deployment_share_root,
        task_sequence_group_name = EXCLUDED.task_sequence_group_name,
        scripts_folder = EXCLUDED.scripts_folder,
        status = EXCLUDED.status,
        last_seen_at = NOW(),
        last_job_id = EXCLUDED.last_job_id,
        last_error = EXCLUDED.last_error,
        updated_at = NOW()
    `,
    [
      agentId,
      hostname,
      deploymentShareRoot,
      taskSequenceGroupName,
      scriptsFolder,
      status,
      normalizeUuid(lastJobId),
      cleanString(lastError, 2000)
    ]
  );
}

async function requeueStaleMdtBetaJobs(client) {
  const staleResult = await client.query(
    `
      UPDATE mdt_beta_jobs
      SET
        status = 'queued',
        agent_id = NULL,
        claimed_at = NULL,
        heartbeat_at = NULL,
        started_at = NULL,
        updated_at = NOW(),
        error = COALESCE(error, 'Job requeue after timeout')
      WHERE status = 'running'
        AND COALESCE(heartbeat_at, claimed_at, started_at, created_at) < NOW() - ($1 * INTERVAL '1 millisecond')
      RETURNING technician_id
    `,
    [MDT_BETA_JOB_RUNNING_TIMEOUT_MS]
  );
  const technicianIds = staleResult.rows
    .map((row) => normalizeUuid(row.technician_id))
    .filter(Boolean);
  if (technicianIds.length) {
    await client.query(
      `
        UPDATE mdt_beta_technicians
        SET status = 'queued', updated_at = NOW()
        WHERE id = ANY($1::uuid[])
      `,
      [technicianIds]
    );
  }
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

function normalizePalletCode(value) {
  const code = cleanString(value, 96);
  if (!code) {
    return null;
  }
  return code.toUpperCase();
}

function normalizePalletCodeKey(value) {
  const code = normalizePalletCode(value);
  return code ? code.toLowerCase() : null;
}

function normalizePalletMovementType(value) {
  if (value == null) {
    return null;
  }
  const lowered = String(value).trim().toLowerCase();
  if (!lowered) {
    return null;
  }
  if (['entry', 'entree', 'entrée', 'in', 'stock', 'in_stock'].includes(lowered)) {
    return 'entry';
  }
  if (['exit', 'sortie', 'out', 'shipped', 'expedition', 'expédition'].includes(lowered)) {
    return 'exit';
  }
  return null;
}

function getPalletMovementLabel(value) {
  return normalizePalletMovementType(value) === 'exit' ? 'Sortie' : 'Entree';
}

function buildPalletLabel(code, movementType) {
  const palletCode = normalizePalletCode(code) || 'Palette inconnue';
  const typeLabel = movementType ? getPalletMovementLabel(movementType) : null;
  return typeLabel ? `${palletCode} - ${typeLabel}` : palletCode;
}

function normalizeShipmentDate(value) {
  if (value == null) {
    return null;
  }
  if (value instanceof Date && !Number.isNaN(value.getTime())) {
    return value.toISOString().slice(0, 10);
  }
  const raw = String(value).trim();
  if (!raw) {
    return null;
  }
  if (/^\d{4}-\d{2}-\d{2}$/.test(raw)) {
    return raw;
  }
  const dateMatch = raw.match(/^(\d{1,2})[\/.-](\d{1,2})[\/.-](\d{2,4})$/);
  if (dateMatch) {
    const day = Number.parseInt(dateMatch[1], 10);
    const month = Number.parseInt(dateMatch[2], 10);
    let year = Number.parseInt(dateMatch[3], 10);
    if (year < 100) {
      year += year >= 70 ? 1900 : 2000;
    }
    const parsed = new Date(Date.UTC(year, month - 1, day));
    if (
      parsed.getUTCFullYear() === year &&
      parsed.getUTCMonth() === month - 1 &&
      parsed.getUTCDate() === day
    ) {
      return parsed.toISOString().slice(0, 10);
    }
  }
  const parsedTimestamp = Date.parse(raw);
  if (Number.isFinite(parsedTimestamp)) {
    return new Date(parsedTimestamp).toISOString().slice(0, 10);
  }
  return null;
}

function normalizeShipmentClient(value) {
  return cleanString(value, 160) || null;
}

function normalizeShipmentOrderNumber(value) {
  return cleanString(value, 128) || null;
}

function normalizeShipmentFromRow(row) {
  if (!row || typeof row !== 'object') {
    return null;
  }
  if (row.shipment && typeof row.shipment === 'object') {
    const nestedDate = normalizeShipmentDate(row.shipment.date);
    const nestedClient = normalizeShipmentClient(row.shipment.client);
    const nestedOrder = normalizeShipmentOrderNumber(row.shipment.orderNumber);
    const nestedPalletCode = normalizePalletCode(row.shipment.palletCode || row.shipment.pallet);
    if (!nestedDate && !nestedClient && !nestedOrder && !nestedPalletCode) {
      return null;
    }
    return {
      date: nestedDate,
      client: nestedClient,
      orderNumber: nestedOrder,
      palletCode: nestedPalletCode
    };
  }

  const shipmentDate = normalizeShipmentDate(
    row.shipment_date ||
      row.report_shipment_date ||
      row.machine_shipment_date ||
      row.expedition_date ||
      row.date_expedition
  );
  const shipmentClient = normalizeShipmentClient(
    row.shipment_client || row.report_shipment_client || row.machine_shipment_client || row.client
  );
  const shipmentOrderNumber = normalizeShipmentOrderNumber(
    row.shipment_order_number ||
      row.report_shipment_order_number ||
      row.machine_shipment_order_number ||
      row.order_number ||
      row.commande
  );
  const shipmentPalletCode = normalizePalletCode(
    row.shipment_pallet_code ||
      row.report_shipment_pallet_code ||
      row.machine_shipment_pallet_code ||
      row.pallet_code ||
      row.report_pallet_code ||
      row.machine_pallet_code ||
      row.code
  );
  if (!shipmentDate && !shipmentClient && !shipmentOrderNumber && !shipmentPalletCode) {
    return null;
  }
  return {
    date: shipmentDate,
    client: shipmentClient,
    orderNumber: shipmentOrderNumber,
    palletCode: shipmentPalletCode
  };
}

function normalizeNullableInteger(value) {
  if (value == null || value === '') {
    return null;
  }
  const parsed = Number.parseInt(String(value), 10);
  return Number.isFinite(parsed) ? parsed : null;
}

function normalizeNullableNumber(value) {
  if (value == null || value === '') {
    return null;
  }
  const parsed = Number.parseFloat(String(value).replace(',', '.').trim());
  return Number.isFinite(parsed) ? parsed : null;
}

function parseClockAlertCodes(value) {
  if (!value) {
    return [];
  }
  const parts = Array.isArray(value)
    ? value
    : String(value)
        .split(',')
        .map((item) => item.trim().toLowerCase())
        .filter(Boolean);
  const normalized = Array.from(new Set(parts));
  return CLOCK_ALERT_REASON_ORDER.filter((code) => normalized.includes(code));
}

function normalizeClientGeneratedAt(value) {
  if (value == null) {
    return null;
  }
  if (value instanceof Date && !Number.isNaN(value.getTime())) {
    return value.toISOString();
  }
  const raw = cleanString(value, 96);
  if (!raw) {
    return null;
  }
  const timestamp = Date.parse(raw);
  if (!Number.isFinite(timestamp)) {
    return null;
  }
  return new Date(timestamp).toISOString();
}

function extractClientGeneratedAt(body, extraSources = []) {
  const nestedDiag =
    body && body.diag && typeof body.diag === 'object' && !Array.isArray(body.diag) ? body.diag : null;
  const nestedRawArtifacts =
    body && body.rawArtifacts && typeof body.rawArtifacts === 'object' && !Array.isArray(body.rawArtifacts)
      ? body.rawArtifacts
      : null;
  const sources = [
    body,
    ...(Array.isArray(extraSources) ? extraSources : []),
    nestedDiag,
    nestedRawArtifacts
  ].filter((item) => item && typeof item === 'object' && !Array.isArray(item));
  return normalizeClientGeneratedAt(
    pickFirstFromSources(sources, [
      'clientGeneratedAt',
      'client_generated_at',
      'generatedAt',
      'generated_at',
      'completedAt',
      'completed_at',
      'collectedAt',
      'collected_at',
      'timestamp',
      'reportTime',
      'report_time'
    ])
  );
}

function buildClockAlertAssessment({
  serverSeenAt,
  clientGeneratedAt,
  previousServerSeenAt = null,
  previousClientGeneratedAt = null
} = {}) {
  const currentServerMs = Date.parse(serverSeenAt || '');
  const currentClientMs = Date.parse(clientGeneratedAt || '');
  if (!Number.isFinite(currentServerMs) || !Number.isFinite(currentClientMs)) {
    return {
      clientGeneratedAt: normalizeClientGeneratedAt(clientGeneratedAt),
      driftSeconds: null,
      deltaSeconds: null,
      active: false,
      code: null,
      reasons: []
    };
  }

  const reasons = [];
  const driftSeconds = Math.round(Math.abs(currentServerMs - currentClientMs) / 1000);
  if (driftSeconds >= BIOS_CLOCK_DRIFT_ALERT_THRESHOLD_SECONDS) {
    reasons.push('clock_drift');
  }

  let deltaSeconds = null;
  const previousServerMs = Date.parse(previousServerSeenAt || '');
  const previousClientMs = Date.parse(previousClientGeneratedAt || '');
  if (Number.isFinite(previousServerMs) && Number.isFinite(previousClientMs)) {
    const serverDeltaSeconds = Math.round((currentServerMs - previousServerMs) / 1000);
    const clientDeltaSeconds = Math.round((currentClientMs - previousClientMs) / 1000);
    deltaSeconds = Math.round(Math.abs(serverDeltaSeconds - clientDeltaSeconds));
    if (clientDeltaSeconds < -BIOS_CLOCK_BACKWARD_GRACE_SECONDS) {
      reasons.push('clock_backwards');
    }
    if (deltaSeconds >= BIOS_CLOCK_DELTA_ALERT_THRESHOLD_SECONDS) {
      reasons.push('delta_mismatch');
    }
  }

  const orderedReasons = CLOCK_ALERT_REASON_ORDER.filter((reason) => reasons.includes(reason));
  return {
    clientGeneratedAt: new Date(currentClientMs).toISOString(),
    driftSeconds,
    deltaSeconds: Number.isFinite(deltaSeconds) ? deltaSeconds : null,
    active: orderedReasons.length > 0,
    code: orderedReasons.length ? orderedReasons.join(',') : null,
    reasons: orderedReasons
  };
}

function normalizeClockAlertFromRow(row) {
  if (!row || typeof row !== 'object') {
    return null;
  }
  const clientGeneratedAt = normalizeClientGeneratedAt(
    row.client_generated_at || row.report_client_generated_at || row.machine_client_generated_at
  );
  const firstClientGeneratedAt = normalizeClientGeneratedAt(
    row.first_client_generated_at || row.machine_first_client_generated_at
  ) || clientGeneratedAt;
  const driftSeconds = normalizeNullableInteger(
    row.clock_drift_seconds || row.report_clock_drift_seconds || row.machine_clock_drift_seconds
  );
  const deltaSeconds = normalizeNullableInteger(
    row.clock_delta_seconds || row.report_clock_delta_seconds || row.machine_clock_delta_seconds
  );
  const reasons = parseClockAlertCodes(
    row.bios_clock_alert_code || row.report_bios_clock_alert_code || row.machine_bios_clock_alert_code
  );
  const active =
    parseBooleanFlag(
      row.bios_clock_alert ?? row.report_bios_clock_alert ?? row.machine_bios_clock_alert,
      false
    ) || reasons.length > 0;
  if (!clientGeneratedAt && !firstClientGeneratedAt && driftSeconds == null && deltaSeconds == null && !active) {
    return null;
  }
  return {
    active,
    reasons,
    clientGeneratedAt,
    firstClientGeneratedAt,
    serverSeenAt: row.report_last_seen || row.machine_last_seen || row.last_seen || null,
    firstServerSeenAt: row.machine_created_at || row.report_created_at || row.created_at || null,
    driftSeconds,
    deltaSeconds
  };
}

function applyClockAlertToComponents(components, row) {
  const merged =
    components && typeof components === 'object' && !Array.isArray(components)
      ? { ...components }
      : {};
  const clockAlert = normalizeClockAlertFromRow(row);
  if (clockAlert && clockAlert.active) {
    merged.biosBattery = 'nok';
  }
  return merged;
}

function normalizePalletFromRow(row) {
  if (!row) {
    return null;
  }
  const palletId = normalizeUuid(row.pallet_id || row.report_pallet_id || row.machine_pallet_id || row.id);
  const palletCode =
    normalizePalletCode(
      row.pallet_code ||
        row.report_pallet_code ||
        row.machine_pallet_code ||
        row.code
    ) || null;
  const movementType = normalizePalletMovementType(
    row.pallet_status ||
      row.report_pallet_status ||
      row.machine_pallet_status ||
      row.movement_type ||
      row.last_movement_type
  );
  const lastMovementAt =
    row.pallet_last_movement_at ||
    row.report_pallet_last_movement_at ||
    row.machine_pallet_last_movement_at ||
    row.last_movement_at ||
    row.updated_at ||
    null;
  if (!palletId && !palletCode && !movementType) {
    return null;
  }
  return {
    id: palletId || null,
    code: palletCode,
    status: movementType || null,
    statusLabel: movementType ? getPalletMovementLabel(movementType) : null,
    label: buildPalletLabel(palletCode, movementType),
    lastMovementAt,
    createdAt: row.created_at || null,
    updatedAt: row.updated_at || null
  };
}

function mapPalletRowForResponse(row) {
  const pallet = normalizePalletFromRow(row);
  if (!pallet) {
    return null;
  }
  return {
    ...pallet,
    totalCount: Number.parseInt(row.total_count || '0', 10) || 0,
    entryCount: Number.parseInt(row.entry_count || '0', 10) || 0,
    exitCount: Number.parseInt(row.exit_count || '0', 10) || 0,
    linkedCount: Number.parseInt(row.linked_count || '0', 10) || 0,
    createdBy: row.created_by || null
  };
}

function mapPalletImportRowForResponse(row) {
  if (!row) {
    return null;
  }
  const importType = normalizePalletMovementType(row.import_type);
  return {
    id: normalizeUuid(row.id) || null,
    importType: importType || null,
    importTypeLabel: importType ? getPalletMovementLabel(importType) : null,
    fileName: row.file_name || null,
    rowCount: Number.parseInt(row.row_count || '0', 10) || 0,
    appliedCount: Number.parseInt(row.applied_count || '0', 10) || 0,
    skippedCount: Number.parseInt(row.skipped_count || '0', 10) || 0,
    createdBy: row.created_by || null,
    createdAt: row.created_at || null,
    summary: (() => {
      if (!row.summary) {
        return null;
      }
      try {
        return JSON.parse(row.summary);
      } catch (error) {
        return null;
      }
    })()
  };
}

function detectCsvDelimiter(text) {
  const sampleLine = String(text || '')
    .replace(/^\uFEFF/, '')
    .split(/\r?\n/)
    .find((line) => line && line.trim());
  if (!sampleLine) {
    return ',';
  }
  const counts = [
    [',', (sampleLine.match(/,/g) || []).length],
    [';', (sampleLine.match(/;/g) || []).length],
    ['\t', (sampleLine.match(/\t/g) || []).length]
  ];
  counts.sort((a, b) => b[1] - a[1]);
  return counts[0][1] > 0 ? counts[0][0] : ',';
}

function parseCsvRows(text, delimiter = ',') {
  const source = String(text || '').replace(/^\uFEFF/, '');
  const rows = [];
  let row = [];
  let field = '';
  let inQuotes = false;

  for (let index = 0; index < source.length; index += 1) {
    const char = source[index];
    if (inQuotes) {
      if (char === '"') {
        if (source[index + 1] === '"') {
          field += '"';
          index += 1;
        } else {
          inQuotes = false;
        }
      } else {
        field += char;
      }
      continue;
    }
    if (char === '"') {
      inQuotes = true;
      continue;
    }
    if (char === delimiter) {
      row.push(field);
      field = '';
      continue;
    }
    if (char === '\n') {
      row.push(field);
      rows.push(row);
      row = [];
      field = '';
      continue;
    }
    if (char === '\r') {
      continue;
    }
    field += char;
  }

  row.push(field);
  if (row.some((item) => String(item || '').trim() !== '') || rows.length === 0) {
    rows.push(row);
  }
  return rows;
}

function normalizeCsvHeader(value) {
  if (value == null) {
    return '';
  }
  return String(value)
    .trim()
    .toLowerCase()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/[^a-z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '');
}

function extractPalletCsvRows(csvText) {
  const rows = parseCsvRows(csvText, detectCsvDelimiter(csvText))
    .map((row) => row.map((cell) => (cell == null ? '' : String(cell).trim())))
    .filter((row) => row.some((cell) => cell !== ''));
  if (!rows.length) {
    return { ok: false, error: 'empty_csv', rows: [], errors: [{ line: 1, error: 'Fichier CSV vide.' }] };
  }

  const headerCandidates = rows[0].map((cell) => normalizeCsvHeader(cell));
  const serialHeaders = new Set([
    'serial',
    'serial_number',
    'serialnumber',
    'numero_serie',
    'numero_de_serie',
    'sn'
  ]);
  const palletHeaders = new Set([
    'palette',
    'palette_code',
    'palettecode',
    'pallet',
    'pallet_code',
    'palletcode'
  ]);
  const shipmentDateHeaders = new Set([
    'date_expedition',
    'date_d_expedition',
    'expedition_date',
    'shipment_date',
    'shipping_date',
    'date_sortie'
  ]);
  const shipmentClientHeaders = new Set([
    'client',
    'customer',
    'customer_name',
    'nom_client'
  ]);
  const shipmentOrderHeaders = new Set([
    'commande',
    'order',
    'order_number',
    'ordernumber',
    'numero_commande',
    'numero_de_commande',
    'n_commande',
    'n_de_commande'
  ]);
  let serialIndex = headerCandidates.findIndex((cell) => serialHeaders.has(cell));
  let palletIndex = headerCandidates.findIndex((cell) => palletHeaders.has(cell));
  const shipmentDateIndex = headerCandidates.findIndex((cell) => shipmentDateHeaders.has(cell));
  const shipmentClientIndex = headerCandidates.findIndex((cell) => shipmentClientHeaders.has(cell));
  const shipmentOrderIndex = headerCandidates.findIndex((cell) => shipmentOrderHeaders.has(cell));
  let dataStart = 1;

  if (serialIndex === -1 || palletIndex === -1) {
    serialIndex = 0;
    palletIndex = rows[0].length > 1 ? 1 : -1;
    dataStart = 0;
  }

  if (serialIndex === -1 || palletIndex === -1) {
    return {
      ok: false,
      error: 'invalid_columns',
      rows: [],
      errors: [{ line: 1, error: 'Colonnes attendues: serial_number et palette_code.' }]
    };
  }

  const records = [];
  const errors = [];
  const seenSerials = new Set();

  for (let index = dataStart; index < rows.length; index += 1) {
    const lineNumber = index + 1;
    const row = rows[index];
    const serialNumber = normalizeSerial(row[serialIndex]);
    const palletCode = normalizePalletCode(row[palletIndex]);
    const shipmentDateRaw = shipmentDateIndex >= 0 ? row[shipmentDateIndex] : '';
    const shipmentClientRaw = shipmentClientIndex >= 0 ? row[shipmentClientIndex] : '';
    const shipmentOrderRaw = shipmentOrderIndex >= 0 ? row[shipmentOrderIndex] : '';
    const shipmentDate = normalizeShipmentDate(shipmentDateRaw);
    const shipmentClient = normalizeShipmentClient(shipmentClientRaw);
    const shipmentOrderNumber = normalizeShipmentOrderNumber(shipmentOrderRaw);

    if (!serialNumber && !palletCode) {
      continue;
    }
    if (!serialNumber) {
      errors.push({ line: lineNumber, error: 'Numero de serie manquant ou invalide.' });
      continue;
    }
    if (!palletCode) {
      errors.push({ line: lineNumber, error: 'Code palette manquant ou invalide.', serialNumber });
      continue;
    }
    if (shipmentDateRaw && !shipmentDate) {
      errors.push({ line: lineNumber, error: "Date d'expedition invalide.", serialNumber });
      continue;
    }
    if (seenSerials.has(serialNumber)) {
      errors.push({ line: lineNumber, error: 'Numero de serie duplique dans le CSV.', serialNumber });
      continue;
    }
    seenSerials.add(serialNumber);
    records.push({
      lineNumber,
      serialNumber,
      palletCode,
      shipmentDate,
      shipmentClient,
      shipmentOrderNumber,
      shipmentPalletCode: palletCode
    });
  }

  return { ok: true, rows: records, errors };
}

function escapeCsvCell(value) {
  if (value == null) {
    return '';
  }
  const text = String(value);
  if (/[",\n\r]/.test(text)) {
    return `"${text.replace(/"/g, '""')}"`;
  }
  return text;
}

function buildManualReportTemplateCsv() {
  const header = MANUAL_REPORT_TEMPLATE_COLUMNS.join(',');
  const sample = MANUAL_REPORT_TEMPLATE_COLUMNS.map((column) =>
    escapeCsvCell(MANUAL_REPORT_TEMPLATE_SAMPLE[column] || '')
  ).join(',');
  return `${header}\n${sample}\n`;
}

function findCsvHeaderIndex(headerCandidates, aliases) {
  if (!Array.isArray(headerCandidates) || !Array.isArray(aliases) || aliases.length === 0) {
    return -1;
  }
  return headerCandidates.findIndex((cell) => aliases.includes(cell));
}

function normalizeIntegerRange(value, min, max) {
  if (value == null) {
    return null;
  }
  const raw = String(value).trim();
  if (!raw) {
    return null;
  }
  const parsed = Number.parseInt(raw, 10);
  if (!Number.isFinite(parsed) || parsed < min || parsed > max) {
    return null;
  }
  return parsed;
}

function normalizeSizeGb(value) {
  if (value == null) {
    return null;
  }
  const raw = String(value).trim();
  if (!raw) {
    return null;
  }
  const normalized = raw.replace(',', '.').replace(/go\b/gi, 'gb');
  const match = normalized.match(/([0-9]+(?:\.[0-9]+)?)/);
  const parsed = match ? Number(match[1]) : null;
  if (parsed == null || parsed <= 0 || parsed > 32768) {
    return null;
  }
  return Math.round(parsed * 10) / 10;
}

function normalizeManualCsvStatus(value, { label, normalizer = normalizeStatus, errors, serialNumber, line } = {}) {
  if (value == null || String(value).trim() === '') {
    return null;
  }
  const normalized = normalizer(value);
  if (normalized) {
    return normalized;
  }
  if (Array.isArray(errors)) {
    errors.push({
      line,
      serialNumber: serialNumber || undefined,
      error: `${label} invalide.`
    });
  }
  return null;
}

function extractManualReportCsvRows(csvText) {
  const rows = parseCsvRows(csvText, detectCsvDelimiter(csvText))
    .map((row) => row.map((cell) => (cell == null ? '' : String(cell).trim())))
    .filter((row) => row.some((cell) => cell !== ''));
  if (!rows.length) {
    return {
      ok: false,
      error: 'empty_csv',
      rows: [],
      errors: [{ line: 1, error: 'Fichier CSV vide.' }],
      rowCount: 0
    };
  }

  const headerCandidates = rows[0].map((cell) => normalizeCsvHeader(cell));
  const columnMap = Object.fromEntries(
    Object.entries(MANUAL_REPORT_CSV_ALIASES).map(([key, aliases]) => [key, findCsvHeaderIndex(headerCandidates, aliases)])
  );
  const recognizedColumns = Object.values(columnMap).filter((index) => index >= 0).length;
  if (!recognizedColumns) {
    return {
      ok: false,
      error: 'invalid_header',
      rows: [],
      errors: [
        {
          line: 1,
          error: "En-tetes CSV inconnus. Utilise le modele de normalisation telechargeable."
        }
      ],
      rowCount: 0
    };
  }
  if (
    columnMap.hostname < 0 &&
    columnMap.serialNumber < 0 &&
    columnMap.macAddress < 0 &&
    columnMap.macAddresses < 0
  ) {
    return {
      ok: false,
      error: 'missing_identifier_columns',
      rows: [],
      errors: [
        {
          line: 1,
          error: "Colonnes d'identifiant manquantes. Ajoute au moins hostname, serial_number ou mac_address."
        }
      ],
      rowCount: 0
    };
  }

  const dataRows = rows.slice(1);
  if (dataRows.length > MANUAL_REPORT_IMPORT_MAX_ROWS) {
    return {
      ok: false,
      error: 'too_many_rows',
      rows: [],
      errors: [
        {
          line: 1,
          error: `Le fichier depasse la limite de ${MANUAL_REPORT_IMPORT_MAX_ROWS} lignes.`
        }
      ],
      rowCount: dataRows.length
    };
  }

  const parsedRows = [];
  const errors = [];
  const seenKeys = new Set();

  const getCell = (row, key) => {
    const index = columnMap[key];
    return index >= 0 && index < row.length ? row[index] : '';
  };

  dataRows.forEach((row, rowIndex) => {
    const lineNumber = rowIndex + 2;
    const rowErrorStart = errors.length;
    const raw = {
      hostname: getCell(row, 'hostname'),
      serialNumber: getCell(row, 'serialNumber'),
      macAddress: getCell(row, 'macAddress'),
      macAddresses: getCell(row, 'macAddresses'),
      category: getCell(row, 'category'),
      technician: getCell(row, 'technician'),
      lotId: getCell(row, 'lotId'),
      tag: getCell(row, 'tag'),
      tagId: getCell(row, 'tagId'),
      vendor: getCell(row, 'vendor'),
      model: getCell(row, 'model'),
      osVersion: getCell(row, 'osVersion'),
      ramMb: getCell(row, 'ramMb'),
      ramGb: getCell(row, 'ramGb'),
      ramSlotsTotal: getCell(row, 'ramSlotsTotal'),
      ramSlotsFree: getCell(row, 'ramSlotsFree'),
      batteryHealth: getCell(row, 'batteryHealth'),
      cameraStatus: getCell(row, 'cameraStatus'),
      usbStatus: getCell(row, 'usbStatus'),
      keyboardStatus: getCell(row, 'keyboardStatus'),
      padStatus: getCell(row, 'padStatus'),
      badgeReaderStatus: getCell(row, 'badgeReaderStatus'),
      biosBattery: getCell(row, 'biosBattery'),
      biosLanguage: getCell(row, 'biosLanguage'),
      biosPassword: getCell(row, 'biosPassword'),
      wifiStandard: getCell(row, 'wifiStandard'),
      diskReadTest: getCell(row, 'diskReadTest'),
      diskWriteTest: getCell(row, 'diskWriteTest'),
      ramTest: getCell(row, 'ramTest'),
      cpuTest: getCell(row, 'cpuTest'),
      gpuTest: getCell(row, 'gpuTest'),
      networkPing: getCell(row, 'networkPing'),
      fsCheck: getCell(row, 'fsCheck'),
      cpuName: getCell(row, 'cpuName'),
      cpuCores: getCell(row, 'cpuCores'),
      cpuThreads: getCell(row, 'cpuThreads'),
      gpuName: getCell(row, 'gpuName'),
      primaryDiskModel: getCell(row, 'primaryDiskModel'),
      primaryDiskSizeGb: getCell(row, 'primaryDiskSizeGb'),
      storageTotalGb: getCell(row, 'storageTotalGb'),
      autopilotHash: getCell(row, 'autopilotHash'),
      shipmentDate: getCell(row, 'shipmentDate'),
      shipmentClient: getCell(row, 'shipmentClient'),
      shipmentOrderNumber: getCell(row, 'shipmentOrderNumber'),
      shipmentPalletCode: getCell(row, 'shipmentPalletCode'),
      doubleCheck: getCell(row, 'doubleCheck')
    };

    if (!Object.values(raw).some((value) => String(value || '').trim() !== '')) {
      return;
    }

    const hostname = cleanString(raw.hostname, 64);
    let macAddress = raw.macAddress ? normalizeMac(raw.macAddress) : null;
    let macAddresses = raw.macAddresses ? normalizeMacList(raw.macAddresses) : null;
    const serialNumber = raw.serialNumber ? normalizeSerial(raw.serialNumber) : null;
    const category = raw.category ? normalizeCategory(raw.category) : 'unknown';
    const technician = normalizeReportTechnician(raw.technician, {
      fallback: DEFAULT_FALLBACK_TECHNICIAN
    });
    const lotId = raw.lotId ? normalizeUuid(raw.lotId) : null;
    const tag = cleanString(raw.tag, 64);
    const tagId = raw.tagId ? normalizeUuid(raw.tagId) : null;
    const vendor = cleanString(raw.vendor, 64);
    const model = cleanString(raw.model, 64);
    const osVersion = cleanString(raw.osVersion, 64);
    const batteryHealth = raw.batteryHealth ? normalizeBatteryHealth(raw.batteryHealth) : null;
    const ramMb = raw.ramMb
      ? normalizeRamMb(raw.ramMb)
      : raw.ramGb
        ? normalizeRamMb(`${raw.ramGb} GB`)
        : null;
    const ramSlotsTotal = raw.ramSlotsTotal ? normalizeSlots(raw.ramSlotsTotal) : null;
    const ramSlotsFree = raw.ramSlotsFree ? normalizeSlots(raw.ramSlotsFree) : null;
    const cpuCores = raw.cpuCores ? normalizeIntegerRange(raw.cpuCores, 1, 256) : null;
    const cpuThreads = raw.cpuThreads ? normalizeIntegerRange(raw.cpuThreads, 1, 512) : null;
    const primaryDiskSizeGb = raw.primaryDiskSizeGb ? normalizeSizeGb(raw.primaryDiskSizeGb) : null;
    const storageTotalGb = raw.storageTotalGb ? normalizeSizeGb(raw.storageTotalGb) : null;
    const autopilotHash = normalizeAutopilotHashValue(raw.autopilotHash);
    const shipmentDate = raw.shipmentDate ? normalizeShipmentDate(raw.shipmentDate) : null;
    const shipmentClient = normalizeShipmentClient(raw.shipmentClient);
    const shipmentOrderNumber = normalizeShipmentOrderNumber(raw.shipmentOrderNumber);
    const shipmentPalletCode = raw.shipmentPalletCode ? normalizePalletCode(raw.shipmentPalletCode) : null;
    const doubleCheck = parseBooleanFlag(raw.doubleCheck, false);

    if (raw.macAddress && !macAddress) {
      errors.push({ line: lineNumber, serialNumber, error: 'Adresse MAC invalide.' });
    }
    if (raw.macAddresses && !macAddresses) {
      errors.push({ line: lineNumber, serialNumber, error: 'Liste MAC invalide.' });
    }
    if (macAddress && (!macAddresses || !macAddresses.includes(macAddress))) {
      macAddresses = [macAddress, ...(macAddresses || [])];
    }
    if (!macAddress && macAddresses && macAddresses.length > 0) {
      macAddress = macAddresses[0];
    }
    if (raw.lotId && !lotId) {
      errors.push({ line: lineNumber, serialNumber, error: 'Lot ID invalide.' });
    }
    if (raw.tagId && !tagId) {
      errors.push({ line: lineNumber, serialNumber, error: 'Tag ID invalide.' });
    }
    if ((raw.ramMb || raw.ramGb) && ramMb == null) {
      errors.push({ line: lineNumber, serialNumber, error: 'Valeur RAM invalide.' });
    }
    if (raw.ramSlotsTotal && ramSlotsTotal == null) {
      errors.push({ line: lineNumber, serialNumber, error: 'RAM slots total invalide.' });
    }
    if (raw.ramSlotsFree && ramSlotsFree == null) {
      errors.push({ line: lineNumber, serialNumber, error: 'RAM slots libres invalide.' });
    }
    if (raw.batteryHealth && batteryHealth == null) {
      errors.push({ line: lineNumber, serialNumber, error: 'Sante batterie invalide.' });
    }
    if (raw.cpuCores && cpuCores == null) {
      errors.push({ line: lineNumber, serialNumber, error: 'Nombre de coeurs CPU invalide.' });
    }
    if (raw.cpuThreads && cpuThreads == null) {
      errors.push({ line: lineNumber, serialNumber, error: 'Nombre de threads CPU invalide.' });
    }
    if (raw.primaryDiskSizeGb && primaryDiskSizeGb == null) {
      errors.push({ line: lineNumber, serialNumber, error: 'Taille disque principale invalide.' });
    }
    if (raw.storageTotalGb && storageTotalGb == null) {
      errors.push({ line: lineNumber, serialNumber, error: 'Stockage total invalide.' });
    }
    if (raw.shipmentDate && !shipmentDate) {
      errors.push({ line: lineNumber, serialNumber, error: "Date d'expedition invalide." });
    }
    if (raw.shipmentPalletCode && !shipmentPalletCode) {
      errors.push({ line: lineNumber, serialNumber, error: "Numero de palette d'expedition invalide." });
    }

    const cameraStatus = normalizeManualCsvStatus(raw.cameraStatus, {
      label: 'Camera',
      errors,
      serialNumber,
      line: lineNumber
    });
    const usbStatus = normalizeManualCsvStatus(raw.usbStatus, {
      label: 'USB',
      errors,
      serialNumber,
      line: lineNumber
    });
    const keyboardStatus = normalizeManualCsvStatus(raw.keyboardStatus, {
      label: 'Clavier',
      errors,
      serialNumber,
      line: lineNumber
    });
    const padStatus = normalizeManualCsvStatus(raw.padStatus, {
      label: 'Pave tactile',
      errors,
      serialNumber,
      line: lineNumber
    });
    const badgeReaderStatus = normalizeManualCsvStatus(raw.badgeReaderStatus, {
      label: 'Lecteur badge',
      errors,
      serialNumber,
      line: lineNumber
    });
    const biosBattery = normalizeManualCsvStatus(raw.biosBattery, {
      label: 'Pile BIOS',
      errors,
      serialNumber,
      line: lineNumber
    });
    const biosLanguage = normalizeManualCsvStatus(raw.biosLanguage, {
      label: 'Langue BIOS',
      normalizer: normalizeBiosLanguage,
      errors,
      serialNumber,
      line: lineNumber
    });
    const biosPassword = normalizeManualCsvStatus(raw.biosPassword, {
      label: 'Mot de passe BIOS',
      normalizer: normalizeBiosPasswordStatus,
      errors,
      serialNumber,
      line: lineNumber
    });
    const wifiStandard = normalizeManualCsvStatus(raw.wifiStandard, {
      label: 'Norme Wi-Fi',
      normalizer: normalizeWifiStandardStatus,
      errors,
      serialNumber,
      line: lineNumber
    });
    const diskReadTest = normalizeManualCsvStatus(raw.diskReadTest, {
      label: 'Lecture disque',
      errors,
      serialNumber,
      line: lineNumber
    });
    const diskWriteTest = normalizeManualCsvStatus(raw.diskWriteTest, {
      label: 'Ecriture disque',
      errors,
      serialNumber,
      line: lineNumber
    });
    const ramTest = normalizeManualCsvStatus(raw.ramTest, {
      label: 'Test RAM',
      errors,
      serialNumber,
      line: lineNumber
    });
    const cpuTest = normalizeManualCsvStatus(raw.cpuTest, {
      label: 'Test CPU',
      errors,
      serialNumber,
      line: lineNumber
    });
    const gpuTest = normalizeManualCsvStatus(raw.gpuTest, {
      label: 'Test GPU',
      errors,
      serialNumber,
      line: lineNumber
    });
    const networkPing = normalizeManualCsvStatus(raw.networkPing, {
      label: 'Ping reseau',
      errors,
      serialNumber,
      line: lineNumber
    });
    const fsCheck = normalizeManualCsvStatus(raw.fsCheck, {
      label: 'Check disque',
      errors,
      serialNumber,
      line: lineNumber
    });

    if (!hostname && !macAddress && !serialNumber) {
      errors.push({
        line: lineNumber,
        error: 'Aucun identifiant exploitable. Renseigne au moins hostname, serial_number ou mac_address.'
      });
      return;
    }

    const machineKey = buildMachineKey(serialNumber, macAddress, hostname);
    if (!machineKey) {
      errors.push({
        line: lineNumber,
        serialNumber,
        error: "Impossible de construire la cle machine a partir des identifiants fournis."
      });
      return;
    }
    if (errors.length > rowErrorStart) {
      return;
    }
    if (seenKeys.has(machineKey)) {
      errors.push({
        line: lineNumber,
        serialNumber,
        error: 'Machine dupliquee dans le CSV.'
      });
      return;
    }

    seenKeys.add(machineKey);
    parsedRows.push({
      lineNumber,
      machineKey,
      hostname,
      serialNumber,
      macAddress,
      macAddresses,
      category,
      technician,
      lotId,
      tag,
      tagId,
      vendor,
      model,
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
      biosBattery,
      biosLanguage,
      biosPassword,
      wifiStandard,
      diskReadTest,
      diskWriteTest,
      ramTest,
      cpuTest,
      gpuTest,
      networkPing,
      fsCheck,
      cpuName: cleanString(raw.cpuName, 128),
      cpuCores,
      cpuThreads,
      gpuName: cleanString(raw.gpuName, 128),
      primaryDiskModel: cleanString(raw.primaryDiskModel, 160),
      primaryDiskSizeGb,
      storageTotalGb,
      autopilotHash,
      shipmentDate,
      shipmentClient,
      shipmentOrderNumber,
      shipmentPalletCode,
      doubleCheck
    });
  });

  return {
    ok: true,
    rows: parsedRows,
    errors,
    rowCount: dataRows.length
  };
}

function buildManualCsvReportPayload(row, reportId) {
  const tests = {};
  if (row.diskReadTest) tests.diskRead = row.diskReadTest;
  if (row.diskWriteTest) tests.diskWrite = row.diskWriteTest;
  if (row.ramTest) tests.ramTest = row.ramTest;
  if (row.cpuTest) tests.cpuTest = row.cpuTest;
  if (row.gpuTest) tests.gpuTest = row.gpuTest;
  if (row.networkPing) {
    tests.networkPing = row.networkPing;
    tests.networkPingTarget = '1.1.1.1';
  }
  if (row.fsCheck) tests.fsCheck = row.fsCheck;

  const cpu =
    row.cpuName || row.cpuCores || row.cpuThreads
      ? {
        name: row.cpuName || undefined,
        cores: row.cpuCores || undefined,
        threads: row.cpuThreads || undefined
      }
      : undefined;
  const gpu = row.gpuName ? { name: row.gpuName } : undefined;
  const diskSizeGb = row.primaryDiskSizeGb || row.storageTotalGb || null;
  const disks =
    row.primaryDiskModel || diskSizeGb
      ? [
        {
          model: row.primaryDiskModel || undefined,
          sizeGb: diskSizeGb || undefined
        }
      ]
      : undefined;
  const wifi =
    row.wifiStandard
      ? {
        standard: row.wifiStandard,
        standards: [row.wifiStandard]
      }
      : undefined;

  const payload = {
    reportId,
    hostname: row.hostname || null,
    macAddress: row.macAddress || null,
    macAddresses: row.macAddresses || undefined,
    serialNumber: row.serialNumber || null,
    category: row.category || 'unknown',
    technician: row.technician || null,
    vendor: row.vendor || null,
    model: row.model || null,
    osVersion: row.osVersion || null,
    diag: {
      type: row.doubleCheck ? 'double_check' : 'manual_csv',
      diagnosticsPerformed: Object.keys(tests).filter((key) => key !== 'networkPingTarget').length,
      appVersion: 'manual-csv'
    },
    manualImport: {
      source: 'csv',
      lineNumber: row.lineNumber
    }
  };

  if (Object.keys(tests).length > 0) {
    payload.tests = tests;
  }
  if (cpu) {
    payload.cpu = cpu;
  }
  if (gpu) {
    payload.gpu = gpu;
  }
  if (disks) {
    payload.disks = disks;
  }
  if (wifi) {
    payload.wifi = wifi;
  }
  if (row.autopilotHash) {
    payload.autopilotHash = row.autopilotHash;
  }

  const explicitComponents = {};
  if (row.cameraStatus) explicitComponents.camera = row.cameraStatus;
  if (row.usbStatus) explicitComponents.usb = row.usbStatus;
  if (row.keyboardStatus) explicitComponents.keyboard = row.keyboardStatus;
  if (row.padStatus) explicitComponents.pad = row.padStatus;
  if (row.badgeReaderStatus) explicitComponents.badgeReader = row.badgeReaderStatus;
  if (row.biosBattery) explicitComponents.biosBattery = row.biosBattery;
  if (row.biosLanguage) explicitComponents.biosLanguage = row.biosLanguage;
  if (row.biosPassword) explicitComponents.biosPassword = row.biosPassword;
  if (row.wifiStandard) explicitComponents.wifiStandard = row.wifiStandard;
  if (row.diskReadTest) explicitComponents.diskReadTest = row.diskReadTest;
  if (row.diskWriteTest) explicitComponents.diskWriteTest = row.diskWriteTest;
  if (row.ramTest) explicitComponents.ramTest = row.ramTest;
  if (row.cpuTest) explicitComponents.cpuTest = row.cpuTest;
  if (row.gpuTest) explicitComponents.gpuTest = row.gpuTest;
  if (row.networkPing) explicitComponents.networkPing = row.networkPing;
  if (row.fsCheck) explicitComponents.fsCheck = row.fsCheck;

  const derivedComponents = buildDerivedComponents(payload, [payload, payload.wifi]);
  const components = withManualComponentDefaults(mergeComponentSets(derivedComponents, explicitComponents));
  return {
    payload,
    components
  };
}

async function insertManualCsvReportRow(client, req, row) {
  const now = new Date().toISOString();
  const reportId = generateUuid();
  const actor = row.technician || row.machineKey;
  const { payload, components } = buildManualCsvReportPayload(row, reportId);
  const payloadText = safeJsonStringify(payload, 64 * 1024);
  const resolvedTag = await resolveTagForIngest(client, row.tagId || null, row.tag || null);
  const lotResolution = await resolveLotForIngest(client, {
    explicitLotId: row.lotId || null,
    technician: row.technician || null
  });
  const resolvedLot = lotResolution.lot;
  const resolvedLotId = resolvedLot && resolvedLot.id ? resolvedLot.id : null;
  const resolvedPallet = await resolvePalletForSerial(client, {
    serialNumber: row.serialNumber,
    machineKey: row.machineKey,
    actor
  });
  const resolvedPalletId = resolvedPallet && resolvedPallet.id ? resolvedPallet.id : null;
  const resolvedPalletStatus = resolvedPallet && resolvedPallet.status ? resolvedPallet.status : null;
  const palletShipment = normalizeShipmentFromRow(resolvedPallet);
  const resolvedShipment =
    row.shipmentDate || row.shipmentClient || row.shipmentOrderNumber || row.shipmentPalletCode
      ? {
        date: row.shipmentDate || null,
        client: row.shipmentClient || null,
        orderNumber: row.shipmentOrderNumber || null,
        palletCode: row.shipmentPalletCode || (palletShipment ? palletShipment.palletCode : null) || null
      }
      : palletShipment;
  const shouldCountLot = Boolean(
    resolvedLotId &&
    row.machineKey &&
    !row.doubleCheck &&
    !parseBooleanFlag(resolvedLot ? resolvedLot.is_paused : false, false)
  );

  const reportValues = [
    reportId,
    row.machineKey,
    row.hostname,
    row.macAddress,
    row.macAddresses ? JSON.stringify(row.macAddresses) : null,
    row.serialNumber,
    row.category || 'unknown',
    resolvedTag.name || DEFAULT_REPORT_TAG,
    resolvedTag.id || null,
    resolvedLotId,
    resolvedPalletId,
    resolvedPalletStatus,
    resolvedShipment ? resolvedShipment.date : null,
    resolvedShipment ? resolvedShipment.client : null,
    resolvedShipment ? resolvedShipment.orderNumber : null,
    resolvedShipment ? resolvedShipment.palletCode : null,
    row.model,
    row.vendor,
    row.technician,
    row.osVersion,
    row.ramMb,
    row.ramSlotsTotal,
    row.ramSlotsFree,
    row.batteryHealth,
    row.cameraStatus,
    row.usbStatus,
    row.keyboardStatus,
    row.padStatus,
    row.badgeReaderStatus,
    null,
    now,
    now,
    null,
    null,
    false,
    null,
    components ? JSON.stringify(components) : null,
    payloadText,
    getClientIp(req)
  ];
  const machineValues = [
    row.machineKey,
    row.hostname,
    row.macAddress,
    row.macAddresses ? JSON.stringify(row.macAddresses) : null,
    row.serialNumber,
    row.category || 'unknown',
    resolvedTag.name || DEFAULT_REPORT_TAG,
    resolvedTag.id || null,
    resolvedLotId,
    resolvedPalletId,
    resolvedPalletStatus,
    resolvedShipment ? resolvedShipment.date : null,
    resolvedShipment ? resolvedShipment.client : null,
    resolvedShipment ? resolvedShipment.orderNumber : null,
    resolvedShipment ? resolvedShipment.palletCode : null,
    row.model,
    row.vendor,
    row.technician,
    row.osVersion,
    row.ramMb,
    row.ramSlotsTotal,
    row.ramSlotsFree,
    row.batteryHealth,
    row.cameraStatus,
    row.usbStatus,
    row.keyboardStatus,
    row.padStatus,
    row.badgeReaderStatus,
    null,
    null,
    now,
    now,
    null,
    null,
    false,
    null,
    components ? JSON.stringify(components) : null,
    payloadText,
    getClientIp(req)
  ];

  await client.query(upsertReportQuery, reportValues);
  await client.query(upsertMachineQuery, machineValues);
  const lotProgress = await registerLotProgress(client, {
    lot: resolvedLot,
    machineKey: row.machineKey,
    reportId,
    technician: row.technician,
    source: 'manual-csv',
    isDoubleCheck: row.doubleCheck,
    shouldCount: shouldCountLot
  });

  return {
    reportId,
    machineKey: row.machineKey,
    lot: normalizeLotFromRow(lotProgress && lotProgress.lot ? lotProgress.lot : resolvedLot),
    pallet: resolvedPallet,
    lotCounted: Boolean(lotProgress && lotProgress.counted)
  };
}

async function listPalletsWithStats(client) {
  const result = await client.query(`
    SELECT
      pallets.id,
      pallets.code,
      pallets.last_movement_type,
      pallets.last_movement_at,
      pallets.created_by,
      pallets.created_at,
      pallets.updated_at,
      COALESCE(stats.total_count, 0) AS total_count,
      COALESCE(stats.entry_count, 0) AS entry_count,
      COALESCE(stats.exit_count, 0) AS exit_count,
      COALESCE(stats.linked_count, 0) AS linked_count
    FROM pallets
    LEFT JOIN (
      SELECT
        pallet_id,
        COUNT(*)::integer AS total_count,
        COUNT(*) FILTER (WHERE movement_type = 'entry')::integer AS entry_count,
        COUNT(*) FILTER (WHERE movement_type = 'exit')::integer AS exit_count,
        COUNT(*) FILTER (WHERE machine_key IS NOT NULL AND machine_key <> '')::integer AS linked_count
      FROM pallet_serials
      GROUP BY pallet_id
    ) stats ON stats.pallet_id = pallets.id
    ORDER BY pallets.last_movement_at DESC NULLS LAST, pallets.created_at DESC
  `);
  return result.rows || [];
}

async function listRecentPalletImports(client, limit = 12) {
  const boundedLimit = Math.min(Math.max(Number.parseInt(limit || '12', 10) || 12, 1), 50);
  const result = await client.query(
    `
      SELECT
        id,
        import_type,
        file_name,
        row_count,
        applied_count,
        skipped_count,
        created_by,
        created_at,
        summary
      FROM pallet_imports
      ORDER BY created_at DESC
      LIMIT $1
    `,
    [boundedLimit]
  );
  return result.rows || [];
}

async function syncPalletOnExistingRows(
  client,
  {
    serialNumber,
    palletId = null,
    palletStatus = null,
    shipmentDate = null,
    shipmentClient = null,
    shipmentOrderNumber = null,
    shipmentPalletCode = null
  } = {}
) {
  const serial = normalizeSerial(serialNumber);
  if (!serial) {
    return;
  }
  const normalizedShipmentDate = normalizeShipmentDate(shipmentDate);
  const normalizedShipmentClient = normalizeShipmentClient(shipmentClient);
  const normalizedShipmentOrderNumber = normalizeShipmentOrderNumber(shipmentOrderNumber);
  const normalizedShipmentPalletCode = normalizePalletCode(shipmentPalletCode);
  await client.query(
    `
      UPDATE machines
      SET pallet_id = $2,
          pallet_status = $3,
          shipment_date = COALESCE($4, shipment_date),
          shipment_client = COALESCE($5, shipment_client),
          shipment_order_number = COALESCE($6, shipment_order_number),
          shipment_pallet_code = COALESCE($7, shipment_pallet_code)
      WHERE serial_number = $1
    `,
    [
      serial,
      palletId,
      palletStatus,
      normalizedShipmentDate,
      normalizedShipmentClient,
      normalizedShipmentOrderNumber,
      normalizedShipmentPalletCode
    ]
  );
  await client.query(
    `
      UPDATE reports
      SET pallet_id = $2,
          pallet_status = $3,
          shipment_date = COALESCE($4, shipment_date),
          shipment_client = COALESCE($5, shipment_client),
          shipment_order_number = COALESCE($6, shipment_order_number),
          shipment_pallet_code = COALESCE($7, shipment_pallet_code)
      WHERE serial_number = $1
    `,
    [
      serial,
      palletId,
      palletStatus,
      normalizedShipmentDate,
      normalizedShipmentClient,
      normalizedShipmentOrderNumber,
      normalizedShipmentPalletCode
    ]
  );
}

async function upsertPallet(client, { code, movementType, actor = null, timestamp = null } = {}) {
  const normalizedCode = normalizePalletCode(code);
  const normalizedType = normalizePalletMovementType(movementType);
  if (!normalizedCode || !normalizedType) {
    return null;
  }
  const codeKey = normalizePalletCodeKey(normalizedCode);
  const movedAt = timestamp || new Date().toISOString();
  const result = await client.query(
    `
      INSERT INTO pallets (
        id,
        code,
        code_key,
        last_movement_type,
        last_movement_at,
        created_by,
        created_at,
        updated_at
      ) VALUES ($1, $2, $3, $4, $5, $6, $5, $5)
      ON CONFLICT (code_key) DO UPDATE SET
        code = EXCLUDED.code,
        last_movement_type = EXCLUDED.last_movement_type,
        last_movement_at = EXCLUDED.last_movement_at,
        updated_at = EXCLUDED.updated_at
      RETURNING *
    `,
    [generateUuid(), normalizedCode, codeKey, normalizedType, movedAt, actor || null]
  );
  return result.rows && result.rows[0] ? result.rows[0] : null;
}

async function applyPalletAssignment(
  client,
  {
    serialNumber,
    pallet,
    movementType,
    importId = null,
    machineKey = null,
    shipmentDate = null,
    shipmentClient = null,
    shipmentOrderNumber = null,
    shipmentPalletCode = null,
    actor = null,
    timestamp = null
  } = {}
) {
  const serial = normalizeSerial(serialNumber);
  const normalizedType = normalizePalletMovementType(movementType);
  const palletId = normalizeUuid(pallet && pallet.id ? pallet.id : null);
  const normalizedShipmentDate = normalizeShipmentDate(shipmentDate);
  const normalizedShipmentClient = normalizeShipmentClient(shipmentClient);
  const normalizedShipmentOrderNumber = normalizeShipmentOrderNumber(shipmentOrderNumber);
  const normalizedShipmentPalletCode = normalizePalletCode(
    shipmentPalletCode || (pallet && pallet.code ? pallet.code : null)
  );
  if (!serial || !normalizedType || !palletId) {
    return null;
  }
  const movedAt = timestamp || new Date().toISOString();
  await client.query(
    `
      INSERT INTO pallet_serials (
        serial_number,
        pallet_id,
        movement_type,
        shipment_date,
        shipment_client,
        shipment_order_number,
        shipment_pallet_code,
        machine_key,
        last_import_id,
        updated_by,
        updated_at
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
      ON CONFLICT (serial_number) DO UPDATE SET
        pallet_id = EXCLUDED.pallet_id,
        movement_type = EXCLUDED.movement_type,
        shipment_date = COALESCE(EXCLUDED.shipment_date, pallet_serials.shipment_date),
        shipment_client = COALESCE(EXCLUDED.shipment_client, pallet_serials.shipment_client),
        shipment_order_number = COALESCE(EXCLUDED.shipment_order_number, pallet_serials.shipment_order_number),
        shipment_pallet_code = COALESCE(EXCLUDED.shipment_pallet_code, pallet_serials.shipment_pallet_code),
        machine_key = COALESCE(EXCLUDED.machine_key, pallet_serials.machine_key),
        last_import_id = EXCLUDED.last_import_id,
        updated_by = EXCLUDED.updated_by,
        updated_at = EXCLUDED.updated_at
    `,
    [
      serial,
      palletId,
      normalizedType,
      normalizedShipmentDate,
      normalizedShipmentClient,
      normalizedShipmentOrderNumber,
      normalizedShipmentPalletCode,
      machineKey || null,
      normalizeUuid(importId),
      actor || null,
      movedAt
    ]
  );
  await client.query(
    `
      INSERT INTO pallet_movements (
        pallet_id,
        serial_number,
        machine_key,
        import_id,
        movement_type,
        created_by,
        created_at
      ) VALUES ($1, $2, $3, $4, $5, $6, $7)
    `,
    [palletId, serial, machineKey || null, normalizeUuid(importId), normalizedType, actor || null, movedAt]
  );
  await syncPalletOnExistingRows(client, {
    serialNumber: serial,
    palletId,
    palletStatus: normalizedType,
    shipmentDate: normalizedShipmentDate,
    shipmentClient: normalizedShipmentClient,
    shipmentOrderNumber: normalizedShipmentOrderNumber,
    shipmentPalletCode: normalizedShipmentPalletCode
  });
  return normalizedType;
}

async function resolvePalletForSerial(client, { serialNumber, machineKey = null, actor = null } = {}) {
  const serial = normalizeSerial(serialNumber);
  if (!serial) {
    return null;
  }
  const result = await client.query(
    `
      SELECT
        pallet_serials.serial_number,
        pallet_serials.pallet_id,
        pallet_serials.movement_type AS pallet_status,
        pallet_serials.shipment_date,
        pallet_serials.shipment_client,
        pallet_serials.shipment_order_number,
        pallet_serials.shipment_pallet_code,
        pallet_serials.machine_key AS pallet_machine_key,
        pallet_serials.updated_at AS pallet_last_movement_at,
        pallets.code AS pallet_code,
        pallets.last_movement_at
      FROM pallet_serials
      INNER JOIN pallets ON pallets.id = pallet_serials.pallet_id
      WHERE pallet_serials.serial_number = $1
      LIMIT 1
    `,
    [serial]
  );
  const row = result.rows && result.rows[0] ? result.rows[0] : null;
  if (!row) {
    return null;
  }
  if (machineKey && row.pallet_machine_key !== machineKey) {
    await client.query(
      `
        UPDATE pallet_serials
        SET machine_key = $2,
            updated_by = COALESCE($3, updated_by),
            updated_at = NOW()
        WHERE serial_number = $1
      `,
      [serial, machineKey, actor || null]
    );
  }
  const pallet = normalizePalletFromRow(row);
  if (!pallet) {
    return null;
  }
  const shipment = normalizeShipmentFromRow(row);
  return shipment ? { ...pallet, shipment } : pallet;
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
  const parts = getTimeZoneParts(now, APP_TIMEZONE);
  if (dateFilter === 'today') {
    const start = makeDateInTimeZone(
      {
        year: parts.year,
        month: parts.month,
        day: parts.day,
        hour: 0,
        minute: 0,
        second: 0,
        millisecond: 0
      },
      APP_TIMEZONE
    );
    const end = makeDateInTimeZone(
      {
        year: parts.year,
        month: parts.month,
        day: parts.day,
        hour: 23,
        minute: 59,
        second: 59,
        millisecond: 999
      },
      APP_TIMEZONE
    );
    return { start: start.toISOString(), end: end.toISOString() };
  }
  if (dateFilter === 'week') {
    const weekday = getTimeZoneWeekday(now, APP_TIMEZONE);
    const weekdayOffsets = {
      monday: 0,
      tuesday: 1,
      wednesday: 2,
      thursday: 3,
      friday: 4,
      saturday: 5,
      sunday: 6
    };
    const diff = weekdayOffsets[weekday] ?? 0;
    const start = makeDateInTimeZone(
      {
        year: parts.year,
        month: parts.month,
        day: parts.day - diff,
        hour: 0,
        minute: 0,
        second: 0,
        millisecond: 0
      },
      APP_TIMEZONE
    );
    const end = makeDateInTimeZone(
      {
        year: parts.year,
        month: parts.month,
        day: parts.day - diff + 6,
        hour: 23,
        minute: 59,
        second: 59,
        millisecond: 999
      },
      APP_TIMEZONE
    );
    return { start: start.toISOString(), end: end.toISOString() };
  }
  if (dateFilter === 'month') {
    const start = makeDateInTimeZone(
      {
        year: parts.year,
        month: parts.month,
        day: 1,
        hour: 0,
        minute: 0,
        second: 0,
        millisecond: 0
      },
      APP_TIMEZONE
    );
    const end = makeDateInTimeZone(
      {
        year: parts.year,
        month: parts.month + 1,
        day: 0,
        hour: 23,
        minute: 59,
        second: 59,
        millisecond: 999
      },
      APP_TIMEZONE
    );
    return { start: start.toISOString(), end: end.toISOString() };
  }
  if (dateFilter === 'year') {
    const start = makeDateInTimeZone(
      {
        year: parts.year,
        month: 1,
        day: 1,
        hour: 0,
        minute: 0,
        second: 0,
        millisecond: 0
      },
      APP_TIMEZONE
    );
    const end = makeDateInTimeZone(
      {
        year: parts.year,
        month: 12,
        day: 31,
        hour: 23,
        minute: 59,
        second: 59,
        millisecond: 999
      },
      APP_TIMEZONE
    );
    return { start: start.toISOString(), end: end.toISOString() };
  }
  return null;
}

function normalizeDateInputBoundary(value, endOfDay = false) {
  if (value == null) {
    return null;
  }
  const raw = String(value).trim();
  const match = raw.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!match) {
    return null;
  }
  const year = Number.parseInt(match[1], 10);
  const month = Number.parseInt(match[2], 10);
  const day = Number.parseInt(match[3], 10);
  const date = makeDateInTimeZone(
    {
      year,
      month,
      day,
      hour: endOfDay ? 23 : 0,
      minute: endOfDay ? 59 : 0,
      second: endOfDay ? 59 : 0,
      millisecond: endOfDay ? 999 : 0
    },
    APP_TIMEZONE
  );
  if (
    Number.isNaN(date.getTime()) ||
    formatLocalDateKey(date, APP_TIMEZONE) !== raw
  ) {
    return null;
  }
  return date.toISOString();
}

function getExplicitDateRange(query = {}) {
  const startRaw = normalizeDateInputBoundary(query.dateFrom || query.startDate || query.fromDate, false);
  const endRaw = normalizeDateInputBoundary(query.dateTo || query.endDate || query.toDate, true);
  if (!startRaw && !endRaw) {
    return null;
  }
  if (startRaw && endRaw && startRaw > endRaw) {
    return { start: endRaw, end: startRaw };
  }
  return { start: startRaw, end: endRaw };
}

function getResolvedDateRange(query = {}) {
  const explicitRange = getExplicitDateRange(query);
  if (explicitRange) {
    return explicitRange;
  }
  return getDateRange(query.date);
}

function normalizeTimelineGranularity(value) {
  const raw = String(value || '').trim().toLowerCase();
  if (raw === 'week') {
    return 'week';
  }
  if (raw === 'month') {
    return 'month';
  }
  return 'day';
}

function buildReportFilters(
  query,
  {
    includeCategory = true,
    activeTagId = null,
    forcedTechKeys = null,
    tableAlias = ''
  } = {}
) {
  const clauses = [];
  const values = [];
  let idx = 1;
  const prefix = tableAlias ? `${tableAlias}.` : '';
  const col = (name) => `${prefix}${name}`;
  const normalizedTextColumn = (name) => normalizeTextSql(col(name));

  const forcedTechList = Array.isArray(forcedTechKeys)
    ? Array.from(new Set(forcedTechKeys.map((value) => normalizeTechKey(value)).filter(Boolean)))
    : null;
  if (forcedTechList) {
    if (!forcedTechList.length) {
      clauses.push('1 = 0');
    } else {
      clauses.push(`${normalizedTextColumn('technician')} = ANY($${idx}::text[])`);
      values.push(forcedTechList);
      idx += 1;
    }
  }

  const techRaw = cleanString(query.tech, 64);
  const tech = techRaw ? normalizeTechKey(techRaw) : '';
  if (tech) {
    clauses.push(`${normalizedTextColumn('technician')} = $${idx}`);
    values.push(tech);
    idx += 1;
  }

  const tagIds = parseTagIds(query.tags || query.tagIds);
  if (tagIds.length) {
    const activeId = normalizeUuid(activeTagId);
    const includeNull = activeId && tagIds.includes(activeId);
    if (includeNull) {
      clauses.push(`(${col('tag_id')} = ANY($${idx}::uuid[]) OR ${col('tag_id')} IS NULL)`);
    } else {
      clauses.push(`${col('tag_id')} = ANY($${idx}::uuid[])`);
    }
    values.push(tagIds);
    idx += 1;
  }

  const legacyFlag = String(query.legacy || '').toLowerCase();
  if (legacyFlag === '1' || legacyFlag === 'true') {
    clauses.push(`safe_jsonb(${col('payload')}) ? 'legacy'`);
  } else if (legacyFlag === '0' || legacyFlag === 'false') {
    clauses.push(`NOT (safe_jsonb(${col('payload')}) ? 'legacy')`);
  }

  const scope = normalizeReportScope(query.scope || query.reportScope || query.viewScope);
  if (scope === 'servers') {
    clauses.push(`${col('category')} = 'server'`);
  } else if (scope === 'machines') {
    clauses.push(`COALESCE(${col('category')}, 'unknown') <> 'server'`);
  }

  if (includeCategory) {
    const categoryRaw = cleanString(query.category, 32);
    if (categoryRaw && categoryRaw !== 'all') {
      const category = normalizeCategory(categoryRaw);
      clauses.push(`${col('category')} = $${idx}`);
      values.push(category);
      idx += 1;
    }
  }

  const commentFilter = query.comment;
  if (commentFilter === 'with') {
    clauses.push(`(${col('comment')} IS NOT NULL AND ${col('comment')} <> '')`);
  } else if (commentFilter === 'without') {
    clauses.push(`(${col('comment')} IS NULL OR ${col('comment')} = '')`);
  }

  const shipmentDate = normalizeShipmentDate(
    query.shipmentDate || query.dateExpedition || query.expeditionDate || query.shippingDate
  );
  if (shipmentDate) {
    clauses.push(`${col('shipment_date')} = $${idx}`);
    values.push(shipmentDate);
    idx += 1;
  }

  const shipmentClient = normalizeTechKey(
    normalizeShipmentClient(query.shipmentClient || query.client || query.customer) || ''
  );
  if (shipmentClient) {
    clauses.push(`${normalizedTextColumn('shipment_client')} = $${idx}`);
    values.push(shipmentClient);
    idx += 1;
  }

  const shipmentOrderNumber = normalizeTechKey(
    normalizeShipmentOrderNumber(query.shipmentOrderNumber || query.orderNumber || query.order || query.commande) ||
      ''
  );
  if (shipmentOrderNumber) {
    clauses.push(`${normalizedTextColumn('shipment_order_number')} = $${idx}`);
    values.push(shipmentOrderNumber);
    idx += 1;
  }

  const shipmentPalletCode = normalizeTechKey(
    normalizePalletCode(query.shipmentPalletCode || query.palletCode || query.pallet || query.palette) || ''
  );
  if (shipmentPalletCode) {
    clauses.push(`${normalizedTextColumn('shipment_pallet_code')} = $${idx}`);
    values.push(shipmentPalletCode);
    idx += 1;
  }

  const component = cleanString(query.component, 64);
  if (component && component !== 'all') {
    if (component === 'biosBattery') {
      clauses.push(
        `(lower(safe_jsonb(${col('components')}) ->> $${idx}) = 'nok' OR COALESCE(${col(
          'bios_clock_alert'
        )}, false))`
      );
      values.push(component);
      idx += 1;
    } else if (component === 'serverRaid') {
      clauses.push(
        `lower(COALESCE(NULLIF(btrim(safe_jsonb(${col('components')}) ->> 'serverRaid'), ''), NULLIF(btrim(safe_jsonb(${col(
          'payload'
        )}) -> 'server' -> 'raid' ->> 'status'), ''))) = 'nok'`
      );
    } else if (component === 'powerSupply') {
      clauses.push(
        `(
          lower(COALESCE(NULLIF(btrim(safe_jsonb(${col('components')}) ->> 'powerSupply'), ''), '')) = 'nok'
          OR EXISTS (
            SELECT 1
            FROM jsonb_array_elements(COALESCE(safe_jsonb(${col('payload')}) -> 'inventory' -> 'powerSupplies', '[]'::jsonb)) AS psu(item)
            WHERE lower(COALESCE(psu.item ->> 'status', '')) = 'nok'
          )
        )`
      );
    } else if (component === 'serverFans') {
      clauses.push(
        `(
          lower(COALESCE(NULLIF(btrim(safe_jsonb(${col('components')}) ->> 'serverFans'), ''), '')) = 'nok'
          OR EXISTS (
            SELECT 1
            FROM jsonb_array_elements(COALESCE(safe_jsonb(${col('payload')}) -> 'inventory' -> 'fans', '[]'::jsonb)) AS fan(item)
            WHERE lower(COALESCE(fan.item ->> 'status', '')) = 'nok'
          )
        )`
      );
    } else if (component === 'serverBmc') {
      clauses.push(
        `lower(COALESCE(NULLIF(btrim(safe_jsonb(${col('components')}) ->> 'serverBmc'), ''), NULLIF(btrim(safe_jsonb(${col(
          'payload'
        )}) -> 'inventory' -> 'bmc' ->> 'status'), ''))) = 'nok'`
      );
    } else if (component === 'serverServices') {
      clauses.push(
        `(
          lower(COALESCE(NULLIF(btrim(safe_jsonb(${col('components')}) ->> 'serverServices'), ''), '')) = 'nok'
          OR jsonb_array_length(COALESCE(safe_jsonb(${col('payload')}) -> 'server' -> 'failedServices', '[]'::jsonb)) > 0
          OR EXISTS (
            SELECT 1
            FROM jsonb_array_elements(COALESCE(safe_jsonb(${col('payload')}) -> 'server' -> 'selectedServices', '[]'::jsonb)) AS svc(item)
            WHERE lower(COALESCE(svc.item ->> 'activeState', '')) <> 'active'
          )
        )`
      );
    } else {
      clauses.push(`lower(safe_jsonb(${col('components')}) ->> $${idx}) = 'nok'`);
      values.push(component);
      idx += 1;
    }
  }

  const batteryUnderRaw =
    query.batteryUnder ?? query.batteryBelow ?? query.batteryHealthBelow ?? query.lowBattery;
  const batteryUnder = Number.parseInt(String(batteryUnderRaw || '').trim(), 10);
  if (Number.isFinite(batteryUnder) && batteryUnder >= 0 && batteryUnder <= 100) {
    clauses.push(`${col('battery_health')} IS NOT NULL AND ${col('battery_health')} < $${idx}`);
    values.push(batteryUnder);
    idx += 1;
  }

  const alertModeRaw = String(query.alertMode || query.alerts || '').trim().toLowerCase();
  if (['1', 'true', 'yes', 'on'].includes(alertModeRaw)) {
    clauses.push(
      `((${col('battery_health')} IS NOT NULL AND ${col('battery_health')} < $${idx}) OR COALESCE(${col(
        'bios_clock_alert'
      )}, false))`
    );
    values.push(ALERT_BATTERY_THRESHOLD);
    idx += 1;
  }

  const range = getResolvedDateRange(query);
  if (range) {
    if (range.start && range.end) {
      clauses.push(`${col('last_seen')} >= $${idx} AND ${col('last_seen')} <= $${idx + 1}`);
      values.push(range.start, range.end);
      idx += 2;
    } else if (range.start) {
      clauses.push(`${col('last_seen')} >= $${idx}`);
      values.push(range.start);
      idx += 1;
    } else if (range.end) {
      clauses.push(`${col('last_seen')} <= $${idx}`);
      values.push(range.end);
      idx += 1;
    }
  }

  const search = cleanString(query.search, 128);
  if (search) {
    clauses.push(
      `lower(` +
        `coalesce(${col('hostname')},'') || ' ' || coalesce(${col('serial_number')},'') || ' ' || coalesce(${col(
          'mac_address'
        )},'') || ' ' || ` +
        `coalesce(${col('mac_addresses')},'') || ' ' || coalesce(${col('machine_key')},'') || ' ' || coalesce(${col(
          'technician'
        )},'') || ' ' || ` +
        `coalesce(${col('vendor')},'') || ' ' || coalesce(${col('model')},'') || ' ' || coalesce(${col(
          'comment'
        )},'') || ' ' || coalesce(${col('tag')},'') || ' ' || ` +
        `coalesce(${col('shipment_client')},'') || ' ' || coalesce(${col('shipment_order_number')},'') || ' ' || coalesce(${col(
          'shipment_pallet_code'
        )},'') || ' ' || coalesce(${col('shipment_date')}::text,'')` +
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

function normalizeReportScope(value) {
  const normalized = cleanString(value, 24);
  if (!normalized) {
    return 'all';
  }
  const lower = normalized.toLowerCase();
  if (['server', 'servers', 'serveur', 'serveurs'].includes(lower)) {
    return 'servers';
  }
  if (['machine', 'machines', 'atelier', 'workstation', 'workstations'].includes(lower)) {
    return 'machines';
  }
  return 'all';
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
    if (SERVER_CHASSIS_CODES.has(value)) {
      return 'server';
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
    if (SERVER_CHASSIS_CODES.has(numeric)) {
      return 'server';
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
  if (
    normalized.includes('server') ||
    normalized.includes('serveur') ||
    normalized.includes('rack mount') ||
    normalized.includes('rackmount') ||
    normalized.includes('rack') ||
    normalized.includes('blade')
  ) {
    return 'server';
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

async function sendSmtpMail({ from, to, subject, text, html, authUser, authPass }) {
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
        const boundary = `mdt-${crypto.randomBytes(12).toString('hex')}`;
        const lines = html
          ? [
            `From: MDT Live Ops <${from}>`,
            `To: ${to}`,
            `Subject: ${subject}`,
            'MIME-Version: 1.0',
            `Content-Type: multipart/alternative; boundary="${boundary}"`,
            '',
            `--${boundary}`,
            'Content-Type: text/plain; charset=utf-8',
            'Content-Transfer-Encoding: 8bit',
            '',
            text || '',
            '',
            `--${boundary}`,
            'Content-Type: text/html; charset=utf-8',
            'Content-Transfer-Encoding: 8bit',
            '',
            html,
            '',
            `--${boundary}--`
          ]
          : [
            `From: MDT Live Ops <${from}>`,
            `To: ${to}`,
            `Subject: ${subject}`,
            'MIME-Version: 1.0',
            'Content-Type: text/plain; charset=utf-8',
            'Content-Transfer-Encoding: 8bit',
            '',
            text || ''
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

function escapeHtmlEmail(value) {
  if (value == null) {
    return '';
  }
  return String(value)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function safeRecapCount(value) {
  const parsed = Number.parseInt(String(value || '0'), 10);
  return Number.isFinite(parsed) ? parsed : 0;
}

function mapWeeklyRecapRunRow(row) {
  if (!row) {
    return null;
  }
  let summary = null;
  if (row.summary) {
    try {
      summary = JSON.parse(row.summary);
    } catch (error) {
      summary = null;
    }
  }
  return {
    id: row.id || null,
    periodKey: row.period_key || null,
    periodStart: row.period_start || null,
    periodEnd: row.period_end || null,
    triggerSource: row.trigger_source || null,
    createdBy: row.created_by || null,
    recipients: parseEmailAddressList(row.recipients || ''),
    status: row.status || null,
    sentAt: row.sent_at || null,
    summary,
    error: row.error || null
  };
}

async function getLatestWeeklyRecapRun(client = pool) {
  const result = await client.query(
    `
      SELECT
        id,
        period_key,
        period_start,
        period_end,
        trigger_source,
        created_by,
        recipients,
        status,
        sent_at,
        summary,
        error
      FROM weekly_recap_runs
      ORDER BY sent_at DESC, id DESC
      LIMIT 1
    `
  );
  return mapWeeklyRecapRunRow(result.rows && result.rows[0] ? result.rows[0] : null);
}

async function hasSchedulerWeeklyRecapRunForPeriod(client, periodKey) {
  const result = await client.query(
    `
      SELECT 1
      FROM weekly_recap_runs
      WHERE period_key = $1
        AND trigger_source = 'scheduler'
        AND status IN ('sent', 'partial')
      LIMIT 1
    `,
    [periodKey]
  );
  return Boolean(result.rows && result.rows[0]);
}

function buildWeeklyRecapIdentity(row) {
  const primary =
    cleanString(row.hostname || '', 64) ||
    cleanString(row.serial_number || '', 64) ||
    cleanString(row.machine_key || '', 64) ||
    'Poste';
  const secondary = cleanString(row.serial_number || row.machine_key || '', 64);
  return secondary && secondary !== primary ? `${primary} (${secondary})` : primary;
}

async function collectWeeklyRecapSummary(client, { now = new Date() } = {}) {
  const window = computeWeeklyRecapWindow(now, WEEKLY_RECAP_TIMEZONE);
  const startIso = window.periodStart.toISOString();
  const endIso = window.periodEnd.toISOString();

  const snapshotResult = await client.query(
    `
      WITH latest AS (
        SELECT DISTINCT ON (reports.machine_key)
          reports.machine_key,
          reports.battery_health,
          reports.components
        FROM reports
        WHERE COALESCE(reports.machine_key, '') <> ''
        ORDER BY reports.machine_key, reports.last_seen DESC, reports.id DESC
      )
      SELECT
        COUNT(*)::integer AS total_machines,
        COUNT(*) FILTER (
          WHERE latest.battery_health IS NOT NULL
            AND latest.battery_health < $1
        )::integer AS battery_alerts_active,
        COUNT(*) FILTER (
          WHERE EXISTS (
            SELECT 1
            FROM jsonb_each_text(safe_jsonb(latest.components)) comp
            WHERE comp.key NOT IN ('diskSmart', 'networkTest', 'memDiag', 'thermal')
              AND lower(comp.value) IN ('nok', 'timeout', 'denied', 'absent')
          )
        )::integer AS nok_machines_active
      FROM latest
    `,
    [WEEKLY_RECAP_BATTERY_THRESHOLD]
  );

  const insertStatsResult = await client.query(
    `
      SELECT
        COUNT(*) FILTER (
          WHERE source ~* '^POST /api/ingest'
        )::integer AS ingest_reports,
        COUNT(*) FILTER (
          WHERE source ~* '^POST /api/reports/report-zero$'
        )::integer AS manual_report_zero,
        COUNT(*) FILTER (
          WHERE source ~* '^POST /api/machines/.+/report-zero$'
        )::integer AS cloned_report_zero,
        COUNT(*) FILTER (
          WHERE source ~* '^POST /api/reports/import-manual-csv$'
        )::integer AS manual_csv_reports
      FROM audit_log
      WHERE table_name = 'reports'
        AND action = 'INSERT'
        AND occurred_at >= $1
        AND occurred_at < $2
    `,
    [startIso, endIso]
  );

  const manualActivityResult = await client.query(
    `
      SELECT
        COUNT(*) FILTER (
          WHERE source ~* '^PUT /api/machines/.+/comment$'
        )::integer AS comments_count,
        COUNT(*) FILTER (
          WHERE source ~* '^PUT /api/reports/.+/component$'
            OR source ~* '^PUT /api/machines/.+/(pad|usb)$'
        )::integer AS component_updates,
        COUNT(*) FILTER (
          WHERE source ~* '^PUT /api/reports/.+/category$'
        )::integer AS category_updates,
        COUNT(*) FILTER (
          WHERE source ~* '^PUT /api/machines/.+/lot$'
        )::integer AS lot_updates
      FROM audit_log
      WHERE occurred_at >= $1
        AND occurred_at < $2
    `,
    [startIso, endIso]
  );

  const batterySeenResult = await client.query(
    `
      WITH affected AS (
        SELECT DISTINCT ON (COALESCE(NULLIF(machine_key, ''), id::text))
          COALESCE(NULLIF(machine_key, ''), id::text) AS scope_key
        FROM reports
        WHERE last_seen >= $1
          AND last_seen < $2
          AND battery_health IS NOT NULL
          AND battery_health < $3
        ORDER BY COALESCE(NULLIF(machine_key, ''), id::text), last_seen DESC, id DESC
      )
      SELECT COUNT(*)::integer AS battery_alerts_seen
      FROM affected
    `,
    [startIso, endIso, WEEKLY_RECAP_BATTERY_THRESHOLD]
  );

  const correctionResult = await client.query(
    `
      SELECT
        COUNT(*) FILTER (WHERE is_corrected)::integer AS corrected_count,
        COUNT(*) FILTER (WHERE is_regressed)::integer AS manual_regression_count
      FROM report_component_manual_changes
      WHERE occurred_at >= $1
        AND occurred_at < $2
    `,
    [startIso, endIso]
  );

  const regressionResult = await client.query(
    `
      SELECT
        COUNT(*)::integer AS regression_count,
        COUNT(DISTINCT machine_key)::integer AS regressed_machines
      FROM report_component_regressions
      WHERE event_time >= $1
        AND event_time < $2
    `,
    [startIso, endIso]
  );

  const palletImportResult = await client.query(
    `
      SELECT
        COUNT(*)::integer AS import_count,
        COALESCE(SUM(applied_count), 0)::integer AS imported_rows
      FROM pallet_imports
      WHERE created_at >= $1
        AND created_at < $2
    `,
    [startIso, endIso]
  );

  const operatorActivityResult = await client.query(
    `
      WITH activity AS (
        SELECT
          COALESCE(NULLIF(actor, ''), 'systeme') AS actor,
          COUNT(*) FILTER (
            WHERE source ~* '^PUT /api/machines/.+/comment$'
          )::integer AS comments_count,
          COUNT(*) FILTER (
            WHERE source ~* '^PUT /api/reports/.+/component$'
              OR source ~* '^PUT /api/machines/.+/(pad|usb)$'
          )::integer AS component_updates,
          COUNT(*) FILTER (
            WHERE source ~* '^PUT /api/reports/.+/category$'
          )::integer AS category_updates,
          COUNT(*) FILTER (
            WHERE source ~* '^POST /api/reports/report-zero$'
              OR source ~* '^POST /api/machines/.+/report-zero$'
          )::integer AS report_zero_count,
          COUNT(*) FILTER (
            WHERE source ~* '^POST /api/reports/import-manual-csv$'
          )::integer AS manual_csv_reports,
          COUNT(*) FILTER (
            WHERE source ~* '^PUT /api/machines/.+/lot$'
          )::integer AS lot_updates,
          COUNT(*) FILTER (
            WHERE source ~* '^POST /api/pallets/imports$'
          )::integer AS pallet_imports,
          COUNT(*)::integer AS total_actions
        FROM audit_log
        WHERE occurred_at >= $1
          AND occurred_at < $2
          AND COALESCE(NULLIF(actor, ''), '') <> ''
          AND actor_type IN ('microsoft', 'local')
          AND (
            source ~* '^PUT /api/machines/.+/comment$'
            OR source ~* '^PUT /api/reports/.+/component$'
            OR source ~* '^PUT /api/machines/.+/(pad|usb)$'
            OR source ~* '^PUT /api/reports/.+/category$'
            OR source ~* '^POST /api/reports/report-zero$'
            OR source ~* '^POST /api/machines/.+/report-zero$'
            OR source ~* '^POST /api/reports/import-manual-csv$'
            OR source ~* '^PUT /api/machines/.+/lot$'
            OR source ~* '^POST /api/pallets/imports$'
          )
        GROUP BY actor
      )
      SELECT *
      FROM activity
      WHERE total_actions > 0
      ORDER BY total_actions DESC, actor
      LIMIT 12
    `,
    [startIso, endIso]
  );

  const batteryAlertListResult = await client.query(
    `
      WITH latest AS (
        SELECT DISTINCT ON (reports.machine_key)
          reports.machine_key,
          reports.hostname,
          reports.serial_number,
          reports.technician,
          reports.battery_health,
          reports.last_seen
        FROM reports
        WHERE COALESCE(reports.machine_key, '') <> ''
        ORDER BY reports.machine_key, reports.last_seen DESC, reports.id DESC
      )
      SELECT
        machine_key,
        hostname,
        serial_number,
        technician,
        battery_health,
        last_seen
      FROM latest
      WHERE battery_health IS NOT NULL
        AND battery_health < $1
      ORDER BY battery_health ASC, last_seen DESC
      LIMIT 12
    `,
    [WEEKLY_RECAP_BATTERY_THRESHOLD]
  );

  const recentRegressionsResult = await client.query(
    `
      SELECT
        event_time,
        machine_key,
        component_label,
        previous_status_key,
        current_status_key,
        technician,
        actor,
        source_label
      FROM report_component_regressions
      WHERE event_time >= $1
        AND event_time < $2
      ORDER BY event_time DESC
      LIMIT 12
    `,
    [startIso, endIso]
  );

  const snapshotRow = snapshotResult.rows && snapshotResult.rows[0] ? snapshotResult.rows[0] : {};
  const insertRow = insertStatsResult.rows && insertStatsResult.rows[0] ? insertStatsResult.rows[0] : {};
  const activityRow = manualActivityResult.rows && manualActivityResult.rows[0] ? manualActivityResult.rows[0] : {};
  const batterySeenRow = batterySeenResult.rows && batterySeenResult.rows[0] ? batterySeenResult.rows[0] : {};
  const correctionRow = correctionResult.rows && correctionResult.rows[0] ? correctionResult.rows[0] : {};
  const regressionRow = regressionResult.rows && regressionResult.rows[0] ? regressionResult.rows[0] : {};
  const palletImportRow = palletImportResult.rows && palletImportResult.rows[0] ? palletImportResult.rows[0] : {};

  return {
    generatedAt: new Date().toISOString(),
    periodKey: window.periodKey,
    periodLabel: window.label,
    periodStart: startIso,
    periodEnd: endIso,
    timeZone: window.timeZone,
    batteryThreshold: WEEKLY_RECAP_BATTERY_THRESHOLD,
    scheduleLabel: getWeeklyRecapScheduleLabel(),
    recipients: WEEKLY_RECAP_RECIPIENTS,
    snapshot: {
      totalMachines: safeRecapCount(snapshotRow.total_machines),
      batteryAlertsActive: safeRecapCount(snapshotRow.battery_alerts_active),
      nokMachinesActive: safeRecapCount(snapshotRow.nok_machines_active)
    },
    weekly: {
      ingestReports: safeRecapCount(insertRow.ingest_reports),
      manualReportZero: safeRecapCount(insertRow.manual_report_zero),
      clonedReportZero: safeRecapCount(insertRow.cloned_report_zero),
      manualCsvReports: safeRecapCount(insertRow.manual_csv_reports),
      commentsCount: safeRecapCount(activityRow.comments_count),
      componentUpdates: safeRecapCount(activityRow.component_updates),
      categoryUpdates: safeRecapCount(activityRow.category_updates),
      lotUpdates: safeRecapCount(activityRow.lot_updates),
      palletImportCount: safeRecapCount(palletImportRow.import_count),
      palletImportedRows: safeRecapCount(palletImportRow.imported_rows),
      batteryAlertsSeen: safeRecapCount(batterySeenRow.battery_alerts_seen),
      correctedCount: safeRecapCount(correctionRow.corrected_count),
      manualRegressionCount: safeRecapCount(correctionRow.manual_regression_count),
      regressionCount: safeRecapCount(regressionRow.regression_count),
      regressedMachines: safeRecapCount(regressionRow.regressed_machines)
    },
    operatorActivity: (operatorActivityResult.rows || []).map((row) => ({
      actor: row.actor || 'systeme',
      commentsCount: safeRecapCount(row.comments_count),
      componentUpdates: safeRecapCount(row.component_updates),
      categoryUpdates: safeRecapCount(row.category_updates),
      reportZeroCount: safeRecapCount(row.report_zero_count),
      manualCsvReports: safeRecapCount(row.manual_csv_reports),
      lotUpdates: safeRecapCount(row.lot_updates),
      palletImports: safeRecapCount(row.pallet_imports),
      totalActions: safeRecapCount(row.total_actions)
    })),
    activeBatteryAlerts: (batteryAlertListResult.rows || []).map((row) => ({
      machineKey: row.machine_key || null,
      hostname: row.hostname || null,
      serialNumber: row.serial_number || null,
      technician: row.technician || null,
      batteryHealth: safeRecapCount(row.battery_health),
      lastSeen: row.last_seen || null,
      label: buildWeeklyRecapIdentity(row)
    })),
    recentRegressions: (recentRegressionsResult.rows || []).map((row) => ({
      eventTime: row.event_time || null,
      machineKey: row.machine_key || null,
      componentLabel: row.component_label || null,
      previousStatusKey: row.previous_status_key || null,
      currentStatusKey: row.current_status_key || null,
      technician: row.technician || null,
      actor: row.actor || null,
      sourceLabel: row.source_label || null
    }))
  };
}

function renderWeeklyRecapText(summary) {
  const lines = [
    `Recap hebdo MDT`,
    `Periode: ${summary.periodLabel}`,
    `Genere le: ${formatLocalDateTimeFr(new Date(summary.generatedAt), summary.timeZone)}`,
    '',
    'Synthese parc',
    `- Machines suivies: ${summary.snapshot.totalMachines}`,
    `- Alertes batterie actives (< ${summary.batteryThreshold}%): ${summary.snapshot.batteryAlertsActive}`,
    `- Machines NOK actives: ${summary.snapshot.nokMachinesActive}`,
    '',
    'Activite sur 7 jours',
    `- Remontees MDT: ${summary.weekly.ingestReports}`,
    `- Report 0 manuels: ${summary.weekly.manualReportZero}`,
    `- Report 0 clones: ${summary.weekly.clonedReportZero}`,
    `- Imports CSV manuels: ${summary.weekly.manualCsvReports}`,
    `- Commentaires saisis: ${summary.weekly.commentsCount}`,
    `- Mises a jour composants: ${summary.weekly.componentUpdates}`,
    `- Changements de categorie: ${summary.weekly.categoryUpdates}`,
    `- Affectations de lot: ${summary.weekly.lotUpdates}`,
    `- Imports palettes: ${summary.weekly.palletImportCount} fichier(s) / ${summary.weekly.palletImportedRows} ligne(s) appliquee(s)`,
    `- Alertes batterie remontees dans la semaine: ${summary.weekly.batteryAlertsSeen}`,
    `- Regressions de composants: ${summary.weekly.regressionCount}`,
    `- Corrections manuelles de composants: ${summary.weekly.correctedCount}`,
    ''
  ];

  lines.push('Activite par operateur');
  if (summary.operatorActivity.length) {
    summary.operatorActivity.forEach((item) => {
      lines.push(
        `- ${item.actor}: ${item.totalActions} action(s) ` +
          `(commentaires ${item.commentsCount}, composants ${item.componentUpdates}, ` +
          `report 0 ${item.reportZeroCount}, categories ${item.categoryUpdates}, ` +
          `imports CSV ${item.manualCsvReports}, lots ${item.lotUpdates}, palettes ${item.palletImports})`
      );
    });
  } else {
    lines.push('- Aucune action manuelle tracee sur la periode.');
  }

  lines.push('', 'Alertes batterie ouvertes');
  if (summary.activeBatteryAlerts.length) {
    summary.activeBatteryAlerts.forEach((item) => {
      lines.push(
        `- ${item.label} | Tech ${item.technician || '--'} | Batterie ${item.batteryHealth}% | Vu ${formatLocalDateTimeFr(
          new Date(item.lastSeen),
          summary.timeZone
        )}`
      );
    });
  } else {
    lines.push('- Aucune alerte batterie active.');
  }

  lines.push('', 'Regressions recentes');
  if (summary.recentRegressions.length) {
    summary.recentRegressions.forEach((item) => {
      lines.push(
        `- ${formatLocalDateTimeFr(new Date(item.eventTime), summary.timeZone)} | ${item.machineKey || '--'} | ${item.componentLabel || '--'} | ${item.actor || '--'} | ${item.sourceLabel || '--'}`
      );
    });
  } else {
    lines.push('- Aucune regression sur la periode.');
  }

  return lines.join('\n');
}

function renderWeeklyRecapHtml(summary) {
  const operatorRows = summary.operatorActivity.length
    ? summary.operatorActivity
        .map(
          (item) => `
            <tr>
              <td style="padding:8px 10px;border-bottom:1px solid #e6edf4;">${escapeHtmlEmail(item.actor)}</td>
              <td style="padding:8px 10px;border-bottom:1px solid #e6edf4;text-align:right;">${item.totalActions}</td>
              <td style="padding:8px 10px;border-bottom:1px solid #e6edf4;text-align:right;">${item.componentUpdates}</td>
              <td style="padding:8px 10px;border-bottom:1px solid #e6edf4;text-align:right;">${item.commentsCount}</td>
              <td style="padding:8px 10px;border-bottom:1px solid #e6edf4;text-align:right;">${item.reportZeroCount}</td>
            </tr>
          `
        )
        .join('')
    : `
      <tr>
        <td colspan="5" style="padding:10px;color:#5a7182;">Aucune action manuelle tracee.</td>
      </tr>
    `;
  const batteryRows = summary.activeBatteryAlerts.length
    ? summary.activeBatteryAlerts
        .map(
          (item) => `
            <li style="margin:0 0 8px;">
              <strong>${escapeHtmlEmail(item.label)}</strong>
              <span style="color:#5a7182;"> | Tech ${escapeHtmlEmail(item.technician || '--')} | Batterie ${item.batteryHealth}% | Vu ${escapeHtmlEmail(
                formatLocalDateTimeFr(new Date(item.lastSeen), summary.timeZone)
              )}</span>
            </li>
          `
        )
        .join('')
    : '<li style="color:#5a7182;">Aucune alerte batterie active.</li>';
  const regressionRows = summary.recentRegressions.length
    ? summary.recentRegressions
        .map(
          (item) => `
            <li style="margin:0 0 8px;">
              <strong>${escapeHtmlEmail(item.machineKey || '--')}</strong>
              <span style="color:#5a7182;"> | ${escapeHtmlEmail(item.componentLabel || '--')} | ${escapeHtmlEmail(
                item.actor || '--'
              )} | ${escapeHtmlEmail(item.sourceLabel || '--')} | ${escapeHtmlEmail(
                formatLocalDateTimeFr(new Date(item.eventTime), summary.timeZone)
              )}</span>
            </li>
          `
        )
        .join('')
    : '<li style="color:#5a7182;">Aucune regression sur la periode.</li>';

  return `
    <div style="font-family:IBM Plex Sans,Arial,sans-serif;background:#f4f8fb;color:#173042;padding:24px;">
      <div style="max-width:960px;margin:0 auto;background:#ffffff;border:1px solid #d9e4ec;border-radius:18px;overflow:hidden;box-shadow:0 12px 30px rgba(21,41,57,0.08);">
        <div style="padding:24px 28px;background:linear-gradient(135deg,#0f2b3f,#13556a);color:#ffffff;">
          <div style="font-size:12px;letter-spacing:0.18em;text-transform:uppercase;opacity:0.76;">MMA Automation</div>
          <h1 style="margin:10px 0 8px;font-family:'Space Grotesk',Arial,sans-serif;font-size:32px;line-height:1.1;">Recap hebdo MDT</h1>
          <p style="margin:0;font-size:15px;opacity:0.92;">Periode ${escapeHtmlEmail(summary.periodLabel)} · Genere le ${escapeHtmlEmail(
            formatLocalDateTimeFr(new Date(summary.generatedAt), summary.timeZone)
          )}</p>
        </div>
        <div style="padding:24px 28px;">
          <div style="display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:14px;margin-bottom:22px;">
            <div style="border:1px solid #d9e4ec;border-radius:14px;padding:14px 16px;background:#f9fbfd;">
              <div style="font-size:12px;letter-spacing:0.14em;text-transform:uppercase;color:#5a7182;">Machines suivies</div>
              <div style="margin-top:8px;font-size:34px;font-weight:700;">${summary.snapshot.totalMachines}</div>
            </div>
            <div style="border:1px solid #f0d3ce;border-radius:14px;padding:14px 16px;background:#fff7f5;">
              <div style="font-size:12px;letter-spacing:0.14em;text-transform:uppercase;color:#8c3d2c;">Alertes batterie actives</div>
              <div style="margin-top:8px;font-size:34px;font-weight:700;color:#8c3d2c;">${summary.snapshot.batteryAlertsActive}</div>
            </div>
            <div style="border:1px solid #f0d3ce;border-radius:14px;padding:14px 16px;background:#fff7f5;">
              <div style="font-size:12px;letter-spacing:0.14em;text-transform:uppercase;color:#8c3d2c;">Machines NOK actives</div>
              <div style="margin-top:8px;font-size:34px;font-weight:700;color:#8c3d2c;">${summary.snapshot.nokMachinesActive}</div>
            </div>
          </div>
          <div style="margin-bottom:22px;">
            <h2 style="margin:0 0 12px;font-size:18px;">Synthese semaine glissante</h2>
            <table style="width:100%;border-collapse:collapse;border:1px solid #e6edf4;border-radius:14px;overflow:hidden;">
              <tbody>
                <tr><td style="padding:10px;border-bottom:1px solid #e6edf4;">Remontees MDT</td><td style="padding:10px;border-bottom:1px solid #e6edf4;text-align:right;">${summary.weekly.ingestReports}</td></tr>
                <tr><td style="padding:10px;border-bottom:1px solid #e6edf4;">Report 0 manuels</td><td style="padding:10px;border-bottom:1px solid #e6edf4;text-align:right;">${summary.weekly.manualReportZero}</td></tr>
                <tr><td style="padding:10px;border-bottom:1px solid #e6edf4;">Imports CSV manuels</td><td style="padding:10px;border-bottom:1px solid #e6edf4;text-align:right;">${summary.weekly.manualCsvReports}</td></tr>
                <tr><td style="padding:10px;border-bottom:1px solid #e6edf4;">Commentaires</td><td style="padding:10px;border-bottom:1px solid #e6edf4;text-align:right;">${summary.weekly.commentsCount}</td></tr>
                <tr><td style="padding:10px;border-bottom:1px solid #e6edf4;">Mises a jour composants</td><td style="padding:10px;border-bottom:1px solid #e6edf4;text-align:right;">${summary.weekly.componentUpdates}</td></tr>
                <tr><td style="padding:10px;border-bottom:1px solid #e6edf4;">Alertes batterie remontees</td><td style="padding:10px;border-bottom:1px solid #e6edf4;text-align:right;">${summary.weekly.batteryAlertsSeen}</td></tr>
                <tr><td style="padding:10px;border-bottom:1px solid #e6edf4;">Regressions</td><td style="padding:10px;border-bottom:1px solid #e6edf4;text-align:right;">${summary.weekly.regressionCount}</td></tr>
                <tr><td style="padding:10px;">Corrections manuelles</td><td style="padding:10px;text-align:right;">${summary.weekly.correctedCount}</td></tr>
              </tbody>
            </table>
          </div>
          <div style="margin-bottom:22px;">
            <h2 style="margin:0 0 12px;font-size:18px;">Activite par operateur</h2>
            <table style="width:100%;border-collapse:collapse;border:1px solid #e6edf4;border-radius:14px;overflow:hidden;">
              <thead style="background:#f7fafc;color:#5a7182;text-transform:uppercase;font-size:12px;letter-spacing:0.08em;">
                <tr>
                  <th style="padding:10px;text-align:left;">Operateur</th>
                  <th style="padding:10px;text-align:right;">Actions</th>
                  <th style="padding:10px;text-align:right;">Composants</th>
                  <th style="padding:10px;text-align:right;">Commentaires</th>
                  <th style="padding:10px;text-align:right;">Report 0</th>
                </tr>
              </thead>
              <tbody>${operatorRows}</tbody>
            </table>
          </div>
          <div style="display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:18px;">
            <div>
              <h2 style="margin:0 0 12px;font-size:18px;">Alertes batterie ouvertes</h2>
              <ul style="margin:0;padding-left:18px;">${batteryRows}</ul>
            </div>
            <div>
              <h2 style="margin:0 0 12px;font-size:18px;">Regressions recentes</h2>
              <ul style="margin:0;padding-left:18px;">${regressionRows}</ul>
            </div>
          </div>
        </div>
      </div>
    </div>
  `;
}

function buildWeeklyRecapEmailContent(summary) {
  return {
    subject: `Recap hebdo MDT - ${summary.periodLabel}`,
    text: renderWeeklyRecapText(summary),
    html: renderWeeklyRecapHtml(summary)
  };
}

async function recordWeeklyRecapRun(
  client,
  { summary, triggerSource, createdBy, recipients, status, error }
) {
  const window = summary || computeWeeklyRecapWindow(new Date(), WEEKLY_RECAP_TIMEZONE);
  await client.query(
    `
      INSERT INTO weekly_recap_runs (
        id,
        period_key,
        period_start,
        period_end,
        trigger_source,
        created_by,
        recipients,
        status,
        sent_at,
        summary,
        error
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW(), $9, $10)
    `,
    [
      generateUuid(),
      summary ? summary.periodKey : window.periodKey,
      summary ? summary.periodStart : window.periodStart.toISOString(),
      summary ? summary.periodEnd : window.periodEnd.toISOString(),
      triggerSource,
      createdBy || null,
      Array.isArray(recipients) ? recipients.join(', ') : '',
      status,
      summary ? JSON.stringify(summary) : null,
      error || null
    ]
  );
}

async function deliverWeeklyRecap({
  triggerSource = 'manual',
  createdBy = 'systeme',
  auditContext = null,
  now = new Date()
} = {}) {
  const recipients = WEEKLY_RECAP_RECIPIENTS.slice();
  if (!recipients.length) {
    return { ok: false, error: 'missing_recipients' };
  }
  const creds = await getSuggestionSmtpCredentials();
  if (!creds || !creds.password || !creds.username) {
    return { ok: false, error: 'missing_credentials' };
  }

  const client = await pool.connect();
  let summary = null;
  try {
    summary = await collectWeeklyRecapSummary(client, { now });
  } finally {
    client.release();
  }

  const { subject, text, html } = buildWeeklyRecapEmailContent(summary);
  const fromAddress = WEEKLY_RECAP_FROM || SUGGESTION_EMAIL_FROM || creds.username;
  const failedRecipients = [];
  for (const recipient of recipients) {
    try {
      await sendSmtpMail({
        from: fromAddress,
        to: recipient,
        subject,
        text,
        html,
        authUser: creds.username,
        authPass: creds.password
      });
    } catch (error) {
      console.error('Failed to send weekly recap email', { recipient, error: error.message });
      failedRecipients.push(recipient);
    }
  }

  const status = failedRecipients.length
    ? failedRecipients.length === recipients.length
      ? 'failed'
      : 'partial'
    : 'sent';
  const error = failedRecipients.length
    ? `Destinataires en echec: ${failedRecipients.join(', ')}`
    : null;

  let recordClient = null;
  try {
    recordClient = await pool.connect();
    await recordClient.query('BEGIN');
    if (auditContext) {
      await setAuditContext(recordClient, auditContext);
    }
    await recordWeeklyRecapRun(recordClient, {
      summary,
      triggerSource,
      createdBy,
      recipients,
      status,
      error
    });
    await recordClient.query('COMMIT');
  } catch (recordError) {
    if (recordClient) {
      try {
        await recordClient.query('ROLLBACK');
      } catch (rollbackError) {
        console.error('Failed to rollback weekly recap record', rollbackError);
      }
    }
    console.error('Failed to record weekly recap run', recordError);
  } finally {
    if (recordClient) {
      recordClient.release();
    }
  }

  return {
    ok: failedRecipients.length < recipients.length,
    status,
    recipients,
    failedRecipients,
    summary,
    subject,
    error
  };
}

let weeklyRecapTimer = null;
let weeklyRecapInFlight = false;

function shouldCheckWeeklyRecap(now = new Date()) {
  if (!WEEKLY_RECAP_ENABLED || !WEEKLY_RECAP_RECIPIENTS.length) {
    return false;
  }
  const weekday = getTimeZoneWeekday(now, WEEKLY_RECAP_TIMEZONE);
  if (weekday !== WEEKLY_RECAP_DAY) {
    return false;
  }
  const parts = getTimeZoneParts(now, WEEKLY_RECAP_TIMEZONE);
  if (parts.hour < WEEKLY_RECAP_HOUR) {
    return false;
  }
  if (parts.hour === WEEKLY_RECAP_HOUR && parts.minute < WEEKLY_RECAP_MINUTE) {
    return false;
  }
  return true;
}

async function maybeSendScheduledWeeklyRecap() {
  if (weeklyRecapInFlight || !shouldCheckWeeklyRecap(new Date())) {
    return;
  }
  weeklyRecapInFlight = true;
  const client = await pool.connect();
  try {
    const summary = await collectWeeklyRecapSummary(client, { now: new Date() });
    const alreadySent = await hasSchedulerWeeklyRecapRunForPeriod(client, summary.periodKey);
    if (!alreadySent) {
      await deliverWeeklyRecap({
        triggerSource: 'scheduler',
        createdBy: 'scheduler',
        auditContext: {
          actor: 'scheduler',
          actorType: 'system',
          actorIp: null,
          userAgent: 'weekly-recap',
          requestId: generateRequestId(),
          source: 'SCHEDULER weekly-recap'
        },
        now: new Date()
      });
    }
  } catch (error) {
    console.error('Failed scheduled weekly recap check', error);
  } finally {
    client.release();
    weeklyRecapInFlight = false;
  }
}

function startWeeklyRecapScheduler() {
  if (weeklyRecapTimer || !WEEKLY_RECAP_ENABLED) {
    return;
  }
  const intervalMs = clampInteger(WEEKLY_RECAP_CHECK_INTERVAL_MS, 300000, 60000, 3600000);
  weeklyRecapTimer = setInterval(() => {
    maybeSendScheduledWeeklyRecap().catch((error) => {
      console.error('Weekly recap scheduler tick failed', error);
    });
  }, intervalMs);
  maybeSendScheduledWeeklyRecap().catch((error) => {
    console.error('Weekly recap scheduler bootstrap failed', error);
  });
}

async function buildWeeklyRecapAdminPayload(now = new Date()) {
  const client = await pool.connect();
  try {
    const preview = await collectWeeklyRecapSummary(client, { now });
    const latestRun = await getLatestWeeklyRecapRun(client);
    return {
      enabled: WEEKLY_RECAP_ENABLED,
      recipients: WEEKLY_RECAP_RECIPIENTS,
      schedule: {
        day: WEEKLY_RECAP_DAY,
        hour: WEEKLY_RECAP_HOUR,
        minute: WEEKLY_RECAP_MINUTE,
        timeZone: WEEKLY_RECAP_TIMEZONE,
        label: getWeeklyRecapScheduleLabel()
      },
      batteryThreshold: WEEKLY_RECAP_BATTERY_THRESHOLD,
      preview,
      latestRun
    };
  } finally {
    client.release();
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
  const accessLevel = ACCESS_LEVELS.platformAdmin;
  return {
    username,
    type: 'local',
    accessLevel,
    isHydraAdmin: true,
    permissions: buildPermissionsForAccessLevel(accessLevel)
  };
}

function buildLdapSessionUser(username, ldapUser) {
  const groups = extractLdapGroups(ldapUser);
  const isHydraAdmin = isHydraAdminMember(groups);
  const accessLevel = isHydraAdmin ? ACCESS_LEVELS.admin : ACCESS_LEVELS.operator;
  return {
    username,
    type: 'ldap',
    displayName: ldapUser.cn || ldapUser.displayName || ldapUser.uid || username,
    dn: ldapUser.dn || null,
    mail: ldapUser.mail || null,
    groups,
    accessLevel,
    isHydraAdmin,
    permissions: buildPermissionsForAccessLevel(accessLevel)
  };
}

function normalizeEmailAddress(value) {
  if (value == null) {
    return null;
  }
  const email = String(value).trim().toLowerCase();
  if (!email || !email.includes('@')) {
    return null;
  }
  return email;
}

function parseEmailAddressList(raw) {
  return String(raw || '')
    .split(/[,\n;]+/)
    .map((item) => normalizeEmailAddress(item))
    .filter(Boolean);
}

function clampInteger(value, fallback, min, max) {
  const parsed = Number.parseInt(String(value ?? '').trim(), 10);
  if (!Number.isFinite(parsed)) {
    return fallback;
  }
  return Math.min(Math.max(parsed, min), max);
}

const WEEKDAY_TOKEN_MAP = Object.freeze({
  0: 0,
  1: 1,
  2: 2,
  3: 3,
  4: 4,
  5: 5,
  6: 6,
  7: 0,
  sunday: 0,
  dimanche: 0,
  sun: 0,
  monday: 1,
  lundi: 1,
  mon: 1,
  tuesday: 2,
  mardi: 2,
  tue: 2,
  wednesday: 3,
  mercredi: 3,
  wed: 3,
  thursday: 4,
  jeudi: 4,
  thu: 4,
  friday: 5,
  vendredi: 5,
  fri: 5,
  saturday: 6,
  samedi: 6,
  sat: 6
});

const WEEKDAY_LABELS_FR = Object.freeze([
  'dimanche',
  'lundi',
  'mardi',
  'mercredi',
  'jeudi',
  'vendredi',
  'samedi'
]);

function normalizeWeekdayToken(value) {
  const raw = String(value || '').trim().toLowerCase();
  if (!raw) {
    return null;
  }
  return Object.prototype.hasOwnProperty.call(WEEKDAY_TOKEN_MAP, raw)
    ? WEEKDAY_TOKEN_MAP[raw]
    : null;
}

const WEEKLY_RECAP_RECIPIENTS = Object.freeze(parseEmailAddressList(WEEKLY_RECAP_RECIPIENTS_RAW));
const WEEKLY_RECAP_DAY = normalizeWeekdayToken(WEEKLY_RECAP_DAY_RAW) ?? 1;
const WEEKLY_RECAP_HOUR = clampInteger(WEEKLY_RECAP_HOUR_RAW, 7, 0, 23);
const WEEKLY_RECAP_MINUTE = clampInteger(WEEKLY_RECAP_MINUTE_RAW, 30, 0, 59);
const WEEKLY_RECAP_BATTERY_THRESHOLD = clampInteger(WEEKLY_RECAP_BATTERY_THRESHOLD_RAW, 75, 1, 100);

function getTimeZoneParts(date, timeZone = WEEKLY_RECAP_TIMEZONE) {
  const formatter = new Intl.DateTimeFormat('en-CA', {
    timeZone,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hourCycle: 'h23'
  });
  const parts = formatter.formatToParts(date);
  const values = {};
  parts.forEach((part) => {
    if (part.type !== 'literal') {
      values[part.type] = part.value;
    }
  });
  return {
    year: Number.parseInt(values.year || '0', 10),
    month: Number.parseInt(values.month || '0', 10),
    day: Number.parseInt(values.day || '0', 10),
    hour: Number.parseInt(values.hour || '0', 10),
    minute: Number.parseInt(values.minute || '0', 10),
    second: Number.parseInt(values.second || '0', 10)
  };
}

function getTimeZoneWeekday(date, timeZone = WEEKLY_RECAP_TIMEZONE) {
  const formatter = new Intl.DateTimeFormat('en-US', {
    timeZone,
    weekday: 'short'
  });
  const token = String(formatter.format(date) || '').trim().toLowerCase();
  return normalizeWeekdayToken(token);
}

function getTimeZoneOffsetMs(date, timeZone = WEEKLY_RECAP_TIMEZONE) {
  const parts = getTimeZoneParts(date, timeZone);
  const utcFromParts = Date.UTC(
    parts.year,
    parts.month - 1,
    parts.day,
    parts.hour,
    parts.minute,
    parts.second
  );
  return utcFromParts - date.getTime();
}

function makeDateInTimeZone(parts, timeZone = WEEKLY_RECAP_TIMEZONE) {
  const utcGuess = new Date(
    Date.UTC(
      parts.year,
      parts.month - 1,
      parts.day,
      parts.hour || 0,
      parts.minute || 0,
      parts.second || 0,
      parts.millisecond || 0
    )
  );
  const offsetMs = getTimeZoneOffsetMs(utcGuess, timeZone);
  return new Date(utcGuess.getTime() - offsetMs);
}

function formatLocalDateKey(date, timeZone = WEEKLY_RECAP_TIMEZONE) {
  const parts = getTimeZoneParts(date, timeZone);
  const year = String(parts.year).padStart(4, '0');
  const month = String(parts.month).padStart(2, '0');
  const day = String(parts.day).padStart(2, '0');
  return `${year}-${month}-${day}`;
}

function formatLocalDateFr(date, timeZone = WEEKLY_RECAP_TIMEZONE) {
  return new Intl.DateTimeFormat('fr-FR', {
    timeZone,
    day: '2-digit',
    month: '2-digit',
    year: 'numeric'
  }).format(date);
}

function formatLocalDateTimeFr(date, timeZone = WEEKLY_RECAP_TIMEZONE) {
  return new Intl.DateTimeFormat('fr-FR', {
    timeZone,
    day: '2-digit',
    month: '2-digit',
    year: 'numeric',
    hour: '2-digit',
    minute: '2-digit'
  }).format(date);
}

function computeWeeklyRecapWindow(now = new Date(), timeZone = WEEKLY_RECAP_TIMEZONE) {
  const localParts = getTimeZoneParts(now, timeZone);
  const todayStart = makeDateInTimeZone(
    {
      year: localParts.year,
      month: localParts.month,
      day: localParts.day,
      hour: 0,
      minute: 0,
      second: 0,
      millisecond: 0
    },
    timeZone
  );
  const periodEnd = todayStart;
  const periodStart = new Date(periodEnd.getTime() - 7 * 24 * 60 * 60 * 1000);
  const displayEnd = new Date(periodEnd.getTime() - 1000);
  return {
    periodStart,
    periodEnd,
    periodKey: `${formatLocalDateKey(periodStart, timeZone)}_${formatLocalDateKey(displayEnd, timeZone)}`,
    label: `${formatLocalDateFr(periodStart, timeZone)} au ${formatLocalDateFr(displayEnd, timeZone)}`,
    timeZone
  };
}

function getWeeklyRecapScheduleLabel() {
  const weekday = WEEKDAY_LABELS_FR[WEEKLY_RECAP_DAY] || 'lundi';
  const hour = String(WEEKLY_RECAP_HOUR).padStart(2, '0');
  const minute = String(WEEKLY_RECAP_MINUTE).padStart(2, '0');
  return `${weekday} a ${hour}:${minute} (${WEEKLY_RECAP_TIMEZONE})`;
}

function parseMicrosoftAdminEmails() {
  return String(MICROSOFT_ADMIN_EMAILS_RAW || '')
    .split(',')
    .map((item) => normalizeEmailAddress(item))
    .filter(Boolean);
}

function normalizeMicrosoftGroupIds(values) {
  return Array.isArray(values)
    ? values.map((value) => normalizeDirectoryObjectId(value)).filter(Boolean)
    : [];
}

function hasMicrosoftGroup(groups, allowedGroupIds) {
  if (!Array.isArray(groups) || !groups.length || !Array.isArray(allowedGroupIds) || !allowedGroupIds.length) {
    return false;
  }
  const known = new Set(normalizeMicrosoftGroupIds(groups));
  return allowedGroupIds.some((groupId) => known.has(groupId));
}

function hasMicrosoftGroupOverageClaims(claims) {
  const sourceClaims = claims && typeof claims === 'object' ? claims : {};
  if (sourceClaims.hasgroups === true) {
    return true;
  }
  const claimNames =
    sourceClaims._claim_names && typeof sourceClaims._claim_names === 'object'
      ? sourceClaims._claim_names
      : null;
  return Boolean(claimNames && claimNames.groups);
}

async function resolveMicrosoftGroupIdsForSignIn(accessToken, claims) {
  const sourceClaims = claims && typeof claims === 'object' ? claims : {};
  const tokenGroups = normalizeMicrosoftGroupIds(sourceClaims.groups);
  if (tokenGroups.length) {
    return {
      groupIds: tokenGroups,
      source: 'token',
      error: null
    };
  }

  const configuredGroupIds = getConfiguredMicrosoftGroupIds();
  if (!configuredGroupIds.length) {
    return {
      groupIds: [],
      source: 'config',
      error: null
    };
  }

  const shouldUseGraph =
    !Array.isArray(sourceClaims.groups) || hasMicrosoftGroupOverageClaims(sourceClaims);
  if (!shouldUseGraph) {
    return {
      groupIds: [],
      source: 'token',
      error: null
    };
  }

  if (!accessToken) {
    return {
      groupIds: [],
      source: 'graph',
      error: 'missing_access_token'
    };
  }

  try {
    const response = await fetch(MICROSOFT_GRAPH_ME_CHECK_MEMBER_OBJECTS_URL, {
      method: 'POST',
      headers: {
        Authorization: `Bearer ${accessToken}`,
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({ ids: configuredGroupIds })
    });
    if (!response.ok) {
      const bodyText = await response.text();
      console.error('Failed Microsoft Graph group check', {
        status: response.status,
        body: cleanString(bodyText, 800)
      });
      return {
        groupIds: [],
        source: 'graph',
        error: 'graph_lookup_failed'
      };
    }
    const data = await response.json();
    return {
      groupIds: normalizeMicrosoftGroupIds(Array.isArray(data?.value) ? data.value : []),
      source: 'graph',
      error: null
    };
  } catch (error) {
    console.error('Failed Microsoft Graph group check', error);
    return {
      groupIds: [],
      source: 'graph',
      error: 'graph_lookup_failed'
    };
  }
}

function resolveMicrosoftAccessLevel(groups) {
  let accessLevel = null;
  if (hasMicrosoftGroup(groups, MICROSOFT_GROUP_IDS[ACCESS_LEVELS.reader])) {
    accessLevel = ACCESS_LEVELS.reader;
  }
  if (hasMicrosoftGroup(groups, MICROSOFT_GROUP_IDS[ACCESS_LEVELS.operator])) {
    accessLevel = accessLevel
      ? maxAccessLevel(accessLevel, ACCESS_LEVELS.operator)
      : ACCESS_LEVELS.operator;
  }
  if (hasMicrosoftGroup(groups, MICROSOFT_GROUP_IDS[ACCESS_LEVELS.logistics])) {
    accessLevel = accessLevel
      ? maxAccessLevel(accessLevel, ACCESS_LEVELS.logistics)
      : ACCESS_LEVELS.logistics;
  }
  if (hasMicrosoftGroup(groups, MICROSOFT_GROUP_IDS[ACCESS_LEVELS.admin])) {
    accessLevel = accessLevel
      ? maxAccessLevel(accessLevel, ACCESS_LEVELS.admin)
      : ACCESS_LEVELS.admin;
  }
  if (hasMicrosoftGroup(groups, MICROSOFT_GROUP_IDS[ACCESS_LEVELS.platformAdmin])) {
    accessLevel = accessLevel
      ? maxAccessLevel(accessLevel, ACCESS_LEVELS.platformAdmin)
      : ACCESS_LEVELS.platformAdmin;
  }
  return accessLevel;
}

function getMicrosoftAuthority() {
  if (!MICROSOFT_ENTRA_TENANT_ID) {
    return '';
  }
  return `https://login.microsoftonline.com/${MICROSOFT_ENTRA_TENANT_ID}`;
}

function buildPublicAppUrl(req, pathname = '/') {
  if (HTTPS_PUBLIC_ORIGIN) {
    try {
      const base = new URL(HTTPS_PUBLIC_ORIGIN);
      return new URL(pathname, `${base.origin}/`).toString();
    } catch (error) {
      // Fallback to request origin below.
    }
  }
  const forwardedProto = cleanString(req.get('x-forwarded-proto'), 32);
  const protocol =
    (forwardedProto ? forwardedProto.split(',')[0].trim().toLowerCase() : '') ||
    (req.secure || req.protocol === 'https' ? 'https' : 'http');
  const host = cleanString(req.get('x-forwarded-host'), 255) || cleanString(req.get('host'), 255);
  if (!host) {
    return pathname;
  }
  return `${protocol}://${host}${pathname.startsWith('/') ? pathname : `/${pathname}`}`;
}

function getMicrosoftRedirectUri(req) {
  if (MICROSOFT_ENTRA_REDIRECT_URI) {
    return MICROSOFT_ENTRA_REDIRECT_URI;
  }
  return buildPublicAppUrl(req, '/auth/microsoft/callback');
}

function getMicrosoftPostLogoutRedirectUri(req) {
  return buildPublicAppUrl(req, '/login');
}

function buildMicrosoftLogoutUrl(req) {
  const authority = getMicrosoftAuthority();
  if (!authority) {
    return '/login';
  }
  const url = new URL(`${authority}/oauth2/v2.0/logout`);
  url.searchParams.set('post_logout_redirect_uri', getMicrosoftPostLogoutRedirectUri(req));
  return url.toString();
}

let microsoftClientApp = null;

function getMicrosoftClientApp() {
  if (!MICROSOFT_SSO_ENABLED) {
    return null;
  }
  if (!microsoftClientApp) {
    microsoftClientApp = new ConfidentialClientApplication({
      auth: {
        clientId: MICROSOFT_ENTRA_CLIENT_ID,
        authority: getMicrosoftAuthority(),
        clientSecret: MICROSOFT_ENTRA_CLIENT_SECRET
      }
    });
  }
  return microsoftClientApp;
}

function canUseMicrosoftAdminRole(roles) {
  if (!MICROSOFT_ADMIN_ROLE) {
    return false;
  }
  return Array.isArray(roles) && roles.some((role) => String(role || '').trim() === MICROSOFT_ADMIN_ROLE);
}

function buildMicrosoftSessionUser(account, claims) {
  const sourceClaims = claims && typeof claims === 'object' ? claims : {};
  const roles = Array.isArray(sourceClaims.roles)
    ? sourceClaims.roles.map((role) => String(role || '').trim()).filter(Boolean)
    : [];
  const groups = normalizeMicrosoftGroupIds(sourceClaims.groups);
  const email =
    normalizeEmailAddress(sourceClaims.preferred_username) ||
    normalizeEmailAddress(sourceClaims.email) ||
    normalizeEmailAddress(sourceClaims.upn) ||
    normalizeEmailAddress(account && account.username);
  const displayName = cleanString(
    sourceClaims.name || (account && (account.name || account.username)) || email || 'Utilisateur Microsoft',
    128
  );
  const accessLevel = resolveMicrosoftAccessLevel(groups);
  if (!accessLevel) {
    return null;
  }
  const permissions = buildPermissionsForAccessLevel(accessLevel);
  const isHydraAdmin = accessLevelRank(accessLevel) >= accessLevelRank(ACCESS_LEVELS.admin);
  return {
    username: email || cleanString((account && account.username) || displayName, 128) || 'microsoft-user',
    type: 'microsoft',
    displayName: displayName || email || 'Utilisateur Microsoft',
    dn: null,
    mail: email,
    groups,
    roles,
    accessLevel,
    tenantId: cleanString(sourceClaims.tid || (account && account.tenantId) || '', 64),
    oid: cleanString(sourceClaims.oid || (account && account.localAccountId) || '', 128),
    isHydraAdmin,
    permissions
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

const DISK_READ_OK_MIN_MBPS = 400;
const DISK_WRITE_OK_MIN_MBPS = 350;

function getPayloadTests(payload) {
  if (!payload || typeof payload !== 'object' || Array.isArray(payload)) {
    return null;
  }
  return payload.tests && typeof payload.tests === 'object' && !Array.isArray(payload.tests)
    ? payload.tests
    : null;
}

function getPayloadWinSatDisk(payload) {
  if (!payload || typeof payload !== 'object' || Array.isArray(payload)) {
    return null;
  }
  const winsat =
    payload.winsat && typeof payload.winsat === 'object' && !Array.isArray(payload.winsat)
      ? payload.winsat
      : null;
  return winsat && winsat.disk && typeof winsat.disk === 'object' && !Array.isArray(winsat.disk)
    ? winsat.disk
    : null;
}

function resolveDiskMetricValue(payload, testMetricKey, winsatMetricKey) {
  const tests = getPayloadTests(payload);
  const winsatDisk = getPayloadWinSatDisk(payload);
  const candidates = [
    tests ? tests[testMetricKey] : null,
    winsatDisk ? winsatDisk[winsatMetricKey] : null
  ];
  for (const candidate of candidates) {
    const numeric = parseMetricNumber(candidate);
    if (numeric != null) {
      return Math.round(numeric * 10) / 10;
    }
  }
  return null;
}

function evaluateDiskMetricStatus(metricValue, fallbackValue, minimumMbps) {
  if (metricValue != null) {
    return metricValue >= minimumMbps ? 'ok' : 'nok';
  }
  return normalizeStatus(fallbackValue);
}

function resolveAuthoritativeDiskTests(payload) {
  if (!payload || typeof payload !== 'object' || Array.isArray(payload)) {
    return null;
  }
  const tests = getPayloadTests(payload);
  const diskReadMBps = resolveDiskMetricValue(payload, 'diskReadMBps', 'seqReadMBps');
  const diskWriteMBps = resolveDiskMetricValue(payload, 'diskWriteMBps', 'seqWriteMBps');
  const hasDiskReadSource = diskReadMBps != null || (tests && tests.diskRead != null);
  const hasDiskWriteSource = diskWriteMBps != null || (tests && tests.diskWrite != null);
  if (!hasDiskReadSource && !hasDiskWriteSource) {
    return null;
  }
  return {
    diskRead: hasDiskReadSource
      ? evaluateDiskMetricStatus(
          diskReadMBps,
          tests ? tests.diskRead : null,
          DISK_READ_OK_MIN_MBPS
        ) || 'not_tested'
      : null,
    diskWrite: hasDiskWriteSource
      ? evaluateDiskMetricStatus(
          diskWriteMBps,
          tests ? tests.diskWrite : null,
          DISK_WRITE_OK_MIN_MBPS
        ) || 'not_tested'
      : null,
    diskReadMBps,
    diskWriteMBps
  };
}

function applyAuthoritativeDiskTests(payload) {
  const diskTests = resolveAuthoritativeDiskTests(payload);
  if (!diskTests) {
    return null;
  }
  const currentTests = getPayloadTests(payload);
  const shouldWriteTests = Boolean(
    currentTests || diskTests.diskReadMBps != null || diskTests.diskWriteMBps != null
  );
  if (!shouldWriteTests) {
    return diskTests;
  }
  const nextTests = currentTests ? { ...currentTests } : {};
  if (diskTests.diskRead) {
    nextTests.diskRead = diskTests.diskRead;
  }
  if (diskTests.diskWrite) {
    nextTests.diskWrite = diskTests.diskWrite;
  }
  if (diskTests.diskReadMBps != null) {
    nextTests.diskReadMBps = diskTests.diskReadMBps;
  }
  if (diskTests.diskWriteMBps != null) {
    nextTests.diskWriteMBps = diskTests.diskWriteMBps;
  }
  payload.tests = nextTests;
  return diskTests;
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

  const diskTests = resolveAuthoritativeDiskTests(body);
  if (diskTests) {
    addStatus('diskReadTest', diskTests.diskRead);
    addStatus('diskWriteTest', diskTests.diskWrite);
  }

  const tests =
    body && typeof body.tests === 'object' && !Array.isArray(body.tests) ? body.tests : null;
  if (tests) {
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

  const server =
    body && body.server && typeof body.server === 'object' && !Array.isArray(body.server) ? body.server : null;
  if (server) {
    if (server.raid && typeof server.raid === 'object') {
      addStatus('serverRaid', server.raid.status);
    }
    const powerSupplyStatus = deriveServerHardwareListStatusFromPayload(body, 'powerSupplies');
    if (powerSupplyStatus) {
      derived.powerSupply = powerSupplyStatus;
    }
    const fanStatus = deriveServerHardwareListStatusFromPayload(body, 'fans');
    if (fanStatus) {
      derived.serverFans = fanStatus;
    }
    const bmcStatus = deriveServerBmcStatusFromPayload(body);
    if (bmcStatus) {
      derived.serverBmc = bmcStatus;
    }
    const serverServicesStatus = deriveServerServicesStatusFromPayload(body);
    if (serverServicesStatus) {
      derived.serverServices = serverServicesStatus;
    }
  }

  return Object.keys(derived).length > 0 ? derived : null;
}

function safeJsonStringify(value, maxBytes) {
  void maxBytes;
  try {
    return JSON.stringify(value);
  } catch (error) {
    return null;
  }
}

function isStoredTruncatedPayload(value) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    return false;
  }
  const keys = Object.keys(value);
  return value.truncated === true && keys.length === 1;
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

function buildMachineIdentityLabel(source, { includeSerial = true, fallback = '--' } = {}) {
  if (!source || typeof source !== 'object') {
    return fallback;
  }
  const vendor = cleanString(source.vendor, 64);
  const model = cleanString(source.model, 96);
  const serial = includeSerial ? cleanString(source.serialNumber || source.serial_number, 128) : null;
  const hardwareLabel = [vendor, model].filter(Boolean).join(' ');
  if (hardwareLabel && serial) {
    return `${hardwareLabel} - ${serial}`;
  }
  if (hardwareLabel) {
    return hardwareLabel;
  }
  if (serial) {
    return serial;
  }
  return fallback;
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

function parseJsonObjectOrNull(value) {
  if (!value) {
    return null;
  }
  if (typeof value === 'object' && !Array.isArray(value)) {
    return value;
  }
  if (typeof value !== 'string') {
    return null;
  }
  try {
    const parsed = JSON.parse(value);
    return parsed && typeof parsed === 'object' && !Array.isArray(parsed) ? parsed : null;
  } catch (error) {
    return null;
  }
}

function isServerCategoryValue(value) {
  return normalizeCategory(value) === 'server';
}

function hasComponentStatusValue(components, key) {
  return Boolean(
    components &&
      typeof components === 'object' &&
      !Array.isArray(components) &&
      Object.prototype.hasOwnProperty.call(components, key)
  );
}

function deriveServerServicesStatusFromPayload(payload) {
  const server =
    payload && payload.server && typeof payload.server === 'object' && !Array.isArray(payload.server)
      ? payload.server
      : null;
  if (!server) {
    return null;
  }
  const failedServices = Array.isArray(server.failedServices)
    ? server.failedServices
        .map((item) => (item == null ? '' : String(item).trim()))
        .filter(Boolean)
    : [];
  const selectedServices = Array.isArray(server.selectedServices)
    ? server.selectedServices.filter((item) => item && typeof item === 'object' && !Array.isArray(item))
    : [];
  const failingSelectedServices = selectedServices.filter((item) => {
    const activeState = String(item.activeState || '')
      .trim()
      .toLowerCase();
    return activeState && activeState !== 'active';
  });
  if (failedServices.length || failingSelectedServices.length) {
    return 'nok';
  }
  if (selectedServices.length) {
    return 'ok';
  }
  return null;
}

function listServerInventoryItems(payload, key) {
  const inventory =
    payload && payload.inventory && typeof payload.inventory === 'object' && !Array.isArray(payload.inventory)
      ? payload.inventory
      : null;
  const raw = inventory ? inventory[key] : null;
  return Array.isArray(raw)
    ? raw.filter((item) => item && typeof item === 'object' && !Array.isArray(item))
    : [];
}

function deriveServerHardwareListStatusFromPayload(payload, inventoryKey) {
  const items = listServerInventoryItems(payload, inventoryKey);
  if (!items.length) {
    return null;
  }
  let hasOk = false;
  let hasKnown = false;
  for (const item of items) {
    const normalized = normalizeStatusKey(item && item.status);
    if (!normalized) {
      continue;
    }
    hasKnown = true;
    if (normalized === 'nok') {
      return 'nok';
    }
    if (normalized === 'ok') {
      hasOk = true;
    }
  }
  if (hasOk) {
    return 'ok';
  }
  return hasKnown ? 'not_tested' : null;
}

function deriveServerBmcStatusFromPayload(payload) {
  const inventory =
    payload && payload.inventory && typeof payload.inventory === 'object' && !Array.isArray(payload.inventory)
      ? payload.inventory
      : null;
  const bmc =
    inventory && inventory.bmc && typeof inventory.bmc === 'object' && !Array.isArray(inventory.bmc)
      ? inventory.bmc
      : null;
  if (!bmc) {
    return null;
  }
  const explicit = normalizeStatusKey(bmc.status);
  if (explicit) {
    return explicit;
  }
  return bmc.ipAddress || bmc.macAddress || bmc.firmwareRevision ? 'ok' : null;
}

function applyServerTelemetryToComponents(components, payload, categoryValue = null) {
  const payloadObject = parseJsonObjectOrNull(payload);
  const isServer =
    isServerCategoryValue(categoryValue) ||
    (payloadObject && isServerCategoryValue(payloadObject.category));
  if (!isServer) {
    return components;
  }
  const next =
    components && typeof components === 'object' && !Array.isArray(components)
      ? { ...components }
      : {};
  const server =
    payloadObject && payloadObject.server && typeof payloadObject.server === 'object' && !Array.isArray(payloadObject.server)
      ? payloadObject.server
      : null;
  const raid =
    server && server.raid && typeof server.raid === 'object' && !Array.isArray(server.raid)
      ? server.raid
      : null;
  const thermal =
    payloadObject && payloadObject.thermal && typeof payloadObject.thermal === 'object' && !Array.isArray(payloadObject.thermal)
      ? payloadObject.thermal
      : null;
  if (!hasComponentStatusValue(next, 'serverRaid') && raid && raid.status) {
    next.serverRaid = raid.status;
  }
  if (!hasComponentStatusValue(next, 'powerSupply')) {
    const powerSupplyStatus = deriveServerHardwareListStatusFromPayload(payloadObject, 'powerSupplies');
    if (powerSupplyStatus) {
      next.powerSupply = powerSupplyStatus;
    }
  }
  if (!hasComponentStatusValue(next, 'serverFans')) {
    const fanStatus = deriveServerHardwareListStatusFromPayload(payloadObject, 'fans');
    if (fanStatus) {
      next.serverFans = fanStatus;
    }
  }
  if (!hasComponentStatusValue(next, 'serverBmc')) {
    const bmcStatus = deriveServerBmcStatusFromPayload(payloadObject);
    if (bmcStatus) {
      next.serverBmc = bmcStatus;
    }
  }
  if (!hasComponentStatusValue(next, 'serverServices')) {
    const servicesStatus = deriveServerServicesStatusFromPayload(payloadObject);
    if (servicesStatus) {
      next.serverServices = servicesStatus;
    }
  }
  if (!hasComponentStatusValue(next, 'thermal') && thermal && thermal.status) {
    next.thermal = thermal.status;
  }
  return next;
}

function shouldIncludeServerPrimaryKey(components, key) {
  return hasComponentStatusValue(components, key);
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

function buildDashboardSummaryComponents(row) {
  let rawComponents = {};
  if (row && row.components && typeof row.components === 'object' && !Array.isArray(row.components)) {
    rawComponents = row.components;
  } else if (row && typeof row.components === 'string') {
    try {
      const parsed = JSON.parse(row.components);
      if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) {
        rawComponents = parsed;
      }
    } catch (error) {
      rawComponents = {};
    }
  }
  const merged = withManualComponentDefaults(rawComponents);
  const topLevelFallbacks = {
    camera: row ? row.camera_status || row.cameraStatus : null,
    usb: row ? row.usb_status || row.usbStatus : null,
    keyboard: row ? row.keyboard_status || row.keyboardStatus : null,
    pad: row ? row.pad_status || row.padStatus : null,
    badgeReader: row ? row.badge_reader_status || row.badgeReaderStatus : null
  };
  Object.entries(topLevelFallbacks).forEach(([key, value]) => {
    if (!Object.prototype.hasOwnProperty.call(merged, key) && value != null && value !== '') {
      merged[key] = value;
    }
  });
  return applyServerTelemetryToComponents(applyClockAlertToComponents(merged, row), row ? row.payload : null, row ? row.category : null);
}

function summarizeDashboardMachine(row) {
  const summary = { ok: 0, nok: 0, other: 0, total: 0 };
  const components = buildDashboardSummaryComponents(row);
  if (isServerCategoryValue(row ? row.category : null)) {
    SERVER_PRIMARY_KEYS.filter((key) => shouldIncludeServerPrimaryKey(components, key)).forEach((key) => {
      const normalized = normalizeSummaryStatusForKey(key, components[key] || 'not_tested');
      if (normalized) {
        addSummaryStatus(summary, normalized);
      }
    });
  } else {
    MACHINE_PRIMARY_COMPONENT_KEYS.forEach((key) => {
      const normalized = normalizeSummaryStatusForKey(key, components[key] || 'not_tested');
      if (normalized) {
        addSummaryStatus(summary, normalized);
      }
    });

    MACHINE_PRIMARY_DIAGNOSTIC_KEYS.forEach((key) => {
      const normalized = normalizeSummaryStatusForKey(key, components[key] || 'not_tested');
      if (normalized) {
        addSummaryStatus(summary, normalized);
      }
    });

    if (Object.prototype.hasOwnProperty.call(components, 'fsCheck')) {
      const normalized = normalizeSummaryStatusForKey('fsCheck', components.fsCheck || 'not_tested');
      if (normalized) {
        addSummaryStatus(summary, normalized);
      }
    }

    const batteryHealth = normalizeBatteryHealth(row ? row.battery_health || row.batteryHealth : null);
    if (batteryHealth != null) {
      addSummaryStatus(summary, batteryHealth < ALERT_BATTERY_THRESHOLD ? 'nok' : 'ok');
    }
  }

  const commentValue = row && typeof row.comment === 'string' ? row.comment.trim() : '';
  if (commentValue) {
    addSummaryStatus(summary, 'nok');
  }

  return summary;
}

function getDashboardMachinePrimaryStatus(row) {
  const summary = summarizeDashboardMachine(row);
  if (summary.nok > 0) {
    return 'nok';
  }
  if (summary.other > 0) {
    return 'nt';
  }
  if (summary.ok > 0) {
    return 'ok';
  }
  return 'nt';
}

function buildDashboardPrimaryStatusCounts(rows) {
  const counts = { ok: 0, nok: 0, nt: 0, total: 0 };
  (rows || []).forEach((row) => {
    const status = getDashboardMachinePrimaryStatus(row);
    counts.total += 1;
    if (status === 'ok') {
      counts.ok += 1;
    } else if (status === 'nok') {
      counts.nok += 1;
    } else {
      counts.nt += 1;
    }
  });
  return counts;
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

function summarizePdfDetailForReport(components, payload, commentValue = '', categoryValue = null) {
  const summary = { ok: 0, nok: 0, other: 0, total: 0 };
  const payloadObject = parseJsonObjectOrNull(payload);
  const isServer =
    isServerCategoryValue(categoryValue) ||
    (payloadObject && isServerCategoryValue(payloadObject.category));
  const mergedComponents = applyServerTelemetryToComponents(
    isServer
      ? components && typeof components === 'object' && !Array.isArray(components)
        ? components
        : {}
      : withManualComponentDefaults(
          components && typeof components === 'object' && !Array.isArray(components) ? components : {}
        ),
    payloadObject,
    categoryValue
  );

  if (isServer) {
    SERVER_PRIMARY_KEYS.filter((key) => shouldIncludeServerPrimaryKey(mergedComponents, key)).forEach((key) => {
      const normalized = normalizeSummaryStatusForKey(key, mergedComponents[key] || 'not_tested');
      if (normalized) {
        addSummaryStatus(summary, normalized);
      }
    });
  } else {
    MACHINE_PRIMARY_COMPONENT_KEYS.forEach((key) => {
      const raw = Object.prototype.hasOwnProperty.call(mergedComponents, key)
        ? mergedComponents[key]
        : 'not_tested';
      const normalized = normalizeSummaryStatusForKey(key, raw || 'not_tested');
      if (normalized) {
        addSummaryStatus(summary, normalized);
      }
    });

    const tests =
      payloadObject && payloadObject.tests && typeof payloadObject.tests === 'object' && !Array.isArray(payloadObject.tests)
        ? payloadObject.tests
        : null;
    const diskTests = resolveAuthoritativeDiskTests(payloadObject);
    const diagnosticCandidates = [];
    if (tests) {
      diagnosticCandidates.push(
        (diskTests && diskTests.diskRead) || mergedComponents.diskReadTest || tests.diskRead || 'not_tested',
        (diskTests && diskTests.diskWrite) || mergedComponents.diskWriteTest || tests.diskWrite || 'not_tested',
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
  }

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

function formatDateOnly(value) {
  const normalized = normalizeShipmentDate(value);
  if (!normalized) {
    return '--';
  }
  const date = new Date(`${normalized}T00:00:00Z`);
  if (Number.isNaN(date.getTime())) {
    return normalized;
  }
  return date.toLocaleDateString('fr-FR', {
    day: '2-digit',
    month: '2-digit',
    year: 'numeric'
  });
}

function mergeHardwarePayload(primary, fallback) {
  const base =
    primary &&
    typeof primary === 'object' &&
    !Array.isArray(primary) &&
    !isStoredTruncatedPayload(primary)
      ? { ...primary }
      : null;
  const fallbackObj =
    fallback &&
    typeof fallback === 'object' &&
    !Array.isArray(fallback) &&
    !isStoredTruncatedPayload(fallback)
      ? fallback
      : null;

  if (!base && fallbackObj) {
    return fallbackObj;
  }
  if (!base || !fallbackObj) {
    return base || null;
  }

  const keys = [
    'cpu',
    'gpu',
    'disks',
    'volumes',
    'autopilot',
    'autopilotHash',
    'deviceHardwareData',
    'device_hardware_data',
    'hardwareHash'
  ];
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

function normalizeAutopilotHashValue(value) {
  if (value == null) {
    return null;
  }
  const trimmed = String(value).trim();
  if (!trimmed) {
    return null;
  }
  return trimmed.replace(/\s+/g, '');
}

function buildAutopilotHash(payload) {
  if (!payload || typeof payload !== 'object' || Array.isArray(payload)) {
    return null;
  }
  const autopilot =
    payload.autopilot && typeof payload.autopilot === 'object' && !Array.isArray(payload.autopilot)
      ? payload.autopilot
      : null;
  const device =
    payload.device && typeof payload.device === 'object' && !Array.isArray(payload.device)
      ? payload.device
      : null;
  const inventory =
    payload.inventory && typeof payload.inventory === 'object' && !Array.isArray(payload.inventory)
      ? payload.inventory
      : null;
  const inventoryAutopilot =
    inventory &&
    inventory.autopilot &&
    typeof inventory.autopilot === 'object' &&
    !Array.isArray(inventory.autopilot)
      ? inventory.autopilot
      : null;

  const candidates = [
    payload.autopilotHash,
    payload.deviceHardwareData,
    payload.device_hardware_data,
    payload.hardwareHash
  ];

  if (autopilot) {
    candidates.push(
      autopilot.hardwareHash,
      autopilot.hash,
      autopilot.deviceHardwareData,
      autopilot.device_hardware_data,
      autopilot.blob
    );
  }
  if (device) {
    candidates.push(device.autopilotHash, device.hardwareHash, device.deviceHardwareData);
  }
  if (inventory) {
    candidates.push(inventory.autopilotHash);
  }
  if (inventoryAutopilot) {
    candidates.push(
      inventoryAutopilot.hardwareHash,
      inventoryAutopilot.hash,
      inventoryAutopilot.deviceHardwareData
    );
  }

  const normalized = candidates.map((value) => normalizeAutopilotHashValue(value)).filter(Boolean);
  if (!normalized.length) {
    return null;
  }
  return normalized.reduce((longest, current) => (current.length > longest.length ? current : longest));
}

function formatPdfAutopilotHash(hashValue) {
  const normalized = normalizeAutopilotHashValue(hashValue);
  if (!normalized) {
    return null;
  }
  const maxLength = 72;
  if (normalized.length <= maxLength) {
    return normalized;
  }
  return `${normalized.slice(0, maxLength)}... (${normalized.length} chars)`;
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

function formatServerDiskInventorySummary(disks) {
  const list = Array.isArray(disks) ? disks.filter((item) => item && typeof item === 'object') : [];
  if (!list.length) {
    return null;
  }
  return `${list.length} disque${list.length > 1 ? 's' : ''}`;
}

function formatServerPowerSupplySummary(powerSupplies) {
  const list = Array.isArray(powerSupplies) ? powerSupplies.filter((item) => item && typeof item === 'object') : [];
  if (!list.length) {
    return null;
  }
  const nokCount = list.filter((item) => normalizeStatusKey(item.status) === 'nok').length;
  if (nokCount > 0) {
    return `${list.length} alims · ${nokCount} alerte${nokCount > 1 ? 's' : ''}`;
  }
  return `${list.length} alim${list.length > 1 ? 's' : ''} OK`;
}

function formatServerFanSummary(fans) {
  const list = Array.isArray(fans) ? fans.filter((item) => item && typeof item === 'object') : [];
  if (!list.length) {
    return null;
  }
  const nokCount = list.filter((item) => normalizeStatusKey(item.status) === 'nok').length;
  if (nokCount > 0) {
    return `${list.length} ventilos · ${nokCount} alerte${nokCount > 1 ? 's' : ''}`;
  }
  return `${list.length} ventilos OK`;
}

function formatServerBmcSummary(bmc) {
  if (!bmc || typeof bmc !== 'object' || Array.isArray(bmc)) {
    return null;
  }
  const ipAddress = safeString(bmc.ipAddress, '');
  const firmwareRevision = safeString(bmc.firmwareRevision, '');
  const manufacturer = safeString(bmc.manufacturer, '');
  const product = safeString(bmc.product, '');
  if (ipAddress && firmwareRevision) {
    return `${ipAddress} · FW ${firmwareRevision}`;
  }
  if (ipAddress) {
    return ipAddress;
  }
  if (manufacturer && product) {
    return `${manufacturer} ${product}`;
  }
  if (product) {
    return product;
  }
  if (firmwareRevision) {
    return `FW ${firmwareRevision}`;
  }
  return 'Present';
}

function formatServerControllerSummary(controllers) {
  const list = Array.isArray(controllers) ? controllers.filter((item) => item && typeof item === 'object') : [];
  if (!list.length) {
    return null;
  }
  const names = list
    .map((item) => safeString(item.name || item.description, ''))
    .filter(Boolean);
  return names.length ? names.join(' • ') : `${list.length} controleur${list.length > 1 ? 's' : ''}`;
}

function formatServerPlatformLabel(value) {
  const normalized = safeString(value, '').toLowerCase();
  if (!normalized) {
    return null;
  }
  if (normalized === 'physical') {
    return 'Physique';
  }
  if (normalized === 'virtual') {
    return 'Virtualise';
  }
  if (normalized.startsWith('virtual:')) {
    return `Virtualise (${normalized.slice(8)})`;
  }
  return normalized.charAt(0).toUpperCase() + normalized.slice(1);
}

function buildDiagnosticsRows(payload, components = null, categoryValue = null) {
  const rows = [];
  const payloadObject = parseJsonObjectOrNull(payload);
  const componentMap = applyServerTelemetryToComponents(
    components && typeof components === 'object' && !Array.isArray(components) ? components : {},
    payloadObject,
    categoryValue
  );
  const isServer =
    isServerCategoryValue(categoryValue) ||
    (payloadObject && isServerCategoryValue(payloadObject.category));
  const tests =
    payloadObject && payloadObject.tests && typeof payloadObject.tests === 'object' && !Array.isArray(payloadObject.tests)
      ? payloadObject.tests
      : null;
  const diskTests = resolveAuthoritativeDiskTests(payloadObject);
  const winSat =
    payloadObject && payloadObject.winsat && typeof payloadObject.winsat === 'object' ? payloadObject.winsat : null;
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

  if (isServer) {
    const server =
      payloadObject && payloadObject.server && typeof payloadObject.server === 'object' && !Array.isArray(payloadObject.server)
        ? payloadObject.server
        : null;
    const inventory =
      payloadObject &&
      payloadObject.inventory &&
      typeof payloadObject.inventory === 'object' &&
      !Array.isArray(payloadObject.inventory)
        ? payloadObject.inventory
        : null;
    const raid =
      server && server.raid && typeof server.raid === 'object' && !Array.isArray(server.raid)
        ? server.raid
        : null;
    const powerSupplies = listServerInventoryItems(payloadObject, 'powerSupplies');
    const fans = listServerInventoryItems(payloadObject, 'fans');
    const bmc =
      inventory && inventory.bmc && typeof inventory.bmc === 'object' && !Array.isArray(inventory.bmc)
        ? inventory.bmc
        : null;
    const thermal =
      payloadObject && payloadObject.thermal && typeof payloadObject.thermal === 'object' && !Array.isArray(payloadObject.thermal)
        ? payloadObject.thermal
        : null;
    const selectedServices = Array.isArray(server && server.selectedServices)
      ? server.selectedServices.filter((item) => item && typeof item === 'object' && !Array.isArray(item))
      : [];
    const failedServices = Array.isArray(server && server.failedServices)
      ? server.failedServices
          .map((item) => (item == null ? '' : String(item).trim()))
          .filter(Boolean)
      : [];
    const disks = Array.isArray(payloadObject && payloadObject.disks)
      ? payloadObject.disks.filter((item) => item && typeof item === 'object' && !Array.isArray(item))
      : [];
    const thermalMetric =
      thermal && typeof thermal.maxCelsius === 'number' && Number.isFinite(thermal.maxCelsius)
        ? `${thermal.maxCelsius.toFixed(1).replace(/\\.0$/, '')} °C`
        : null;
    const servicesMetric = selectedServices.length
      ? selectedServices
          .map((item) => String(item.name || '').trim())
          .filter(Boolean)
          .join(' • ')
      : failedServices.length
        ? failedServices.join(' • ')
        : null;
    const loadMetric =
      server &&
      [server.loadAverage1m, server.loadAverage5m, server.loadAverage15m]
        .map((value) => parseMetricNumber(value))
        .some((value) => value != null)
        ? [
            ['1m', parseMetricNumber(server.loadAverage1m)],
            ['5m', parseMetricNumber(server.loadAverage5m)],
            ['15m', parseMetricNumber(server.loadAverage15m)]
          ]
            .filter(([, value]) => value != null)
            .map(([label, value]) => `${label} ${value.toFixed(2).replace(/\\.00$/, '')}`)
            .join(' / ')
        : null;

    if (Object.prototype.hasOwnProperty.call(componentMap, 'diskSmart')) {
      addRow('SMART disques', componentMap.diskSmart || 'not_tested', formatServerDiskInventorySummary(disks));
    }
    if (Object.prototype.hasOwnProperty.call(componentMap, 'serverRaid')) {
      addRow('RAID', componentMap.serverRaid || 'not_tested', raid && raid.summary ? raid.summary : raid && raid.mdstat ? 'mdstat' : null);
    }
    if (Object.prototype.hasOwnProperty.call(componentMap, 'powerSupply')) {
      addRow('Alimentations', componentMap.powerSupply || 'not_tested', formatServerPowerSupplySummary(powerSupplies));
    }
    if (Object.prototype.hasOwnProperty.call(componentMap, 'serverFans')) {
      addRow('Ventilos', componentMap.serverFans || 'not_tested', formatServerFanSummary(fans));
    }
    if (Object.prototype.hasOwnProperty.call(componentMap, 'serverBmc')) {
      addRow('BMC', componentMap.serverBmc || 'not_tested', formatServerBmcSummary(bmc));
    }
    if (Object.prototype.hasOwnProperty.call(componentMap, 'serverServices')) {
      addRow('Services critiques', componentMap.serverServices || 'not_tested', servicesMetric);
    }
    if (Object.prototype.hasOwnProperty.call(componentMap, 'thermal')) {
      addRow('Thermique', componentMap.thermal || 'not_tested', thermalMetric);
    }
    addRow('Ping', pickStatus(tests && tests.networkPing, componentMap.networkPing, 'not_tested'), tests ? tests.networkPingTarget || null : null);
    addRow('Check disque', pickStatus(tests && tests.fsCheck, componentMap.fsCheck, null), null);
    if (server && server.uptimeSeconds != null) {
      addRow('Uptime', null, `${Math.round(server.uptimeSeconds / 3600)} h`);
    }
    if (loadMetric) {
      addRow('Charge systeme', null, loadMetric);
    }
    return rows;
  }

  const diskReadStatus = pickStatus(
    diskTests && diskTests.diskRead,
    componentMap.diskReadTest,
    tests && tests.diskRead,
    'not_tested'
  );
  const diskWriteStatus = pickStatus(
    diskTests && diskTests.diskWrite,
    componentMap.diskWriteTest,
    tests && tests.diskWrite,
    'not_tested'
  );
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
    diskTests && diskTests.diskReadMBps != null
      ? diskTests.diskReadMBps
      : tests && tests.diskReadMBps != null
        ? tests.diskReadMBps
        : winSat && winSat.disk && winSat.disk.seqReadMBps != null
          ? winSat.disk.seqReadMBps
          : null
  );
  const diskWriteMetric = formatMbps(
    diskTests && diskTests.diskWriteMBps != null
      ? diskTests.diskWriteMBps
      : tests && tests.diskWriteMBps != null
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

  const chassis =
    inventory.chassis && typeof inventory.chassis === 'object' && !Array.isArray(inventory.chassis)
      ? inventory.chassis
      : null;
  if (chassis) {
    const chassisLabel = [chassis.vendor, chassis.name, chassis.type]
      .map((value) => safeString(value, ''))
      .filter(Boolean)
      .join(' • ');
    if (chassisLabel) {
      rows.push({ label: 'Chassis', value: chassisLabel });
    }
  }

  const bmc =
    inventory.bmc && typeof inventory.bmc === 'object' && !Array.isArray(inventory.bmc) ? inventory.bmc : null;
  const bmcSummary = formatServerBmcSummary(bmc);
  if (bmcSummary) {
    rows.push({ label: 'BMC', value: bmcSummary });
  }

  const controllers = listServerInventoryItems({ inventory }, 'storageControllers');
  const controllerSummary = formatServerControllerSummary(controllers);
  if (controllerSummary) {
    rows.push({ label: 'Controleurs stockage', value: controllerSummary });
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

  const powerSupplies = listServerInventoryItems({ inventory }, 'powerSupplies');
  const powerSupplySummary = formatServerPowerSupplySummary(powerSupplies);
  if (powerSupplySummary) {
    rows.push({ label: 'Alimentations', value: powerSupplySummary });
  }

  const fans = listServerInventoryItems({ inventory }, 'fans');
  const fanSummary = formatServerFanSummary(fans);
  if (fanSummary) {
    rows.push({ label: 'Ventilos', value: fanSummary });
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
  const modelText = safeString(data.productLabel || data.subtitle, '--');
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
  if (isServerCategoryValue(data.category)) {
    doc.fillColor(palette.accentPillText).font('Helvetica-Bold').fontSize(7.8).text('Rapport serveur', badgeX, headerY + 16, {
      width: badgeWidth,
      align: 'center'
    });
  } else {
    doc.fillColor(palette.accentPillText).font('Helvetica-Bold').fontSize(7.8).text('Rapport machine', badgeX, headerY + 16, {
      width: badgeWidth,
      align: 'center'
    });
  }
  doc
    .fillColor(palette.headerSubText)
    .font('Helvetica-Bold')
    .fontSize(7.1)
    .text('Marque / modele / SN', badgeX, headerY + 40, {
      width: badgeWidth,
      align: 'right',
      lineBreak: false
    });
  doc
    .fillColor(palette.headerText)
    .font('Helvetica')
    .fontSize(7.8)
    .text(truncatePdfText(data.productLabel || data.subtitle, 34), badgeX, headerY + 49, {
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
  const diagnosticsOrder = isServerCategoryValue(data.category)
    ? [
        'SMART disques',
        'RAID',
        'Alimentations',
        'Ventilos',
        'BMC',
        'Thermique',
        'Ping',
        'Check disk',
        'Services critiques',
        'Uptime',
        'Charge systeme'
      ]
    : ['Lecture disque', 'Ecriture disque', 'RAM (WinSAT)', 'CPU (WinSAT)', 'GPU (WinSAT)', 'Ping', 'Check disk'];
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
    { label: 'Commande', value: data.shipmentOrderNumber || '--' },
    { label: 'Palette', value: data.shipmentPalletCode || '--' },
    { label: 'Client', value: data.shipmentClient || '--' },
    { label: 'Date expedition', value: data.shipmentDate || '--' },
    { label: 'Hash Autopilot', value: data.autopilotHashDisplay || data.autopilotHash },
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
  const leftIdHeight = Math.max(158, Math.floor(contentHeight * 0.27));
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

app.get('/servers', requireAuth, (req, res) => {
  res.sendFile(path.join(PUBLIC_DIR, 'servers.html'));
});

app.get('/servers.html', requireAuth, (req, res) => {
  res.sendFile(path.join(PUBLIC_DIR, 'servers.html'));
});

app.get('/admin', requireAuth, requireAdminPage, (req, res) => {
  res.sendFile(path.join(PUBLIC_DIR, 'admin.html'));
});

app.get('/admin.html', requireAuth, requireAdminPage, (req, res) => {
  res.sendFile(path.join(PUBLIC_DIR, 'admin.html'));
});

app.get('/lots', requireAuth, requireLogisticsPage, (req, res) => {
  res.sendFile(path.join(PUBLIC_DIR, 'lots.html'));
});

app.get('/lots.html', requireAuth, requireLogisticsPage, (req, res) => {
  res.sendFile(path.join(PUBLIC_DIR, 'lots.html'));
});

app.get('/pallets', requireAuth, requireLogisticsPage, (req, res) => {
  res.sendFile(path.join(PUBLIC_DIR, 'pallets.html'));
});

app.get('/pallets.html', requireAuth, requireLogisticsPage, (req, res) => {
  res.sendFile(path.join(PUBLIC_DIR, 'pallets.html'));
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

app.get('/auth/microsoft/signin', async (req, res) => {
  if (req.session && req.session.user && !isSupportedSessionUser(req.session.user)) {
    delete req.session.user;
    delete req.session.microsoftAuthState;
    delete req.session.microsoftAuthStartedAt;
  }
  if (isSupportedSessionUser(req.session?.user)) {
    return res.redirect('/');
  }
  if (!MICROSOFT_SSO_ENABLED) {
    return res.redirect('/login?error=1');
  }

  const clientApp = getMicrosoftClientApp();
  if (!clientApp) {
    return res.redirect('/login?error=1');
  }

  const state = generateRequestId();
  req.session.microsoftAuthState = state;
  req.session.microsoftAuthStartedAt = Date.now();

  return req.session.save(async (saveError) => {
    if (saveError) {
      return res.redirect('/login?error=1');
    }
    try {
      const authUrl = await clientApp.getAuthCodeUrl({
        scopes: MICROSOFT_AUTH_SCOPES,
        redirectUri: getMicrosoftRedirectUri(req),
        state,
        prompt: 'select_account'
      });
      return res.redirect(authUrl);
    } catch (error) {
      console.error('Failed to build Microsoft auth URL', error);
      return res.redirect('/login?error=1');
    }
  });
});

app.get('/auth/microsoft/callback', async (req, res) => {
  if (!MICROSOFT_SSO_ENABLED) {
    return res.redirect('/login?error=1');
  }

  const code = cleanString(typeof req.query?.code === 'string' ? req.query.code : '', 8192);
  const state = cleanString(typeof req.query?.state === 'string' ? req.query.state : '', 256);
  const expectedState = cleanString(req.session?.microsoftAuthState || '', 256);
  delete req.session.microsoftAuthState;
  delete req.session.microsoftAuthStartedAt;

  if (!code || !state || !expectedState || state !== expectedState) {
    return req.session.save(() => res.redirect('/login?error=1'));
  }

  const clientApp = getMicrosoftClientApp();
  if (!clientApp) {
    return req.session.save(() => res.redirect('/login?error=1'));
  }

  try {
    const tokenResponse = await clientApp.acquireTokenByCode({
      code,
      scopes: MICROSOFT_AUTH_SCOPES,
      redirectUri: getMicrosoftRedirectUri(req)
    });
    const claims =
      tokenResponse && tokenResponse.idTokenClaims && typeof tokenResponse.idTokenClaims === 'object'
        ? tokenResponse.idTokenClaims
        : {};
    const tenantId = cleanString(
      claims.tid || (tokenResponse.account && tokenResponse.account.tenantId) || '',
      64
    );
    if (!tenantId || tenantId.toLowerCase() !== MICROSOFT_ENTRA_TENANT_ID.toLowerCase()) {
      return req.session.save(() => res.redirect('/login?error=1'));
    }

    const groupResolution = await resolveMicrosoftGroupIdsForSignIn(tokenResponse.accessToken || '', claims);
    if (groupResolution.error) {
      return req.session.save(() => res.redirect('/login?error=group_resolution_failed'));
    }

    const resolvedClaims =
      groupResolution.groupIds.length || !Array.isArray(claims.groups)
        ? {
            ...claims,
            groups: groupResolution.groupIds
          }
        : claims;

    const user = buildMicrosoftSessionUser(tokenResponse.account || null, resolvedClaims);
    if (!user || !user.username) {
      return req.session.save(() => res.redirect('/login?error=group_required'));
    }

    return req.session.regenerate((err) => {
      if (err) {
        return res.redirect('/login?error=1');
      }
      req.session.user = user;
      return req.session.save(() => res.redirect('/'));
    });
  } catch (error) {
    console.error('Failed Microsoft callback', error);
    return req.session.save(() => res.redirect('/login?error=1'));
  }
});

app.get('/login', (req, res) => {
  if (req.session && req.session.user && !isSupportedSessionUser(req.session.user)) {
    return clearSessionUser(req, () => res.sendFile(path.join(PUBLIC_DIR, 'login.html')));
  }
  if (isSupportedSessionUser(req.session?.user)) {
    return res.redirect('/');
  }
  return res.sendFile(path.join(PUBLIC_DIR, 'login.html'));
});

app.post('/login', (req, res) => {
  return res.redirect('/login?error=sso_only');
});

app.post('/api/login', (req, res) => {
  return res.status(403).json({ ok: false, error: 'sso_only' });
});

app.get('/logout', (req, res) => {
  if (!req.session) {
    return res.redirect('/login');
  }
  const nextUrl =
    req.session.user && req.session.user.type === 'microsoft' && MICROSOFT_SSO_ENABLED
      ? buildMicrosoftLogoutUrl(req)
      : '/login';
  return req.session.destroy(() => res.redirect(nextUrl));
});

app.get('/guide', requireAuth, (req, res) => {
  res.sendFile(path.join(PUBLIC_DIR, 'guide.html'));
});

app.get('/guide.html', requireAuth, (req, res) => {
  res.sendFile(path.join(PUBLIC_DIR, 'guide.html'));
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

app.get('/api/health', async (req, res) => {
  try {
    await pool.query('SELECT 1');
    return res.json({ ok: true, db: { ok: true } });
  } catch (error) {
    console.error('Healthcheck failed', error);
    return res.status(503).json({ ok: false, db: { ok: false, error: 'db_unavailable' } });
  }
});

app.get('/api/auth/providers', (req, res) => {
  res.json({
    ok: true,
    microsoft: {
      enabled: MICROSOFT_SSO_ENABLED,
      tenantOnly: true
    }
  });
});

app.get('/api/barcode/serial/:serial.png', requireAuth, async (req, res) => {
  const serialValue = cleanString(req.params.serial, 128);
  const vendorValue = cleanString(req.query?.vendor, 64);
  const modelValue = cleanString(req.query?.model, 96);
  if (!serialValue) {
    return res.status(400).json({ ok: false, error: 'invalid_serial' });
  }
  const barcodeLabel =
    cleanString(
      buildMachineIdentityLabel(
        {
          vendor: vendorValue,
          model: modelValue,
          serialNumber: serialValue
        },
        { includeSerial: true, fallback: serialValue }
      ),
      160
    ) || serialValue;
  try {
    const imageBuffer = await bwipjs.toBuffer({
      bcid: 'code128',
      text: serialValue,
      scale: 2,
      height: 10,
      includetext: true,
      alttext: barcodeLabel,
      textxalign: 'center',
      textsize: 11,
      backgroundcolor: 'FFFFFF'
    });
    res.setHeader('Content-Type', 'image/png');
    res.setHeader('Cache-Control', 'private, max-age=600');
    return res.send(imageBuffer);
  } catch (error) {
    console.error('Barcode generation failed', {
      error: error && error.message ? error.message : String(error),
      requestId: req.requestId
    });
    return res.status(400).json({ ok: false, error: 'barcode_generation_failed' });
  }
});

app.get('/api/me', requireAuth, (req, res) => {
  refreshLdapPermissions(req)
    .then((user) => {
      const resolvedUser = user || req.session.user;
      res.json({ ok: true, user: buildClientUserPayload(resolvedUser) });
    })
    .catch(() => {
      res.json({ ok: true, user: buildClientUserPayload(req.session.user) });
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

function readMdtBetaAgentIdentity(req) {
  const headerToken = cleanString(req.get('x-mdt-beta-agent-token'), 512);
  const bodyToken = cleanString(typeof req.body?.token === 'string' ? req.body.token : '', 512);
  const token = headerToken || bodyToken || '';
  const agentId = cleanString(
    req.get('x-mdt-beta-agent-id') ||
      (typeof req.body?.agentId === 'string' ? req.body.agentId : ''),
    128
  );
  return { token, agentId: agentId || '' };
}

function isAuthorizedMdtBetaAgent(req) {
  const identity = readMdtBetaAgentIdentity(req);
  return MDT_BETA_AUTOMATION_ENABLED && identity.agentId && identity.token && identity.token === MDT_BETA_AGENT_TOKEN
    ? identity
    : null;
}

app.get('/api/admin/mdt-beta', requireAuth, requireAdmin, async (req, res) => {
  try {
    const payload = await buildMdtBetaAdminPayload();
    return res.json({ ok: true, automation: payload });
  } catch (error) {
    console.error('Failed to build MDT beta admin payload', error);
    return res.status(500).json({ ok: false, error: 'db_error' });
  }
});

app.post('/api/admin/mdt-beta/technicians', requireAuth, requireAdmin, async (req, res) => {
  if (!MDT_BETA_AUTOMATION_ENABLED) {
    return res.status(503).json({ ok: false, error: 'automation_disabled' });
  }

  const displayName = cleanString(req.body?.displayName || req.body?.name, 64);
  const sourceTaskSequenceId = normalizeMdtTaskSequenceId(MDT_BETA_DEFAULT_SOURCE_TASK_SEQUENCE_ID);
  const slug = normalizeMdtBetaSlug(req.body?.slug || displayName || '');
  const betaTaskSequenceId = buildMdtBetaTaskSequenceId(slug);
  const betaTaskSequenceName = buildMdtBetaTaskSequenceName(displayName);

  if (!displayName || !slug || !sourceTaskSequenceId || !betaTaskSequenceId) {
    return res.status(400).json({ ok: false, error: 'invalid_payload' });
  }

  const technicianId = generateUuid();
  const jobId = generateUuid();
  const createdBy = getSessionActorName(req);
  const technician = {
    id: technicianId,
    displayName,
    slug,
    sourceTaskSequenceId,
    betaTaskSequenceId,
    betaTaskSequenceName,
    taskSequenceGroupName: MDT_BETA_GROUP_NAME,
    scriptsFolder: MDT_BETA_SCRIPTS_FOLDER
  };
  const payload = buildMdtBetaJobPayload(technician);
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    await setAuditContext(client, buildAuditContext(req));
    const existing = await client.query(
      `
        SELECT id
        FROM mdt_beta_technicians
        WHERE slug = $1 OR beta_task_sequence_id = $2
        LIMIT 1
      `,
      [slug, betaTaskSequenceId]
    );
    if (existing.rows && existing.rows[0]) {
      await client.query('ROLLBACK');
      return res.status(409).json({ ok: false, error: 'technician_exists' });
    }
    await client.query(
      `
        INSERT INTO mdt_beta_technicians (
          id,
          display_name,
          slug,
          source_task_sequence_id,
          beta_task_sequence_id,
          beta_task_sequence_name,
          task_sequence_group_name,
          scripts_folder,
          status,
          created_by,
          created_at,
          updated_at,
          last_job_id,
          last_result
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'queued', $9, NOW(), NOW(), $10, '{}'::jsonb)
      `,
      [
        technicianId,
        displayName,
        slug,
        sourceTaskSequenceId,
        betaTaskSequenceId,
        betaTaskSequenceName,
        MDT_BETA_GROUP_NAME,
        MDT_BETA_SCRIPTS_FOLDER,
        createdBy,
        jobId
      ]
    );
    await client.query(
      `
        INSERT INTO mdt_beta_jobs (
          id,
          technician_id,
          job_type,
          status,
          payload,
          requested_by,
          created_at,
          updated_at
        )
        VALUES ($1, $2, 'provision', 'queued', $3::jsonb, $4, NOW(), NOW())
      `,
      [jobId, technicianId, JSON.stringify(payload), createdBy]
    );
    await client.query('COMMIT');
    const automation = await buildMdtBetaAdminPayload();
    return res.status(201).json({ ok: true, technician: payload, automation });
  } catch (error) {
    await client.query('ROLLBACK');
    console.error('Failed to create MDT beta technician', error);
    return res.status(500).json({ ok: false, error: 'create_failed' });
  } finally {
    client.release();
  }
});

app.post('/api/admin/mdt-beta/technicians/:id/reprovision', requireAuth, requireAdmin, async (req, res) => {
  if (!MDT_BETA_AUTOMATION_ENABLED) {
    return res.status(503).json({ ok: false, error: 'automation_disabled' });
  }

  const technicianId = normalizeUuid(req.params.id);
  if (!technicianId) {
    return res.status(400).json({ ok: false, error: 'invalid_id' });
  }

  const requestedBy = getSessionActorName(req);
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    await setAuditContext(client, buildAuditContext(req));
    const techResult = await client.query(
      `
        SELECT *
        FROM mdt_beta_technicians
        WHERE id = $1
        LIMIT 1
        FOR UPDATE
      `,
      [technicianId]
    );
    const techRow = techResult.rows && techResult.rows[0] ? techResult.rows[0] : null;
    if (!techRow) {
      await client.query('ROLLBACK');
      return res.status(404).json({ ok: false, error: 'not_found' });
    }
    const runningJob = await client.query(
      `
        SELECT id
        FROM mdt_beta_jobs
        WHERE technician_id = $1 AND status = 'running'
        LIMIT 1
      `,
      [technicianId]
    );
    if (runningJob.rows && runningJob.rows[0]) {
      await client.query('ROLLBACK');
      return res.status(409).json({ ok: false, error: 'job_running' });
    }

    const jobId = generateUuid();
    const payload = buildMdtBetaJobPayload({
      id: techRow.id,
      displayName: techRow.display_name,
      slug: techRow.slug,
      sourceTaskSequenceId: techRow.source_task_sequence_id,
      betaTaskSequenceId: techRow.beta_task_sequence_id,
      betaTaskSequenceName: techRow.beta_task_sequence_name,
      taskSequenceGroupName: techRow.task_sequence_group_name,
      scriptsFolder: techRow.scripts_folder
    });
    await client.query(
      `
        INSERT INTO mdt_beta_jobs (
          id,
          technician_id,
          job_type,
          status,
          payload,
          requested_by,
          created_at,
          updated_at
        )
        VALUES ($1, $2, 'provision', 'queued', $3::jsonb, $4, NOW(), NOW())
      `,
      [jobId, technicianId, JSON.stringify(payload), requestedBy]
    );
    await client.query(
      `
        UPDATE mdt_beta_technicians
        SET status = 'queued', updated_at = NOW(), last_job_id = $2, last_error = NULL
        WHERE id = $1
      `,
      [technicianId, jobId]
    );
    await client.query('COMMIT');
    const automation = await buildMdtBetaAdminPayload();
    return res.json({ ok: true, automation });
  } catch (error) {
    await client.query('ROLLBACK');
    console.error('Failed to queue MDT beta reprovision', error);
    return res.status(500).json({ ok: false, error: 'reprovision_failed' });
  } finally {
    client.release();
  }
});

app.delete('/api/admin/mdt-beta/technicians/:id', requireAuth, requireAdmin, async (req, res) => {
  if (!MDT_BETA_AUTOMATION_ENABLED) {
    return res.status(503).json({ ok: false, error: 'automation_disabled' });
  }

  const technicianId = normalizeUuid(req.params.id);
  if (!technicianId) {
    return res.status(400).json({ ok: false, error: 'invalid_id' });
  }

  const requestedBy = getSessionActorName(req);
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    await setAuditContext(client, buildAuditContext(req));
    const techResult = await client.query(
      `
        SELECT *
        FROM mdt_beta_technicians
        WHERE id = $1
        LIMIT 1
        FOR UPDATE
      `,
      [technicianId]
    );
    const techRow = techResult.rows && techResult.rows[0] ? techResult.rows[0] : null;
    if (!techRow) {
      await client.query('ROLLBACK');
      return res.status(404).json({ ok: false, error: 'not_found' });
    }

    const runningJob = await client.query(
      `
        SELECT id
        FROM mdt_beta_jobs
        WHERE technician_id = $1 AND status = 'running'
        LIMIT 1
      `,
      [technicianId]
    );
    if (runningJob.rows && runningJob.rows[0]) {
      await client.query('ROLLBACK');
      return res.status(409).json({ ok: false, error: 'job_running' });
    }

    const jobId = generateUuid();
    const payload = buildMdtBetaJobPayload({
      id: techRow.id,
      displayName: techRow.display_name,
      slug: techRow.slug,
      sourceTaskSequenceId: techRow.source_task_sequence_id,
      betaTaskSequenceId: techRow.beta_task_sequence_id,
      betaTaskSequenceName: techRow.beta_task_sequence_name,
      taskSequenceGroupName: techRow.task_sequence_group_name,
      scriptsFolder: techRow.scripts_folder
    });
    await client.query(
      `
        INSERT INTO mdt_beta_jobs (
          id,
          technician_id,
          job_type,
          status,
          payload,
          requested_by,
          created_at,
          updated_at
        )
        VALUES ($1, $2, 'delete', 'queued', $3::jsonb, $4, NOW(), NOW())
      `,
      [jobId, technicianId, JSON.stringify(payload), requestedBy]
    );
    await client.query(
      `
        UPDATE mdt_beta_technicians
        SET status = 'queued', updated_at = NOW(), last_job_id = $2, last_error = NULL
        WHERE id = $1
      `,
      [technicianId, jobId]
    );
    await client.query('COMMIT');
    const automation = await buildMdtBetaAdminPayload();
    return res.json({ ok: true, automation });
  } catch (error) {
    await client.query('ROLLBACK');
    console.error('Failed to queue MDT beta delete', error);
    return res.status(500).json({ ok: false, error: 'delete_failed' });
  } finally {
    client.release();
  }
});

app.post('/api/mdt-beta-agent/heartbeat', async (req, res) => {
  const identity = isAuthorizedMdtBetaAgent(req);
  if (!identity) {
    return res.status(MDT_BETA_AUTOMATION_ENABLED ? 401 : 404).json({ ok: false, error: 'unauthorized' });
  }

  const hostname = cleanString(req.body?.hostname, 128);
  const deploymentShareRoot = cleanString(req.body?.deploymentShareRoot, 255);
  const status = cleanString(req.body?.status, 32) || 'idle';
  const lastJobId = normalizeUuid(req.body?.lastJobId);
  const lastError = cleanString(req.body?.lastError, 2000);
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    await setAuditContext(client, buildMdtBetaAgentAuditContext(req, identity.agentId, { source: 'POST /api/mdt-beta-agent/heartbeat' }));
    await upsertMdtBetaAgentState(client, {
      agentId: identity.agentId,
      hostname,
      deploymentShareRoot,
      taskSequenceGroupName: cleanString(req.body?.taskSequenceGroupName, 128) || MDT_BETA_GROUP_NAME,
      scriptsFolder: cleanString(req.body?.scriptsFolder, 64) || MDT_BETA_SCRIPTS_FOLDER,
      status,
      lastJobId,
      lastError
    });
    if (lastJobId) {
      await client.query(
        `
          UPDATE mdt_beta_jobs
          SET heartbeat_at = NOW(), updated_at = NOW()
          WHERE id = $1 AND status = 'running'
        `,
        [lastJobId]
      );
    }
    await client.query('COMMIT');
    return res.json({ ok: true });
  } catch (error) {
    await client.query('ROLLBACK');
    console.error('Failed to record MDT beta agent heartbeat', error);
    return res.status(500).json({ ok: false, error: 'heartbeat_failed' });
  } finally {
    client.release();
  }
});

app.post('/api/mdt-beta-agent/jobs/claim', async (req, res) => {
  const identity = isAuthorizedMdtBetaAgent(req);
  if (!identity) {
    return res.status(MDT_BETA_AUTOMATION_ENABLED ? 401 : 404).json({ ok: false, error: 'unauthorized' });
  }

  const hostname = cleanString(req.body?.hostname, 128);
  const deploymentShareRoot = cleanString(req.body?.deploymentShareRoot, 255);
  const taskSequenceGroupName = cleanString(req.body?.taskSequenceGroupName, 128) || MDT_BETA_GROUP_NAME;
  const scriptsFolder = cleanString(req.body?.scriptsFolder, 64) || MDT_BETA_SCRIPTS_FOLDER;
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    await setAuditContext(client, buildMdtBetaAgentAuditContext(req, identity.agentId, { source: 'POST /api/mdt-beta-agent/jobs/claim' }));
    await requeueStaleMdtBetaJobs(client);
    await upsertMdtBetaAgentState(client, {
      agentId: identity.agentId,
      hostname,
      deploymentShareRoot,
      taskSequenceGroupName,
      scriptsFolder,
      status: 'idle'
    });
    const jobResult = await client.query(
      `
        SELECT
          j.id,
          j.technician_id,
          j.job_type,
          j.payload
        FROM mdt_beta_jobs j
        WHERE j.status = 'queued'
        ORDER BY j.created_at ASC, j.id ASC
        FOR UPDATE SKIP LOCKED
        LIMIT 1
      `
    );
    const jobRow = jobResult.rows && jobResult.rows[0] ? jobResult.rows[0] : null;
    if (!jobRow) {
      await client.query('COMMIT');
      return res.json({ ok: true, job: null });
    }
    await client.query(
      `
        UPDATE mdt_beta_jobs
        SET
          status = 'running',
          agent_id = $2,
          claimed_at = NOW(),
          heartbeat_at = NOW(),
          started_at = NOW(),
          updated_at = NOW()
        WHERE id = $1
      `,
      [jobRow.id, identity.agentId]
    );
    await client.query(
      `
        UPDATE mdt_beta_technicians
        SET status = 'provisioning', updated_at = NOW(), last_job_id = $2
        WHERE id = $1
      `,
      [jobRow.technician_id, jobRow.id]
    );
    await upsertMdtBetaAgentState(client, {
      agentId: identity.agentId,
      hostname,
      deploymentShareRoot,
      taskSequenceGroupName,
      scriptsFolder,
      status: 'running',
      lastJobId: jobRow.id
    });
    await client.query('COMMIT');
    return res.json({
      ok: true,
      job: {
        id: jobRow.id,
        type: jobRow.job_type,
        payload: jobRow.payload || {}
      }
    });
  } catch (error) {
    await client.query('ROLLBACK');
    console.error('Failed to claim MDT beta job', error);
    return res.status(500).json({ ok: false, error: 'claim_failed' });
  } finally {
    client.release();
  }
});

app.post('/api/mdt-beta-agent/jobs/:id/complete', async (req, res) => {
  const identity = isAuthorizedMdtBetaAgent(req);
  if (!identity) {
    return res.status(MDT_BETA_AUTOMATION_ENABLED ? 401 : 404).json({ ok: false, error: 'unauthorized' });
  }

  const jobId = normalizeUuid(req.params.id);
  if (!jobId) {
    return res.status(400).json({ ok: false, error: 'invalid_id' });
  }

  const resultPayload = req.body && typeof req.body.result === 'object' && !Array.isArray(req.body.result) ? req.body.result : {};
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    await setAuditContext(client, buildMdtBetaAgentAuditContext(req, identity.agentId, { source: 'POST /api/mdt-beta-agent/jobs/complete' }));
    const jobResult = await client.query(
      `
        SELECT id, technician_id, job_type
        FROM mdt_beta_jobs
        WHERE id = $1 AND status = 'running' AND (agent_id = $2 OR agent_id IS NULL)
        LIMIT 1
        FOR UPDATE
      `,
      [jobId, identity.agentId]
    );
    const jobRow = jobResult.rows && jobResult.rows[0] ? jobResult.rows[0] : null;
    if (!jobRow) {
      await client.query('ROLLBACK');
      return res.status(404).json({ ok: false, error: 'job_not_found' });
    }
    await client.query(
      `
        UPDATE mdt_beta_jobs
        SET
          status = 'succeeded',
          result = $2::jsonb,
          error = NULL,
          heartbeat_at = NOW(),
          finished_at = NOW(),
          updated_at = NOW(),
          agent_id = $3
        WHERE id = $1
      `,
      [jobId, JSON.stringify(resultPayload), identity.agentId]
    );
    if (jobRow.job_type === 'delete') {
      await client.query(
        `
          DELETE FROM mdt_beta_technicians
          WHERE id = $1
        `,
        [jobRow.technician_id]
      );
    } else {
      await client.query(
        `
          UPDATE mdt_beta_technicians
          SET
            status = 'ready',
            updated_at = NOW(),
            last_error = NULL,
            last_job_id = $2,
            last_result = $3::jsonb
          WHERE id = $1
        `,
        [jobRow.technician_id, jobId, JSON.stringify(resultPayload)]
      );
    }
    await upsertMdtBetaAgentState(client, {
      agentId: identity.agentId,
      hostname: cleanString(req.body?.hostname, 128),
      deploymentShareRoot: cleanString(req.body?.deploymentShareRoot, 255),
      taskSequenceGroupName: cleanString(req.body?.taskSequenceGroupName, 128) || MDT_BETA_GROUP_NAME,
      scriptsFolder: cleanString(req.body?.scriptsFolder, 64) || MDT_BETA_SCRIPTS_FOLDER,
      status: 'idle',
      lastJobId: jobId,
      lastError: null
    });
    await client.query('COMMIT');
    return res.json({ ok: true });
  } catch (error) {
    await client.query('ROLLBACK');
    console.error('Failed to complete MDT beta job', error);
    return res.status(500).json({ ok: false, error: 'complete_failed' });
  } finally {
    client.release();
  }
});

app.post('/api/mdt-beta-agent/jobs/:id/fail', async (req, res) => {
  const identity = isAuthorizedMdtBetaAgent(req);
  if (!identity) {
    return res.status(MDT_BETA_AUTOMATION_ENABLED ? 401 : 404).json({ ok: false, error: 'unauthorized' });
  }

  const jobId = normalizeUuid(req.params.id);
  if (!jobId) {
    return res.status(400).json({ ok: false, error: 'invalid_id' });
  }

  const errorMessage = cleanString(req.body?.error || req.body?.message, 2000) || 'agent_failed';
  const resultPayload = req.body && typeof req.body.result === 'object' && !Array.isArray(req.body.result) ? req.body.result : {};
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    await setAuditContext(client, buildMdtBetaAgentAuditContext(req, identity.agentId, { source: 'POST /api/mdt-beta-agent/jobs/fail' }));
    const jobResult = await client.query(
      `
        SELECT id, technician_id
        FROM mdt_beta_jobs
        WHERE id = $1 AND status = 'running' AND (agent_id = $2 OR agent_id IS NULL)
        LIMIT 1
        FOR UPDATE
      `,
      [jobId, identity.agentId]
    );
    const jobRow = jobResult.rows && jobResult.rows[0] ? jobResult.rows[0] : null;
    if (!jobRow) {
      await client.query('ROLLBACK');
      return res.status(404).json({ ok: false, error: 'job_not_found' });
    }
    await client.query(
      `
        UPDATE mdt_beta_jobs
        SET
          status = 'failed',
          result = $2::jsonb,
          error = $3,
          heartbeat_at = NOW(),
          finished_at = NOW(),
          updated_at = NOW(),
          agent_id = $4
        WHERE id = $1
      `,
      [jobId, JSON.stringify(resultPayload), errorMessage, identity.agentId]
    );
    await client.query(
      `
        UPDATE mdt_beta_technicians
        SET
          status = 'failed',
          updated_at = NOW(),
          last_error = $2,
          last_job_id = $3,
          last_result = $4::jsonb
        WHERE id = $1
      `,
      [jobRow.technician_id, errorMessage, jobId, JSON.stringify(resultPayload)]
    );
    await upsertMdtBetaAgentState(client, {
      agentId: identity.agentId,
      hostname: cleanString(req.body?.hostname, 128),
      deploymentShareRoot: cleanString(req.body?.deploymentShareRoot, 255),
      taskSequenceGroupName: cleanString(req.body?.taskSequenceGroupName, 128) || MDT_BETA_GROUP_NAME,
      scriptsFolder: cleanString(req.body?.scriptsFolder, 64) || MDT_BETA_SCRIPTS_FOLDER,
      status: 'idle',
      lastJobId: jobId,
      lastError: errorMessage
    });
    await client.query('COMMIT');
    return res.json({ ok: true });
  } catch (error) {
    await client.query('ROLLBACK');
    console.error('Failed to fail MDT beta job', error);
    return res.status(500).json({ ok: false, error: 'fail_failed' });
  } finally {
    client.release();
  }
});

app.get('/api/admin/ldap', requireAuth, requireAdmin, async (req, res) => {
  return res.status(410).json({ ok: false, error: 'ldap_removed' });
});

app.put('/api/admin/ldap', requireAuth, requireAdmin, async (req, res) => {
  return res.status(410).json({ ok: false, error: 'ldap_removed' });
});

app.get('/api/admin/weekly-recap', requireAuth, requireAdmin, async (req, res) => {
  try {
    const recap = await buildWeeklyRecapAdminPayload(new Date());
    return res.json({ ok: true, recap });
  } catch (error) {
    console.error('Failed to build weekly recap admin payload', error);
    return res.status(500).json({ ok: false, error: 'db_error' });
  }
});

app.post('/api/admin/weekly-recap/send', requireAuth, requireAdmin, async (req, res) => {
  try {
    const result = await deliverWeeklyRecap({
      triggerSource: 'manual',
      createdBy:
        cleanString(
          req.session?.user?.displayName ||
            req.session?.user?.username ||
            req.session?.user?.mail ||
            'systeme',
          128
        ) || 'systeme',
      auditContext: buildAuditContext(req, {
        source: 'POST /api/admin/weekly-recap/send'
      }),
      now: new Date()
    });
    if (!result.ok && result.error === 'missing_recipients') {
      return res.status(400).json({ ok: false, error: 'missing_recipients' });
    }
    if (!result.ok && result.error === 'missing_credentials') {
      return res.status(400).json({ ok: false, error: 'missing_credentials' });
    }
    return res.json({
      ok: result.ok,
      status: result.status,
      recipients: result.recipients,
      failedRecipients: result.failedRecipients,
      summary: result.summary,
      subject: result.subject,
      error: result.error || null
    });
  } catch (error) {
    console.error('Failed to send weekly recap manually', error);
    return res.status(500).json({ ok: false, error: 'send_failed' });
  }
});

app.post(
  '/api/ingest/artifacts',
  ingestLimiter,
  express.raw({ limit: ARTIFACT_UPLOAD_LIMIT, type: () => true }),
  async (req, res) => {
    const buffer = Buffer.isBuffer(req.body) ? req.body : null;
    if (!buffer || !buffer.length) {
      return res.status(400).json({ ok: false, error: 'empty_body' });
    }

    const reportId = normalizeUuid(req.query.reportId || req.get('x-report-id')) || generateUuid();
    const clientRunId =
      normalizeUuid(req.query.clientRunId || req.get('x-client-run-id')) || reportId;
    const macSerialKey =
      cleanString(req.query.macSerialKey || req.get('x-mac-serial-key'), 256) || 'unknown';
    const archiveName = normalizeArtifactArchiveName(
      req.query.archiveName || req.get('x-archive-name') || 'run-artifacts.zip'
    );
    const prefix = normalizeObjectStoragePrefix(
      req.query.prefix || req.get('x-object-prefix') || OBJECT_STORAGE_PREFIX || 'run'
    );
    const tagSegment = normalizeObjectStorageSegment(
      cleanString(req.query.tag || req.get('x-report-tag'), 64)
    );
    const rootPrefix = tagSegment ? `${prefix}/${tagSegment}` : prefix;
    const safeKey = normalizeObjectStorageSegment(macSerialKey) || 'unknown';

    try {
      const stored = await storeRelayedArtifactArchive({
        buffer,
        rootPrefix,
        safeKey,
        clientRunId,
        archiveName
      });
      if (!stored || !stored.ok) {
        return res.status(500).json({
          ok: false,
          error: stored && stored.error ? stored.error : 'artifact_store_failed'
        });
      }
      return res.status(200).json({
        ok: true,
        reportId,
        clientRunId,
        storage: stored.storage,
        destination: stored.destination,
        archiveName: stored.archiveName,
        warning: stored.warning || null,
        sizeBytes: buffer.length
      });
    } catch (error) {
      console.error('Failed to relay artifact archive', error);
      return res.status(500).json({ ok: false, error: 'artifact_store_failed' });
    }
  }
);

app.post('/api/ingest', ingestLimiter, async (req, res) => {
  const body = req.body;
  if (!body || typeof body !== 'object') {
    return res.status(400).json({ ok: false, error: 'invalid_payload' });
  }

  applyAuthoritativeDiskTests(body);

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
  const technician = normalizeReportTechnician(
    pickFirst(body, ['technician', 'technicianName', 'tech', 'techName', 'operator']),
    { fallback: DEFAULT_FALLBACK_TECHNICIAN }
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

  const clientGeneratedAt = extractClientGeneratedAt(body, [bodyComponents, bodyHardware, bodyWifi, bodyNetwork]);
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
    const resolvedPallet = await resolvePalletForSerial(client, {
      serialNumber,
      machineKey,
      actor: technician || machineKey
    });
    const resolvedPalletId = resolvedPallet && resolvedPallet.id ? resolvedPallet.id : null;
    const resolvedPalletStatus =
      resolvedPallet && resolvedPallet.status ? resolvedPallet.status : null;
    const resolvedShipment = normalizeShipmentFromRow(resolvedPallet);
    const previousClockResult = machineKey
      ? await client.query(
        `
          SELECT client_generated_at, last_seen
          FROM reports
          WHERE machine_key = $1
            AND id <> $2
            AND client_generated_at IS NOT NULL
          ORDER BY last_seen DESC, id DESC
          LIMIT 1
        `,
        [machineKey, reportId]
      )
      : { rows: [] };
    const previousClockRow =
      previousClockResult.rows && previousClockResult.rows[0] ? previousClockResult.rows[0] : null;
    const existingMachineResult = machineKey
      ? await client.query(
        `
          SELECT first_client_generated_at
          FROM machines
          WHERE machine_key = $1
          LIMIT 1
        `,
        [machineKey]
      )
      : { rows: [] };
    const existingMachineRow =
      existingMachineResult.rows && existingMachineResult.rows[0] ? existingMachineResult.rows[0] : null;
    const clockAssessment = buildClockAlertAssessment({
      serverSeenAt: now,
      clientGeneratedAt,
      previousServerSeenAt: previousClockRow ? previousClockRow.last_seen : null,
      previousClientGeneratedAt: previousClockRow ? previousClockRow.client_generated_at : null
    });
    const firstClientGeneratedAt =
      normalizeClientGeneratedAt(existingMachineRow ? existingMachineRow.first_client_generated_at : null) ||
      clockAssessment.clientGeneratedAt ||
      null;
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
      resolvedPalletId,
      resolvedPalletStatus,
      resolvedShipment ? resolvedShipment.date : null,
      resolvedShipment ? resolvedShipment.client : null,
      resolvedShipment ? resolvedShipment.orderNumber : null,
      resolvedShipment ? resolvedShipment.palletCode : null,
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
      clockAssessment.clientGeneratedAt,
      now,
      now,
      clockAssessment.driftSeconds,
      clockAssessment.deltaSeconds,
      clockAssessment.active,
      clockAssessment.code,
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
      resolvedPalletId,
      resolvedPalletStatus,
      resolvedShipment ? resolvedShipment.date : null,
      resolvedShipment ? resolvedShipment.client : null,
      resolvedShipment ? resolvedShipment.orderNumber : null,
      resolvedShipment ? resolvedShipment.palletCode : null,
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
      clockAssessment.clientGeneratedAt,
      firstClientGeneratedAt,
      now,
      now,
      clockAssessment.driftSeconds,
      clockAssessment.deltaSeconds,
      clockAssessment.active,
      clockAssessment.code,
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
      pallet: resolvedPallet,
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
  const useLatest = shouldUseLatest(req.query);
  const { clauses, values } = buildReportFilters(req.query, {
    includeCategory: true,
    activeTagId,
    forcedTechKeys: getForcedReportTechKeys(req.session?.user),
    tableAlias: useLatest ? 'latest' : 'reports'
  });
  const where = clauses.length ? `WHERE ${clauses.join(' AND ')}` : '';

  try {
    let total = null;
    let statusCounts = null;
    if (includeTotal) {
      const totalsResult = useLatest
        ? await pool.query(
          `
            WITH latest AS (
              SELECT DISTINCT ON (reports.machine_key)
                reports.machine_key,
                reports.hostname,
                reports.serial_number,
                reports.mac_address,
                reports.mac_addresses,
                reports.category,
                reports.tag,
                reports.tag_id,
                reports.technician,
                reports.vendor,
                reports.model,
                reports.shipment_date,
                reports.shipment_client,
                reports.shipment_order_number,
                reports.shipment_pallet_code,
                reports.components,
                reports.comment,
                reports.battery_health,
                reports.camera_status,
                reports.usb_status,
                reports.keyboard_status,
                reports.pad_status,
                reports.badge_reader_status,
                reports.bios_clock_alert,
                reports.payload,
                reports.last_seen,
                reports.id
              FROM reports
              ORDER BY reports.machine_key, reports.last_seen DESC, reports.id DESC
            )
            SELECT
              latest.components,
              latest.comment,
              latest.battery_health,
              latest.camera_status,
              latest.usb_status,
              latest.keyboard_status,
              latest.pad_status,
              latest.badge_reader_status
            FROM latest
            ${where}
          `,
          values
        )
        : await pool.query(
          `
            SELECT
              reports.components,
              reports.comment,
              reports.battery_health,
              reports.camera_status,
              reports.usb_status,
              reports.keyboard_status,
              reports.pad_status,
              reports.badge_reader_status
            FROM reports
            ${where}
          `,
          values
        );
      total = totalsResult.rows.length;
      statusCounts = buildDashboardPrimaryStatusCounts(totalsResult.rows);
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
              reports.pallet_id,
              reports.pallet_status,
              reports.shipment_date,
              reports.shipment_client,
              reports.shipment_order_number,
              reports.shipment_pallet_code,
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
              reports.client_generated_at,
              reports.last_seen,
              reports.clock_drift_seconds,
              reports.clock_delta_seconds,
              reports.bios_clock_alert,
              reports.bios_clock_alert_code,
              reports.last_ip,
              reports.components,
              reports.comment,
              reports.payload
            FROM reports
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
            latest.pallet_id,
            latest.pallet_status,
            latest.shipment_date,
            latest.shipment_client,
            latest.shipment_order_number,
            latest.shipment_pallet_code,
            COALESCE(tags.name, latest.tag) AS tag_name,
            lots.supplier AS lot_supplier,
            lots.lot_number AS lot_number,
            lots.target_count AS lot_target_count,
            lots.produced_count AS lot_produced_count,
            lots.is_paused AS lot_is_paused,
            pallets.code AS pallet_code,
            pallets.last_movement_at AS pallet_last_movement_at,
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
            latest.client_generated_at,
            latest.last_seen,
            latest.clock_drift_seconds,
            latest.clock_delta_seconds,
            latest.bios_clock_alert,
            latest.bios_clock_alert_code,
            latest.last_ip,
            latest.components,
            latest.comment
          FROM latest
          LEFT JOIN tags ON tags.id = latest.tag_id
          LEFT JOIN lots ON lots.id = latest.lot_id
          LEFT JOIN pallets ON pallets.id = latest.pallet_id
          ${where}
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
            reports.pallet_id,
            reports.pallet_status,
            reports.shipment_date,
            reports.shipment_client,
            reports.shipment_order_number,
            reports.shipment_pallet_code,
            COALESCE(tags.name, reports.tag) AS tag_name,
            lots.supplier AS lot_supplier,
            lots.lot_number AS lot_number,
            lots.target_count AS lot_target_count,
            lots.produced_count AS lot_produced_count,
            lots.is_paused AS lot_is_paused,
            pallets.code AS pallet_code,
            pallets.last_movement_at AS pallet_last_movement_at,
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
            reports.client_generated_at,
            reports.last_seen,
            reports.clock_drift_seconds,
            reports.clock_delta_seconds,
            reports.bios_clock_alert,
            reports.bios_clock_alert_code,
            reports.last_ip,
            reports.components,
            reports.comment
          FROM reports
          LEFT JOIN tags ON tags.id = reports.tag_id
          LEFT JOIN lots ON lots.id = reports.lot_id
          LEFT JOIN pallets ON pallets.id = reports.pallet_id
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
        pallet: normalizePalletFromRow(row),
        shipment: normalizeShipmentFromRow(row),
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
        clockAlert: normalizeClockAlertFromRow(row),
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
      total,
      statusCounts
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
  const forcedTechKeys = getForcedReportTechKeys(req.session?.user);
  const useLatest = shouldUseLatest(req.query);
  const { clauses, values } = buildReportFilters(req.query, {
    includeCategory: false,
    activeTagId,
    forcedTechKeys,
    tableAlias: useLatest ? 'latest' : 'reports'
  });
  const queryWithoutTech = { ...req.query };
  delete queryWithoutTech.tech;
  const { clauses: techClauses, values: techValues } = buildReportFilters(queryWithoutTech, {
    includeCategory: false,
    activeTagId,
    forcedTechKeys,
    tableAlias: useLatest ? 'latest' : 'reports'
  });
  const where = clauses.length ? `WHERE ${clauses.join(' AND ')}` : '';
  const techWhere = techClauses.length ? `WHERE ${techClauses.join(' AND ')}` : '';

  try {
    const result = useLatest
      ? await pool.query(
        `
          WITH latest AS (
            SELECT DISTINCT ON (reports.machine_key)
              reports.machine_key,
              reports.category,
              reports.technician,
              reports.hostname,
              reports.serial_number,
              reports.mac_address,
              reports.mac_addresses,
              reports.tag,
              reports.tag_id,
              reports.vendor,
              reports.model,
              reports.comment,
              reports.shipment_date,
              reports.shipment_client,
              reports.shipment_order_number,
              reports.shipment_pallet_code,
              reports.battery_health,
              reports.bios_clock_alert,
              reports.components,
              reports.payload,
              reports.last_seen
            FROM reports
            ORDER BY reports.machine_key, reports.last_seen DESC, reports.id DESC
          )
          SELECT
            COUNT(*) AS total,
            COUNT(*) FILTER (WHERE category = 'laptop') AS laptop,
            COUNT(*) FILTER (WHERE category = 'desktop') AS desktop,
            COUNT(*) FILTER (WHERE category = 'server') AS server,
            COUNT(*) FILTER (WHERE category = 'unknown') AS unknown
          FROM latest
          ${where}
        `,
        values
      )
      : await pool.query(
        `
          SELECT
            COUNT(*) AS total,
            COUNT(*) FILTER (WHERE category = 'laptop') AS laptop,
            COUNT(*) FILTER (WHERE category = 'desktop') AS desktop,
            COUNT(*) FILTER (WHERE category = 'server') AS server,
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
              reports.category,
              reports.technician,
              reports.hostname,
              reports.serial_number,
              reports.mac_address,
              reports.mac_addresses,
              reports.tag,
              reports.tag_id,
              reports.vendor,
              reports.model,
              reports.comment,
              reports.shipment_date,
              reports.shipment_client,
              reports.shipment_order_number,
              reports.shipment_pallet_code,
              reports.battery_health,
              reports.bios_clock_alert,
              reports.components,
              reports.payload,
              reports.last_seen
            FROM reports
            ORDER BY reports.machine_key, reports.last_seen DESC, reports.id DESC
          )
          SELECT DISTINCT technician
          FROM latest
          ${techWhere ? `${techWhere} AND technician IS NOT NULL AND technician <> ''` : `WHERE technician IS NOT NULL AND technician <> ''`}
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
      server: Number.parseInt(row?.server || '0', 10),
      unknown: Number.parseInt(row?.unknown || '0', 10),
      techs
    });
  } catch (error) {
    console.error('Failed to fetch stats', error);
    return res.status(500).json({ ok: false, error: 'db_error' });
  }
});

app.get('/api/stats/timeline', requireAuth, async (req, res) => {
  const activeTagId = Boolean(req.query.tags || req.query.tagIds)
    ? await (async () => {
      try {
        const activeTag = await getActiveTag(pool);
        return activeTag ? activeTag.id : null;
      } catch (error) {
        return null;
      }
    })()
    : null;
  const forcedTechKeys = getForcedReportTechKeys(req.session?.user);
  const granularity = normalizeTimelineGranularity(req.query.granularity);
  const explicitRange = getResolvedDateRange(req.query);
  if (!explicitRange) {
    return res.json({ ok: true, granularity, buckets: [] });
  }
  const { clauses, values } = buildReportFilters(req.query, {
    includeCategory: true,
    activeTagId,
    forcedTechKeys,
    tableAlias: 'reports'
  });
  const where = clauses.length ? `WHERE ${clauses.join(' AND ')}` : '';
  const timezoneParamIndex = values.length + 1;
  const bucketExpr =
    granularity === 'month'
      ? `date_trunc('month', timezone($${timezoneParamIndex}, reports.last_seen))`
      : granularity === 'week'
        ? `date_trunc('week', timezone($${timezoneParamIndex}, reports.last_seen))`
        : `date_trunc('day', timezone($${timezoneParamIndex}, reports.last_seen))`;
  const limit = granularity === 'month' ? 36 : granularity === 'week' ? 104 : 120;
  const limitParamIndex = values.length + 2;

  try {
    const result = await pool.query(
      `
        SELECT
          to_char(${bucketExpr}, 'YYYY-MM-DD"T"HH24:MI:SS') AS bucket_start,
          COUNT(DISTINCT reports.machine_key) AS machine_count,
          COUNT(*) AS report_count
        FROM reports
        ${where}
        GROUP BY bucket_start
        ORDER BY bucket_start DESC
        LIMIT $${limitParamIndex}
      `,
      [...values, APP_TIMEZONE, limit]
    );
    return res.json({
      ok: true,
      granularity,
      buckets: (result.rows || []).map((row) => ({
        bucketStart: row.bucket_start,
        machineCount: Number.parseInt(row.machine_count || '0', 10) || 0,
        reportCount: Number.parseInt(row.report_count || '0', 10) || 0
      }))
    });
  } catch (error) {
    console.error('Failed to fetch timeline stats', error);
    return res.status(500).json({ ok: false, error: 'db_error' });
  }
});

app.get('/api/machines', requireAuth, async (req, res) => {
  const metaOnly = req.query.meta === '1';
  const legacyFlag = req.query.legacy;
  let permissions = null;
  let operatorScope = null;
  try {
    const user = await refreshLdapPermissions(req);
    permissions = getUserPermissions(user);
    operatorScope = getUserTechnicianScope(user);
  } catch (error) {
    permissions = null;
    operatorScope = getUserTechnicianScope(req.session?.user);
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
        operatorScope,
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
        pallet: normalizePalletFromRow(row),
        shipment: normalizeShipmentFromRow(row),
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
        clockAlert: normalizeClockAlertFromRow(row),
        lastSeen: row.last_seen,
        lastIp: row.last_ip,
        comment: row.comment,
        components
      };
    });

    res.json({
      machines,
      permissions,
      operatorScope,
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

app.get('/api/lots', requireLogistics, async (req, res) => {
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

app.get('/api/pallets', requireTagEdit, async (req, res) => {
  try {
    const palletRows = await listPalletsWithStats(pool);
    const importRows = await listRecentPalletImports(pool);
    return res.json({
      ok: true,
      pallets: palletRows.map((row) => mapPalletRowForResponse(row)).filter(Boolean),
      recentImports: importRows.map((row) => mapPalletImportRowForResponse(row)).filter(Boolean)
    });
  } catch (error) {
    console.error('Failed to list pallets', error);
    return res.status(500).json({ ok: false, error: 'db_error' });
  }
});

app.post('/api/pallets/imports', requireTagEdit, async (req, res) => {
  const body = req.body && typeof req.body === 'object' ? req.body : {};
  const importType = normalizePalletMovementType(body.importType || body.type || body.mode);
  const csvText = typeof body.csvText === 'string' ? body.csvText : '';
  const fileName =
    cleanString(body.fileName || body.filename || body.name, 255) ||
    `${importType || 'pallets'}-${Date.now()}.csv`;
  if (!importType || !csvText.trim()) {
    return res.status(400).json({ ok: false, error: 'invalid_payload' });
  }

  const parsed = extractPalletCsvRows(csvText);
  if (!parsed.ok || !parsed.rows.length) {
    return res.status(400).json({
      ok: false,
      error: parsed.error || 'invalid_csv',
      errors: Array.isArray(parsed.errors) ? parsed.errors.slice(0, 20) : []
    });
  }

  const createdBy =
    cleanString(
      req.session?.user?.displayName ||
        req.session?.user?.username ||
        req.session?.user?.dn ||
        req.session?.user?.mail ||
        'systeme',
      128
    ) || 'systeme';
  const totalRows = parsed.rows.length + parsed.errors.length;
  const importId = generateUuid();
  const now = new Date().toISOString();

  let client;
  try {
    client = await pool.connect();
    await client.query('BEGIN');
    await setAuditContext(client, buildAuditContext(req, { actor: createdBy }));
    await client.query(
      `
        INSERT INTO pallet_imports (
          id,
          import_type,
          file_name,
          row_count,
          applied_count,
          skipped_count,
          created_by,
          created_at,
          summary
        ) VALUES ($1, $2, $3, 0, 0, 0, $4, $5, NULL)
      `,
      [importId, importType, fileName, createdBy, now]
    );

    const machineLookup = await client.query(
      `
        SELECT DISTINCT ON (serial_number)
          serial_number,
          machine_key
        FROM machines
        WHERE serial_number = ANY($1::text[])
        ORDER BY serial_number, last_seen DESC
      `,
      [parsed.rows.map((item) => item.serialNumber)]
    );
    const machineKeyBySerial = new Map();
    (machineLookup.rows || []).forEach((row) => {
      const serial = normalizeSerial(row.serial_number);
      if (serial && row.machine_key && !machineKeyBySerial.has(serial)) {
        machineKeyBySerial.set(serial, row.machine_key);
      }
    });

    const palletCache = new Map();
    let appliedCount = 0;
    for (const row of parsed.rows) {
      const cacheKey = normalizePalletCodeKey(row.palletCode);
      if (!cacheKey) {
        continue;
      }
      let pallet = palletCache.get(cacheKey);
      if (!pallet) {
        pallet = await upsertPallet(client, {
          code: row.palletCode,
          movementType: importType,
          actor: createdBy,
          timestamp: now
        });
        if (!pallet) {
          continue;
        }
        palletCache.set(cacheKey, pallet);
      }
      await applyPalletAssignment(client, {
        serialNumber: row.serialNumber,
        pallet,
        movementType: importType,
        importId,
        machineKey: machineKeyBySerial.get(row.serialNumber) || null,
        shipmentDate: row.shipmentDate || null,
        shipmentClient: row.shipmentClient || null,
        shipmentOrderNumber: row.shipmentOrderNumber || null,
        shipmentPalletCode: row.shipmentPalletCode || row.palletCode || null,
        actor: createdBy,
        timestamp: now
      });
      appliedCount += 1;
    }

    const summary = {
      importType,
      importTypeLabel: getPalletMovementLabel(importType),
      uniquePalletCount: palletCache.size,
      errorCount: parsed.errors.length,
      errors: parsed.errors.slice(0, 20)
    };
    await client.query(
      `
        UPDATE pallet_imports
        SET row_count = $2,
            applied_count = $3,
            skipped_count = $4,
            summary = $5
        WHERE id = $1
      `,
      [
        importId,
        totalRows,
        appliedCount,
        parsed.errors.length,
        JSON.stringify(summary)
      ]
    );

    const palletRows = await listPalletsWithStats(client);
    const importRows = await listRecentPalletImports(client);

    await client.query('COMMIT');
    return res.json({
      ok: true,
      import: {
        id: importId,
        importType,
        importTypeLabel: getPalletMovementLabel(importType),
        fileName,
        rowCount: totalRows,
        appliedCount,
        skippedCount: parsed.errors.length,
        createdBy,
        createdAt: now,
        summary
      },
      pallets: palletRows.map((row) => mapPalletRowForResponse(row)).filter(Boolean),
      recentImports: importRows.map((row) => mapPalletImportRowForResponse(row)).filter(Boolean),
      warnings: parsed.errors.slice(0, 20)
    });
  } catch (error) {
    if (client) {
      try {
        await client.query('ROLLBACK');
      } catch (rollbackError) {
        console.error('Failed to rollback pallet import', rollbackError);
      }
    }
    console.error('Failed to import pallets CSV', error);
    return res.status(500).json({ ok: false, error: 'db_error' });
  } finally {
    if (client) {
      client.release();
    }
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
  if (!canUserAccessReportTechnician(req.session?.user, row.technician)) {
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
  components = applyServerTelemetryToComponents(components, payload, row.category);
  const autopilotHash = buildAutopilotHash(payload);

  const relatedSerial = normalizeSerial(row.serial_number);
  let relatedMac = normalizeMac(row.mac_address);
  if (!relatedMac) {
    const macList = normalizeMacList(row.mac_addresses);
    if (Array.isArray(macList) && macList.length) {
      relatedMac = macList[0];
    }
  }
  const mapRelatedReportRow = (item) => ({
    id: item.id,
    hostname: cleanString(item.hostname, 128),
    technician: cleanString(item.technician, 128),
    lastSeen: item.last_seen,
    createdAt: item.created_at,
    clientGeneratedAt: normalizeClientGeneratedAt(item.client_generated_at),
    diagCompletedAt: normalizeClientGeneratedAt(item.diag_completed_at),
    diagType: cleanString(item.diag_type, 64),
    appVersion: cleanString(item.diag_app_version, 32),
    batteryHealth: normalizeNullableInteger(item.battery_health),
    batteryDesignWh: normalizeNullableNumber(item.battery_design_wh),
    batteryFullWh: normalizeNullableNumber(item.battery_full_wh),
    batteryRemainingWh: normalizeNullableNumber(item.battery_remaining_wh),
    batteryChargePercent: normalizeNullableInteger(item.battery_charge_percent),
    batteryPowerSource: cleanString(item.battery_power_source, 32),
    clockAlert: normalizeClockAlertFromRow(item)
  });
  if (relatedSerial && relatedMac) {
    try {
      const reportResult = await pool.query(
        `
          SELECT
            id,
            hostname,
            technician,
            battery_health,
            client_generated_at,
            last_seen,
            created_at,
            bios_clock_alert,
            bios_clock_alert_code,
            payload::jsonb #>> '{device,batteryCapacity,designCapacityWh}' AS battery_design_wh,
            payload::jsonb #>> '{device,batteryCapacity,fullChargeCapacityWh}' AS battery_full_wh,
            payload::jsonb #>> '{device,batteryCapacity,remainingCapacityWh}' AS battery_remaining_wh,
            payload::jsonb #>> '{device,batteryCapacity,chargePercent}' AS battery_charge_percent,
            payload::jsonb #>> '{device,batteryCapacity,powerSource}' AS battery_power_source,
            payload::jsonb #>> '{diag,completedAt}' AS diag_completed_at,
            payload::jsonb #>> '{diag,type}' AS diag_type,
            payload::jsonb #>> '{diag,appVersion}' AS diag_app_version
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
      relatedReports = reportResult.rows.map(mapRelatedReportRow);
    } catch (error) {
      relatedReports = [];
    }
  } else if (row.machine_key) {
    try {
      const reportResult = await pool.query(
        `
          SELECT
            id,
            hostname,
            technician,
            battery_health,
            client_generated_at,
            last_seen,
            created_at,
            bios_clock_alert,
            bios_clock_alert_code,
            payload::jsonb #>> '{device,batteryCapacity,designCapacityWh}' AS battery_design_wh,
            payload::jsonb #>> '{device,batteryCapacity,fullChargeCapacityWh}' AS battery_full_wh,
            payload::jsonb #>> '{device,batteryCapacity,remainingCapacityWh}' AS battery_remaining_wh,
            payload::jsonb #>> '{device,batteryCapacity,chargePercent}' AS battery_charge_percent,
            payload::jsonb #>> '{device,batteryCapacity,powerSource}' AS battery_power_source,
            payload::jsonb #>> '{diag,completedAt}' AS diag_completed_at,
            payload::jsonb #>> '{diag,type}' AS diag_type,
            payload::jsonb #>> '{diag,appVersion}' AS diag_app_version
          FROM reports
          WHERE machine_key = $1
          ORDER BY last_seen DESC
          LIMIT 10
        `,
        [row.machine_key]
      );
      relatedReports = reportResult.rows.map(mapRelatedReportRow);
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

  const pallet = normalizePalletFromRow({
    report_pallet_id: row.report_pallet_id,
    report_pallet_code: row.report_pallet_code,
    report_pallet_status: row.report_pallet_status,
    report_pallet_last_movement_at: row.report_pallet_last_movement_at,
    machine_pallet_id: row.machine_pallet_id,
    machine_pallet_code: row.machine_pallet_code,
    machine_pallet_status: row.machine_pallet_status,
    machine_pallet_last_movement_at: row.machine_pallet_last_movement_at
  });
  const shipment = normalizeShipmentFromRow({
    report_shipment_date: row.report_shipment_date,
    report_shipment_client: row.report_shipment_client,
    report_shipment_order_number: row.report_shipment_order_number,
    report_shipment_pallet_code: row.report_shipment_pallet_code,
    report_pallet_code: row.report_pallet_code,
    machine_shipment_date: row.machine_shipment_date,
    machine_shipment_client: row.machine_shipment_client,
    machine_shipment_order_number: row.machine_shipment_order_number,
    machine_shipment_pallet_code: row.machine_shipment_pallet_code,
    machine_pallet_code: row.machine_pallet_code
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
      pallet,
      shipment,
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
      clockAlert: normalizeClockAlertFromRow(row),
      lastSeen: row.machine_last_seen || row.report_last_seen,
      createdAt: row.machine_created_at || row.report_created_at,
      reportLastSeen: row.report_last_seen,
      reportCreatedAt: row.report_created_at,
      lastIp: row.last_ip,
      comment: row.comment,
      commentedAt: row.commented_at,
      components,
      autopilotHash,
      payload,
      relatedReports
    }
  });
});

app.put('/api/machines/:id/battery-health', requireBatteryHealthEdit, async (req, res) => {
  const id = normalizeUuid(req.params.id);
  if (!id) {
    return res.status(400).json({ ok: false, error: 'invalid_id' });
  }

  const batteryHealth = normalizeBatteryHealth(req.body?.batteryHealth);
  if (batteryHealth == null) {
    return res.status(400).json({ ok: false, error: 'invalid_battery_health' });
  }

  let client;
  try {
    client = await pool.connect();
    await client.query('BEGIN');
    await setAuditContext(client, buildAuditContext(req));
    const row = await getScopedReportRowById(client, id, req.session?.user, {
      columns: 'id, machine_key, technician',
      forUpdate: true
    });
    if (!row) {
      await client.query('ROLLBACK');
      return res.status(404).json({ ok: false, error: 'not_found' });
    }

    await client.query('UPDATE reports SET battery_health = $1 WHERE id = $2', [batteryHealth, id]);
    if (row.machine_key) {
      await client.query('UPDATE machines SET battery_health = $1 WHERE machine_key = $2', [
        batteryHealth,
        row.machine_key
      ]);
    }

    await client.query('COMMIT');
    return res.json({ ok: true, batteryHealth });
  } catch (error) {
    if (client) {
      try {
        await client.query('ROLLBACK');
      } catch (rollbackError) {
        console.error('Failed to rollback battery health update', rollbackError);
      }
    }
    console.error('Failed to update battery health', error);
    return res.status(500).json({ ok: false, error: 'db_error' });
  } finally {
    if (client) {
      client.release();
    }
  }
});

app.put('/api/machines/:id/technician', requireTechnicianEdit, async (req, res) => {
  const id = normalizeUuid(req.params.id);
  if (!id) {
    return res.status(400).json({ ok: false, error: 'invalid_id' });
  }

  const technician = normalizeReportTechnician(req.body?.technician, {
    fallback: DEFAULT_FALLBACK_TECHNICIAN
  });
  if (!technician) {
    return res.status(400).json({ ok: false, error: 'invalid_technician' });
  }

  let client;
  try {
    client = await pool.connect();
    await client.query('BEGIN');
    await setAuditContext(client, buildAuditContext(req));
    const row = await getScopedReportRowById(client, id, req.session?.user, {
      columns: 'id, machine_key, technician',
      forUpdate: true
    });
    if (!row) {
      await client.query('ROLLBACK');
      return res.status(404).json({ ok: false, error: 'not_found' });
    }

    if (row.machine_key) {
      await client.query('UPDATE reports SET technician = $1 WHERE machine_key = $2', [
        technician,
        row.machine_key
      ]);
      await client.query('UPDATE machines SET technician = $1 WHERE machine_key = $2', [
        technician,
        row.machine_key
      ]);
      await client.query('UPDATE lot_progress SET technician = $1 WHERE machine_key = $2', [
        technician,
        row.machine_key
      ]);
    } else {
      await client.query('UPDATE reports SET technician = $1 WHERE id = $2', [technician, id]);
    }

    await client.query('COMMIT');
    return res.json({ ok: true, technician });
  } catch (error) {
    if (client) {
      try {
        await client.query('ROLLBACK');
      } catch (rollbackError) {
        console.error('Failed to rollback technician update', rollbackError);
      }
    }
    console.error('Failed to update technician', error);
    return res.status(500).json({ ok: false, error: 'db_error' });
  } finally {
    if (client) {
      client.release();
    }
  }
});

app.put('/api/machines/:id/pad', requireOperator, async (req, res) => {
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
    const row = await getScopedReportRowById(client, id, req.session?.user, {
      columns: 'id, components, machine_key, technician',
      forUpdate: true
    });
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

app.put('/api/machines/:id/usb', requireOperator, async (req, res) => {
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
    const row = await getScopedReportRowById(client, id, req.session?.user, {
      columns: 'id, components, machine_key, technician',
      forUpdate: true
    });
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

app.put('/api/reports/:id/component', requireOperator, async (req, res) => {
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
    const row = await getScopedReportRowById(client, id, req.session?.user, {
      columns: 'id, components, machine_key, technician',
      forUpdate: true
    });
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

app.put('/api/reports/:id/category', requireOperator, async (req, res) => {
  const id = normalizeUuid(req.params.id);
  if (!id) {
    return res.status(400).json({ ok: false, error: 'invalid_id' });
  }

  const rawValue = typeof req.body?.category === 'string' ? req.body.category.trim() : '';
  const category = normalizeCategory(rawValue);
  if (!['unknown', 'laptop', 'desktop', 'server'].includes(category)) {
    return res.status(400).json({ ok: false, error: 'invalid_category' });
  }

  let client;
  try {
    client = await pool.connect();
    await client.query('BEGIN');
    await setAuditContext(client, buildAuditContext(req));
    const row = await getScopedReportRowById(client, id, req.session?.user, {
      columns: 'id, machine_key, technician',
      forUpdate: true
    });
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

app.put('/api/machines/:id/lot', requireTagEdit, async (req, res) => {
  const id = normalizeUuid(req.params.id);
  if (!id) {
    return res.status(400).json({ ok: false, error: 'invalid_id' });
  }

  const body = req.body && typeof req.body === 'object' ? req.body : {};
  const lotKeys = ['lotId', 'lot_id', 'batchId', 'batch_id'];
  const rawLotValue = lotKeys.reduce((value, key) => {
    if (value !== undefined) {
      return value;
    }
    return Object.prototype.hasOwnProperty.call(body, key) ? body[key] : undefined;
  }, undefined);
  const lotValueText = rawLotValue == null ? '' : String(rawLotValue).trim();
  const nextLotId = lotValueText ? normalizeUuid(lotValueText) : null;
  if (lotValueText && !nextLotId) {
    return res.status(400).json({ ok: false, error: 'invalid_lot_id' });
  }

  let client;
  try {
    client = await pool.connect();
    await client.query('BEGIN');
    await setAuditContext(client, buildAuditContext(req));

    const reportResult = await client.query(
      `
        SELECT id, machine_key, technician, lot_id
        FROM reports
        WHERE id = $1
        FOR UPDATE
      `,
      [id]
    );
    const reportRow = reportResult.rows && reportResult.rows[0] ? reportResult.rows[0] : null;
    if (!reportRow) {
      await client.query('ROLLBACK');
      return res.status(404).json({ ok: false, error: 'not_found' });
    }

    if (reportRow.machine_key) {
      await client.query(
        `
          SELECT machine_key, lot_id
          FROM machines
          WHERE machine_key = $1
          FOR UPDATE
        `,
        [reportRow.machine_key]
      );
    }

    const nextLot = nextLotId ? await getLotById(client, nextLotId, { forUpdate: true }) : null;
    if (nextLotId && !nextLot) {
      await client.query('ROLLBACK');
      return res.status(404).json({ ok: false, error: 'lot_not_found' });
    }

    if (reportRow.machine_key) {
      await client.query('UPDATE reports SET lot_id = $1 WHERE machine_key = $2', [
        nextLotId,
        reportRow.machine_key
      ]);
      await client.query('UPDATE machines SET lot_id = $1 WHERE machine_key = $2', [
        nextLotId,
        reportRow.machine_key
      ]);
    } else {
      await client.query('UPDATE reports SET lot_id = $1 WHERE id = $2', [nextLotId, id]);
    }

    const lotProgress = await replaceMachineLotProgress(client, {
      lot: nextLot,
      machineKey: reportRow.machine_key || null,
      reportId: id,
      technician: reportRow.technician || null,
      source: 'manual-lot-update'
    });

    const lotRows = await listLotsWithAssignments(client);
    const lots = lotRows.map((row) => mapLotRowForResponse(row)).filter(Boolean);
    const activeLot = lots.find(
      (lot) => lot && !lot.isPaused && Number.isFinite(lot.targetCount) && lot.producedCount < lot.targetCount
    ) || null;

    await client.query('COMMIT');
    return res.json({
      ok: true,
      machineKey: reportRow.machine_key || null,
      lot: normalizeLotFromRow(lotProgress && lotProgress.lot ? lotProgress.lot : nextLot),
      lotCounted: Boolean(lotProgress && lotProgress.counted),
      lots,
      activeLotId: activeLot ? activeLot.id : null
    });
  } catch (error) {
    if (client) {
      try {
        await client.query('ROLLBACK');
      } catch (rollbackError) {
        console.error('Failed to rollback lot update', rollbackError);
      }
    }
    console.error('Failed to update machine lot', error);
    return res.status(500).json({ ok: false, error: 'db_error' });
  } finally {
    if (client) {
      client.release();
    }
  }
});

app.post('/api/machines/:id/report-zero', requireOperator, async (req, res) => {
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
    const row = await getScopedReportRowById(client, id, req.session?.user, {
      columns: '*',
      forUpdate: true
    });
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
    const resolvedPallet =
      (row.serial_number
        ? await resolvePalletForSerial(client, {
          serialNumber: row.serial_number,
          machineKey: row.machine_key || null,
          actor: row.technician || row.machine_key || null
        })
        : null) || normalizePalletFromRow({ pallet_id: row.pallet_id, pallet_status: row.pallet_status });
    const resolvedPalletId = resolvedPallet && resolvedPallet.id ? resolvedPallet.id : null;
    const resolvedPalletStatus =
      resolvedPallet && resolvedPallet.status ? resolvedPallet.status : null;
    const resolvedShipment =
      normalizeShipmentFromRow(resolvedPallet) ||
      normalizeShipmentFromRow({
        shipment_date: row.shipment_date,
        shipment_client: row.shipment_client,
        shipment_order_number: row.shipment_order_number,
        shipment_pallet_code: row.shipment_pallet_code,
        pallet_code: row.pallet_code
      });
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
      resolvedPalletId,
      resolvedPalletStatus,
      resolvedShipment ? resolvedShipment.date : null,
      resolvedShipment ? resolvedShipment.client : null,
      resolvedShipment ? resolvedShipment.orderNumber : null,
      resolvedShipment ? resolvedShipment.palletCode : null,
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
      null,
      now,
      now,
      null,
      null,
      false,
      null,
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
        resolvedPalletId,
        resolvedPalletStatus,
        resolvedShipment ? resolvedShipment.date : null,
        resolvedShipment ? resolvedShipment.client : null,
        resolvedShipment ? resolvedShipment.orderNumber : null,
        resolvedShipment ? resolvedShipment.palletCode : null,
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
        null,
        null,
        now,
        row.created_at || now,
        null,
        null,
        false,
        null,
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
      pallet: resolvedPallet,
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

app.get('/api/reports/manual-template.csv', requireAuth, (req, res) => {
  const csv = buildManualReportTemplateCsv();
  res.setHeader('Content-Type', 'text/csv; charset=utf-8');
  res.setHeader('Content-Disposition', 'attachment; filename="mdt-manual-import-template.csv"');
  return res.status(200).send(csv);
});

app.post('/api/reports/import-manual-csv', requireOperator, async (req, res) => {
  const body = req.body && typeof req.body === 'object' ? req.body : {};
  const csvText = typeof body.csvText === 'string' ? body.csvText : '';
  const operatorScope = getUserTechnicianScope(req.session?.user);
  if (!csvText.trim()) {
    return res.status(400).json({
      ok: false,
      error: 'missing_csv',
      message: 'Fichier CSV vide.'
    });
  }

  const parsed = extractManualReportCsvRows(csvText);
  if (!parsed.ok) {
    return res.status(400).json({
      ok: false,
      error: parsed.error || 'invalid_csv',
      appliedCount: 0,
      skippedCount: parsed.errors.length,
      rowCount: parsed.rowCount || 0,
      errors: parsed.errors
    });
  }
  if (!parsed.rows.length) {
    return res.status(400).json({
      ok: false,
      error: 'no_valid_rows',
      appliedCount: 0,
      skippedCount: parsed.errors.length,
      rowCount: parsed.rowCount || 0,
      errors: parsed.errors.length
        ? parsed.errors
        : [{ line: 1, error: 'Aucune ligne exploitable dans le CSV.' }]
    });
  }

  let client;
  try {
    client = await pool.connect();
    await client.query('BEGIN');
    await setAuditContext(
      client,
      buildAuditContext(req, { source: 'POST /api/reports/import-manual-csv' })
    );
    const imported = [];
    for (const rawRow of parsed.rows) {
      const row = operatorScope
        ? {
            ...rawRow,
            technician: normalizeReportTechnician(operatorScope.primaryLabel || rawRow.technician, {
              fallback: DEFAULT_FALLBACK_TECHNICIAN
            })
          }
        : rawRow;
      const result = await insertManualCsvReportRow(client, req, row);
      imported.push(result);
    }
    await client.query('COMMIT');
    return res.json({
      ok: true,
      appliedCount: imported.length,
      skippedCount: parsed.errors.length,
      rowCount: parsed.rowCount || imported.length + parsed.errors.length,
      errors: parsed.errors
    });
  } catch (error) {
    if (client) {
      try {
        await client.query('ROLLBACK');
      } catch (rollbackError) {
        console.error('Failed to rollback manual CSV import', rollbackError);
      }
    }
    console.error('Failed to import manual CSV', error);
    return res.status(500).json({ ok: false, error: 'db_error' });
  } finally {
    if (client) {
      client.release();
    }
  }
});

app.post('/api/reports/report-zero', requireOperator, async (req, res) => {
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
  const requestedTechnician = normalizeReportTechnician(body.technician, {
    fallback: DEFAULT_FALLBACK_TECHNICIAN
  });
  const operatorScope = getUserTechnicianScope(req.session?.user);
  const technician = normalizeReportTechnician(
    operatorScope ? operatorScope.primaryLabel || requestedTechnician : requestedTechnician,
    { fallback: DEFAULT_FALLBACK_TECHNICIAN }
  );
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
    const resolvedPallet = await resolvePalletForSerial(client, {
      serialNumber,
      machineKey,
      actor: technician || machineKey
    });
    const resolvedPalletId = resolvedPallet && resolvedPallet.id ? resolvedPallet.id : null;
    const resolvedPalletStatus =
      resolvedPallet && resolvedPallet.status ? resolvedPallet.status : null;
    const resolvedShipment = normalizeShipmentFromRow(resolvedPallet);
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
      resolvedPalletId,
      resolvedPalletStatus,
      resolvedShipment ? resolvedShipment.date : null,
      resolvedShipment ? resolvedShipment.client : null,
      resolvedShipment ? resolvedShipment.orderNumber : null,
      resolvedShipment ? resolvedShipment.palletCode : null,
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
      null,
      now,
      now,
      null,
      null,
      false,
      null,
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
      resolvedPalletId,
      resolvedPalletStatus,
      resolvedShipment ? resolvedShipment.date : null,
      resolvedShipment ? resolvedShipment.client : null,
      resolvedShipment ? resolvedShipment.orderNumber : null,
      resolvedShipment ? resolvedShipment.palletCode : null,
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
      null,
      null,
      now,
      now,
      null,
      null,
      false,
      null,
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
      pallet: resolvedPallet,
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

app.put('/api/machines/:id/comment', requireOperator, async (req, res) => {
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
    const reportRow = await getScopedReportRowById(client, id, req.session?.user, {
      columns: 'id, technician',
      forUpdate: true
    });
    if (!reportRow) {
      await client.query('ROLLBACK');
      return res.status(404).json({ ok: false, error: 'not_found' });
    }
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
    const updatedRow = result.rows && result.rows[0] ? result.rows[0] : null;
    if (!updatedRow) {
      await client.query('ROLLBACK');
      return res.status(404).json({ ok: false, error: 'not_found' });
    }
    await client.query('COMMIT');
    return res.json({ ok: true, comment: updatedRow.comment, commentedAt: updatedRow.commented_at });
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

app.put('/api/tags/rename', requirePermission('canRenameTags'), async (req, res) => {
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

async function getReportDetailRow(reportId) {
  const result = await pool.query(getReportByIdQuery, [reportId]);
  return result.rows && result.rows[0] ? result.rows[0] : null;
}

function buildReportPdfPayload(row) {
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
  components = applyClockAlertToComponents(components, row);

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
  const autopilotHash = buildAutopilotHash(payload);

  const macAddresses = normalizeMacList(row.mac_addresses);
  const macList = Array.isArray(macAddresses) ? macAddresses.filter(Boolean) : [];
  const macPrimary = row.mac_address || macList[0] || '--';
  const category = normalizeCategory(row.category);
  const title = safeString(row.hostname || row.serial_number || row.mac_address || macList[0], `Machine ${row.id}`);
  const subtitle = [row.vendor, row.model].filter(Boolean).join(' ') || 'Modele non renseigne';
  const productLabel = buildMachineIdentityLabel(
    {
      vendor: row.vendor,
      model: row.model,
      serial_number: row.serial_number
    },
    {
      includeSerial: true,
      fallback: subtitle
    }
  );
  const payloadCpu = payload && payload.cpu && typeof payload.cpu === 'object' ? payload.cpu : null;
  const payloadGpu = payload && payload.gpu && typeof payload.gpu === 'object' ? payload.gpu : null;
  const diskInfoRaw = payload ? payload.disks : null;
  const diskInfo = Array.isArray(diskInfoRaw) ? diskInfoRaw : diskInfoRaw ? [diskInfoRaw] : [];
  const volumeInfoRaw = payload ? payload.volumes : null;
  const volumeInfo = Array.isArray(volumeInfoRaw) ? volumeInfoRaw : volumeInfoRaw ? [volumeInfoRaw] : [];
  const shipment = normalizeShipmentFromRow({
    report_shipment_date: row.report_shipment_date,
    report_shipment_client: row.report_shipment_client,
    report_shipment_order_number: row.report_shipment_order_number,
    report_shipment_pallet_code: row.report_shipment_pallet_code,
    report_pallet_code: row.report_pallet_code,
    machine_shipment_date: row.machine_shipment_date,
    machine_shipment_client: row.machine_shipment_client,
    machine_shipment_order_number: row.machine_shipment_order_number,
    machine_shipment_pallet_code: row.machine_shipment_pallet_code,
    machine_pallet_code: row.machine_pallet_code
  });

  const reportData = {
    id: row.id,
    title,
    subtitle,
    productLabel,
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
    autopilotHash,
    autopilotHashDisplay: formatPdfAutopilotHash(autopilotHash),
    shipmentDate: shipment ? formatDateOnly(shipment.date) : '--',
    shipmentClient: shipment && shipment.client ? shipment.client : '--',
    shipmentOrderNumber: shipment && shipment.orderNumber ? shipment.orderNumber : '--',
    shipmentPalletCode: shipment && shipment.palletCode ? shipment.palletCode : '--',
    cameraStatus: row.camera_status,
    usbStatus: row.usb_status,
    keyboardStatus: row.keyboard_status,
    padStatus: row.pad_status,
    badgeReaderStatus: row.badge_reader_status,
    diagnostics: buildDiagnosticsRows(payload, components, category),
    inventoryRows: buildInventoryRows(payload),
    components: buildComponentRows(components),
    summary: summarizeComponents(components),
    summaryForPdf: summarizePdfDetailForReport(components, payload, row.comment || '', category),
    generatedAt: formatDateTime(new Date())
  };

  const downloadBaseName = safeString(
    productLabel || row.serial_number || row.hostname || row.mac_address || macList[0],
    `Machine ${row.id}`
  );
  return {
    reportData,
    filename: `rapport-atelier-${sanitizeFilename(downloadBaseName)}.pdf`
  };
}

function decorateReportPdfDocument(doc, reportData) {
  doc.info.Title = `Rapport atelier - ${reportData.productLabel || reportData.title}`;
  doc.info.Author = 'Atelier Ops';
  drawReportPdf(doc, reportData);
}

function renderReportPdfBuffer(reportData) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    const doc = new PDFDocument({ size: 'A4', margin: 40 });
    doc.on('data', (chunk) => {
      chunks.push(chunk);
    });
    doc.on('end', () => {
      resolve(Buffer.concat(chunks));
    });
    doc.on('error', (error) => {
      reject(error);
    });
    decorateReportPdfDocument(doc, reportData);
    doc.end();
  });
}

function buildShipmentExportFilters(query) {
  return {
    shipmentDate: normalizeShipmentDate(
      query.shipmentDate || query.dateExpedition || query.expeditionDate || query.shippingDate
    ),
    shipmentClient: normalizeShipmentClient(query.shipmentClient || query.client || query.customer),
    shipmentOrderNumber: normalizeShipmentOrderNumber(
      query.shipmentOrderNumber || query.orderNumber || query.order || query.commande
    ),
    shipmentPalletCode: normalizePalletCode(
      query.shipmentPalletCode || query.palletCode || query.pallet || query.palette
    )
  };
}

function buildShipmentExportArchiveName(filters) {
  const parts = ['rapports-atelier'];
  if (filters.shipmentOrderNumber) {
    parts.push(`commande-${sanitizeFilename(filters.shipmentOrderNumber)}`);
  }
  if (filters.shipmentPalletCode) {
    parts.push(`palette-${sanitizeFilename(filters.shipmentPalletCode)}`);
  }
  if (!filters.shipmentOrderNumber && !filters.shipmentPalletCode) {
    parts.push(`export-${Date.now()}`);
  }
  return `${parts.join('-')}.zip`;
}

app.get('/api/machines/:id/report.pdf', requireAuth, async (req, res) => {
  const id = normalizeUuid(req.params.id);
  if (!id) {
    return res.status(400).json({ ok: false, error: 'invalid_id' });
  }

  let row;
  try {
    row = await getReportDetailRow(id);
  } catch (error) {
    console.error('Failed to fetch machine detail for PDF', error);
    return res.status(500).json({ ok: false, error: 'db_error' });
  }

  if (!row) {
    return res.status(404).json({ ok: false, error: 'not_found' });
  }
  if (!canUserAccessReportTechnician(req.session?.user, row.technician)) {
    return res.status(404).json({ ok: false, error: 'not_found' });
  }

  const { reportData, filename } = buildReportPdfPayload(row);
  res.setHeader('Content-Type', 'application/pdf');
  res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
  res.setHeader('Cache-Control', 'no-store');

  const doc = new PDFDocument({ size: 'A4', margin: 40 });
  doc.on('error', (error) => {
    console.error('PDF generation error', error);
  });
  doc.pipe(res);
  decorateReportPdfDocument(doc, reportData);
  doc.end();
});

app.get('/api/reports/export.zip', requireLogistics, async (req, res) => {
  const filters = buildShipmentExportFilters(req.query || {});
  if (!filters.shipmentOrderNumber && !filters.shipmentPalletCode) {
    return res.status(400).json({
      ok: false,
      error: 'missing_export_filter',
      message: 'Renseigne au minimum un numero de commande ou un numero de palette.'
    });
  }

  const filterQuery = {
    shipmentDate: filters.shipmentDate || undefined,
    shipmentClient: filters.shipmentClient || undefined,
    shipmentOrderNumber: filters.shipmentOrderNumber || undefined,
    shipmentPalletCode: filters.shipmentPalletCode || undefined
  };
  const { clauses, values } = buildReportFilters(filterQuery, {
    includeCategory: false
  });
  const where = clauses.length ? `WHERE ${clauses.join(' AND ')}` : '';

  let reportRows;
  try {
    const result = await pool.query(
      `
        WITH latest AS (
          SELECT DISTINCT ON (reports.machine_key)
            reports.id,
            reports.machine_key,
            reports.last_seen
          FROM reports
          ${where}
          ORDER BY reports.machine_key, reports.last_seen DESC, reports.id DESC
        )
        SELECT id, machine_key, last_seen
        FROM latest
        ORDER BY last_seen DESC
        LIMIT $${values.length + 1}
      `,
      [...values, PDF_BATCH_EXPORT_LIMIT + 1]
    );
    reportRows = result.rows || [];
  } catch (error) {
    console.error('Failed to resolve report export selection', error);
    return res.status(500).json({ ok: false, error: 'db_error' });
  }

  if (!reportRows.length) {
    return res.status(404).json({
      ok: false,
      error: 'no_reports_found',
      message: 'Aucun rapport correspondant a ces filtres.'
    });
  }
  if (reportRows.length > PDF_BATCH_EXPORT_LIMIT) {
    return res.status(413).json({
      ok: false,
      error: 'too_many_reports',
      message: `Export limite a ${PDF_BATCH_EXPORT_LIMIT} rapports. Raffine ta recherche.`
    });
  }

  const archiveName = buildShipmentExportArchiveName(filters);
  res.setHeader('Content-Type', 'application/zip');
  res.setHeader('Content-Disposition', `attachment; filename="${archiveName}"`);
  res.setHeader('Cache-Control', 'no-store');

  const archive = archiver('zip', { zlib: { level: 9 } });
  archive.on('error', (error) => {
    console.error('ZIP export error', error);
    if (!res.headersSent) {
      res.status(500).json({ ok: false, error: 'zip_error' });
      return;
    }
    res.destroy(error);
  });
  archive.pipe(res);

  try {
    for (const item of reportRows) {
      const row = await getReportDetailRow(item.id);
      if (!row) {
        continue;
      }
      const { reportData, filename } = buildReportPdfPayload(row);
      const pdfBuffer = await renderReportPdfBuffer(reportData);
      archive.append(pdfBuffer, { name: filename });
    }
    archive.append(
      Buffer.from(
        JSON.stringify(
          {
            generatedAt: new Date().toISOString(),
            reportCount: reportRows.length,
            filters
          },
          null,
          2
        )
      ),
      { name: 'export.json' }
    );
    await archive.finalize();
  } catch (error) {
    console.error('Failed to build ZIP export', error);
    archive.abort();
    if (!res.headersSent) {
      return res.status(500).json({ ok: false, error: 'pdf_export_failed' });
    }
    return res.destroy(error);
  }
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
    await backfillUnknownTechniciansToLuka();
    await repairStoredTruncatedPayloads();
    await repairTechniciansFromPayload();
    startWeeklyRecapScheduler();
    app.listen(PORT, () => {
      console.log(`MDT web listening on http://localhost:${PORT}`);
    });
  } catch (error) {
    console.error('Failed to initialize database', error);
    process.exit(1);
  }
}

startServer();
