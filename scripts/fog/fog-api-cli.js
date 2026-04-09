#!/usr/bin/env node
'use strict';

const fs = require('fs');
const path = require('path');

function printUsage() {
  console.log(`Usage:
  node scripts/fog/fog-api-cli.js status
  node scripts/fog/fog-api-cli.js images list
  node scripts/fog/fog-api-cli.js hosts list
  node scripts/fog/fog-api-cli.js hosts upsert --name NAME --mac MAC[,MAC2] --image-id ID [--ip IP]
  node scripts/fog/fog-api-cli.js hosts delete --name NAME
  node scripts/fog/fog-api-cli.js hosts delete --id ID
  node scripts/fog/fog-api-cli.js tasks create --host-id ID --task-type-id ID [--task-name NAME] [--shutdown] [--debug] [--wol]
  node scripts/fog/fog-api-cli.js tasks create --host-name NAME --task-type-id ID [--task-name NAME] [--shutdown] [--debug] [--wol]
  node scripts/fog/fog-api-cli.js deploy queue --name NAME --mac MAC[,MAC2] (--image-id ID | --image-name NAME) [--ip IP] [--debug] [--shutdown] [--wol]
  node scripts/fog/fog-api-cli.js capture queue --name NAME --mac MAC[,MAC2] (--image-id ID | --image-name NAME) [--ip IP] [--debug] [--shutdown] [--wol]

Configuration:
  --config PATH
  --base-url URL
  --api-token TOKEN
  --user-token TOKEN
  --timeout-ms 10000

Environment:
  FOG_CONFIG_PATH
  FOG_BASE_URL
  FOG_API_TOKEN
  FOG_USER_TOKEN
  FOG_TIMEOUT_MS`);
}

function parseArgs(argv) {
  const positionals = [];
  const options = {};

  for (let index = 0; index < argv.length; index += 1) {
    const value = argv[index];
    if (!value.startsWith('--')) {
      positionals.push(value);
      continue;
    }

    const key = value.slice(2);
    const next = argv[index + 1];
    if (!next || next.startsWith('--')) {
      options[key] = true;
      continue;
    }
    options[key] = next;
    index += 1;
  }

  return { positionals, options };
}

function loadJsonConfig(configPath) {
  if (!configPath) {
    return {};
  }
  const resolvedPath = path.resolve(configPath);
  const raw = fs.readFileSync(resolvedPath, 'utf8');
  return JSON.parse(raw);
}

function normalizeBaseUrl(value) {
  const trimmed = String(value || '').trim();
  if (!trimmed) {
    return '';
  }
  return trimmed.replace(/\/+$/, '');
}

function base64(value) {
  return Buffer.from(String(value), 'utf8').toString('base64');
}

function parseMacList(value) {
  if (Array.isArray(value)) {
    return value.flatMap((item) => parseMacList(item));
  }
  const text = String(value || '').trim();
  if (!text) {
    return [];
  }
  return text
    .split(/[,\s]+/)
    .map((item) => item.trim())
    .filter(Boolean);
}

function pickConfigValue(...values) {
  for (const value of values) {
    if (value === undefined || value === null) {
      continue;
    }
    const text = String(value).trim();
    if (!text) {
      continue;
    }
    return text;
  }
  return '';
}

function coerceInteger(value, label) {
  const parsed = Number.parseInt(String(value), 10);
  if (!Number.isFinite(parsed)) {
    throw new Error(`Invalid ${label}: ${value}`);
  }
  return parsed;
}

class FogApiClient {
  constructor(config) {
    this.baseUrl = normalizeBaseUrl(config.baseUrl);
    this.apiToken = String(config.apiToken || '');
    this.userToken = String(config.userToken || '');
    this.timeoutMs = Number.parseInt(String(config.timeoutMs || '10000'), 10) || 10000;

    if (!this.baseUrl) {
      throw new Error('Missing FOG base URL');
    }
    if (!this.apiToken) {
      throw new Error('Missing FOG API token');
    }
    if (!this.userToken) {
      throw new Error('Missing FOG user token');
    }
  }

  async request(endpoint, { method = 'GET', body } = {}) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), this.timeoutMs);
    const headers = {
      'fog-api-token': base64(this.apiToken),
      'fog-user-token': base64(this.userToken)
    };

    const init = {
      method,
      headers,
      signal: controller.signal
    };

    if (body !== undefined) {
      headers['Content-Type'] = 'application/json';
      init.body = JSON.stringify(body);
    }

    const response = await fetch(`${this.baseUrl}${endpoint}`, init).finally(() => {
      clearTimeout(timer);
    });

    const text = await response.text();
    let data = null;
    if (text) {
      try {
        data = JSON.parse(text);
      } catch {
        data = text;
      }
    }

    if (!response.ok) {
      const error = new Error(`FOG API ${method} ${endpoint} failed with ${response.status}`);
      error.status = response.status;
      error.payload = data;
      throw error;
    }

    return data;
  }

  status() {
    return this.request('/system/status');
  }

  listImages() {
    return this.request('/image/list');
  }

  listHosts() {
    return this.request('/host/list');
  }

  getHost(id) {
    return this.request(`/host/${encodeURIComponent(id)}`);
  }

  createHost(payload) {
    return this.request('/host/create', { method: 'POST', body: payload });
  }

  updateHost(id, payload) {
    return this.request(`/host/${encodeURIComponent(id)}/edit`, { method: 'PUT', body: payload });
  }

  deleteHost(id) {
    return this.request(`/host/${encodeURIComponent(id)}/delete`, { method: 'DELETE' });
  }

  createTask(hostId, payload) {
    return this.request(`/host/${encodeURIComponent(hostId)}/task`, { method: 'POST', body: payload });
  }
}

function unwrapCollection(data, key) {
  if (!data) {
    return [];
  }
  const value = data[key];
  if (Array.isArray(value)) {
    return value;
  }
  return [];
}

async function findHostByName(client, name) {
  const hosts = unwrapCollection(await client.listHosts(), 'hosts');
  return hosts.find((item) => String(item.name || '').toLowerCase() === String(name).toLowerCase()) || null;
}

async function findImageByName(client, name) {
  const images = unwrapCollection(await client.listImages(), 'images');
  return images.find((item) => String(item.name || '').toLowerCase() === String(name).toLowerCase()) || null;
}

function extractHostMacs(host) {
  const macs = [];
  if (Array.isArray(host && host.macs)) {
    macs.push(...host.macs);
  }
  if (host && host.primac) {
    macs.push(host.primac);
  }
  return [...new Set(parseMacList(macs))];
}

async function resolveImageId(client, options) {
  if (options['image-id']) {
    return coerceInteger(options['image-id'], 'image ID');
  }
  const imageName = pickConfigValue(options['image-name']);
  if (!imageName) {
    throw new Error('Missing --image-id or --image-name');
  }
  const image = await findImageByName(client, imageName);
  if (!image) {
    throw new Error(`Image not found: ${imageName}`);
  }
  return coerceInteger(image.id, 'image ID');
}

async function ensureHost(client, { name, imageId, macs, ip }) {
  const existing = await findHostByName(client, name);
  if (!existing) {
    if (!macs.length) {
      throw new Error('Missing --mac for new host');
    }

    return client.createHost({
      name,
      imageID: imageId,
      macs,
      ...(ip ? { ip } : {})
    });
  }

  const current = await client.getHost(existing.id);
  const resolvedMacs = macs.length ? macs : extractHostMacs(current);
  const resolvedImageId = imageId || current.imageID || (current.image && current.image.id);
  const resolvedIp = pickConfigValue(ip, current.ip);

  if (!resolvedMacs.length) {
    throw new Error(`Missing MAC addresses for existing host: ${name}`);
  }
  if (!resolvedImageId) {
    throw new Error(`Missing image for existing host: ${name}`);
  }

  return client.updateHost(existing.id, {
    name,
    imageID: coerceInteger(resolvedImageId, 'image ID'),
    macs: resolvedMacs,
    ...(resolvedIp ? { ip: resolvedIp } : {})
  });
}

function buildWorkflowTaskPayload(kind, hostName, imageRef, options) {
  const normalizedKind = String(kind || '').toLowerCase();
  const taskTypeID = normalizedKind === 'capture'
    ? (options.debug ? 16 : 2)
    : (options.debug ? 15 : 1);

  const actionLabel = normalizedKind === 'capture' ? 'Capture' : 'Deploy';
  const payload = {
    taskTypeID,
    taskName: pickConfigValue(options['task-name']) || `${actionLabel} ${imageRef} - ${hostName}`
  };

  if (options.shutdown) {
    payload.shutdown = true;
  }
  if (options.debug) {
    payload.debug = true;
  }
  if (options.wol) {
    payload.wol = true;
  }
  if (options['deploy-snapins']) {
    payload.deploySnapins = options['deploy-snapins'] === 'true'
      ? true
      : coerceInteger(options['deploy-snapins'], 'deploy snapins');
  }

  return payload;
}

async function run() {
  const { positionals, options } = parseArgs(process.argv.slice(2));
  if (!positionals.length || options.help) {
    printUsage();
    process.exit(positionals.length ? 0 : 1);
  }

  const config = loadJsonConfig(pickConfigValue(options.config, process.env.FOG_CONFIG_PATH));
  const client = new FogApiClient({
    baseUrl: pickConfigValue(options['base-url'], process.env.FOG_BASE_URL, config.baseUrl),
    apiToken: pickConfigValue(options['api-token'], process.env.FOG_API_TOKEN, config.apiToken),
    userToken: pickConfigValue(options['user-token'], process.env.FOG_USER_TOKEN, config.userToken),
    timeoutMs: pickConfigValue(options['timeout-ms'], process.env.FOG_TIMEOUT_MS, config.timeoutMs)
  });

  const [domain, action = ''] = positionals;

  if (domain === 'status') {
    const data = await client.status();
    console.log(typeof data === 'string' ? data : JSON.stringify(data, null, 2));
    return;
  }

  if (domain === 'images' && action === 'list') {
    const data = await client.listImages();
    console.log(JSON.stringify(data, null, 2));
    return;
  }

  if (domain === 'hosts' && action === 'list') {
    const data = await client.listHosts();
    console.log(JSON.stringify(data, null, 2));
    return;
  }

  if (domain === 'hosts' && action === 'upsert') {
    const name = pickConfigValue(options.name);
    const imageId = coerceInteger(options['image-id'], 'image ID');
    const macs = parseMacList(options.mac);
    if (!name) {
      throw new Error('Missing --name');
    }
    if (!macs.length) {
      throw new Error('Missing --mac');
    }

    const payload = {
      name,
      imageID: imageId,
      macs
    };
    if (options.ip) {
      payload.ip = options.ip;
    }

    const existing = await findHostByName(client, name);
    const data = existing
      ? await client.updateHost(existing.id, payload)
      : await client.createHost(payload);

    console.log(JSON.stringify(data, null, 2));
    return;
  }

  if (domain === 'hosts' && action === 'delete') {
    let hostId = options.id ? coerceInteger(options.id, 'host ID') : null;
    if (!hostId) {
      const name = pickConfigValue(options.name);
      if (!name) {
        throw new Error('Missing --id or --name');
      }
      const existing = await findHostByName(client, name);
      if (!existing) {
        throw new Error(`Host not found: ${name}`);
      }
      hostId = existing.id;
    }
    const data = await client.deleteHost(hostId);
    console.log(JSON.stringify(data, null, 2));
    return;
  }

  if (domain === 'tasks' && action === 'create') {
    let hostId = options['host-id'] ? coerceInteger(options['host-id'], 'host ID') : null;
    if (!hostId) {
      const hostName = pickConfigValue(options['host-name']);
      if (!hostName) {
        throw new Error('Missing --host-id or --host-name');
      }
      const existing = await findHostByName(client, hostName);
      if (!existing) {
        throw new Error(`Host not found: ${hostName}`);
      }
      hostId = existing.id;
    }

    const payload = {
      taskTypeID: coerceInteger(options['task-type-id'], 'task type ID')
    };
    if (options['task-name']) {
      payload.taskName = options['task-name'];
    }
    if (options.shutdown) {
      payload.shutdown = true;
    }
    if (options.debug) {
      payload.debug = true;
    }
    if (options.wol) {
      payload.wol = true;
    }
    if (options['deploy-snapins']) {
      payload.deploySnapins = options['deploy-snapins'] === 'true'
        ? true
        : coerceInteger(options['deploy-snapins'], 'deploy snapins');
    }

    const data = await client.createTask(hostId, payload);
    console.log(JSON.stringify(data, null, 2));
    return;
  }

  if ((domain === 'deploy' || domain === 'capture') && action === 'queue') {
    const name = pickConfigValue(options.name);
    const macs = parseMacList(options.mac);
    if (!name) {
      throw new Error('Missing --name');
    }

    const imageId = await resolveImageId(client, options);
    const host = await ensureHost(client, {
      name,
      imageId,
      macs,
      ip: pickConfigValue(options.ip)
    });

    const imageRef = pickConfigValue(options['image-name'], options['image-id'], imageId);
    const taskPayload = buildWorkflowTaskPayload(domain, name, imageRef, options);
    const task = await client.createTask(coerceInteger(host.id, 'host ID'), taskPayload);

    console.log(JSON.stringify({
      workflow: domain,
      host,
      task
    }, null, 2));
    return;
  }

  printUsage();
  process.exit(1);
}

run().catch((error) => {
  const payload = error && error.payload !== undefined ? error.payload : null;
  console.error(error.message || String(error));
  if (payload !== null) {
    if (typeof payload === 'string') {
      console.error(payload);
    } else {
      console.error(JSON.stringify(payload, null, 2));
    }
  }
  process.exit(1);
});
