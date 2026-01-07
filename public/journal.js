const state = {
  logs: [],
  filters: {
    q: '',
    table: '',
    action: '',
    limit: '100'
  },
  details: new Map(),
  lastUpdated: null
};

const listEl = document.getElementById('log-list');
const searchInput = document.getElementById('log-search');
const tableSelect = document.getElementById('log-table');
const actionSelect = document.getElementById('log-action');
const limitSelect = document.getElementById('log-limit');
const refreshBtn = document.getElementById('refresh-btn');
const lastUpdatedEl = document.getElementById('last-updated');

const actionLabels = {
  INSERT: 'Ajout',
  UPDATE: 'Mise a jour',
  DELETE: 'Suppression'
};

const tableLabels = {
  machines: 'Machines',
  ldap_settings: 'Config LDAP'
};

const fieldLabels = {
  machine_key: 'Machine key',
  hostname: 'Nom',
  mac_address: 'MAC',
  mac_addresses: 'MACs',
  serial_number: 'Serie',
  category: 'Categorie',
  model: 'Modele',
  vendor: 'Constructeur',
  technician: 'Technicien',
  os_version: 'OS',
  ram_mb: 'RAM',
  ram_slots_total: 'Slots RAM',
  ram_slots_free: 'Slots libres',
  battery_health: 'Batterie',
  camera_status: 'Camera',
  usb_status: 'USB',
  keyboard_status: 'Clavier',
  pad_status: 'Pave tactile',
  badge_reader_status: 'Badge',
  last_seen: 'Dernier passage',
  created_at: 'Creation',
  components: 'Composants',
  payload: 'Payload',
  last_ip: 'IP',
  enabled: 'Actif',
  url: 'URL',
  bind_dn: 'Bind DN',
  bind_password: 'Bind password',
  search_base: 'Search base',
  search_filter: 'Search filter',
  search_attributes: 'Search attributes',
  tls_reject_unauthorized: 'TLS strict',
  updated_at: 'Mise a jour'
};

function escapeHtml(value) {
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
    hour: '2-digit',
    minute: '2-digit'
  });
}

function formatAction(action) {
  if (!action) {
    return '--';
  }
  const upper = String(action).toUpperCase();
  return actionLabels[upper] || upper;
}

function formatTable(table) {
  if (!table) {
    return '--';
  }
  return tableLabels[table] || table;
}

function formatActor(log) {
  if (!log) {
    return '--';
  }
  const actor = log.actor || 'systeme';
  const type = log.actorType ? ` (${log.actorType})` : '';
  return `${actor}${type}`;
}

function formatMachine(log) {
  if (!log) {
    return '--';
  }
  const primary = log.hostname || log.machineKey || log.rowId;
  if (!primary) {
    return '--';
  }
  if (log.hostname && log.machineKey) {
    return `${log.hostname} (${log.machineKey})`;
  }
  return primary;
}

function formatFieldName(name) {
  if (!name) {
    return '';
  }
  if (fieldLabels[name]) {
    return fieldLabels[name];
  }
  return name.replace(/_/g, ' ');
}

function stringifyJson(value) {
  if (!value) {
    return '--';
  }
  try {
    return JSON.stringify(value, null, 2);
  } catch (error) {
    return '--';
  }
}

function updateLastUpdated() {
  if (!lastUpdatedEl) {
    return;
  }
  if (!state.lastUpdated) {
    lastUpdatedEl.textContent = 'Derniere mise a jour : --';
    return;
  }
  lastUpdatedEl.textContent = `Derniere mise a jour : ${formatDateTime(state.lastUpdated)}`;
}

function buildQuery() {
  const params = new URLSearchParams();
  if (state.filters.q) {
    params.set('q', state.filters.q);
  }
  if (state.filters.table) {
    params.set('table', state.filters.table);
  }
  if (state.filters.action) {
    params.set('action', state.filters.action);
  }
  if (state.filters.limit) {
    params.set('limit', state.filters.limit);
  }
  return params.toString();
}

async function fetchLogs() {
  const query = buildQuery();
  const url = query ? `/api/logs?${query}` : '/api/logs';
  try {
    const response = await fetch(url);
    if (response.status === 401) {
      window.location.href = '/login';
      return;
    }
    if (!response.ok) {
      throw new Error('fetch_failed');
    }
    const data = await response.json();
    state.logs = Array.isArray(data.logs) ? data.logs : [];
    state.lastUpdated = new Date();
    renderLogs();
    updateLastUpdated();
  } catch (error) {
    listEl.innerHTML = '<div class="empty">Impossible de charger le journal.</div>';
  }
}

function renderLogs() {
  if (!listEl) {
    return;
  }
  if (!state.logs.length) {
    listEl.innerHTML = '<div class="empty">Aucune modification recente.</div>';
    return;
  }

  const rows = state.logs.map((log, index) => {
    const action = log.action || '--';
    const changedFields = Array.isArray(log.changedFields) ? log.changedFields : [];
    const chips = changedFields.slice(0, 12).map((field) => {
      return `<span class="log-chip">${escapeHtml(formatFieldName(field))}</span>`;
    });
    if (changedFields.length > 12) {
      chips.push(`<span class="log-chip">+${changedFields.length - 12}</span>`);
    }

    return `
      <article class="log-card" data-id="${escapeHtml(log.id)}">
        <div class="log-header">
          <div>
            <p class="log-kicker">${escapeHtml(formatTable(log.table))}</p>
            <h3 class="log-title">${escapeHtml(formatAction(log.action))}</h3>
          </div>
          <div class="log-badges">
            <span class="log-badge" data-action="${escapeHtml(action)}">${escapeHtml(action)}</span>
            <span class="log-badge" data-table="${escapeHtml(log.table || '')}">${escapeHtml(
              formatTable(log.table)
            )}</span>
          </div>
        </div>
        <div class="log-meta">
          <span>${escapeHtml(formatDateTime(log.occurredAt))}</span>
          <span>${escapeHtml(formatActor(log))}</span>
          <span>${escapeHtml(log.actorIp || '--')}</span>
          <span>${escapeHtml(log.source || '--')}</span>
        </div>
        <div class="log-grid">
          <div class="log-field">
            <span>Machine</span>
            <strong>${escapeHtml(formatMachine(log))}</strong>
          </div>
          <div class="log-field">
            <span>Request ID</span>
            <strong>${escapeHtml(log.requestId || '--')}</strong>
          </div>
        </div>
        <div class="log-chips">
          ${chips.join('') || '<span class="log-chip">Aucun champ</span>'}
        </div>
        <details class="log-details" data-id="${escapeHtml(log.id)}" data-loaded="false">
          <summary>Voir le detail</summary>
          <div class="log-detail-content" data-state="loading">Chargement...</div>
        </details>
      </article>
    `;
  });

  listEl.innerHTML = rows.join('');
  wireDetailToggles();
}

function wireDetailToggles() {
  const details = document.querySelectorAll('.log-details');
  details.forEach((detail) => {
    detail.addEventListener('toggle', async () => {
      if (!detail.open || detail.dataset.loaded === 'true') {
        return;
      }
      const id = detail.dataset.id;
      const content = detail.querySelector('.log-detail-content');
      if (!id || !content) {
        return;
      }
      content.dataset.state = 'loading';
      content.textContent = 'Chargement...';
      try {
        const response = await fetch(`/api/logs/${id}`);
        if (response.status === 401) {
          window.location.href = '/login';
          return;
        }
        if (!response.ok) {
          throw new Error('detail_failed');
        }
        const data = await response.json();
        if (!data || !data.log) {
          throw new Error('detail_missing');
        }
        const oldData = stringifyJson(data.log.oldData);
        const newData = stringifyJson(data.log.newData);
        content.innerHTML = `
          <div class="log-json-block">
            <span>Avant</span>
            <pre>${escapeHtml(oldData)}</pre>
          </div>
          <div class="log-json-block">
            <span>Apres</span>
            <pre>${escapeHtml(newData)}</pre>
          </div>
        `;
        detail.dataset.loaded = 'true';
      } catch (error) {
        content.dataset.state = 'error';
        content.textContent = 'Impossible de charger le detail.';
      }
    });
  });
}

let searchTimer = null;
if (searchInput) {
  searchInput.addEventListener('input', () => {
    state.filters.q = searchInput.value.trim();
    if (searchTimer) {
      clearTimeout(searchTimer);
    }
    searchTimer = setTimeout(fetchLogs, 300);
  });
}

if (tableSelect) {
  tableSelect.addEventListener('change', () => {
    state.filters.table = tableSelect.value;
    fetchLogs();
  });
}

if (actionSelect) {
  actionSelect.addEventListener('change', () => {
    state.filters.action = actionSelect.value;
    fetchLogs();
  });
}

if (limitSelect) {
  limitSelect.addEventListener('change', () => {
    state.filters.limit = limitSelect.value;
    fetchLogs();
  });
}

if (refreshBtn) {
  refreshBtn.addEventListener('click', () => {
    fetchLogs();
  });
}

fetchLogs();
