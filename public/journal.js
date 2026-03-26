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
const totalLogsEl = document.getElementById('journal-total-logs');
const totalTablesEl = document.getElementById('journal-total-tables');
const totalActorsEl = document.getElementById('journal-total-actors');
const lastActionEl = document.getElementById('journal-last-action');

const actionLabels = {
  INSERT: 'Ajout',
  UPDATE: 'Mise a jour',
  DELETE: 'Suppression'
};

const tableLabels = {
  machines: 'Machines',
  reports: 'Rapports',
  ldap_settings: 'Config auth legacy',
  pallets: 'Palettes',
  pallet_imports: 'Imports palettes',
  pallet_serials: 'Liaisons palettes',
  pallet_movements: 'Mouvements palettes'
};

const componentStatusLabels = {
  ok: 'OK',
  nok: 'NOK',
  fr: 'FR',
  en: 'EN',
  absent: 'Absent',
  not_tested: 'Non teste',
  denied: 'Refuse',
  timeout: 'Timeout'
};

const fieldLabels = {
  machine_key: 'Machine key',
  hostname: 'Nom',
  mac_address: 'MAC',
  mac_addresses: 'MACs',
  serial_number: 'Serie',
  category: 'Categorie',
  pallet_id: 'Palette',
  pallet_status: 'Statut palette',
  shipment_date: 'Date expedition',
  shipment_client: 'Client',
  shipment_order_number: 'N° commande',
  shipment_pallet_code: 'N° palette expedition',
  code: 'Code palette',
  code_key: 'Cle palette',
  import_type: 'Type import',
  row_count: 'Lignes',
  applied_count: 'Lignes appliquees',
  skipped_count: 'Lignes ignorees',
  movement_type: 'Type mouvement',
  last_import_id: 'Dernier import',
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
  created_by: 'Cree par',
  updated_by: 'Mis a jour par',
  comment: 'Commentaire',
  commented_at: 'Commentaire date',
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

function truncateValue(value, maxLength) {
  if (value.length <= maxLength) {
    return value;
  }
  return `${value.slice(0, Math.max(0, maxLength - 3))}...`;
}

function formatValue(value, maxLength = 140) {
  if (value == null) {
    return '--';
  }
  if (typeof value === 'string') {
    const trimmed = value.trim();
    return truncateValue(trimmed || '--', maxLength);
  }
  if (typeof value === 'number' || typeof value === 'boolean') {
    return String(value);
  }
  try {
    return truncateValue(JSON.stringify(value), maxLength);
  } catch (error) {
    return '[complex]';
  }
}

function formatComponentStatus(statusKey) {
  if (!statusKey) {
    return '--';
  }
  return componentStatusLabels[statusKey] || String(statusKey).toUpperCase();
}

function renderComponentChanges(componentChanges) {
  if (!Array.isArray(componentChanges) || componentChanges.length === 0) {
    return '';
  }
  const rows = componentChanges
    .map((change) => {
      const type = change.changeType || 'updated';
      const typeLabel =
        type === 'corrected' ? 'Corrige' : type === 'regressed' ? 'Regression' : 'Mise a jour';
      const label = change.componentLabel || change.componentKey || '--';
      const before = formatComponentStatus(change.fromStatus);
      const after = formatComponentStatus(change.toStatus);
      return `
        <div class="log-component-row" data-change-type="${escapeHtml(type)}">
          <span class="log-component-name">${escapeHtml(label)}</span>
          <div class="log-component-values">
            <span class="log-change-before">${escapeHtml(before)}</span>
            <span class="log-change-arrow">-></span>
            <span class="log-change-after">${escapeHtml(after)}</span>
          </div>
          <span class="log-component-type">${escapeHtml(typeLabel)}</span>
        </div>
      `;
    })
    .join('');
  return `
    <div class="log-component-changes">
      <p class="log-component-kicker">Transitions composants</p>
      ${rows}
    </div>
  `;
}

function renderChangeRows(changes, componentChanges = []) {
  const hasComponentChanges =
    Array.isArray(componentChanges) && componentChanges.length > 0;
  const sourceRows = Array.isArray(changes) ? changes : [];
  const filteredRows = hasComponentChanges
    ? sourceRows.filter((change) => change && change.field !== 'components')
    : sourceRows;
  const baseHtml = filteredRows
    .map((change) => {
      const label = formatFieldName(change.field);
      const before = formatValue(change.before);
      const after = formatValue(change.after);
      return `
        <div class="log-change-row">
          <span class="log-change-label">${escapeHtml(label)}</span>
          <div class="log-change-values">
            <span class="log-change-before">${escapeHtml(before)}</span>
            <span class="log-change-arrow">-></span>
            <span class="log-change-after">${escapeHtml(after)}</span>
          </div>
        </div>
      `;
    })
    .join('');
  const componentHtml = hasComponentChanges ? renderComponentChanges(componentChanges) : '';
  if (!baseHtml && !componentHtml) {
    return '<div class="log-change-empty">Aucun champ modifie.</div>';
  }
  return `${baseHtml}${componentHtml}`;
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

function updateSummary() {
  const logs = Array.isArray(state.logs) ? state.logs : [];

  if (totalLogsEl) {
    totalLogsEl.textContent = String(logs.length);
  }

  if (totalTablesEl) {
    const tables = new Set(
      logs
        .map((log) => (log && log.table ? String(log.table).trim() : ''))
        .filter(Boolean)
    );
    totalTablesEl.textContent = String(tables.size);
  }

  if (totalActorsEl) {
    const actors = new Set(
      logs
        .map((log) => {
          const actor = log && log.actor ? String(log.actor).trim() : 'systeme';
          const actorType = log && log.actorType ? String(log.actorType).trim() : '';
          return `${actor}::${actorType}`;
        })
        .filter(Boolean)
    );
    totalActorsEl.textContent = String(actors.size);
  }

  if (lastActionEl) {
    const first = logs[0];
    lastActionEl.textContent = first
      ? `${formatAction(first.action)} / ${formatTable(first.table)}`
      : '--';
  }
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
    updateSummary();
  } catch (error) {
    state.logs = [];
    listEl.innerHTML = '<div class="empty">Impossible de charger le journal.</div>';
    updateSummary();
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
    const changes = Array.isArray(log.changes) ? log.changes : [];
    const componentChanges = Array.isArray(log.componentChanges) ? log.componentChanges : [];
    const changeRows = renderChangeRows(changes, componentChanges);

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
        <div class="log-changes">
          ${changeRows}
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
        const changedFields = Array.isArray(data.log.changedFields)
          ? data.log.changedFields
          : [];
        const changes = changedFields.map((field) => ({
          field,
          before: data.log.oldData ? data.log.oldData[field] : null,
          after: data.log.newData ? data.log.newData[field] : null
        }));
        const oldData = stringifyJson(data.log.oldData);
        const newData = stringifyJson(data.log.newData);
        content.innerHTML = `
          <div class="log-changes">
            ${renderChangeRows(changes)}
          </div>
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
