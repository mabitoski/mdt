const ldapForm = document.getElementById('ldap-form');
const ldapEnabledInput = document.getElementById('ldap-enabled');
const ldapUrlInput = document.getElementById('ldap-url');
const ldapSearchBaseInput = document.getElementById('ldap-search-base');
const ldapBindDnInput = document.getElementById('ldap-bind-dn');
const ldapBindPasswordInput = document.getElementById('ldap-bind-password');
const ldapClearPasswordInput = document.getElementById('ldap-clear-password');
const ldapSearchFilterInput = document.getElementById('ldap-search-filter');
const ldapSearchAttributesInput = document.getElementById('ldap-search-attributes');
const ldapTlsRejectInput = document.getElementById('ldap-tls-reject');
const ldapStatusEl = document.getElementById('ldap-status');
const ldapSourceEl = document.getElementById('ldap-source');
const ldapFeedbackEl = document.getElementById('ldap-feedback');
const ldapReloadBtn = document.getElementById('ldap-reload');
const ldapPasswordHint = document.getElementById('ldap-password-hint');

let currentConfig = null;

function setLdapFeedback(state, message) {
  if (!ldapFeedbackEl) {
    return;
  }
  if (state) {
    ldapFeedbackEl.dataset.state = state;
  } else {
    delete ldapFeedbackEl.dataset.state;
  }
  ldapFeedbackEl.textContent = message || '';
}

function setLdapFormDisabled(disabled) {
  if (!ldapForm) {
    return;
  }
  const fields = ldapForm.querySelectorAll('input, button');
  fields.forEach((field) => {
    field.disabled = disabled;
  });
}

function renderLdapStatus(config) {
  if (!ldapStatusEl) {
    return;
  }
  if (!config) {
    ldapStatusEl.dataset.status = 'unknown';
    ldapStatusEl.textContent = 'LDAP inconnu';
    if (ldapSourceEl) {
      ldapSourceEl.textContent = '';
    }
    return;
  }
  if (config.enabled) {
    ldapStatusEl.dataset.status = 'enabled';
    ldapStatusEl.textContent = 'LDAP actif';
  } else {
    ldapStatusEl.dataset.status = 'disabled';
    ldapStatusEl.textContent = 'LDAP inactif';
  }
  if (ldapSourceEl) {
    ldapSourceEl.textContent = config.source ? `Source: ${config.source}` : '';
  }
}

function updateLdapPasswordHint(config) {
  if (!ldapPasswordHint) {
    return;
  }
  if (config && config.bindPasswordSet) {
    ldapPasswordHint.textContent = 'Mot de passe enregistre. Laisse vide pour conserver.';
  } else {
    ldapPasswordHint.textContent = 'Aucun mot de passe enregistre.';
  }
  if (ldapClearPasswordInput) {
    ldapClearPasswordInput.disabled = !(config && config.bindPasswordSet);
    if (!config || !config.bindPasswordSet) {
      ldapClearPasswordInput.checked = false;
    }
  }
}

function applyLdapConfigToForm(config) {
  if (!config) {
    return;
  }
  if (ldapEnabledInput) {
    ldapEnabledInput.checked = Boolean(config.enabled);
  }
  if (ldapUrlInput) {
    ldapUrlInput.value = config.url || '';
  }
  if (ldapSearchBaseInput) {
    ldapSearchBaseInput.value = config.searchBase || '';
  }
  if (ldapBindDnInput) {
    ldapBindDnInput.value = config.bindDn || '';
  }
  if (ldapBindPasswordInput) {
    ldapBindPasswordInput.value = '';
  }
  if (ldapClearPasswordInput) {
    ldapClearPasswordInput.checked = false;
  }
  if (ldapSearchFilterInput) {
    ldapSearchFilterInput.value = config.searchFilter || '';
  }
  if (ldapSearchAttributesInput) {
    ldapSearchAttributesInput.value = config.searchAttributes || '';
  }
  if (ldapTlsRejectInput) {
    ldapTlsRejectInput.checked = config.tlsRejectUnauthorized !== false;
  }
  updateLdapPasswordHint(config);
}

function readLdapForm() {
  return {
    enabled: ldapEnabledInput ? ldapEnabledInput.checked : false,
    url: ldapUrlInput ? ldapUrlInput.value.trim() : '',
    searchBase: ldapSearchBaseInput ? ldapSearchBaseInput.value.trim() : '',
    bindDn: ldapBindDnInput ? ldapBindDnInput.value.trim() : '',
    bindPassword: ldapBindPasswordInput ? ldapBindPasswordInput.value : '',
    clearBindPassword: ldapClearPasswordInput ? ldapClearPasswordInput.checked : false,
    searchFilter: ldapSearchFilterInput ? ldapSearchFilterInput.value.trim() : '',
    searchAttributes: ldapSearchAttributesInput ? ldapSearchAttributesInput.value.trim() : '',
    tlsRejectUnauthorized: ldapTlsRejectInput ? ldapTlsRejectInput.checked : true
  };
}

async function loadLdapConfig() {
  if (!ldapForm) {
    return;
  }
  setLdapFeedback('info', 'Chargement de la configuration LDAP...');
  setLdapFormDisabled(true);
  try {
    const response = await fetch('/api/admin/ldap');
    if (response.status === 401) {
      window.location.href = '/login';
      return;
    }
    if (response.status === 403) {
      window.location.href = '/';
      return;
    }
    if (!response.ok) {
      throw new Error('ldap_fetch_failed');
    }
    const data = await response.json();
    if (!data.ok) {
      throw new Error('ldap_fetch_failed');
    }
    currentConfig = data.config;
    applyLdapConfigToForm(data.config);
    renderLdapStatus(data.config);
    setLdapFeedback('success', 'Configuration chargee.');
  } catch (error) {
    setLdapFeedback('error', 'Impossible de charger la configuration LDAP.');
  } finally {
    setLdapFormDisabled(false);
    if (currentConfig) {
      updateLdapPasswordHint(currentConfig);
    }
  }
}

async function saveLdapConfig(event) {
  event.preventDefault();
  if (!ldapForm) {
    return;
  }
  const payload = readLdapForm();
  if (payload.enabled && (!payload.url || !payload.searchBase)) {
    setLdapFeedback('error', 'URL et base de recherche obligatoires.');
    return;
  }
  setLdapFeedback('info', 'Enregistrement en cours...');
  setLdapFormDisabled(true);
  try {
    const response = await fetch('/api/admin/ldap', {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload)
    });
    if (response.status === 401) {
      window.location.href = '/login';
      return;
    }
    if (response.status === 403) {
      window.location.href = '/';
      return;
    }
    const data = await response.json();
    if (!response.ok || !data.ok) {
      throw new Error('ldap_save_failed');
    }
    currentConfig = data.config;
    applyLdapConfigToForm(data.config);
    renderLdapStatus(data.config);
    setLdapFeedback('success', 'Configuration LDAP enregistree.');
  } catch (error) {
    setLdapFeedback('error', "Echec lors de l'enregistrement LDAP.");
  } finally {
    setLdapFormDisabled(false);
    if (currentConfig) {
      updateLdapPasswordHint(currentConfig);
    }
  }
}

if (ldapForm) {
  ldapForm.addEventListener('submit', saveLdapConfig);
}

if (ldapReloadBtn) {
  ldapReloadBtn.addEventListener('click', () => {
    loadLdapConfig();
  });
}

if (ldapBindPasswordInput && ldapClearPasswordInput) {
  ldapBindPasswordInput.addEventListener('input', () => {
    if (ldapBindPasswordInput.value.trim()) {
      ldapClearPasswordInput.checked = false;
    }
  });
}

loadLdapConfig();

const lotCreateForm = document.getElementById('lot-create-form');
const lotSupplierInput = document.getElementById('lot-supplier');
const lotNumberInput = document.getElementById('lot-number');
const lotTargetCountInput = document.getElementById('lot-target-count');
const lotPriorityInput = document.getElementById('lot-priority');
const lotCreateSubmit = document.getElementById('lot-create-submit');
const lotReloadBtn = document.getElementById('lot-reload');
const lotFeedbackEl = document.getElementById('lot-feedback');
const lotListEl = document.getElementById('lot-list');

const lotState = {
  lots: []
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

function buildLotLabel(lot) {
  if (!lot) {
    return 'Lot';
  }
  const supplier = String(lot.supplier || '').trim();
  const lotNumber = String(lot.lotNumber || '').trim();
  if (supplier && lotNumber) {
    return `${supplier} - lot ${lotNumber}`;
  }
  return supplier || lotNumber || 'Lot';
}

function setLotFeedback(state, message) {
  if (!lotFeedbackEl) {
    return;
  }
  if (state) {
    lotFeedbackEl.dataset.state = state;
  } else {
    delete lotFeedbackEl.dataset.state;
  }
  lotFeedbackEl.textContent = message || '';
}

function setLotCreateDisabled(disabled) {
  if (!lotCreateForm) {
    return;
  }
  const fields = lotCreateForm.querySelectorAll('input, button');
  fields.forEach((field) => {
    field.disabled = disabled;
  });
}

function renderLots() {
  if (!lotListEl) {
    return;
  }
  const lots = Array.isArray(lotState.lots) ? lotState.lots : [];
  if (!lots.length) {
    lotListEl.innerHTML = '<p class="admin-note">Aucun lot configure.</p>';
    return;
  }

  lotListEl.innerHTML = lots
    .map((lot) => {
      const lotId = escapeHtml(lot.id || '');
      const label = escapeHtml(buildLotLabel(lot));
      const produced = Number.isFinite(lot.producedCount) ? lot.producedCount : 0;
      const target = Number.isFinite(lot.targetCount) ? lot.targetCount : 0;
      const percent = Number.isFinite(lot.progressPercent) ? lot.progressPercent : 0;
      const remaining = Number.isFinite(lot.remainingCount) ? lot.remainingCount : Math.max(target - produced, 0);
      const assignments = Array.isArray(lot.assignments) ? lot.assignments : [];
      const assignmentHtml = assignments.length
        ? assignments
          .map(
            (item) => `
              <span class="lot-chip">
                ${escapeHtml(item.technician || item.technicianKey || '')}
                <button type="button" class="lot-chip-remove" data-action="remove-assignment" data-lot-id="${lotId}" data-tech-key="${escapeHtml(item.technicianKey || '')}">x</button>
              </span>
            `
          )
          .join('')
        : '<span class="admin-note">Aucune assignation.</span>';

      return `
        <article class="lot-item" data-lot-id="${lotId}">
          <div class="lot-item-head">
            <h3>${label}</h3>
            <span class="admin-status" data-status="${lot.isPaused ? 'disabled' : 'enabled'}">${lot.isPaused ? 'Pause' : 'Actif'}</span>
          </div>
          <p class="admin-sub">Production: <strong>${produced}/${target}</strong> (${percent}%) - reste ${remaining}</p>
          <div class="admin-grid lot-grid">
            <label class="admin-field">
              <span>Pieces cible</span>
              <input type="number" min="1" step="1" data-field="targetCount" value="${escapeHtml(target)}" />
            </label>
            <label class="admin-field">
              <span>Priorite</span>
              <input type="number" min="1" step="1" data-field="priority" value="${escapeHtml(lot.priority || 100)}" />
            </label>
            <label class="admin-check">
              <input type="checkbox" data-field="isPaused" ${lot.isPaused ? 'checked' : ''} />
              <span>Pause de production</span>
            </label>
          </div>
          <div class="admin-actions">
            <button type="button" class="admin-primary" data-action="save-lot" data-lot-id="${lotId}">Enregistrer</button>
          </div>
          <form class="lot-assignment-form" data-lot-id="${lotId}">
            <label class="admin-field">
              <span>Assigner un technicien</span>
              <input type="text" name="technician" maxlength="64" autocomplete="off" />
            </label>
            <button type="submit" class="admin-secondary">Ajouter</button>
          </form>
          <div class="lot-chip-list">${assignmentHtml}</div>
        </article>
      `;
    })
    .join('');
}

async function loadLots() {
  if (!lotListEl) {
    return;
  }
  setLotFeedback('info', 'Chargement des lots...');
  try {
    const response = await fetch('/api/lots');
    if (response.status === 401) {
      window.location.href = '/login';
      return;
    }
    if (response.status === 403) {
      window.location.href = '/';
      return;
    }
    if (!response.ok) {
      throw new Error('lot_fetch_failed');
    }
    const data = await response.json();
    if (!data.ok) {
      throw new Error('lot_fetch_failed');
    }
    lotState.lots = Array.isArray(data.lots) ? data.lots : [];
    renderLots();
    setLotFeedback('success', 'Lots charges.');
  } catch (error) {
    setLotFeedback('error', 'Impossible de charger les lots.');
  }
}

async function createLot(event) {
  event.preventDefault();
  if (!lotCreateForm) {
    return;
  }
  const supplier = lotSupplierInput ? lotSupplierInput.value.trim() : '';
  const lotNumber = lotNumberInput ? lotNumberInput.value.trim() : '';
  const targetCountRaw = lotTargetCountInput ? Number.parseInt(lotTargetCountInput.value, 10) : 0;
  const priorityRaw = lotPriorityInput ? Number.parseInt(lotPriorityInput.value, 10) : 100;
  if (!supplier || !lotNumber || !Number.isFinite(targetCountRaw) || targetCountRaw <= 0) {
    setLotFeedback('error', 'Renseigne fournisseur, numero de lot et nombre de pieces.');
    return;
  }

  const payload = {
    supplier,
    lotNumber,
    targetCount: targetCountRaw,
    priority: Number.isFinite(priorityRaw) && priorityRaw > 0 ? priorityRaw : 100
  };

  setLotCreateDisabled(true);
  setLotFeedback('info', 'Creation du lot...');
  try {
    const response = await fetch('/api/lots', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload)
    });
    if (response.status === 401) {
      window.location.href = '/login';
      return;
    }
    if (response.status === 403) {
      window.location.href = '/';
      return;
    }
    const data = await response.json();
    if (!response.ok || !data.ok) {
      throw new Error(data.error || 'lot_create_failed');
    }
    lotCreateForm.reset();
    if (lotPriorityInput) {
      lotPriorityInput.value = '100';
    }
    setLotFeedback('success', 'Lot cree.');
    await loadLots();
  } catch (error) {
    setLotFeedback('error', 'Creation du lot impossible.');
  } finally {
    setLotCreateDisabled(false);
  }
}

async function saveLot(lotId) {
  if (!lotListEl || !lotId) {
    return;
  }
  const safeLotId = window.CSS && CSS.escape ? CSS.escape(lotId) : String(lotId).replace(/"/g, '\\"');
  const root = lotListEl.querySelector(`.lot-item[data-lot-id="${safeLotId}"]`);
  if (!root) {
    return;
  }
  const targetCountInput = root.querySelector('input[data-field="targetCount"]');
  const priorityInput = root.querySelector('input[data-field="priority"]');
  const pausedInput = root.querySelector('input[data-field="isPaused"]');
  const targetCount = targetCountInput ? Number.parseInt(targetCountInput.value, 10) : NaN;
  const priority = priorityInput ? Number.parseInt(priorityInput.value, 10) : NaN;
  const isPaused = pausedInput ? pausedInput.checked : false;
  if (!Number.isFinite(targetCount) || targetCount <= 0) {
    setLotFeedback('error', 'Nombre de pieces invalide.');
    return;
  }
  const payload = {
    targetCount,
    priority: Number.isFinite(priority) && priority > 0 ? priority : 100,
    isPaused
  };

  setLotFeedback('info', 'Mise a jour du lot...');
  try {
    const response = await fetch(`/api/lots/${encodeURIComponent(lotId)}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload)
    });
    if (response.status === 401) {
      window.location.href = '/login';
      return;
    }
    if (response.status === 403) {
      window.location.href = '/';
      return;
    }
    const data = await response.json();
    if (!response.ok || !data.ok) {
      throw new Error(data.error || 'lot_update_failed');
    }
    setLotFeedback('success', 'Lot mis a jour.');
    await loadLots();
  } catch (error) {
    setLotFeedback('error', 'Mise a jour impossible.');
  }
}

async function addAssignment(lotId, technician) {
  const name = String(technician || '').trim();
  if (!lotId || !name) {
    return;
  }
  setLotFeedback('info', 'Ajout de l assignation...');
  try {
    const response = await fetch(`/api/lots/${encodeURIComponent(lotId)}/assignments`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ technician: name })
    });
    if (response.status === 401) {
      window.location.href = '/login';
      return;
    }
    if (response.status === 403) {
      window.location.href = '/';
      return;
    }
    const data = await response.json();
    if (!response.ok || !data.ok) {
      throw new Error(data.error || 'assignment_add_failed');
    }
    setLotFeedback('success', 'Technicien assigne.');
    await loadLots();
  } catch (error) {
    setLotFeedback('error', "Impossible d'ajouter l'assignation.");
  }
}

async function removeAssignment(lotId, techKey) {
  if (!lotId || !techKey) {
    return;
  }
  setLotFeedback('info', 'Suppression de l assignation...');
  try {
    const response = await fetch(
      `/api/lots/${encodeURIComponent(lotId)}/assignments/${encodeURIComponent(techKey)}`,
      { method: 'DELETE' }
    );
    if (response.status === 401) {
      window.location.href = '/login';
      return;
    }
    if (response.status === 403) {
      window.location.href = '/';
      return;
    }
    const data = await response.json();
    if (!response.ok || !data.ok) {
      throw new Error(data.error || 'assignment_remove_failed');
    }
    setLotFeedback('success', 'Assignation supprimee.');
    await loadLots();
  } catch (error) {
    setLotFeedback('error', "Impossible de supprimer l'assignation.");
  }
}

if (lotCreateForm) {
  lotCreateForm.addEventListener('submit', createLot);
}

if (lotReloadBtn) {
  lotReloadBtn.addEventListener('click', () => {
    loadLots();
  });
}

if (lotListEl) {
  lotListEl.addEventListener('click', (event) => {
    const saveBtn = event.target.closest('[data-action="save-lot"]');
    if (saveBtn && saveBtn.dataset.lotId) {
      saveLot(saveBtn.dataset.lotId);
      return;
    }
    const removeBtn = event.target.closest('[data-action="remove-assignment"]');
    if (removeBtn && removeBtn.dataset.lotId && removeBtn.dataset.techKey) {
      removeAssignment(removeBtn.dataset.lotId, removeBtn.dataset.techKey);
    }
  });

  lotListEl.addEventListener('submit', (event) => {
    const form = event.target.closest('.lot-assignment-form');
    if (!form) {
      return;
    }
    event.preventDefault();
    const lotId = form.dataset.lotId;
    const input = form.querySelector('input[name="technician"]');
    const technician = input ? input.value.trim() : '';
    if (!technician) {
      setLotFeedback('error', 'Nom technicien requis.');
      return;
    }
    addAssignment(lotId, technician).then(() => {
      if (input) {
        input.value = '';
      }
    });
  });
}

loadLots();

const weeklyRecapStatusEl = document.getElementById('weekly-recap-status');
const weeklyRecapRecipientsEl = document.getElementById('weekly-recap-recipients');
const weeklyRecapScheduleEl = document.getElementById('weekly-recap-schedule');
const weeklyRecapThresholdEl = document.getElementById('weekly-recap-threshold');
const weeklyRecapLastRunEl = document.getElementById('weekly-recap-last-run');
const weeklyRecapFeedbackEl = document.getElementById('weekly-recap-feedback');
const weeklyRecapReloadBtn = document.getElementById('weekly-recap-reload');
const weeklyRecapSendBtn = document.getElementById('weekly-recap-send');
const weeklyRecapKpisEl = document.getElementById('weekly-recap-kpis');
const weeklyRecapOperatorsEl = document.getElementById('weekly-recap-operators');
const weeklyRecapAlertsEl = document.getElementById('weekly-recap-alerts');
const weeklyRecapRegressionsEl = document.getElementById('weekly-recap-regressions');

function setWeeklyRecapFeedback(state, message) {
  if (!weeklyRecapFeedbackEl) {
    return;
  }
  if (state) {
    weeklyRecapFeedbackEl.dataset.state = state;
  } else {
    delete weeklyRecapFeedbackEl.dataset.state;
  }
  weeklyRecapFeedbackEl.textContent = message || '';
}

function setWeeklyRecapDisabled(disabled) {
  [weeklyRecapReloadBtn, weeklyRecapSendBtn].forEach((button) => {
    if (button) {
      button.disabled = disabled;
    }
  });
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
    month: '2-digit',
    year: 'numeric',
    hour: '2-digit',
    minute: '2-digit'
  });
}

function renderWeeklyRecapKpis(summary) {
  if (!weeklyRecapKpisEl) {
    return;
  }
  if (!summary) {
    weeklyRecapKpisEl.innerHTML = '';
    return;
  }
  const cards = [
    ['Periode', summary.periodLabel || '--'],
    ['Parc suivi', summary.snapshot ? summary.snapshot.totalMachines : 0],
    ['Alertes actives', summary.snapshot ? summary.snapshot.batteryAlertsActive : 0],
    ['NOK actifs', summary.snapshot ? summary.snapshot.nokMachinesActive : 0],
    ['Regressions semaine', summary.weekly ? summary.weekly.regressionCount : 0],
    ['Corrections semaine', summary.weekly ? summary.weekly.correctedCount : 0]
  ];
  weeklyRecapKpisEl.innerHTML = cards
    .map(
      ([label, value]) => `
        <article class="recap-kpi-card">
          <span>${escapeHtml(label)}</span>
          <strong>${escapeHtml(value)}</strong>
        </article>
      `
    )
    .join('');
}

function renderWeeklyRecapOperatorActivity(items) {
  if (!weeklyRecapOperatorsEl) {
    return;
  }
  const list = Array.isArray(items) ? items : [];
  if (!list.length) {
    weeklyRecapOperatorsEl.innerHTML = '<p class="admin-note">Aucune action manuelle sur la periode.</p>';
    return;
  }
  weeklyRecapOperatorsEl.innerHTML = `
    <table class="recap-table">
      <thead>
        <tr>
          <th>Operateur</th>
          <th>Actions</th>
          <th>Composants</th>
          <th>Commentaires</th>
          <th>Report 0</th>
        </tr>
      </thead>
      <tbody>
        ${list
          .map(
            (item) => `
              <tr>
                <td>${escapeHtml(item.actor || '--')}</td>
                <td>${escapeHtml(item.totalActions || 0)}</td>
                <td>${escapeHtml(item.componentUpdates || 0)}</td>
                <td>${escapeHtml(item.commentsCount || 0)}</td>
                <td>${escapeHtml(item.reportZeroCount || 0)}</td>
              </tr>
            `
          )
          .join('')}
      </tbody>
    </table>
  `;
}

function renderWeeklyRecapBatteryAlerts(items) {
  if (!weeklyRecapAlertsEl) {
    return;
  }
  const list = Array.isArray(items) ? items : [];
  if (!list.length) {
    weeklyRecapAlertsEl.innerHTML = '<p class="admin-note">Aucune alerte batterie active.</p>';
    return;
  }
  weeklyRecapAlertsEl.innerHTML = `
    <ul class="recap-list">
      ${list
        .map(
          (item) => `
            <li>
              <strong>${escapeHtml(item.label || item.machineKey || '--')}</strong>
              <span>Tech ${escapeHtml(item.technician || '--')} · Batterie ${escapeHtml(item.batteryHealth || 0)}% · Vu ${escapeHtml(
                formatDateTime(item.lastSeen)
              )}</span>
            </li>
          `
        )
        .join('')}
    </ul>
  `;
}

function renderWeeklyRecapRegressions(items) {
  if (!weeklyRecapRegressionsEl) {
    return;
  }
  const list = Array.isArray(items) ? items : [];
  if (!list.length) {
    weeklyRecapRegressionsEl.innerHTML = '<p class="admin-note">Aucune regression sur la periode.</p>';
    return;
  }
  weeklyRecapRegressionsEl.innerHTML = `
    <ul class="recap-list">
      ${list
        .map(
          (item) => `
            <li>
              <strong>${escapeHtml(item.machineKey || '--')}</strong>
              <span>${escapeHtml(item.componentLabel || '--')} · ${escapeHtml(item.actor || '--')} · ${escapeHtml(
                item.sourceLabel || '--'
              )} · ${escapeHtml(formatDateTime(item.eventTime))}</span>
            </li>
          `
        )
        .join('')}
    </ul>
  `;
}

function renderWeeklyRecap(recap) {
  const source = recap && typeof recap === 'object' ? recap : null;
  const enabled = Boolean(source && source.enabled);
  if (weeklyRecapStatusEl) {
    weeklyRecapStatusEl.dataset.status = enabled ? 'enabled' : 'disabled';
    weeklyRecapStatusEl.textContent = enabled ? 'Actif' : 'Desactive';
  }
  if (weeklyRecapRecipientsEl) {
    weeklyRecapRecipientsEl.textContent =
      source && Array.isArray(source.recipients) && source.recipients.length
        ? source.recipients.join(', ')
        : 'Aucun destinataire configure';
  }
  if (weeklyRecapScheduleEl) {
    weeklyRecapScheduleEl.textContent = source && source.schedule ? source.schedule.label || '--' : '--';
  }
  if (weeklyRecapThresholdEl) {
    weeklyRecapThresholdEl.textContent = source ? `< ${source.batteryThreshold || 78}%` : '--';
  }
  if (weeklyRecapLastRunEl) {
    const latestRun = source && source.latestRun ? source.latestRun : null;
    if (!latestRun) {
      weeklyRecapLastRunEl.textContent = 'Aucun envoi enregistre';
    } else {
      const status = latestRun.status || 'inconnu';
      weeklyRecapLastRunEl.textContent = `${formatDateTime(latestRun.sentAt)} · ${status}`;
    }
  }

  const summary = source && source.preview ? source.preview : null;
  renderWeeklyRecapKpis(summary);
  renderWeeklyRecapOperatorActivity(summary ? summary.operatorActivity : []);
  renderWeeklyRecapBatteryAlerts(summary ? summary.activeBatteryAlerts : []);
  renderWeeklyRecapRegressions(summary ? summary.recentRegressions : []);
}

async function loadWeeklyRecap() {
  if (!weeklyRecapStatusEl) {
    return;
  }
  setWeeklyRecapFeedback('info', 'Chargement du recap...');
  setWeeklyRecapDisabled(true);
  try {
    const response = await fetch('/api/admin/weekly-recap');
    if (response.status === 401) {
      window.location.href = '/login';
      return;
    }
    if (response.status === 403) {
      window.location.href = '/';
      return;
    }
    const data = await response.json();
    if (!response.ok || !data.ok) {
      throw new Error(data.error || 'weekly_recap_load_failed');
    }
    renderWeeklyRecap(data.recap || null);
    setWeeklyRecapFeedback('success', 'Recap charge.');
  } catch (error) {
    setWeeklyRecapFeedback('error', 'Impossible de charger le recap hebdo.');
  } finally {
    setWeeklyRecapDisabled(false);
  }
}

async function sendWeeklyRecapNow() {
  if (!weeklyRecapSendBtn) {
    return;
  }
  setWeeklyRecapFeedback('info', 'Envoi du recap en cours...');
  setWeeklyRecapDisabled(true);
  try {
    const response = await fetch('/api/admin/weekly-recap/send', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({})
    });
    if (response.status === 401) {
      window.location.href = '/login';
      return;
    }
    if (response.status === 403) {
      window.location.href = '/';
      return;
    }
    const data = await response.json();
    if (!response.ok || !data.ok) {
      throw new Error(data.error || 'weekly_recap_send_failed');
    }
    setWeeklyRecapFeedback('success', `Recap envoye a ${Array.isArray(data.recipients) ? data.recipients.length : 0} destinataire(s).`);
    await loadWeeklyRecap();
  } catch (error) {
    setWeeklyRecapFeedback('error', "Impossible d'envoyer le recap.");
  } finally {
    setWeeklyRecapDisabled(false);
  }
}

if (weeklyRecapReloadBtn) {
  weeklyRecapReloadBtn.addEventListener('click', () => {
    loadWeeklyRecap();
  });
}

if (weeklyRecapSendBtn) {
  weeklyRecapSendBtn.addEventListener('click', () => {
    sendWeeklyRecapNow();
  });
}

loadWeeklyRecap();

const mdtBetaStatusEl = document.getElementById('mdt-beta-status');
const mdtBetaModeEl = document.getElementById('mdt-beta-mode');
const mdtBetaDefaultSourceEl = document.getElementById('mdt-beta-default-source');
const mdtBetaGroupEl = document.getElementById('mdt-beta-group');
const mdtBetaScriptsFolderEl = document.getElementById('mdt-beta-scripts-folder');
const mdtBetaAgentEl = document.getElementById('mdt-beta-agent');
const mdtBetaQueueEl = document.getElementById('mdt-beta-queue');
const mdtBetaFeedbackEl = document.getElementById('mdt-beta-feedback');
const mdtBetaReloadBtn = document.getElementById('mdt-beta-reload');
const mdtBetaTechForm = document.getElementById('mdt-beta-tech-form');
const mdtBetaTechNameInput = document.getElementById('mdt-beta-tech-name');
const mdtBetaTechSourceInput = document.getElementById('mdt-beta-tech-source');
const mdtBetaTechSubmit = document.getElementById('mdt-beta-tech-submit');
const mdtBetaTechListEl = document.getElementById('mdt-beta-tech-list');

function setMdtBetaFeedback(state, message) {
  if (!mdtBetaFeedbackEl) {
    return;
  }
  if (state) {
    mdtBetaFeedbackEl.dataset.state = state;
  } else {
    delete mdtBetaFeedbackEl.dataset.state;
  }
  mdtBetaFeedbackEl.textContent = message || '';
}

function setMdtBetaDisabled(disabled) {
  [mdtBetaReloadBtn, mdtBetaTechSubmit].forEach((button) => {
    if (button) {
      button.disabled = disabled;
    }
  });
  if (mdtBetaTechForm) {
    mdtBetaTechForm.querySelectorAll('input').forEach((input) => {
      input.disabled = disabled;
    });
  }
}

function getMdtBetaStatusMeta(status, enabled) {
  if (!enabled) {
    return { tone: 'disabled', label: 'Desactive' };
  }
  switch (status) {
    case 'ready':
      return { tone: 'enabled', label: 'Pret' };
    case 'failed':
      return { tone: 'disabled', label: 'Echec' };
    case 'provisioning':
      return { tone: 'unknown', label: 'Provisioning' };
    case 'queued':
      return { tone: 'unknown', label: 'En attente' };
    default:
      return { tone: 'unknown', label: 'Actif' };
  }
}

function renderMdtBetaTechnicians(automation) {
  if (!mdtBetaTechListEl) {
    return;
  }
  const list = automation && Array.isArray(automation.technicians) ? automation.technicians : [];
  if (!list.length) {
    mdtBetaTechListEl.innerHTML = '<p class="admin-note">Aucun technicien beta provisionne pour le moment.</p>';
    return;
  }
  mdtBetaTechListEl.innerHTML = list
    .map((item) => {
      const statusMeta = getMdtBetaStatusMeta(item.status, automation && automation.enabled);
      const latestJob = item.latestJob || null;
      const lastRun = latestJob ? formatDateTime(latestJob.finishedAt || latestJob.startedAt || latestJob.createdAt) : '--';
      const backupPath = item.lastResult && item.lastResult.backupPath ? item.lastResult.backupPath : '--';
      const controlPath = item.lastResult && item.lastResult.controlPath ? item.lastResult.controlPath : '--';
      return `
        <article class="lot-item" data-tech-id="${escapeHtml(item.id || '')}">
          <div class="lot-item-head">
            <h3>${escapeHtml(item.displayName || item.slug || 'Technicien')}</h3>
            <span class="admin-status" data-status="${statusMeta.tone}">${escapeHtml(statusMeta.label)}</span>
          </div>
          <p class="admin-sub">
            Source <strong>${escapeHtml(item.sourceTaskSequenceId || '--')}</strong> · Cible
            <strong>${escapeHtml(item.betaTaskSequenceId || '--')}</strong>
          </p>
          <div class="admin-grid lot-grid">
            <div class="admin-field">
              <span>Nom TS beta</span>
              <p>${escapeHtml(item.betaTaskSequenceName || '--')}</p>
            </div>
            <div class="admin-field">
              <span>Dernier passage</span>
              <p>${escapeHtml(lastRun)}</p>
            </div>
            <div class="admin-field">
              <span>Dossier control</span>
              <p>${escapeHtml(controlPath)}</p>
            </div>
            <div class="admin-field">
              <span>Backup MDT</span>
              <p>${escapeHtml(backupPath)}</p>
            </div>
          </div>
          ${
            item.lastError
              ? `<p class="admin-note">Derniere erreur: ${escapeHtml(item.lastError)}</p>`
              : ''
          }
          <div class="admin-actions">
            <button type="button" class="admin-secondary" data-action="reprovision-tech" data-tech-id="${escapeHtml(item.id || '')}">
              Rejouer le provisioning
            </button>
          </div>
        </article>
      `;
    })
    .join('');
}

function renderMdtBetaAutomation(automation) {
  if (!mdtBetaStatusEl) {
    return;
  }
  const enabled = Boolean(automation && automation.enabled);
  const agent = automation && automation.agent ? automation.agent : null;
  const queue = automation && automation.queue ? automation.queue : null;
  const statusMeta = enabled
    ? agent
      ? getMdtBetaStatusMeta(agent.status, true)
      : { tone: 'unknown', label: 'Sans agent' }
    : { tone: 'disabled', label: 'Desactive' };
  mdtBetaStatusEl.dataset.status = statusMeta.tone;
  mdtBetaStatusEl.textContent = statusMeta.label;
  if (mdtBetaModeEl) {
    mdtBetaModeEl.textContent = enabled ? 'Beta uniquement, aucun impact prod' : 'Automatisation inactive';
  }
  if (mdtBetaDefaultSourceEl) {
    mdtBetaDefaultSourceEl.textContent =
      automation && automation.defaults ? automation.defaults.sourceTaskSequenceId || '--' : '--';
  }
  if (mdtBetaGroupEl) {
    mdtBetaGroupEl.textContent =
      automation && automation.defaults ? automation.defaults.taskSequenceGroupName || '--' : '--';
  }
  if (mdtBetaScriptsFolderEl) {
    mdtBetaScriptsFolderEl.textContent =
      automation && automation.defaults ? automation.defaults.scriptsFolder || '--' : '--';
  }
  if (mdtBetaAgentEl) {
    mdtBetaAgentEl.textContent = agent
      ? `${agent.hostname || agent.agentId || '--'} · vu ${formatDateTime(agent.lastSeenAt)}`
      : 'Aucun heartbeat agent';
  }
  if (mdtBetaQueueEl) {
    mdtBetaQueueEl.textContent = queue
      ? `${queue.queuedCount || 0} attente · ${queue.runningCount || 0} en cours · ${queue.failedCount || 0} en erreur`
      : '--';
  }
  if (mdtBetaTechSourceInput && automation && automation.defaults && !mdtBetaTechSourceInput.value.trim()) {
    mdtBetaTechSourceInput.value = automation.defaults.sourceTaskSequenceId || '';
  }
  renderMdtBetaTechnicians(automation);
}

async function loadMdtBetaAutomation() {
  if (!mdtBetaStatusEl) {
    return;
  }
  setMdtBetaFeedback('info', 'Chargement de l automatisation MDT beta...');
  setMdtBetaDisabled(true);
  try {
    const response = await fetch('/api/admin/mdt-beta');
    if (response.status === 401) {
      window.location.href = '/login';
      return;
    }
    if (response.status === 403) {
      window.location.href = '/';
      return;
    }
    const data = await response.json();
    if (!response.ok || !data.ok) {
      throw new Error(data.error || 'mdt_beta_load_failed');
    }
    renderMdtBetaAutomation(data.automation || null);
    setMdtBetaFeedback('success', 'Automatisation MDT beta chargee.');
  } catch (error) {
    setMdtBetaFeedback('error', "Impossible de charger l'automatisation MDT beta.");
  } finally {
    setMdtBetaDisabled(false);
  }
}

async function createMdtBetaTechnician(event) {
  event.preventDefault();
  if (!mdtBetaTechForm) {
    return;
  }
  const displayName = mdtBetaTechNameInput ? mdtBetaTechNameInput.value.trim() : '';
  const sourceTaskSequenceId = mdtBetaTechSourceInput ? mdtBetaTechSourceInput.value.trim() : '';
  if (!displayName || !sourceTaskSequenceId) {
    setMdtBetaFeedback('error', 'Renseigne le nom technicien et la task sequence source.');
    return;
  }
  setMdtBetaFeedback('info', 'Creation du technicien beta en cours...');
  setMdtBetaDisabled(true);
  try {
    const response = await fetch('/api/admin/mdt-beta/technicians', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ displayName, sourceTaskSequenceId })
    });
    if (response.status === 401) {
      window.location.href = '/login';
      return;
    }
    if (response.status === 403) {
      window.location.href = '/';
      return;
    }
    const data = await response.json();
    if (!response.ok || !data.ok) {
      throw new Error(data.error || 'mdt_beta_create_failed');
    }
    if (mdtBetaTechNameInput) {
      mdtBetaTechNameInput.value = '';
    }
    renderMdtBetaAutomation(data.automation || null);
    setMdtBetaFeedback('success', 'Technicien beta cree et job queue.');
  } catch (error) {
    setMdtBetaFeedback('error', 'Creation beta impossible.');
  } finally {
    setMdtBetaDisabled(false);
  }
}

async function reprovisionMdtBetaTechnician(technicianId) {
  if (!technicianId) {
    return;
  }
  setMdtBetaFeedback('info', 'Reprovisionnement beta en cours...');
  setMdtBetaDisabled(true);
  try {
    const response = await fetch(`/api/admin/mdt-beta/technicians/${encodeURIComponent(technicianId)}/reprovision`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({})
    });
    if (response.status === 401) {
      window.location.href = '/login';
      return;
    }
    if (response.status === 403) {
      window.location.href = '/';
      return;
    }
    const data = await response.json();
    if (!response.ok || !data.ok) {
      throw new Error(data.error || 'mdt_beta_reprovision_failed');
    }
    renderMdtBetaAutomation(data.automation || null);
    setMdtBetaFeedback('success', 'Job beta reprovisionne.');
  } catch (error) {
    setMdtBetaFeedback('error', 'Reprovisionnement beta impossible.');
  } finally {
    setMdtBetaDisabled(false);
  }
}

if (mdtBetaReloadBtn) {
  mdtBetaReloadBtn.addEventListener('click', () => {
    loadMdtBetaAutomation();
  });
}

if (mdtBetaTechForm) {
  mdtBetaTechForm.addEventListener('submit', createMdtBetaTechnician);
}

if (mdtBetaTechListEl) {
  mdtBetaTechListEl.addEventListener('click', (event) => {
    const button = event.target.closest('[data-action="reprovision-tech"]');
    if (!button || !button.dataset.techId) {
      return;
    }
    reprovisionMdtBetaTechnician(button.dataset.techId);
  });
}

loadMdtBetaAutomation();
