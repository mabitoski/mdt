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
