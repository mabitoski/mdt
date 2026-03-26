const palletEntryForm = document.getElementById('pallet-entry-form');
const palletEntryFileInput = document.getElementById('pallet-entry-file');
const palletEntryFeedback = document.getElementById('pallet-entry-feedback');
const palletExitForm = document.getElementById('pallet-exit-form');
const palletExitFileInput = document.getElementById('pallet-exit-file');
const palletExitFeedback = document.getElementById('pallet-exit-feedback');
const palletReloadBtn = document.getElementById('pallet-reload');
const palletListEl = document.getElementById('pallet-list');
const palletImportListEl = document.getElementById('pallet-import-list');
const palletWarningBox = document.getElementById('pallet-warning-box');
const palletWarningList = document.getElementById('pallet-warning-list');
const palletTotalCountEl = document.getElementById('pallet-total-count');
const palletSerialCountEl = document.getElementById('pallet-serial-count');
const palletLinkedCountEl = document.getElementById('pallet-linked-count');
const palletLastImportEl = document.getElementById('pallet-last-import');

const state = {
  pallets: [],
  recentImports: [],
  lastWarnings: []
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
    year: 'numeric',
    hour: '2-digit',
    minute: '2-digit'
  });
}

function formatImportLabel(item) {
  if (!item) {
    return '--';
  }
  return item.importTypeLabel || (item.importType === 'exit' ? 'Sortie' : 'Entree');
}

function formatPalletLabel(item) {
  if (!item) {
    return 'Palette inconnue';
  }
  const code = String(item.code || '').trim();
  const statusLabel = String(item.statusLabel || '').trim();
  if (code && statusLabel) {
    return `${code} - ${statusLabel}`;
  }
  return code || item.label || 'Palette inconnue';
}

function setFeedback(target, feedbackState, message) {
  if (!target) {
    return;
  }
  if (feedbackState) {
    target.dataset.state = feedbackState;
  } else {
    delete target.dataset.state;
  }
  target.textContent = message || '';
}

function setFormDisabled(form, disabled) {
  if (!form) {
    return;
  }
  const fields = form.querySelectorAll('input, button');
  fields.forEach((field) => {
    field.disabled = disabled;
  });
}

function readFileAsText(file) {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => resolve(typeof reader.result === 'string' ? reader.result : '');
    reader.onerror = () => reject(new Error('file_read_failed'));
    reader.readAsText(file);
  });
}

function applyPayload(data, warnings = null) {
  state.pallets = Array.isArray(data && data.pallets) ? data.pallets : [];
  state.recentImports = Array.isArray(data && data.recentImports) ? data.recentImports : [];
  state.lastWarnings = Array.isArray(warnings)
    ? warnings
    : Array.isArray(data && data.warnings)
      ? data.warnings
      : [];
  renderSummary();
  renderPallets();
  renderImports();
  renderWarnings();
}

function renderSummary() {
  const pallets = Array.isArray(state.pallets) ? state.pallets : [];
  const recentImports = Array.isArray(state.recentImports) ? state.recentImports : [];
  const totalSerials = pallets.reduce((sum, item) => sum + (Number.isFinite(item.totalCount) ? item.totalCount : 0), 0);
  const linked = pallets.reduce((sum, item) => sum + (Number.isFinite(item.linkedCount) ? item.linkedCount : 0), 0);
  const latestImport = recentImports.length ? recentImports[0] : null;

  if (palletTotalCountEl) {
    palletTotalCountEl.textContent = String(pallets.length);
  }
  if (palletSerialCountEl) {
    palletSerialCountEl.textContent = String(totalSerials);
  }
  if (palletLinkedCountEl) {
    palletLinkedCountEl.textContent = String(linked);
  }
  if (palletLastImportEl) {
    palletLastImportEl.textContent = latestImport ? formatDateTime(latestImport.createdAt) : '--';
  }
}

function renderPallets() {
  if (!palletListEl) {
    return;
  }
  const pallets = Array.isArray(state.pallets) ? state.pallets : [];
  if (!pallets.length) {
    palletListEl.innerHTML = '<p class="admin-note">Aucune palette importee pour le moment.</p>';
    return;
  }

  palletListEl.innerHTML = pallets
    .map((item) => {
      const label = escapeHtml(formatPalletLabel(item));
      const status = escapeHtml(item.statusLabel || '--');
      const totalCount = Number.isFinite(item.totalCount) ? item.totalCount : 0;
      const entryCount = Number.isFinite(item.entryCount) ? item.entryCount : 0;
      const exitCount = Number.isFinite(item.exitCount) ? item.exitCount : 0;
      const linkedCount = Number.isFinite(item.linkedCount) ? item.linkedCount : 0;
      const movementClass = item.status === 'exit' ? 'is-exit' : 'is-entry';

      return `
        <article class="pallet-card">
          <div class="pallet-card-head">
            <h3>${label}</h3>
            <span class="pallet-badge ${movementClass}">${status}</span>
          </div>
          <p class="admin-sub">Dernier mouvement: <strong>${escapeHtml(formatDateTime(item.lastMovementAt))}</strong></p>
          <div class="pallet-stats">
            <div class="pallet-stat"><span>Serials</span><strong>${totalCount}</strong></div>
            <div class="pallet-stat"><span>Entrees</span><strong>${entryCount}</strong></div>
            <div class="pallet-stat"><span>Sorties</span><strong>${exitCount}</strong></div>
            <div class="pallet-stat"><span>Rattaches MDT</span><strong>${linkedCount}</strong></div>
          </div>
        </article>
      `;
    })
    .join('');
}

function renderImports() {
  if (!palletImportListEl) {
    return;
  }
  const imports = Array.isArray(state.recentImports) ? state.recentImports : [];
  if (!imports.length) {
    palletImportListEl.innerHTML = '<p class="admin-note">Aucun import palette enregistre.</p>';
    return;
  }

  palletImportListEl.innerHTML = imports
    .map((item) => {
      const typeLabel = escapeHtml(formatImportLabel(item));
      const fileName = escapeHtml(item.fileName || '--');
      const rowCount = Number.isFinite(item.rowCount) ? item.rowCount : 0;
      const appliedCount = Number.isFinite(item.appliedCount) ? item.appliedCount : 0;
      const skippedCount = Number.isFinite(item.skippedCount) ? item.skippedCount : 0;
      const warningCount =
        item.summary && Number.isFinite(item.summary.errorCount) ? item.summary.errorCount : skippedCount;
      const movementClass = item.importType === 'exit' ? 'is-exit' : 'is-entry';
      return `
        <article class="pallet-import-item">
          <div class="pallet-card-head">
            <h3>${fileName}</h3>
            <span class="pallet-badge ${movementClass}">${typeLabel}</span>
          </div>
          <p class="admin-sub">
            ${escapeHtml(formatDateTime(item.createdAt))} · ${escapeHtml(item.createdBy || 'systeme')}
          </p>
          <div class="pallet-stats">
            <div class="pallet-stat"><span>Lignes</span><strong>${rowCount}</strong></div>
            <div class="pallet-stat"><span>Appliquees</span><strong>${appliedCount}</strong></div>
            <div class="pallet-stat"><span>Ignorees</span><strong>${skippedCount}</strong></div>
            <div class="pallet-stat"><span>Warnings</span><strong>${warningCount}</strong></div>
          </div>
        </article>
      `;
    })
    .join('');
}

function renderWarnings() {
  if (!palletWarningBox || !palletWarningList) {
    return;
  }
  const warnings = Array.isArray(state.lastWarnings) ? state.lastWarnings : [];
  palletWarningBox.hidden = warnings.length === 0;
  if (!warnings.length) {
    palletWarningList.innerHTML = '';
    return;
  }
  palletWarningList.innerHTML = warnings
    .map((warning) => {
      const line = Number.isFinite(warning.line) ? `Ligne ${warning.line}` : 'Ligne ?';
      const serial = warning.serialNumber ? ` · ${escapeHtml(warning.serialNumber)}` : '';
      return `<li><strong>${escapeHtml(line)}</strong>${serial} · ${escapeHtml(warning.error || 'Warning')}</li>`;
    })
    .join('');
}

async function loadPallets() {
  if (palletListEl) {
    palletListEl.innerHTML = '<p class="admin-note">Chargement des palettes...</p>';
  }
  if (palletImportListEl) {
    palletImportListEl.innerHTML = '<p class="admin-note">Chargement des imports...</p>';
  }
  try {
    const response = await fetch('/api/pallets');
    if (response.status === 401) {
      window.location.href = '/login';
      return;
    }
    if (response.status === 403) {
      window.location.href = '/';
      return;
    }
    if (!response.ok) {
      throw new Error('pallets_fetch_failed');
    }
    const data = await response.json();
    if (!data.ok) {
      throw new Error('pallets_fetch_failed');
    }
    applyPayload(data);
    setFeedback(palletEntryFeedback, null, '');
    setFeedback(palletExitFeedback, null, '');
  } catch (error) {
    if (palletListEl) {
      palletListEl.innerHTML = '<p class="admin-note">Impossible de charger les palettes.</p>';
    }
    if (palletImportListEl) {
      palletImportListEl.innerHTML = '<p class="admin-note">Impossible de charger les imports.</p>';
    }
  }
}

async function submitImport(importType, form, fileInput, feedbackEl) {
  if (!form || !fileInput || !fileInput.files || !fileInput.files[0]) {
    setFeedback(feedbackEl, 'error', 'Selectionne un fichier CSV.');
    return;
  }

  const file = fileInput.files[0];
  setFormDisabled(form, true);
  setFeedback(feedbackEl, 'info', `Lecture du CSV ${file.name}...`);
  try {
    const csvText = await readFileAsText(file);
    const response = await fetch('/api/pallets/imports', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        importType,
        fileName: file.name,
        csvText
      })
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
      applyPayload({ pallets: state.pallets, recentImports: state.recentImports }, data.errors || []);
      throw new Error(data.error || 'pallet_import_failed');
    }
    applyPayload(data, data.warnings || []);
    const importInfo = data.import || {};
    setFeedback(
      feedbackEl,
      'success',
      `${formatImportLabel(importInfo)} importee: ${importInfo.appliedCount || 0} lignes appliquees sur ${importInfo.rowCount || 0}.`
    );
    form.reset();
  } catch (error) {
    if (state.lastWarnings.length) {
      setFeedback(feedbackEl, 'error', "Import refuse. Regarde les warnings affiches plus bas.");
    } else {
      setFeedback(feedbackEl, 'error', "Import impossible. Verifie le format du CSV.");
    }
  } finally {
    setFormDisabled(form, false);
  }
}

if (palletEntryForm) {
  palletEntryForm.addEventListener('submit', (event) => {
    event.preventDefault();
    submitImport('entry', palletEntryForm, palletEntryFileInput, palletEntryFeedback);
  });
}

if (palletExitForm) {
  palletExitForm.addEventListener('submit', (event) => {
    event.preventDefault();
    submitImport('exit', palletExitForm, palletExitFileInput, palletExitFeedback);
  });
}

if (palletReloadBtn) {
  palletReloadBtn.addEventListener('click', () => {
    loadPallets();
  });
}

loadPallets();
