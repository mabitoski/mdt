const state = {
  machines: [],
  filter: 'all',
  search: '',
  sort: 'lastSeen',
  selectedId: null,
  details: {},
  lastUpdated: null
};

const listEl = document.getElementById('machine-list');
const detailEl = document.getElementById('machine-detail');
const searchInput = document.getElementById('search-input');
const refreshBtn = document.getElementById('refresh-btn');
const lastUpdatedEl = document.getElementById('last-updated');
const sortSelect = document.getElementById('sort-select');
const statTotal = document.getElementById('stat-total');
const statLaptop = document.getElementById('stat-laptop');
const statDesktop = document.getElementById('stat-desktop');
const statUnknown = document.getElementById('stat-unknown');
const filterButtons = document.querySelectorAll('.filter-btn');

const categoryLabels = {
  laptop: 'Portable',
  desktop: 'Tour',
  unknown: 'Inconnu'
};

const statusLabels = {
  ok: 'OK',
  nok: 'NOK',
  absent: 'Non present',
  not_tested: 'Non teste',
  denied: 'Refuse',
  timeout: 'Timeout',
  scheduled: 'Planifie',
  unknown: '--'
};

const componentLabels = {
  diskSmart: 'SMART disque',
  diskReadTest: 'Lecture disque',
  diskWriteTest: 'Ecriture disque',
  ramTest: 'RAM (WinSAT)',
  cpuTest: 'CPU (WinSAT)',
  gpuTest: 'GPU (WinSAT)',
  cpuStress: 'CPU (stress)',
  gpuStress: 'GPU (stress)',
  networkTest: 'iPerf',
  networkPing: 'Ping',
  fsCheck: 'Check disque',
  memDiag: 'Diag memoire',
  thermal: 'Thermique',
  gpu: 'GPU',
  usb: 'Ports USB',
  keyboard: 'Clavier',
  camera: 'Camera',
  pad: 'Pave tactile',
  badgeReader: 'Lecteur badge'
};

const componentOrder = [
  'diskSmart',
  'diskReadTest',
  'diskWriteTest',
  'ramTest',
  'cpuTest',
  'gpuTest',
  'cpuStress',
  'gpuStress',
  'networkTest',
  'networkPing',
  'fsCheck',
  'memDiag',
  'thermal',
  'gpu',
  'usb',
  'keyboard',
  'camera',
  'pad',
  'badgeReader'
];

const delayClasses = [
  'delay-0',
  'delay-1',
  'delay-2',
  'delay-3',
  'delay-4',
  'delay-5',
  'delay-6',
  'delay-7',
  'delay-8',
  'delay-9'
];

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
    return null;
  }
  const key = String(value).trim().toLowerCase();
  return statusLabels[key] ? key : null;
}

function normalizeCategory(value) {
  if (value === 'laptop' || value === 'desktop' || value === 'unknown') {
    return value;
  }
  return 'unknown';
}

function formatPrimary(machine) {
  const macFallback =
    Array.isArray(machine.macAddresses) && machine.macAddresses.length > 0
      ? machine.macAddresses[0]
      : null;
  return (
    machine.hostname ||
    machine.serialNumber ||
    machine.macAddress ||
    macFallback ||
    'Poste sans identifiant'
  );
}

function formatSubtitle(machine) {
  const chunks = [machine.vendor, machine.model].filter(Boolean);
  return chunks.length ? chunks.join(' ') : 'Modele non renseigne';
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

function timeAgo(value) {
  if (!value) {
    return '--';
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return '--';
  }
  const diff = Date.now() - date.getTime();
  const minutes = Math.floor(diff / 60000);
  if (minutes < 1) {
    return "a l'instant";
  }
  if (minutes < 60) {
    return `il y a ${minutes} min`;
  }
  const hours = Math.floor(minutes / 60);
  if (hours < 24) {
    return `il y a ${hours} h`;
  }
  const days = Math.floor(hours / 24);
  return `il y a ${days} j`;
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

function formatNetMbps(value) {
  return formatMetric(value, 'Mb/s');
}

function formatTemp(value) {
  if (typeof value !== 'number' || !Number.isFinite(value)) {
    return null;
  }
  const rounded = value % 1 === 0 ? value.toFixed(0) : value.toFixed(1);
  return `${rounded} C`;
}

function formatScore(value) {
  if (typeof value !== 'number' || !Number.isFinite(value)) {
    return null;
  }
  const rounded = value % 1 === 0 ? value.toFixed(0) : value.toFixed(1);
  return `Score ${rounded}`;
}

function renderStatus(status) {
  const normalized = normalizeStatusKey(status) || 'unknown';
  const label = statusLabels[normalized] || '--';
  return `<strong class="status-pill" data-status="${normalized}">${label}</strong>`;
}

function renderStatusValue(value) {
  const statusKey = normalizeStatusKey(value);
  if (statusKey) {
    return renderStatus(statusKey);
  }
  if (value == null || value === '') {
    return '<strong>--</strong>';
  }
  return `<strong>${escapeHtml(value)}</strong>`;
}

function updateStats() {
  const total = state.machines.length;
  const laptop = state.machines.filter((m) => normalizeCategory(m.category) === 'laptop').length;
  const desktop = state.machines.filter((m) => normalizeCategory(m.category) === 'desktop').length;
  const unknown = state.machines.filter((m) => normalizeCategory(m.category) === 'unknown').length;

  statTotal.textContent = total;
  statLaptop.textContent = laptop;
  statDesktop.textContent = desktop;
  statUnknown.textContent = unknown;
}

function applyFilters() {
  const term = state.search.trim().toLowerCase();
  const filtered = state.machines.filter((machine) => {
    const category = normalizeCategory(machine.category);
    if (state.filter !== 'all' && category !== state.filter) {
      return false;
    }
    if (!term) {
      return true;
    }
    const haystack = [
      machine.hostname,
      machine.serialNumber,
      machine.macAddress,
      Array.isArray(machine.macAddresses) ? machine.macAddresses.join(' ') : null,
      machine.vendor,
      machine.model
    ]
      .filter(Boolean)
      .join(' ')
      .toLowerCase();
    return haystack.includes(term);
  });

  return sortMachines(filtered);
}

function sortMachines(list) {
  const sorted = [...list];
  if (state.sort === 'name') {
    sorted.sort((a, b) => formatPrimary(a).localeCompare(formatPrimary(b), 'fr'));
    return sorted;
  }

  if (state.sort === 'category') {
    const order = { laptop: 0, desktop: 1, unknown: 2 };
    sorted.sort((a, b) => {
      const categoryA = normalizeCategory(a.category);
      const categoryB = normalizeCategory(b.category);
      if (order[categoryA] !== order[categoryB]) {
        return order[categoryA] - order[categoryB];
      }
      return (b.lastSeen || '').localeCompare(a.lastSeen || '');
    });
    return sorted;
  }

  sorted.sort((a, b) => (b.lastSeen || '').localeCompare(a.lastSeen || ''));
  return sorted;
}

function buildDiagnosticsHtml(detail) {
  const payload =
    detail && detail.payload && typeof detail.payload === 'object' ? detail.payload : null;
  const tests =
    payload && payload.tests && typeof payload.tests === 'object' && !Array.isArray(payload.tests)
      ? payload.tests
      : null;
  const thermal =
    payload && payload.thermal && typeof payload.thermal === 'object' && !Array.isArray(payload.thermal)
      ? payload.thermal
      : null;

  const rows = [];

  function addRow(label, status, extra) {
    const hasStatus = status !== undefined && status !== null && status !== '';
    const statusHtml = hasStatus ? renderStatusValue(status) : '';
    const extraHtml = extra ? `<span class="metric">${escapeHtml(extra)}</span>` : '';
    const content = statusHtml || extraHtml ? `<div class="status-stack">${statusHtml}${extraHtml}</div>` : '<strong>--</strong>';
    rows.push(`
      <div class="component-row">
        <span>${escapeHtml(label)}</span>
        ${content}
      </div>
    `);
  }

  if (tests) {
    if (tests.diskRead || tests.diskReadMBps != null) {
      addRow('Lecture disque', tests.diskRead, formatMbps(tests.diskReadMBps));
    }
    if (tests.diskWrite || tests.diskWriteMBps != null) {
      addRow('Ecriture disque', tests.diskWrite, formatMbps(tests.diskWriteMBps));
    }
    if (tests.ramTest || tests.ramMBps != null) {
      addRow('RAM (WinSAT)', tests.ramTest, formatMbps(tests.ramMBps));
    }
    if (tests.cpuTest || tests.cpuMBps != null) {
      addRow('CPU (WinSAT)', tests.cpuTest, formatMbps(tests.cpuMBps));
    }
    if (tests.gpuTest || tests.gpuScore != null) {
      addRow('GPU (WinSAT)', tests.gpuTest, formatScore(tests.gpuScore));
    }
    if (tests.cpuStress) {
      addRow('CPU (stress)', tests.cpuStress, null);
    }
    if (tests.gpuStress) {
      addRow('GPU (stress)', tests.gpuStress, null);
    }
    if (tests.networkPing || tests.networkPingTarget) {
      addRow('Ping', tests.networkPing, tests.networkPingTarget || null);
    }
    if (tests.network || tests.networkDownMbps != null || tests.networkUpMbps != null) {
      const parts = [];
      const down = formatNetMbps(tests.networkDownMbps);
      const up = formatNetMbps(tests.networkUpMbps);
      if (down) { parts.push(`↓ ${down}`); }
      if (up) { parts.push(`↑ ${up}`); }
      addRow('iPerf', tests.network, parts.length ? parts.join(' · ') : null);
    }
    if (tests.fsCheck) {
      addRow('Check disque', tests.fsCheck, null);
    }
    if (tests.memDiag) {
      addRow('Diag memoire', tests.memDiag, null);
    }
  }

  if (thermal && (thermal.status || thermal.maxC != null)) {
    addRow('Thermique', thermal.status, formatTemp(thermal.maxC));
  }

  if (!rows.length) {
    return '';
  }

  return `
    <div class="diagnostics">
      <h3>Diagnostics et performances</h3>
      <div class="component-list diagnostic-list">
        ${rows.join('')}
      </div>
    </div>
  `;
}

function renderList() {
  const filtered = applyFilters();

  if (!filtered.length) {
    listEl.innerHTML = '<div class="empty">Aucun poste ne correspond a ce filtre.</div>';
    return;
  }

  listEl.innerHTML = filtered
    .map((machine, index) => {
      const category = normalizeCategory(machine.category);
      const label = categoryLabels[category];
      const title = escapeHtml(formatPrimary(machine));
      const subtitle = escapeHtml(formatSubtitle(machine));
      const serial = escapeHtml(machine.serialNumber || '--');
      const mac = escapeHtml(machine.macAddress || '--');
      const lastSeen = escapeHtml(timeAgo(machine.lastSeen));
      const selected = state.selectedId === machine.id ? 'selected' : '';
      const delayClass = delayClasses[index % delayClasses.length];

      return `
        <article class="machine-card ${delayClass} ${selected}" data-id="${machine.id}">
          <div class="card-top">
            <span class="badge" data-category="${category}">${label}</span>
            <span class="machine-meta"><span>${lastSeen}</span></span>
          </div>
          <h3 class="machine-title">${title}</h3>
          <p class="machine-sub">${subtitle}</p>
          <div class="machine-meta">
            <span>SN: ${serial}</span>
            <span>MAC: ${mac}</span>
          </div>
        </article>
      `;
    })
    .join('');
}

function renderDetail() {
  if (!state.selectedId) {
    return;
  }
  const detail = state.details[state.selectedId];
  if (!detail) {
    detailEl.innerHTML = '<div class="loading">Chargement des details...</div>';
    return;
  }

  const category = normalizeCategory(detail.category);
  const title = escapeHtml(formatPrimary(detail));
  const subtitle = escapeHtml(formatSubtitle(detail));
  const macAddresses = Array.isArray(detail.macAddresses) ? detail.macAddresses : [];
  const macListHtml = macAddresses.length
    ? `<div class="mac-list">${macAddresses
        .map((mac) => `<span class="mac-chip">${escapeHtml(mac)}</span>`)
        .join('')}</div>`
    : '<strong>--</strong>';

  const components =
    detail.components && typeof detail.components === 'object' && !Array.isArray(detail.components)
      ? detail.components
      : {};
  const componentEntries = Object.entries(components);
  const componentOrderMap = new Map(componentOrder.map((key, index) => [key, index]));
  const sortedComponentEntries = componentEntries.sort((a, b) => {
    const orderA = componentOrderMap.has(a[0]) ? componentOrderMap.get(a[0]) : 999;
    const orderB = componentOrderMap.has(b[0]) ? componentOrderMap.get(b[0]) : 999;
    if (orderA !== orderB) {
      return orderA - orderB;
    }
    return a[0].localeCompare(b[0], 'fr');
  });

  const componentHtml = sortedComponentEntries.length
    ? sortedComponentEntries
        .map(([key, value]) => {
          const label = componentLabels[key] || key;
          return `
            <div class="component-row">
              <span>${escapeHtml(label)}</span>
              ${renderStatusValue(value)}
            </div>
          `;
        })
        .join('')
    : '<div class="empty">Aucun statut de composant.</div>';

  const hardwareHtml = `
    <div class="hardware">
      <h3>Materiel clef</h3>
      <div class="detail-grid hardware-grid">
        <div class="detail-item">
          <span>RAM totale</span>
          <strong>${escapeHtml(formatRam(detail.ramMb))}</strong>
        </div>
        <div class="detail-item">
          <span>Slots RAM</span>
          <strong>${escapeHtml(formatSlots(detail.ramSlotsFree, detail.ramSlotsTotal))}</strong>
        </div>
        <div class="detail-item">
          <span>Sante batterie</span>
          <strong>${escapeHtml(formatBatteryHealth(detail.batteryHealth))}</strong>
        </div>
        <div class="detail-item">
          <span>Camera</span>
          ${renderStatus(detail.cameraStatus)}
        </div>
        <div class="detail-item">
          <span>Ports USB</span>
          ${renderStatus(detail.usbStatus)}
        </div>
        <div class="detail-item">
          <span>Clavier</span>
          ${renderStatus(detail.keyboardStatus)}
        </div>
        <div class="detail-item">
          <span>Pave tactile</span>
          ${renderStatus(detail.padStatus)}
        </div>
        <div class="detail-item">
          <span>Lecteur badge</span>
          ${renderStatus(detail.badgeReaderStatus)}
        </div>
      </div>
    </div>
  `;

  const payloadHtml = detail.payload
    ? `<pre>${escapeHtml(JSON.stringify(detail.payload, null, 2))}</pre>`
    : '<div class="empty">Payload non disponible.</div>';

  const diagnosticsHtml = buildDiagnosticsHtml(detail);

  detailEl.innerHTML = `
    <div class="detail-header">
      <h2 class="detail-title">${title}</h2>
      <span class="badge detail-category" data-category="${category}">${categoryLabels[category]}</span>
      <p class="machine-sub">${subtitle}</p>
    </div>
    <div class="detail-grid">
      <div class="detail-item">
        <span>Serial</span>
        <strong>${escapeHtml(detail.serialNumber || '--')}</strong>
      </div>
      <div class="detail-item">
        <span>MAC</span>
        <strong>${escapeHtml(detail.macAddress || '--')}</strong>
      </div>
      <div class="detail-item">
        <span>MACs</span>
        ${macListHtml}
      </div>
      <div class="detail-item">
        <span>OS</span>
        <strong>${escapeHtml(detail.osVersion || '--')}</strong>
      </div>
      <div class="detail-item">
        <span>Dernier passage</span>
        <strong>${escapeHtml(formatDateTime(detail.lastSeen))}</strong>
      </div>
      <div class="detail-item">
        <span>Premier passage</span>
        <strong>${escapeHtml(formatDateTime(detail.createdAt))}</strong>
      </div>
      <div class="detail-item">
        <span>IP</span>
        <strong>${escapeHtml(detail.lastIp || '--')}</strong>
      </div>
    </div>
    ${hardwareHtml}
    ${diagnosticsHtml}
    <div class="components">
      <h3>Etat des composants</h3>
      <div class="component-list">${componentHtml}</div>
    </div>
    <div class="payload">
      <details>
        <summary>Payload complet</summary>
        ${payloadHtml}
      </details>
    </div>
  `;
}

async function loadMachines() {
  listEl.innerHTML = '<div class="loading">Chargement des postes...</div>';
  try {
    const response = await fetch('/api/machines');
    if (response.status === 401) {
      window.location.href = '/login';
      return;
    }
    if (!response.ok) {
      throw new Error('fetch_failed');
    }
    const data = await response.json();
    state.machines = Array.isArray(data.machines) ? data.machines : [];
    state.lastUpdated = new Date();
    updateStats();
    renderList();
    updateLastUpdated();
    if (state.selectedId) {
      await selectMachine(state.selectedId);
    }
  } catch (error) {
    listEl.innerHTML = '<div class="empty">Erreur lors du chargement.</div>';
  }
}

async function selectMachine(id) {
  const numericId = Number.parseInt(id, 10);
  if (!Number.isFinite(numericId)) {
    return;
  }
  state.selectedId = numericId;
  renderList();
  if (state.details[numericId]) {
    renderDetail();
  } else {
    detailEl.innerHTML = '<div class="loading">Chargement des details...</div>';
  }
  try {
    const response = await fetch(`/api/machines/${numericId}`);
    if (response.status === 401) {
      window.location.href = '/login';
      return;
    }
    if (!response.ok) {
      throw new Error('detail_failed');
    }
    const data = await response.json();
    state.details[numericId] = data.machine;
    renderDetail();
  } catch (error) {
    detailEl.innerHTML = '<div class="empty">Impossible de charger les details.</div>';
  }
}

function updateLastUpdated() {
  if (!state.lastUpdated) {
    lastUpdatedEl.textContent = 'Derniere mise a jour : --';
    return;
  }
  lastUpdatedEl.textContent = `Derniere mise a jour : ${state.lastUpdated.toLocaleTimeString('fr-FR', {
    hour: '2-digit',
    minute: '2-digit'
  })}`;
}

filterButtons.forEach((button) => {
  button.addEventListener('click', () => {
    filterButtons.forEach((btn) => {
      btn.classList.remove('active');
      btn.setAttribute('aria-pressed', 'false');
    });
    button.classList.add('active');
    button.setAttribute('aria-pressed', 'true');
    state.filter = button.dataset.filter;
    renderList();
  });
});

searchInput.addEventListener('input', (event) => {
  state.search = event.target.value;
  renderList();
});

refreshBtn.addEventListener('click', () => {
  loadMachines();
});

sortSelect.addEventListener('change', (event) => {
  state.sort = event.target.value;
  renderList();
});

listEl.addEventListener('click', (event) => {
  const card = event.target.closest('.machine-card');
  if (!card) {
    return;
  }
  selectMachine(card.dataset.id);
});

loadMachines();
