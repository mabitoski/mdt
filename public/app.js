const state = {
  machines: [],
  filter: 'all',
  techFilter: 'all',
  componentFilter: 'all',
  commentFilter: 'all',
  quickFilter: null,
  activeToken: null,
  search: '',
  sort: 'lastSeen',
  layout: '3',
  expandedId: null,
  details: {},
  lastUpdated: null
};

const listEl = document.getElementById('machine-list');
const searchInput = document.getElementById('search-input');
const refreshBtn = document.getElementById('refresh-btn');
const lastUpdatedEl = document.getElementById('last-updated');
const sortSelect = document.getElementById('sort-select');
const statTotal = document.getElementById('stat-total');
const statLaptop = document.getElementById('stat-laptop');
const statDesktop = document.getElementById('stat-desktop');
const statUnknown = document.getElementById('stat-unknown');
const categoryFilterButtons = document.querySelectorAll('.category-filter-btn');
const techFiltersEl = document.getElementById('tech-filters');
const layoutButtons = document.querySelectorAll('.layout-btn');
const testFilterButtons = document.querySelectorAll('.test-filter-btn');
const commentFilterButtons = document.querySelectorAll('.comment-filter-btn');
const adminLink = document.getElementById('admin-link');
const commentTimers = new Map();

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

const componentOrder = [
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

const hiddenComponents = new Set(['diskSmart', 'networkTest', 'memDiag', 'thermal']);

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

const layoutOptions = new Set(['1', '2', '3', '6']);
const layoutStorageKey = 'mdt-layout';
const storedLayout = window.localStorage ? localStorage.getItem(layoutStorageKey) : null;
if (storedLayout && layoutOptions.has(storedLayout)) {
  state.layout = storedLayout;
}

function applyLayout() {
  if (!listEl) {
    return;
  }
  listEl.classList.remove('columns-1', 'columns-2', 'columns-3', 'columns-6');
  listEl.classList.add(`columns-${state.layout}`);
}

function updateLayoutButtons() {
  if (!layoutButtons.length) {
    return;
  }
  layoutButtons.forEach((btn) => {
    const active = btn.dataset.layout === state.layout;
    btn.classList.toggle('active', active);
    btn.setAttribute('aria-pressed', active ? 'true' : 'false');
  });
}

function updateTestFilterButtons() {
  if (!testFilterButtons.length) {
    return;
  }
  testFilterButtons.forEach((btn) => {
    const active = btn.dataset.component === state.componentFilter;
    btn.classList.toggle('active', active);
    btn.setAttribute('aria-pressed', active ? 'true' : 'false');
  });
}

function normalizeTech(value) {
  if (!value) {
    return '';
  }
  return String(value).trim();
}

function techKey(value) {
  const normalized = normalizeTech(value);
  return normalized ? normalized.toLowerCase() : '';
}

function updateTechFilterButtons() {
  if (!techFiltersEl) {
    return;
  }
  const buttons = techFiltersEl.querySelectorAll('.tech-filter-btn');
  buttons.forEach((btn) => {
    const active = (btn.dataset.tech || 'all') === state.techFilter;
    btn.classList.toggle('active', active);
    btn.setAttribute('aria-pressed', active ? 'true' : 'false');
  });
}

function renderTechFilters() {
  if (!techFiltersEl) {
    return;
  }
  const techMap = new Map();
  state.machines.forEach((machine) => {
    const label = normalizeTech(machine.technician);
    if (!label) {
      return;
    }
    const key = label.toLowerCase();
    if (!techMap.has(key)) {
      techMap.set(key, label);
    }
  });

  const techList = Array.from(techMap.entries())
    .sort((a, b) => a[1].localeCompare(b[1], 'fr'));

  if (state.techFilter !== 'all' && !techMap.has(state.techFilter)) {
    state.techFilter = 'all';
  }

  const buttons = [
    `<button class="filter-btn tech-filter-btn" data-tech="all" type="button" aria-pressed="false">Tous techs</button>`
  ];
  techList.forEach(([key, label]) => {
    buttons.push(
      `<button class="filter-btn tech-filter-btn" data-tech="${escapeHtml(key)}" type="button" aria-pressed="false">${escapeHtml(
        label
      )}</button>`
    );
  });
  techFiltersEl.innerHTML = buttons.join('');
  updateTechFilterButtons();
}

function updateCommentFilterButtons() {
  if (!commentFilterButtons.length) {
    return;
  }
  commentFilterButtons.forEach((btn) => {
    const active = btn.dataset.comment === state.commentFilter;
    btn.classList.toggle('active', active);
    btn.setAttribute('aria-pressed', active ? 'true' : 'false');
  });
}

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

function setAdminLinkVisible(visible) {
  if (!adminLink) {
    return;
  }
  adminLink.hidden = !visible;
}

async function initAdminLink() {
  if (!adminLink) {
    return;
  }
  setAdminLinkVisible(false);
  try {
    const response = await fetch('/api/me');
    if (response.status === 401) {
      window.location.href = '/login';
      return;
    }
    if (!response.ok) {
      return;
    }
    const data = await response.json();
    if (data.user && data.user.type === 'local') {
      setAdminLinkVisible(true);
    }
  } catch (error) {
    setAdminLinkVisible(false);
  }
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

function formatMacSummary(machine) {
  const macs = Array.isArray(machine.macAddresses) ? machine.macAddresses.filter(Boolean) : [];
  const primary = machine.macAddress || macs[0] || null;
  const secondary = macs.find((mac) => mac && mac !== primary) || null;
  if (primary && secondary) {
    return `${primary} / ${secondary}`;
  }
  return primary || '--';
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

function buildMetaChip(label, displayValue, filterValue, type, activeToken, machineId) {
  const display = `${label}: ${displayValue || '--'}`;
  if (!filterValue) {
    return `<span>${escapeHtml(display)}</span>`;
  }
  const isActive =
    activeToken &&
    activeToken.id === machineId &&
    activeToken.type === type &&
    activeToken.value === filterValue;
  return `
    <button
      class="meta-chip${isActive ? ' is-active' : ''}"
      type="button"
      data-filter="${escapeHtml(type)}"
      data-value="${escapeHtml(filterValue)}"
    >
      ${escapeHtml(display)}
    </button>
  `;
}

function getPadStatus(detail) {
  if (!detail || typeof detail !== 'object') {
    return null;
  }
  if (detail.padStatus) {
    return detail.padStatus;
  }
  if (detail.components && typeof detail.components === 'object') {
    return detail.components.pad || null;
  }
  return null;
}

function applyPadStatusUpdate(id, status) {
  state.machines = state.machines.map((machine) => {
    if (machine.id !== id) {
      return machine;
    }
    const components = machine.components && typeof machine.components === 'object'
      ? { ...machine.components }
      : {};
    components.pad = status;
    return {
      ...machine,
      padStatus: status,
      components
    };
  });

  if (state.details[id]) {
    const detail = state.details[id];
    const components = detail.components && typeof detail.components === 'object'
      ? { ...detail.components }
      : {};
    components.pad = status;
    state.details[id] = {
      ...detail,
      padStatus: status,
      components
    };
  }
}

function setPadButtonsLoading(id, loading) {
  const buttons = listEl.querySelectorAll(`[data-action="set-pad"][data-id="${id}"]`);
  buttons.forEach((button) => {
    button.disabled = loading;
    button.classList.toggle('is-loading', loading);
  });
}

async function updatePadStatus(id, status) {
  setPadButtonsLoading(id, true);
  try {
    const response = await fetch(`/api/machines/${encodeURIComponent(id)}/pad`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ status })
    });
    if (response.status === 401) {
      window.location.href = '/login';
      return;
    }
    if (!response.ok) {
      throw new Error('pad_update_failed');
    }
    const data = await response.json();
    if (!data.ok) {
      throw new Error('pad_update_failed');
    }
    applyPadStatusUpdate(id, data.status);
    renderList();
  } catch (error) {
    window.alert("Impossible d'enregistrer le pavé tactile.");
  } finally {
    setPadButtonsLoading(id, false);
  }
}

function applyCommentUpdate(id, comment, commentedAt) {
  state.machines = state.machines.map((machine) => {
    if (machine.id !== id) {
      return machine;
    }
    return {
      ...machine,
      comment,
      commentedAt
    };
  });

  if (state.details[id]) {
    state.details[id] = {
      ...state.details[id],
      comment,
      commentedAt
    };
  }
}

function setCommentButtonsLoading(id, loading) {
  const buttons = listEl.querySelectorAll(
    `[data-action="clear-comment"][data-id="${id}"]`
  );
  buttons.forEach((button) => {
    button.disabled = loading;
    button.classList.toggle('is-loading', loading);
  });
}

async function updateComment(id, comment) {
  setCommentButtonsLoading(id, true);
  try {
    const response = await fetch(`/api/machines/${encodeURIComponent(id)}/comment`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ comment })
    });
    if (response.status === 401) {
      window.location.href = '/login';
      return;
    }
    if (!response.ok) {
      throw new Error('comment_update_failed');
    }
    const data = await response.json();
    if (!data.ok) {
      throw new Error('comment_update_failed');
    }
    applyCommentUpdate(id, data.comment, data.commentedAt);
    renderList();
  } catch (error) {
    window.alert("Impossible d'enregistrer le commentaire.");
  } finally {
    setCommentButtonsLoading(id, false);
  }
}

function normalizeComment(value) {
  if (value == null) {
    return '';
  }
  return String(value).trim();
}

function getCommentFromState(id) {
  if (state.details[id] && typeof state.details[id].comment === 'string') {
    return state.details[id].comment;
  }
  const match = state.machines.find((machine) => machine.id === id);
  return match && typeof match.comment === 'string' ? match.comment : '';
}

function scheduleCommentSave(id, value, immediate = false) {
  const normalized = normalizeComment(value);
  const current = normalizeComment(getCommentFromState(id));
  if (normalized === current) {
    const existing = commentTimers.get(id);
    if (existing) {
      clearTimeout(existing);
      commentTimers.delete(id);
    }
    return;
  }
  if (immediate) {
    const existing = commentTimers.get(id);
    if (existing) {
      clearTimeout(existing);
      commentTimers.delete(id);
    }
    updateComment(id, normalized);
    return;
  }
  const existing = commentTimers.get(id);
  if (existing) {
    clearTimeout(existing);
  }
  const timeoutId = setTimeout(() => {
    commentTimers.delete(id);
    updateComment(id, normalized);
  }, 800);
  commentTimers.set(id, timeoutId);
}

function getUniqueMachines(list) {
  const seen = new Set();
  const unique = [];
  list.forEach((machine) => {
    const key = machine.machineKey || machine.serialNumber || machine.macAddress || machine.hostname;
    if (!key) {
      return;
    }
    const normalized = String(key);
    if (seen.has(normalized)) {
      return;
    }
    seen.add(normalized);
    unique.push(machine);
  });
  return unique;
}

function updateStats() {
  const uniqueMachines = getUniqueMachines(state.machines);
  const total = uniqueMachines.length;
  const laptop = uniqueMachines.filter((m) => normalizeCategory(m.category) === 'laptop').length;
  const desktop = uniqueMachines.filter((m) => normalizeCategory(m.category) === 'desktop').length;
  const unknown = uniqueMachines.filter((m) => normalizeCategory(m.category) === 'unknown').length;

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
    if (state.quickFilter && state.quickFilter.value) {
      const filterValue = state.quickFilter.value.toLowerCase();
      if (state.quickFilter.type === 'serial') {
        const serial = (machine.serialNumber || '').toLowerCase();
        if (!serial.includes(filterValue)) {
          return false;
        }
      } else if (state.quickFilter.type === 'mac') {
        const macs = [
          machine.macAddress,
          Array.isArray(machine.macAddresses) ? machine.macAddresses.join(' ') : null
        ]
          .filter(Boolean)
          .join(' ')
          .toLowerCase();
        if (!macs.includes(filterValue)) {
          return false;
        }
      } else if (state.quickFilter.type === 'tech') {
        const tech = (machine.technician || '').toLowerCase();
        if (!tech.includes(filterValue)) {
          return false;
        }
      }
    }
    if (state.techFilter !== 'all') {
      if (techKey(machine.technician) !== state.techFilter) {
        return false;
      }
    }
    const commentValue = typeof machine.comment === 'string' ? machine.comment.trim() : '';
    if (state.commentFilter === 'with' && !commentValue) {
      return false;
    }
    if (state.commentFilter === 'without' && commentValue) {
      return false;
    }
    if (state.componentFilter !== 'all') {
      const components =
        machine.components && typeof machine.components === 'object' && !Array.isArray(machine.components)
          ? machine.components
          : null;
      const componentStatus = components ? normalizeStatusKey(components[state.componentFilter]) : null;
      if (componentStatus !== 'nok') {
        return false;
      }
    }
    if (!term) {
      return true;
    }
    const haystack = [
      machine.hostname,
      machine.serialNumber,
      machine.macAddress,
      Array.isArray(machine.macAddresses) ? machine.macAddresses.join(' ') : null,
      machine.machineKey,
      machine.technician,
      machine.vendor,
      machine.model,
      machine.comment
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
  const winSat =
    payload && payload.winsat && typeof payload.winsat === 'object' ? payload.winsat : null;
  const winSpr =
    winSat && winSat.winSPR && typeof winSat.winSPR === 'object' ? winSat.winSPR : null;
  const winSatCpuScore =
    winSpr && typeof winSpr.CpuScore === 'number' ? winSpr.CpuScore : null;
  const winSatMemScore =
    winSpr && typeof winSpr.MemoryScore === 'number' ? winSpr.MemoryScore : null;
  const winSatGraphicsScore = winSpr
    ? typeof winSpr.GamingScore === 'number'
      ? winSpr.GamingScore
      : typeof winSpr.GraphicsScore === 'number'
        ? winSpr.GraphicsScore
        : null
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
    const ramNote = tests.ramNote || formatWinSatNote(winSatMemScore);
    const cpuNote = tests.cpuNote || formatWinSatNote(winSatCpuScore);
    const gpuNote =
      tests.gpuNote || formatWinSatNote(winSatGraphicsScore != null ? winSatGraphicsScore : tests.gpuScore);
    if (tests.diskRead || tests.diskReadMBps != null) {
      addRow('Lecture disque', tests.diskRead, formatMbps(tests.diskReadMBps));
    }
    if (tests.diskWrite || tests.diskWriteMBps != null) {
      addRow('Ecriture disque', tests.diskWrite, formatMbps(tests.diskWriteMBps));
    }
    if (tests.ramTest || tests.ramMBps != null) {
      addRow('RAM (WinSAT)', tests.ramTest, ramNote || formatMbps(tests.ramMBps));
    }
    if (tests.cpuTest || tests.cpuMBps != null) {
      addRow('CPU (WinSAT)', tests.cpuTest, cpuNote || formatMbps(tests.cpuMBps));
    }
    if (tests.gpuTest || tests.gpuScore != null) {
      addRow('GPU (WinSAT)', tests.gpuTest, gpuNote || formatScore(tests.gpuScore));
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
    if (tests.fsCheck) {
      addRow('Check disque', tests.fsCheck, null);
    }
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
      const serialValue = machine.serialNumber || '';
      const macLabel = formatMacSummary(machine);
      const macValue = machine.macAddress || (Array.isArray(machine.macAddresses) ? machine.macAddresses[0] : '') || '';
      const technicianValue = machine.technician || '';
      const lastSeen = escapeHtml(timeAgo(machine.lastSeen));
      const commentValue = typeof machine.comment === 'string' ? machine.comment.trim() : '';
      const hasComment = Boolean(commentValue);
      const commentHtml = hasComment
        ? `
          <div class="card-comment" title="${escapeHtml(commentValue)}">
            <span class="comment-label">Commentaire</span>
            <span class="comment-text">${escapeHtml(commentValue)}</span>
          </div>
        `
        : '';
      const cardMainClass = hasComment ? 'card-main' : 'card-main no-comment';
      const expanded = state.expandedId === machine.id;
      const selected = expanded ? 'selected' : '';
      const delayClass = delayClasses[index % delayClasses.length];
      const summary = summarizeComponents(machine.components);
      const summaryHtml =
        summary.total > 0
          ? `
            <div class="machine-summary">
              <span class="summary-chip ok">OK ${summary.ok}</span>
              <span class="summary-chip nok">NOK ${summary.nok}</span>
              <span class="summary-chip nt">NT ${summary.other}</span>
            </div>
          `
          : `
            <div class="machine-summary">
              <span class="summary-chip nt">NT --</span>
            </div>
          `;
      const detailData = expanded ? state.details[machine.id] : null;
      const detailHtml = expanded
        ? detailData && detailData.error
          ? '<div class="card-detail"><div class="empty">Impossible de charger les details.</div></div>'
          : detailData
            ? `<div class="card-detail">${buildDetailHtml(detailData)}</div>`
            : '<div class="card-detail"><div class="loading">Chargement des details...</div></div>'
        : '';
      const toggleLabel = expanded ? 'Masquer les details' : 'Afficher les details';

      return `
        <article class="machine-card ${delayClass} ${selected}" data-id="${machine.id}" aria-expanded="${expanded}">
          <div class="card-top">
            <span class="badge" data-category="${category}">${label}</span>
            <div class="card-top-right">
              <span class="machine-meta"><span>${lastSeen}</span></span>
            </div>
          </div>
          <div class="${cardMainClass}">
            <div class="card-left">
              <h3 class="machine-title">${title}</h3>
              <p class="machine-sub">${subtitle}</p>
              <div class="machine-meta">
                ${buildMetaChip('SN', serialValue, serialValue, 'serial', state.activeToken, machine.id)}
                ${buildMetaChip('MAC', macLabel || '', macValue, 'mac', state.activeToken, machine.id)}
                ${buildMetaChip('Tech', technicianValue, technicianValue, 'tech', state.activeToken, machine.id)}
              </div>
              ${summaryHtml}
            </div>
            <div class="card-right">
              ${commentHtml}
            </div>
          </div>
          <button class="card-toggle" type="button">${toggleLabel}</button>
          ${detailHtml}
        </article>
      `;
    })
    .join('');
}

function buildDetailHtml(detail) {
  const category = normalizeCategory(detail.category);
  const title = escapeHtml(formatPrimary(detail));
  const subtitle = escapeHtml(formatSubtitle(detail));
  const technicianLine = detail.technician
    ? `<p class="detail-tech"><span>Technicien</span><strong>${escapeHtml(detail.technician)}</strong></p>`
    : '';
  const detailId = detail && detail.id != null ? String(detail.id) : '';
  const padStatus = getPadStatus(detail);
  const padOkActive = padStatus === 'ok' ? 'active' : '';
  const padNokActive = padStatus === 'nok' ? 'active' : '';
  const actionBar = detailId
    ? `
      <div class="detail-actions">
        <button class="detail-action" type="button" data-action="export-pdf" data-id="${detailId}">
          Telecharger PDF
        </button>
        <div class="pad-control" data-id="${detailId}">
          <span>Pave tactile</span>
          <button
            class="detail-action pad-action ${padOkActive}"
            type="button"
            data-action="set-pad"
            data-status="ok"
            data-id="${detailId}"
          >
            OK
          </button>
          <button
            class="detail-action pad-action ${padNokActive}"
            type="button"
            data-action="set-pad"
            data-status="nok"
            data-id="${detailId}"
          >
            NOK
          </button>
        </div>
      </div>
    `
    : '';
  const commentValue = typeof detail.comment === 'string' ? detail.comment : '';
  const commentMeta = detail.commentedAt
    ? `<div class="comment-meta">Derniere modif : ${escapeHtml(formatDateTime(detail.commentedAt))}</div>`
    : '';
  const commentHtml = detailId
    ? `
      <div class="comment-block">
        <span class="comment-label">Commentaire</span>
        <textarea class="comment-input" data-comment-id="${detailId}" maxlength="800">${escapeHtml(
          commentValue
        )}</textarea>
        <div class="comment-actions">
          <button class="detail-action" type="button" data-action="clear-comment" data-id="${detailId}">
            Effacer
          </button>
        </div>
        ${commentMeta}
      </div>
    `
    : '';
  const payload =
    detail && detail.payload && typeof detail.payload === 'object' ? detail.payload : null;
  const cpuInfo = payload && payload.cpu && typeof payload.cpu === 'object' ? payload.cpu : null;
  const gpuInfo = payload && payload.gpu && typeof payload.gpu === 'object' ? payload.gpu : null;
  const diskInfoRaw = payload ? payload.disks : null;
  const diskInfo = Array.isArray(diskInfoRaw) ? diskInfoRaw : diskInfoRaw ? [diskInfoRaw] : [];
  const volumeInfoRaw = payload ? payload.volumes : null;
  const volumeInfo = Array.isArray(volumeInfoRaw) ? volumeInfoRaw : volumeInfoRaw ? [volumeInfoRaw] : [];
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
  const componentEntries = Object.entries(components).filter(([key]) => !hiddenComponents.has(key));
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
          <span>CPU</span>
          <strong>${escapeHtml((cpuInfo && cpuInfo.name) || '--')}</strong>
        </div>
        <div class="detail-item">
          <span>Coeurs / Threads</span>
          <strong>${escapeHtml(formatCpuThreads(cpuInfo))}</strong>
        </div>
        <div class="detail-item">
          <span>GPU</span>
          <strong>${escapeHtml((gpuInfo && gpuInfo.name) || '--')}</strong>
        </div>
        <div class="detail-item">
          <span>Stockage total</span>
          <strong>${escapeHtml(formatTotalStorage(diskInfo, volumeInfo))}</strong>
        </div>
        <div class="detail-item">
          <span>Disque principal</span>
          <strong>${escapeHtml(formatPrimaryDisk(diskInfo, volumeInfo))}</strong>
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
          ${renderStatus(padStatus)}
        </div>
        <div class="detail-item">
          <span>Lecteur badge</span>
          ${renderStatus(detail.badgeReaderStatus)}
        </div>
      </div>
    </div>
  `;

  const diagnosticsHtml = buildDiagnosticsHtml(detail);

  return `
    <div class="detail-header">
      <h2 class="detail-title">${title}</h2>
      <span class="badge detail-category" data-category="${category}">${categoryLabels[category]}</span>
      <p class="machine-sub">${subtitle}</p>
      ${technicianLine}
      ${actionBar}
    </div>
    <div class="detail-grid">
      <div class="detail-item">
        <span>Serial</span>
        <strong>${escapeHtml(detail.serialNumber || '--')}</strong>
      </div>
      <div class="detail-item">
        <span>MAC</span>
        <strong>${escapeHtml(formatMacSummary(detail))}</strong>
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
    ${commentHtml}
    ${hardwareHtml}
    ${diagnosticsHtml}
    <div class="components">
      <h3>Etat des composants</h3>
      <div class="component-list">${componentHtml}</div>
    </div>
  `;
}

function buildReportDocument(detail) {
  const category = normalizeCategory(detail.category);
  const title = escapeHtml(formatPrimary(detail));
  const subtitle = escapeHtml(formatSubtitle(detail));
  const generatedAt = new Date().toLocaleString('fr-FR', {
    day: '2-digit',
    month: 'short',
    year: 'numeric',
    hour: '2-digit',
    minute: '2-digit'
  });
  const technicianLine = detail.technician
    ? `<div class="report-meta-row"><span>Technicien</span><strong>${escapeHtml(detail.technician)}</strong></div>`
    : '';

  const payload =
    detail && detail.payload && typeof detail.payload === 'object' ? detail.payload : null;
  const cpuInfo = payload && payload.cpu && typeof payload.cpu === 'object' ? payload.cpu : null;
  const gpuInfo = payload && payload.gpu && typeof payload.gpu === 'object' ? payload.gpu : null;
  const diskInfoRaw = payload ? payload.disks : null;
  const diskInfo = Array.isArray(diskInfoRaw) ? diskInfoRaw : diskInfoRaw ? [diskInfoRaw] : [];
  const volumeInfoRaw = payload ? payload.volumes : null;
  const volumeInfo = Array.isArray(volumeInfoRaw) ? volumeInfoRaw : volumeInfoRaw ? [volumeInfoRaw] : [];
  const macAddresses = Array.isArray(detail.macAddresses) ? detail.macAddresses : [];
  const macListHtml = macAddresses.length
    ? `<div class="mac-list">${macAddresses
        .map((mac) => `<span class="mac-chip">${escapeHtml(mac)}</span>`)
        .join('')}</div>`
    : '<strong>--</strong>';

  const diagnosticsHtml = buildDiagnosticsHtml(detail);
  const diagnosticsSection = diagnosticsHtml
    ? diagnosticsHtml
    : `
      <div class="diagnostics">
        <h3>Diagnostics et performances</h3>
        <div class="empty">Aucun test disponible.</div>
      </div>
    `;

  const components =
    detail.components && typeof detail.components === 'object' && !Array.isArray(detail.components)
      ? detail.components
      : {};
  const componentEntries = Object.entries(components).filter(([key]) => !hiddenComponents.has(key));
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
          <span>CPU</span>
          <strong>${escapeHtml((cpuInfo && cpuInfo.name) || '--')}</strong>
        </div>
        <div class="detail-item">
          <span>Coeurs / Threads</span>
          <strong>${escapeHtml(formatCpuThreads(cpuInfo))}</strong>
        </div>
        <div class="detail-item">
          <span>GPU</span>
          <strong>${escapeHtml((gpuInfo && gpuInfo.name) || '--')}</strong>
        </div>
        <div class="detail-item">
          <span>Stockage total</span>
          <strong>${escapeHtml(formatTotalStorage(diskInfo, volumeInfo))}</strong>
        </div>
        <div class="detail-item">
          <span>Disque principal</span>
          <strong>${escapeHtml(formatPrimaryDisk(diskInfo, volumeInfo))}</strong>
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

  const summary = summarizeComponents(detail.components);
  const summaryHtml =
    summary.total > 0
      ? `
          <span class="summary-chip ok">OK ${summary.ok}</span>
          <span class="summary-chip nok">NOK ${summary.nok}</span>
          <span class="summary-chip nt">NT ${summary.other}</span>
        `
      : '<span class="summary-chip nt">NT --</span>';

  const styles = `
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: "IBM Plex Sans", system-ui, sans-serif;
      color: #1d211f;
      background: radial-gradient(circle at top left, #fff5df 0%, #f7f1e7 35%, #e7f2ea 100%);
    }
    h1, h2, h3 {
      font-family: "Space Grotesk", sans-serif;
    }
    .aura {
      position: fixed;
      inset: 0;
      pointer-events: none;
      background:
        radial-gradient(circle at 80% 10%, rgba(242, 139, 45, 0.2), transparent 35%),
        radial-gradient(circle at 10% 70%, rgba(47, 124, 92, 0.18), transparent 40%);
      mix-blend-mode: multiply;
      opacity: 0.75;
      z-index: -1;
    }
    .report {
      padding: 28px 32px 40px;
      max-width: 980px;
      margin: 24px auto;
      background: rgba(255, 255, 255, 0.75);
      border-radius: 18px;
      border: 1px solid rgba(57, 64, 60, 0.16);
      box-shadow: 0 24px 50px rgba(25, 30, 29, 0.1);
    }
    .report-header {
      display: flex;
      flex-wrap: wrap;
      justify-content: space-between;
      gap: 16px;
      border-bottom: 1px solid rgba(60, 64, 60, 0.18);
      padding-bottom: 16px;
      margin-bottom: 18px;
    }
    .report-kicker {
      text-transform: uppercase;
      letter-spacing: 0.28em;
      font-size: 0.7rem;
      color: #2f7c5c;
      font-weight: 600;
    }
    .report-title {
      margin: 8px 0 6px;
      font-size: 2rem;
    }
    .report-sub {
      margin: 0;
      color: #6b6f6c;
      font-size: 1rem;
    }
    .report-meta {
      display: grid;
      gap: 6px;
      margin-top: 10px;
      font-size: 0.85rem;
      color: #4d4f4e;
    }
    .report-meta-row {
      display: flex;
      gap: 10px;
      align-items: baseline;
    }
    .report-meta-row span {
      text-transform: uppercase;
      letter-spacing: 0.12em;
      font-size: 0.7rem;
      color: #6b6f6c;
    }
    .summary-row {
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
      align-items: center;
    }
    .badge {
      display: inline-flex;
      align-items: center;
      padding: 6px 12px;
      border-radius: 999px;
      font-size: 0.75rem;
      font-weight: 700;
      letter-spacing: 0.1em;
      text-transform: uppercase;
      background: rgba(47, 124, 92, 0.15);
      color: #1b4c38;
    }
    .summary-chip {
      display: inline-flex;
      align-items: center;
      padding: 4px 10px;
      border-radius: 999px;
      font-size: 0.72rem;
      font-weight: 700;
      letter-spacing: 0.08em;
      text-transform: uppercase;
    }
    .summary-chip.ok { background: rgba(47, 124, 92, 0.16); color: #1b4c38; }
    .summary-chip.nok { background: rgba(231, 76, 60, 0.16); color: #a33524; }
    .summary-chip.nt { background: rgba(60, 64, 60, 0.12); color: #4b4b4b; }
    .detail-grid {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 12px 18px;
      margin-bottom: 18px;
    }
    .detail-item {
      display: grid;
      gap: 4px;
    }
    .detail-item span {
      font-size: 0.75rem;
      text-transform: uppercase;
      letter-spacing: 0.12em;
      color: #6b6f6c;
    }
    .detail-item strong {
      font-weight: 600;
      font-size: 0.95rem;
    }
    .mac-list {
      display: flex;
      flex-wrap: wrap;
      gap: 6px;
    }
    .mac-chip {
      display: inline-flex;
      align-items: center;
      padding: 4px 8px;
      border-radius: 999px;
      background: rgba(60, 64, 60, 0.08);
      font-size: 0.75rem;
      font-weight: 600;
      color: #1d211f;
    }
    .hardware, .diagnostics, .components, .payload, .identifiers {
      margin-bottom: 18px;
    }
    .component-list {
      display: grid;
      gap: 8px;
    }
    .component-row {
      display: flex;
      justify-content: space-between;
      gap: 8px;
      padding: 8px 10px;
      background: rgba(255, 255, 255, 0.6);
      border-radius: 12px;
      font-size: 0.9rem;
      align-items: center;
    }
    .status-pill {
      display: inline-flex;
      align-items: center;
      padding: 4px 10px;
      border-radius: 999px;
      font-size: 0.75rem;
      font-weight: 600;
      letter-spacing: 0.08em;
      text-transform: uppercase;
    }
    .status-pill[data-status="ok"] { background: rgba(47, 124, 92, 0.15); color: #1b4c38; }
    .status-pill[data-status="nok"] { background: rgba(214, 73, 53, 0.16); color: #8d1f12; }
    .status-pill[data-status="absent"],
    .status-pill[data-status="unknown"],
    .status-pill[data-status="not_tested"],
    .status-pill[data-status="denied"] { background: rgba(60, 64, 60, 0.12); color: #4b4b4b; }
    .status-pill[data-status="timeout"] { background: rgba(242, 139, 45, 0.2); color: #9b4a16; }
    .payload pre {
      white-space: pre-wrap;
      background: rgba(255, 255, 255, 0.6);
      padding: 12px;
      border-radius: 12px;
      font-size: 0.78rem;
    }
    .status-stack {
      display: flex;
      align-items: center;
      gap: 8px;
      flex-wrap: wrap;
      justify-content: flex-end;
    }
    .status-stack .metric {
      font-size: 0.82rem;
      color: #6b6f6c;
      font-weight: 600;
    }
    .empty { color: #6b6f6c; font-size: 0.9rem; }
    @media print {
      body { background: #ffffff; }
      .report {
        margin: 0;
        max-width: 100%;
        border: none;
        box-shadow: none;
        background: #ffffff;
        padding: 0;
      }
      .aura { display: none; }
    }
  `;

  return `<!doctype html>
  <html lang="fr">
  <head>
    <meta charset="utf-8">
    <title>Rapport ${title}</title>
    <style>${styles}</style>
  </head>
  <body>
    <div class="aura"></div>
    <div class="report">
      <header class="report-header">
        <div>
          <div class="report-kicker">MDT Live Ops</div>
          <h1 class="report-title">${title}</h1>
          <p class="report-sub">${subtitle}</p>
          <div class="report-meta">
            <div class="report-meta-row"><span>Genere</span><strong>${escapeHtml(generatedAt)}</strong></div>
            <div class="report-meta-row"><span>Dernier passage</span><strong>${escapeHtml(
              formatDateTime(detail.lastSeen)
            )}</strong></div>
            <div class="report-meta-row"><span>Premier passage</span><strong>${escapeHtml(
              formatDateTime(detail.createdAt)
            )}</strong></div>
            ${technicianLine}
          </div>
        </div>
        <div class="summary-row">
          <span class="badge" data-category="${category}">${categoryLabels[category]}</span>
          ${summaryHtml}
        </div>
      </header>
      <section class="identifiers">
        <h3>Identifiants</h3>
        <div class="detail-grid">
          <div class="detail-item">
            <span>Serial</span>
            <strong>${escapeHtml(detail.serialNumber || '--')}</strong>
          </div>
          <div class="detail-item">
            <span>MAC</span>
            <strong>${escapeHtml(formatMacSummary(detail))}</strong>
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
            <span>IP</span>
            <strong>${escapeHtml(detail.lastIp || '--')}</strong>
          </div>
        </div>
      </section>
      ${hardwareHtml}
      ${diagnosticsSection}
      <div class="components">
        <h3>Etat des composants</h3>
        <div class="component-list">${componentHtml}</div>
      </div>
      <div class="payload">
        <h3>Payload complet</h3>
        ${payloadHtml}
      </div>
    </div>
    <script>
      window.addEventListener('load', () => {
        setTimeout(() => {
          window.focus();
          window.print();
        }, 200);
      });
    </script>
  </body>
  </html>`;
}

function openReportPdf(detail) {
  if (!detail || !detail.id) {
    return;
  }
  const url = `/api/machines/${encodeURIComponent(detail.id)}/report.pdf`;
  fetch(url)
    .then((response) => {
      if (response.status === 401) {
        window.location.href = '/login';
        return null;
      }
      if (!response.ok) {
        throw new Error('pdf_failed');
      }
      return response.blob();
    })
    .then((blob) => {
      if (!blob) {
        return;
      }
      const objectUrl = URL.createObjectURL(blob);
      const link = document.createElement('a');
      link.href = objectUrl;
      link.download = `mdt-report-${detail.id}.pdf`;
      document.body.appendChild(link);
      link.click();
      link.remove();
      URL.revokeObjectURL(objectUrl);
    })
    .catch(() => {
      window.alert("Impossible de generer le PDF.");
    });
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
    renderTechFilters();
    updateCommentFilterButtons();
    updateStats();
    renderList();
    updateLastUpdated();
    if (state.expandedId) {
      await ensureMachineDetail(state.expandedId);
    }
  } catch (error) {
    listEl.innerHTML = '<div class="empty">Erreur lors du chargement.</div>';
  }
}

async function ensureMachineDetail(id) {
  const detailId = id != null ? String(id) : '';
  if (!detailId) {
    return;
  }
  if (state.details[detailId]) {
    renderList();
    return;
  }
  renderList();
  try {
    const response = await fetch(`/api/machines/${encodeURIComponent(detailId)}`);
    if (response.status === 401) {
      window.location.href = '/login';
      return;
    }
    if (!response.ok) {
      throw new Error('detail_failed');
    }
    const data = await response.json();
    state.details[detailId] = data.machine;
    renderList();
  } catch (error) {
    state.details[detailId] = { error: true };
    renderList();
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

categoryFilterButtons.forEach((button) => {
  button.addEventListener('click', () => {
    categoryFilterButtons.forEach((btn) => {
      btn.classList.remove('active');
      btn.setAttribute('aria-pressed', 'false');
    });
    button.classList.add('active');
    button.setAttribute('aria-pressed', 'true');
    state.filter = button.dataset.filter;
    renderList();
  });
});

if (techFiltersEl) {
  techFiltersEl.addEventListener('click', (event) => {
    const button = event.target.closest('.tech-filter-btn');
    if (!button) {
      return;
    }
    state.techFilter = button.dataset.tech || 'all';
    updateTechFilterButtons();
    renderList();
  });
}

layoutButtons.forEach((button) => {
  button.addEventListener('click', () => {
    const layout = button.dataset.layout;
    if (!layout || !layoutOptions.has(layout)) {
      return;
    }
    state.layout = layout;
    if (window.localStorage) {
      localStorage.setItem(layoutStorageKey, layout);
    }
    updateLayoutButtons();
    applyLayout();
  });
});

testFilterButtons.forEach((button) => {
  button.addEventListener('click', () => {
    testFilterButtons.forEach((btn) => {
      btn.classList.remove('active');
      btn.setAttribute('aria-pressed', 'false');
    });
    button.classList.add('active');
    button.setAttribute('aria-pressed', 'true');
    state.componentFilter = button.dataset.component || 'all';
    renderList();
  });
});

commentFilterButtons.forEach((button) => {
  button.addEventListener('click', () => {
    commentFilterButtons.forEach((btn) => {
      btn.classList.remove('active');
      btn.setAttribute('aria-pressed', 'false');
    });
    button.classList.add('active');
    button.setAttribute('aria-pressed', 'true');
    state.commentFilter = button.dataset.comment || 'all';
    renderList();
  });
});

updateLayoutButtons();
updateTestFilterButtons();
updateCommentFilterButtons();
applyLayout();

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
  const metaBtn = event.target.closest('.meta-chip');
  if (metaBtn) {
    event.preventDefault();
    event.stopPropagation();
    const type = metaBtn.dataset.filter;
    const value = metaBtn.dataset.value;
    const card = metaBtn.closest('.machine-card');
    const id = card ? card.dataset.id : null;
    if (!type || !value || !id) {
      return;
    }
    const isSame =
      state.activeToken &&
      state.activeToken.id === id &&
      state.activeToken.type === type &&
      state.activeToken.value === value;
    if (isSame) {
      state.quickFilter = null;
      state.activeToken = null;
    } else {
      state.quickFilter = { type, value };
      state.activeToken = { id, type, value };
    }
    renderList();
    return;
  }
  const padBtn = event.target.closest('[data-action="set-pad"]');
  if (padBtn) {
    event.preventDefault();
    event.stopPropagation();
    const id = padBtn.dataset.id;
    const status = padBtn.dataset.status;
    if (!id || !status) {
      return;
    }
    updatePadStatus(id, status);
    return;
  }
  const clearCommentBtn = event.target.closest('[data-action="clear-comment"]');
  if (clearCommentBtn) {
    event.preventDefault();
    event.stopPropagation();
    const id = clearCommentBtn.dataset.id;
    if (!id) {
      return;
    }
    const input = listEl.querySelector(`.comment-input[data-comment-id="${id}"]`);
    if (input) {
      input.value = '';
    }
    updateComment(id, '');
    return;
  }
  const exportBtn = event.target.closest('[data-action="export-pdf"]');
  if (exportBtn) {
    event.preventDefault();
    event.stopPropagation();
    const id = exportBtn.dataset.id;
    if (!id) {
      return;
    }
    const detail = state.details[id];
    if (!detail || detail.error) {
      return;
    }
    openReportPdf(detail);
    return;
  }
  const card = event.target.closest('.machine-card');
  if (!card) {
    return;
  }
  if (event.target.closest('.card-detail')) {
    return;
  }
  const id = card.dataset.id;
  if (!id) {
    return;
  }
  if (state.expandedId === id) {
    state.expandedId = null;
    renderList();
    return;
  }
  state.expandedId = id;
  ensureMachineDetail(id);
});

listEl.addEventListener('input', (event) => {
  const input = event.target.closest('.comment-input');
  if (!input) {
    return;
  }
  const id = input.dataset.commentId;
  if (!id) {
    return;
  }
  scheduleCommentSave(id, input.value);
});

listEl.addEventListener(
  'blur',
  (event) => {
    const input = event.target.closest('.comment-input');
    if (!input) {
      return;
    }
    const id = input.dataset.commentId;
    if (!id) {
      return;
    }
    scheduleCommentSave(id, input.value, true);
  },
  true
);

initAdminLink();
loadMachines();
