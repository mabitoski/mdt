const isLegacyView = Boolean(
  (typeof window !== 'undefined' && window.__LEGACY_VIEW__) ||
  (typeof document !== 'undefined' &&
    document.body &&
    document.body.dataset &&
    document.body.dataset.view === 'legacy') ||
  (typeof window !== 'undefined' && window.location && window.location.pathname.includes('legacy'))
);
const storageSuffix = isLegacyView ? '-legacy' : '';

const legacyMode = isLegacyView ? 'legacy' : 'current';

const state = {
  machines: [],
  tags: [],
  activeTagId: null,
  lots: [],
  activeLotId: null,
  stats: null,
  techOptions: [],
  filter: 'all',
  techFilter: 'all',
  tagFilter: [],
  tagFilterNames: [],
  componentFilter: 'all',
  commentFilter: 'all',
  dateFilter: 'all',
  quickFilter: null,
  activeToken: null,
  quickCommentId: null,
  search: '',
  sort: 'lastSeen',
  layout: '3',
  expandedId: null,
  detailOverrideId: null,
  details: {},
  lastUpdated: null,
  canDeleteReport: false,
  canEditTags: false,
  pageSize: 24,
  pageStart: 0,
  pages: [],
  hasMore: true,
  isLoadingPage: false,
  maxPages: 4,
  loadedOffsets: new Set(),
  totalCount: null,
  lastScrollY: 0,
  scrollDirection: 'down',
  lastLoadScrollY: 0,
  virtualRowHeight: 260,
  virtualOverscanRows: 2,
  virtualRange: { start: 0, end: 0 },
  virtualCalibrated: false,
  rowHeights: new Map(),
  rowHeightAdjustTotal: 0,
  rowHeightsSorted: [],
  rowHeightsPrefix: [],
  rowHeightsDirty: false,
  rowMeasureRaf: null,
  scrollIdleTimer: null,
  lastScrollEventAt: 0,
  pendingOffsets: [],
  pendingOffsetSet: new Set(),
  skipAnchorRestore: false,
  listCacheKey: '',
  listCache: [],
  legacyOnly: isLegacyView,
  legacyMode,
  scrollHold: null,
  scrollHoldRaf: null,
  scrollAnchorHold: null,
  scrollAnchorHoldRaf: null,
  reportsEpoch: 0
};

const listEl = document.getElementById('machine-list');
const listScroll = document.getElementById('list-scroll');
const searchInput = document.getElementById('search-input');
const searchWrap = document.getElementById('search-wrap');
const searchToggle = document.querySelector('.search-toggle');
const refreshBtn = document.getElementById('refresh-btn');
const reportZeroBtn = document.getElementById('report-zero-btn');
const reportZeroModal = document.getElementById('report-zero-modal');
const reportZeroForm = document.getElementById('report-zero-form');
const reportZeroError = document.getElementById('report-zero-error');
const reportZeroSubmit = document.getElementById('report-zero-submit');
const reportZeroLotSelect = document.getElementById('report-zero-lot');
const suggestionBtn = document.getElementById('suggestion-btn');
const suggestionModal = document.getElementById('suggestion-modal');
const suggestionListEl = document.getElementById('suggestion-list');
const suggestionEmptyEl = document.getElementById('suggestion-empty');
const suggestionAddBtn = document.getElementById('suggestion-add-btn');
const suggestionForm = document.getElementById('suggestion-form');
const suggestionTitleInput = document.getElementById('suggestion-title-input');
const suggestionBodyInput = document.getElementById('suggestion-body-input');
const suggestionSubmitBtn = document.getElementById('suggestion-submit');
const suggestionCancelBtn = document.getElementById('suggestion-cancel');
const suggestionError = document.getElementById('suggestion-error');
const suggestionCloseButtons = suggestionModal
  ? suggestionModal.querySelectorAll('[data-action="close-suggestions"]')
  : [];
const technicianOptionsEl = document.getElementById('technician-options');
const patchnoteModal = document.getElementById('patchnote-modal');
const patchnoteBodyEl = document.getElementById('patchnote-body');
const patchnoteOkBtn = document.getElementById('patchnote-ok');
const reportZeroCloseButtons = reportZeroModal
  ? reportZeroModal.querySelectorAll('[data-action="close-report-zero"]')
  : [];
const lastUpdatedEl = document.getElementById('last-updated');
const statTotal = document.getElementById('stat-total');
const statLaptop = document.getElementById('stat-laptop');
const statDesktop = document.getElementById('stat-desktop');
const statUnknown = document.getElementById('stat-unknown');
const statLotCard = document.getElementById('stat-lot-card');
const statLotProgress = document.getElementById('stat-lot-progress');
const statLotName = document.getElementById('stat-lot-name');
const lotOverviewCard = document.getElementById('lot-overview-card');
const lotOverviewName = document.getElementById('lot-overview-name');
const lotOverviewCount = document.getElementById('lot-overview-count');
const lotOverviewStatus = document.getElementById('lot-overview-status');
const lotOverviewFill = document.getElementById('lot-overview-fill');
const lotOverviewList = document.getElementById('lot-overview-list');
const statFilterCards = document.querySelectorAll('.stat-card[data-filter]');
const statTotalCard = document.getElementById('stat-total-card');
const statTimeLabel = document.getElementById('stat-time-label');
const techFiltersEl = document.getElementById('tech-filters');
const tagFiltersEl = document.getElementById('tag-filters');
const sidebarNavButtons = document.querySelectorAll('.sidebar-nav-btn[data-scroll-target]');
const layoutButtons = document.querySelectorAll('.layout-btn');
const testFilterButtons = document.querySelectorAll('.test-filter-btn');
const commentFilterButtons = document.querySelectorAll('.comment-filter-btn');
const testFiltersEl = document.querySelector('.test-filters');
const commentFiltersEl = document.querySelector('.comment-filters');
const resetFiltersBtn = document.getElementById('reset-filters-btn');
const activeFiltersChip = document.getElementById('active-filters-chip');
const adminLink = document.getElementById('admin-link');
const lotsLink = document.getElementById('lots-link');
const commentTimers = new Map();
let activePatchnoteId = null;
let infiniteObserver = null;
let topObserver = null;
let searchTimer = null;
let virtualRenderRaf = null;
let suggestionCache = [];

const categoryLabels = {
  laptop: 'Portable',
  desktop: 'Tour',
  unknown: 'Inconnu'
};
const categoryCycle = ['desktop', 'unknown', 'laptop'];
const DEFAULT_TAG_LABEL = 'En cours';
const DEFAULT_LOT_LABEL = 'Aucun lot';

const statusLabels = {
  ok: 'OK',
  nok: 'NOK',
  fr: 'FR',
  en: 'EN',
  absent: 'Non present',
  not_tested: 'Non teste',
  denied: 'Refuse',
  timeout: 'Timeout',
  scheduled: 'Planifie',
  unknown: '--'
};
const statusCycle = ['not_tested', 'ok', 'nok'];

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
  badgeReader: 'Lecteur badge',
  biosBattery: 'Pile BIOS',
  biosLanguage: 'Langue BIOS',
  biosPassword: 'Mot de passe BIOS',
  wifiStandard: 'Norme Wi-Fi'
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
  'badgeReader',
  'biosBattery',
  'biosLanguage',
  'biosPassword',
  'wifiStandard'
];
const componentStatusCycles = {
  biosLanguage: ['not_tested', 'fr', 'en'],
  biosPassword: ['not_tested', 'ok', 'nok']
};

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
const layoutStorageKey = `mdt-layout${storageSuffix}`;
const storedLayout = window.localStorage ? localStorage.getItem(layoutStorageKey) : null;
if (storedLayout && layoutOptions.has(storedLayout)) {
  state.layout = storedLayout;
}
const dateFilterOrder = ['all', 'today', 'week'];
const dateFilterLabels = {
  all: 'Tous',
  today: "Aujourd'hui",
  week: 'Cette semaine'
};

const prefsStorageKey = `mdt-ui-preferences${storageSuffix}`;
const tagFilterStorageKey = `mdt-tag-filter${storageSuffix}`;
const tagFilterNameStorageKey = `mdt-tag-filter-names${storageSuffix}`;
const categoryFilterOptions = new Set(['all', 'laptop', 'desktop', 'unknown']);
const commentFilterOptions = new Set(['all', 'with', 'without']);
const quickFilterTypes = new Set(['serial', 'mac', 'tech', 'summary']);
const summaryFilterValues = new Set(['ok', 'nok', 'nt']);
const componentFilterOptions = new Set(
  ['all', ...Array.from(testFilterButtons).map((btn) => btn.dataset.component).filter(Boolean)]
);
const listSentinel = document.getElementById('scroll-sentinel');
const listTopSentinel = document.getElementById('scroll-top-sentinel');

applyPreferences();
if (searchInput) {
  searchInput.value = state.search;
}
updateSearchCollapse();

function loadPreferences() {
  if (!window.localStorage) {
    return null;
  }
  try {
    const raw = localStorage.getItem(prefsStorageKey);
    return raw ? JSON.parse(raw) : null;
  } catch (error) {
    return null;
  }
}

function savePreferences() {
  if (!window.localStorage) {
    return;
  }
  const tagNames = Array.isArray(state.tagFilter)
    ? state.tagFilter
        .map((id) => {
          const match = Array.isArray(state.tags)
            ? state.tags.find((tag) => normalizeTagId(tag.id || '') === normalizeTagId(id))
            : null;
          return match && match.name ? String(match.name) : null;
        })
        .filter(Boolean)
    : [];
  const payload = {
    filter: state.filter,
    techFilter: state.techFilter,
    tagFilter: Array.isArray(state.tagFilter) ? state.tagFilter : [],
    tagFilterNames: tagNames,
    componentFilter: state.componentFilter,
    commentFilter: state.commentFilter,
    dateFilter: state.dateFilter,
    search: state.search,
    quickFilter: state.quickFilter
  };
  try {
    localStorage.setItem(prefsStorageKey, JSON.stringify(payload));
    localStorage.setItem(tagFilterStorageKey, JSON.stringify(payload.tagFilter));
    localStorage.setItem(tagFilterNameStorageKey, JSON.stringify(payload.tagFilterNames));
  } catch (error) {
    // Ignore storage errors.
  }
}

function applyPreferences() {
  const prefs = loadPreferences();
  if (!prefs || typeof prefs !== 'object') {
    return;
  }
  if (categoryFilterOptions.has(prefs.filter)) {
    state.filter = prefs.filter;
  }
  if (typeof prefs.techFilter === 'string' && prefs.techFilter) {
    state.techFilter = prefs.techFilter;
  }
  if (Array.isArray(prefs.tagFilter)) {
    state.tagFilter = prefs.tagFilter
      .map((value) => normalizeTagId(value))
      .filter(Boolean);
  }
  if (Array.isArray(prefs.tagFilterNames)) {
    state.tagFilterNames = prefs.tagFilterNames;
  } else if (typeof prefs.tagFilter === 'string' && prefs.tagFilter && prefs.tagFilter !== 'all') {
    const normalized = normalizeTagId(prefs.tagFilter);
    state.tagFilter = normalized ? [normalized] : [];
  } else if (window.localStorage) {
    try {
      const raw = localStorage.getItem(tagFilterStorageKey);
      const parsed = raw ? JSON.parse(raw) : null;
      if (Array.isArray(parsed)) {
        state.tagFilter = parsed.map((value) => normalizeTagId(value)).filter(Boolean);
      }
      const rawNames = localStorage.getItem(tagFilterNameStorageKey);
      const parsedNames = rawNames ? JSON.parse(rawNames) : null;
      if (Array.isArray(parsedNames)) {
        state.tagFilterNames = parsedNames;
      }
    } catch (error) {
      // Ignore storage errors.
    }
  }
  if (componentFilterOptions.has(prefs.componentFilter)) {
    state.componentFilter = prefs.componentFilter;
  }
  if (commentFilterOptions.has(prefs.commentFilter)) {
    state.commentFilter = prefs.commentFilter;
  }
  if (dateFilterOrder.includes(prefs.dateFilter)) {
    state.dateFilter = prefs.dateFilter;
  }
  if (typeof prefs.search === 'string') {
    state.search = prefs.search;
  }
  if (
    prefs.quickFilter &&
    quickFilterTypes.has(prefs.quickFilter.type) &&
    typeof prefs.quickFilter.value === 'string'
  ) {
    if (
      prefs.quickFilter.type !== 'summary' ||
      summaryFilterValues.has(prefs.quickFilter.value)
    ) {
      state.quickFilter = {
        type: prefs.quickFilter.type,
        value: prefs.quickFilter.value
      };
    }
  }
}

function buildQueryParams({ includeCategory = true, includeTech = true } = {}) {
  const params = new URLSearchParams();
  if (includeTech && state.techFilter !== 'all') {
    params.set('tech', state.techFilter);
  }
  if (Array.isArray(state.tagFilter) && state.tagFilter.length > 0) {
    params.set('tags', state.tagFilter.join(','));
  }
  if (state.legacyMode === 'legacy') {
    params.set('legacy', '1');
  } else if (state.legacyMode === 'current') {
    params.set('legacy', '0');
    params.set('latest', '1');
  }
  if (state.dateFilter && state.dateFilter !== 'all') {
    params.set('date', state.dateFilter);
  }
  if (state.commentFilter && state.commentFilter !== 'all') {
    params.set('comment', state.commentFilter);
  }
  if (state.componentFilter && state.componentFilter !== 'all') {
    params.set('component', state.componentFilter);
  }
  if (includeCategory && state.filter && state.filter !== 'all') {
    params.set('category', state.filter);
  }
  if (state.search && state.search.trim()) {
    params.set('search', state.search.trim());
  }
  return params;
}

function resetPagination() {
  state.reportsEpoch += 1;
  state.pageStart = 0;
  state.pages = [];
  state.hasMore = true;
  state.isLoadingPage = false;
  state.machines = [];
  state.details = {};
  state.expandedId = null;
  state.detailOverrideId = null;
  state.quickCommentId = null;
  state.loadedOffsets = new Set();
  state.totalCount = null;
  state.lastLoadScrollY = 0;
  state.listCacheKey = '';
  state.listCache = [];
  state.virtualRange = { start: 0, end: 0 };
  state.virtualCalibrated = false;
  state.rowHeights = new Map();
  state.rowHeightAdjustTotal = 0;
  state.rowHeightsSorted = [];
  state.rowHeightsPrefix = [];
  state.rowHeightsDirty = false;
  if (state.rowMeasureRaf) {
    window.cancelAnimationFrame(state.rowMeasureRaf);
    state.rowMeasureRaf = null;
  }
  if (state.scrollIdleTimer) {
    window.clearTimeout(state.scrollIdleTimer);
    state.scrollIdleTimer = null;
  }
  state.lastScrollEventAt = 0;
  state.pendingOffsets = [];
  state.pendingOffsetSet = new Set();
}

function syncMachinesFromPages() {
  const ordered = [...state.pages].sort((a, b) => a.offset - b.offset);
  const machines = [];
  ordered.forEach((page) => {
    page.items.forEach((item) => {
      machines.push(item);
    });
  });
  state.machines = machines;
  state.listCacheKey = '';
  state.listCache = [];
}

function rebuildRowHeightIndex() {
  if (!state.rowHeightsDirty) {
    return;
  }
  const entries = Array.from(state.rowHeights.entries()).sort((a, b) => a[0] - b[0]);
  const base = Math.max(140, state.virtualRowHeight || 260);
  let running = 0;
  state.rowHeightsSorted = entries;
  state.rowHeightsPrefix = entries.map((entry) => {
    running += entry[1] - base;
    return running;
  });
  state.rowHeightsDirty = false;
}

function updateRowHeight(rowIndex, height) {
  const base = Math.max(140, state.virtualRowHeight || 260);
  if (!Number.isFinite(rowIndex) || rowIndex < 0 || !Number.isFinite(height) || height <= 0) {
    return false;
  }
  const rounded = Math.max(140, Math.round(height));
  const prev = state.rowHeights.get(rowIndex);
  if (prev && Math.abs(prev - rounded) <= 2) {
    return false;
  }
  state.rowHeights.set(rowIndex, rounded);
  const prevDelta = prev ? prev - base : 0;
  state.rowHeightAdjustTotal += rounded - base - prevDelta;
  state.rowHeightsDirty = true;
  return true;
}

function getRowDeltaPrefix(index) {
  rebuildRowHeightIndex();
  if (!state.rowHeightsSorted.length) {
    return 0;
  }
  let low = 0;
  let high = state.rowHeightsSorted.length;
  while (low < high) {
    const mid = Math.floor((low + high) / 2);
    if (state.rowHeightsSorted[mid][0] < index) {
      low = mid + 1;
    } else {
      high = mid;
    }
  }
  return low > 0 ? state.rowHeightsPrefix[low - 1] : 0;
}

function getRowOffset(rowIndex) {
  const base = Math.max(140, state.virtualRowHeight || 260);
  if (!Number.isFinite(rowIndex) || rowIndex <= 0) {
    return 0;
  }
  return rowIndex * base + getRowDeltaPrefix(rowIndex);
}

function getTotalHeight(totalRows) {
  const base = Math.max(140, state.virtualRowHeight || 260);
  return totalRows * base + state.rowHeightAdjustTotal;
}

function findRowForOffset(offset, totalRows) {
  let low = 0;
  let high = totalRows;
  while (low < high) {
    const mid = Math.floor((low + high) / 2);
    if (getRowOffset(mid + 1) <= offset) {
      low = mid + 1;
    } else {
      high = mid;
    }
  }
  return Math.min(totalRows, Math.max(0, low));
}

function getColumnCount() {
  if (!listEl) {
    return Number.parseInt(state.layout, 10) || 1;
  }
  const style = window.getComputedStyle(listEl);
  const template = style.getPropertyValue('grid-template-columns');
  if (template && template !== 'none') {
    const count = template.split(' ').filter(Boolean).length;
    if (count) {
      return count;
    }
  }
  return Number.parseInt(state.layout, 10) || 1;
}

function scheduleVirtualRender() {
  if (virtualRenderRaf) {
    return;
  }
  virtualRenderRaf = window.requestAnimationFrame(() => {
    virtualRenderRaf = null;
    renderList(true);
  });
}

function getScrollTop() {
  return listScroll ? listScroll.scrollTop : window.scrollY || 0;
}

function holdScrollTop(renders = 2, skipAnchor = true) {
  const top = getScrollTop();
  state.scrollHold = {
    top,
    remaining: Math.max(1, Number.parseInt(renders, 10) || 1)
  };
  state.skipAnchorRestore = Boolean(skipAnchor);
}

function scheduleScrollHoldAdjustment() {
  if (!state.scrollHold || state.scrollHoldRaf) {
    return;
  }
  const tick = () => {
    if (!state.scrollHold) {
      state.scrollHoldRaf = null;
      return;
    }
    const remaining = state.scrollHold.remaining || 0;
    if (remaining > 0) {
      setScrollTop(state.scrollHold.top);
      state.scrollHold.remaining = remaining - 1;
    }
    if (state.scrollHold.remaining <= 0) {
      state.scrollHold = null;
      state.scrollHoldRaf = null;
      return;
    }
    state.scrollHoldRaf = window.setTimeout(tick, 120);
  };
  state.scrollHoldRaf = window.setTimeout(tick, 80);
}

function scheduleAnchorHoldAdjustment() {
  if (!state.scrollAnchorHold || state.scrollAnchorHoldRaf) {
    return;
  }
  state.scrollAnchorHoldRaf = window.requestAnimationFrame(() => {
    state.scrollAnchorHoldRaf = null;
    window.requestAnimationFrame(() => {
      if (!state.scrollAnchorHold) {
        return;
      }
      if (Date.now() > (state.scrollAnchorHold.until || 0)) {
        state.scrollAnchorHold = null;
        return;
      }
      restoreScrollAnchor({
        id: state.scrollAnchorHold.id,
        top: state.scrollAnchorHold.top
      });
    });
  });
}

function holdCardAnchor(targetId, durationMs = 1800) {
  if (!listEl || !targetId) {
    return;
  }
  const safeId = window.CSS && CSS.escape ? CSS.escape(targetId) : String(targetId).replace(/"/g, '\\"');
  const card = listEl.querySelector(`.machine-card[data-id="${safeId}"]`);
  if (!card) {
    return;
  }
  const containerTop = getScrollContainerTop();
  const rect = card.getBoundingClientRect();
  const ttl = Number.isFinite(durationMs) && durationMs > 0 ? durationMs : 1800;
  state.scrollAnchorHold = {
    id: targetId,
    top: rect.top - containerTop,
    until: Date.now() + ttl
  };
}

function getViewportHeight() {
  return listScroll ? listScroll.clientHeight : window.innerHeight;
}

function getScrollContainerTop() {
  if (listScroll) {
    return listScroll.getBoundingClientRect().top;
  }
  return 0;
}

function getListOffsetTop() {
  if (!listEl) {
    return 0;
  }
  if (listScroll) {
    return listEl.offsetTop;
  }
  return listEl.getBoundingClientRect().top + (window.scrollY || 0);
}

function adjustScrollBy(delta) {
  if (!delta || !Number.isFinite(delta)) {
    return;
  }
  if (listScroll) {
    listScroll.scrollTop += delta;
  } else {
    window.scrollBy(0, delta);
  }
}

function setScrollTop(value) {
  if (!Number.isFinite(value)) {
    return;
  }
  if (listScroll) {
    listScroll.scrollTop = value;
  } else {
    window.scrollTo({ top: value });
  }
}

function captureScrollAnchor() {
  if (!listEl) {
    return null;
  }
  const containerTop = getScrollContainerTop();
  const cards = listEl.querySelectorAll('.machine-card:not(.is-placeholder)');
  if (!cards.length) {
    return null;
  }
  let anchor = null;
  cards.forEach((card) => {
    if (anchor) {
      return;
    }
    const rect = card.getBoundingClientRect();
    const relativeTop = rect.top - containerTop;
    if (relativeTop >= -20) {
      anchor = {
        id: card.dataset.id || '',
        index: card.dataset.index || '',
        top: relativeTop
      };
    }
  });
  if (!anchor) {
    const first = cards[0];
    const rect = first.getBoundingClientRect();
    anchor = {
      id: first.dataset.id || '',
      index: first.dataset.index || '',
      top: rect.top - containerTop
    };
  }
  return anchor;
}

function restoreScrollAnchor(anchor) {
  if (!anchor || !listEl) {
    return;
  }
  let selector = '';
  if (anchor.id) {
    const safeId = window.CSS && CSS.escape ? CSS.escape(anchor.id) : anchor.id.replace(/"/g, '\\"');
    selector = `.machine-card[data-id="${safeId}"]`;
  }
  let target = selector ? listEl.querySelector(selector) : null;
  if (!target && anchor.index) {
    const safeIndex = anchor.index.replace(/"/g, '\\"');
    target = listEl.querySelector(`.machine-card[data-index="${safeIndex}"]`);
  }
  if (!target) {
    return;
  }
  const containerTop = getScrollContainerTop();
  const rect = target.getBoundingClientRect();
  const newTop = rect.top - containerTop;
  const delta = newTop - anchor.top;
  if (Math.abs(delta) > 1) {
    adjustScrollBy(delta);
  }
}

function invalidateListCache() {
  state.listCacheKey = '';
  state.listCache = [];
}

function dropCachedDetails(items = []) {
  if (!items.length) {
    return;
  }
  items.forEach((item) => {
    if (!item || !item.id) {
      return;
    }
    const id = String(item.id);
    if (state.details[id]) {
      delete state.details[id];
    }
    if (state.expandedId === id) {
      state.expandedId = null;
    }
    if (state.quickCommentId === id) {
      state.quickCommentId = null;
    }
  });
}

function trimPagesAround(centerOffset) {
  if (state.pages.length <= state.maxPages) {
    return;
  }
  const center = Number.isFinite(centerOffset) ? centerOffset : 0;
  state.pages.sort((a, b) => a.offset - b.offset);
  while (state.pages.length > state.maxPages) {
    const first = state.pages[0];
    const last = state.pages[state.pages.length - 1];
    const distanceFirst = Math.abs(center - first.offset);
    const distanceLast = Math.abs(center - last.offset);
    const removed = distanceFirst >= distanceLast ? state.pages.shift() : state.pages.pop();
    if (removed) {
      dropCachedDetails(removed.items);
      if (state.loadedOffsets) {
        state.loadedOffsets.delete(removed.offset);
      }
    }
  }
}

async function loadStats() {
  try {
    const params = buildQueryParams({ includeCategory: false, includeTech: true });
    const response = await fetch(`/api/stats?${params.toString()}`);
    if (response.status === 401) {
      window.location.href = '/login';
      return;
    }
    if (!response.ok) {
      throw new Error('stats_failed');
    }
    const data = await response.json();
    if (!data.ok) {
      throw new Error('stats_failed');
    }
    state.stats = {
      total: data.total || 0,
      laptop: data.laptop || 0,
      desktop: data.desktop || 0,
      unknown: data.unknown || 0
    };
    state.techOptions = Array.isArray(data.techs) ? data.techs : [];
    renderTechFilters();
  } catch (error) {
    state.stats = null;
    state.techOptions = [];
  }
  updateStats();
}

async function loadMeta() {
  try {
    const params = new URLSearchParams();
    params.set('meta', '1');
    if (state.legacyMode === 'legacy') {
      params.set('legacy', '1');
    } else if (state.legacyMode === 'current') {
      params.set('legacy', '0');
    }
    const response = await fetch(`/api/machines?${params.toString()}`);
    if (response.status === 401) {
      window.location.href = '/login';
      return;
    }
    if (!response.ok) {
      throw new Error('meta_failed');
    }
    const data = await response.json();
    state.tags = Array.isArray(data.tags) ? data.tags : [];
    state.activeTagId = data.activeTagId ? normalizeTagId(data.activeTagId) : null;
    state.lots = Array.isArray(data.lots) ? data.lots : [];
    state.activeLotId = data.activeLotId ? normalizeLotId(data.activeLotId) : null;
    if (data.permissions && typeof data.permissions.canDeleteReport === 'boolean') {
      state.canDeleteReport = data.permissions.canDeleteReport;
    }
    if (data.permissions && typeof data.permissions.canEditTags === 'boolean') {
      state.canEditTags = data.permissions.canEditTags;
    }
    hydrateTagFilterFromNames();
    renderTagFilters();
    renderTechFilters();
    renderLotMetrics();
    renderReportZeroLotOptions();
  } catch (error) {
    // ignore
  }
}

function getPageForOffset(offset) {
  if (!state.pages.length) {
    return null;
  }
  return state.pages.find((page) => page.offset === offset) || null;
}

function getItemAtIndex(index) {
  const pageSize = state.pageSize;
  if (!Number.isFinite(index) || index < 0) {
    return null;
  }
  const pageOffset = Math.floor(index / pageSize) * pageSize;
  const page = getPageForOffset(pageOffset);
  if (!page) {
    return null;
  }
  return page.items[index - pageOffset] || null;
}

function queueOffset(offset) {
  if (!Number.isFinite(offset) || offset < 0) {
    return;
  }
  if (state.loadedOffsets.has(offset) || state.pendingOffsetSet.has(offset)) {
    return;
  }
  state.pendingOffsets.push(offset);
  state.pendingOffsetSet.add(offset);
}

function pumpOffsetQueue() {
  if (state.isLoadingPage) {
    return;
  }
  if (!state.pendingOffsets.length) {
    return;
  }
  const next = state.pendingOffsets.shift();
  state.pendingOffsetSet.delete(next);
  loadReportsPage(next);
}

function ensurePagesForRange(startIndex, endIndex) {
  const pageSize = state.pageSize;
  const totalCount = Number.isFinite(state.totalCount) ? state.totalCount : null;
  const maxPage = totalCount != null ? Math.max(0, Math.ceil(totalCount / pageSize) - 1) : null;
  const startPage = Math.max(0, Math.floor(startIndex / pageSize) - 1);
  const endPage = Math.floor(Math.max(startIndex, endIndex - 1) / pageSize) + 1;
  const upperBound = maxPage != null ? Math.min(endPage, maxPage) : endPage;
  for (let pageIndex = startPage; pageIndex <= upperBound; pageIndex += 1) {
    const offset = pageIndex * pageSize;
    queueOffset(offset);
  }
  pumpOffsetQueue();
}

async function loadReportsPage(offset) {
  const epoch = state.reportsEpoch;
  if (epoch !== state.reportsEpoch) {
    return;
  }
  if (state.isLoadingPage) {
    return;
  }
  if (!Number.isFinite(offset) || offset < 0) {
    return;
  }
  if (state.loadedOffsets && state.loadedOffsets.has(offset)) {
    return;
  }
  state.isLoadingPage = true;
  try {
    const params = buildQueryParams({ includeCategory: true });
    params.set('limit', String(state.pageSize));
    params.set('offset', String(offset));
    if (!Number.isFinite(state.totalCount)) {
      params.set('includeTotal', '1');
    }
    const response = await fetch(`/api/reports?${params.toString()}`);
    if (response.status === 401) {
      window.location.href = '/login';
      return;
    }
    if (!response.ok) {
      throw new Error('fetch_failed');
    }
    const data = await response.json();
    if (epoch !== state.reportsEpoch) {
      return;
    }
    if (!data.ok) {
      throw new Error('fetch_failed');
    }
    if (data.total != null) {
      const totalValue = Number.parseInt(data.total, 10);
      if (Number.isFinite(totalValue)) {
        state.totalCount = totalValue;
      }
    }
    const items = Array.isArray(data.machines) ? data.machines : [];
    items.forEach((item) => {
      item._page = offset;
    });
    state.pages.push({ offset, items });
    state.pages.sort((a, b) => a.offset - b.offset);
    state.pageStart = state.pages.length ? state.pages[0].offset : 0;
    if (Number.isFinite(state.totalCount)) {
      state.hasMore = offset + items.length < state.totalCount;
    } else {
      state.hasMore = Boolean(data.hasMore);
    }
    if (state.loadedOffsets) {
      state.loadedOffsets.add(offset);
    }
    trimPagesAround(offset);
    state.pageStart = state.pages.length ? state.pages[0].offset : 0;
    syncMachinesFromPages();
    renderList();
    updateLastUpdated();
    state.lastLoadScrollY = getScrollTop();
  } catch (error) {
    if (epoch === state.reportsEpoch) {
      listEl.innerHTML = '<div class="empty">Erreur lors du chargement.</div>';
    }
  } finally {
    if (epoch === state.reportsEpoch) {
      state.isLoadingPage = false;
      pumpOffsetQueue();
    }
  }
}

async function reloadReports() {
  listEl.innerHTML = '<div class="loading">Chargement des postes...</div>';
  resetPagination();
  state.skipAnchorRestore = true;
  if (listScroll) {
    listScroll.scrollTop = 0;
  } else {
    window.scrollTo({ top: 0 });
  }
  await Promise.all([loadStats(), loadReportsPage(0)]);
  renderTagFilters();
  renderTechnicianOptions();
  updateCommentFilterButtons();
}

function initInfiniteScroll() {
  state.lastScrollY = getScrollTop();
  state.scrollDirection = 'down';
  const scrollTarget = listScroll || window;
  scrollTarget.addEventListener(
    'scroll',
    () => {
      const current = getScrollTop();
      state.scrollDirection = current >= state.lastScrollY ? 'down' : 'up';
      state.lastScrollY = current;
      state.lastScrollEventAt = Date.now();
      if (state.scrollIdleTimer) {
        window.clearTimeout(state.scrollIdleTimer);
      }
      state.scrollIdleTimer = window.setTimeout(() => {
        scheduleVirtualRender();
      }, 160);
      scheduleVirtualRender();
    },
    { passive: true }
  );
  window.addEventListener(
    'resize',
    () => {
      updateSearchCollapse();
      scheduleVirtualRender();
    },
    { passive: true }
  );
}

function applyLayout() {
  if (!listEl) {
    return;
  }
  listEl.classList.remove('columns-1', 'columns-2', 'columns-3', 'columns-6');
  listEl.classList.add(`columns-${state.layout}`);
  state.rowHeights = new Map();
  state.rowHeightAdjustTotal = 0;
  state.rowHeightsSorted = [];
  state.rowHeightsPrefix = [];
  state.rowHeightsDirty = false;
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
  if (testFiltersEl) {
    testFiltersEl.classList.toggle('is-all', state.componentFilter === 'all');
  }
  testFilterButtons.forEach((btn) => {
    const active = btn.dataset.component === state.componentFilter;
    btn.classList.toggle('active', active);
    btn.setAttribute('aria-pressed', active ? 'true' : 'false');
  });
  updateFilterDockState();
}

function normalizeTech(value) {
  if (!value) {
    return '';
  }
  return String(value).trim().replace(/\s+/g, ' ');
}

function techKey(value) {
  const normalized = normalizeTech(value);
  if (!normalized) {
    return '';
  }
  return normalized
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase();
}

function pickTechLabel(labels) {
  if (!labels || !labels.length) {
    return '';
  }
  const scored = labels
    .map((label) => {
      const cleaned = normalizeTech(label);
      const hasLower = /[a-zà-öø-ÿ]/.test(cleaned);
      const hasUpper = /[A-ZÀ-ÖØ-ß]/.test(cleaned);
      const hasDiacritics = cleaned.normalize('NFD') !== cleaned;
      let score = 0;
      if (hasUpper && hasLower) {
        score = 2;
      } else if (hasLower) {
        score = 1;
      }
      if (hasDiacritics) {
        score += 2;
      }
      return { label: cleaned, score };
    })
    .filter((item) => item.label);

  if (!scored.length) {
    return '';
  }

  scored.sort((a, b) => {
    if (b.score !== a.score) {
      return b.score - a.score;
    }
    return a.label.localeCompare(b.label, 'fr');
  });

  return scored[0].label;
}

function normalizeTagId(value) {
  if (!value) {
    return '';
  }
  return String(value).trim().toLowerCase();
}

function getTagId(machine) {
  if (!machine) {
    return '';
  }
  const tagId = normalizeTagId(machine.tagId || '');
  if (tagId) {
    return tagId;
  }
  return normalizeTagId(state.activeTagId || '');
}

function getTagLabelById(tagId) {
  if (!tagId) {
    return DEFAULT_TAG_LABEL;
  }
  const normalized = normalizeTagId(tagId);
  const match = state.tags.find((tag) => normalizeTagId(tag.id) === normalized);
  return match && match.name ? match.name : DEFAULT_TAG_LABEL;
}

function getTagLabel(machine) {
  if (!machine) {
    return DEFAULT_TAG_LABEL;
  }
  if (machine.tagName) {
    return String(machine.tagName).trim() || DEFAULT_TAG_LABEL;
  }
  const label = machine.tag ? String(machine.tag).trim() : '';
  if (label) {
    return label;
  }
  const tagId = getTagId(machine);
  return tagId ? getTagLabelById(tagId) : DEFAULT_TAG_LABEL;
}

function normalizeLotId(value) {
  if (!value) {
    return '';
  }
  return String(value).trim().toLowerCase();
}

function buildLotLabel(lot) {
  if (!lot || typeof lot !== 'object') {
    return DEFAULT_LOT_LABEL;
  }
  const supplier = String(lot.supplier || '').trim();
  const lotNumber = String(lot.lotNumber || '').trim();
  if (supplier && lotNumber) {
    return `${supplier} - lot ${lotNumber}`;
  }
  if (lot.label) {
    return String(lot.label).trim();
  }
  if (supplier) {
    return supplier;
  }
  if (lotNumber) {
    return `Lot ${lotNumber}`;
  }
  return DEFAULT_LOT_LABEL;
}

function getMachineLot(machine) {
  if (!machine || !machine.lot || typeof machine.lot !== 'object') {
    return null;
  }
  const lotId = normalizeLotId(machine.lot.id || '');
  if (lotId) {
    const fromMeta = Array.isArray(state.lots)
      ? state.lots.find((lot) => normalizeLotId(lot.id || '') === lotId)
      : null;
    if (fromMeta) {
      return { ...fromMeta, ...machine.lot };
    }
  }
  return machine.lot;
}

function getActiveLot() {
  if (!Array.isArray(state.lots) || !state.lots.length) {
    return null;
  }
  const activeId = normalizeLotId(state.activeLotId || '');
  if (activeId) {
    const exact = state.lots.find((lot) => normalizeLotId(lot.id || '') === activeId);
    if (exact) {
      return exact;
    }
  }
  return (
    state.lots.find(
      (lot) => lot && !lot.isPaused && Number.isFinite(lot.targetCount) && lot.producedCount < lot.targetCount
    ) || null
  );
}

function getLotProgress(lot) {
  const producedRaw = Number.parseInt(lot && lot.producedCount, 10);
  const targetRaw = Number.parseInt(lot && lot.targetCount, 10);
  const remainingRaw = Number.parseInt(lot && lot.remainingCount, 10);
  const produced = Number.isFinite(producedRaw) ? Math.max(producedRaw, 0) : 0;
  const target = Number.isFinite(targetRaw) ? Math.max(targetRaw, 0) : 0;
  const remaining = Number.isFinite(remainingRaw) ? Math.max(remainingRaw, 0) : Math.max(target - produced, 0);
  const ratio = target > 0 ? Math.max(0, Math.min(1, produced / target)) : 0;
  return {
    produced,
    target,
    remaining,
    percent: Math.round(ratio * 100)
  };
}

function getLotStatusText(lot, progress) {
  if (!lot) {
    return 'Aucun lot actif';
  }
  if (lot.isPaused) {
    return 'Production en pause';
  }
  if (progress.target > 0 && progress.produced >= progress.target) {
    return 'Objectif atteint';
  }
  if (progress.remaining > 0) {
    return `${progress.remaining} restants`;
  }
  return 'En cours';
}

function rankLots(lots, activeId) {
  return [...lots].sort((a, b) => {
    const aId = normalizeLotId(a && a.id);
    const bId = normalizeLotId(b && b.id);
    const aActive = aId && activeId && aId === activeId ? 1 : 0;
    const bActive = bId && activeId && bId === activeId ? 1 : 0;
    if (aActive !== bActive) {
      return bActive - aActive;
    }
    const aPaused = a && a.isPaused ? 1 : 0;
    const bPaused = b && b.isPaused ? 1 : 0;
    if (aPaused !== bPaused) {
      return aPaused - bPaused;
    }
    const aPriorityRaw = Number.parseInt(a && a.priority, 10);
    const bPriorityRaw = Number.parseInt(b && b.priority, 10);
    const aPriority = Number.isFinite(aPriorityRaw) ? aPriorityRaw : 99999;
    const bPriority = Number.isFinite(bPriorityRaw) ? bPriorityRaw : 99999;
    if (aPriority !== bPriority) {
      return aPriority - bPriority;
    }
    const aLabel = buildLotLabel(a);
    const bLabel = buildLotLabel(b);
    return aLabel.localeCompare(bLabel, 'fr');
  });
}

function renderLotOverview(activeLot = getActiveLot()) {
  if (!lotOverviewCard) {
    return;
  }
  const lots = Array.isArray(state.lots)
    ? state.lots.filter((lot) => lot && typeof lot === 'object')
    : [];
  const activeId = normalizeLotId(activeLot && activeLot.id ? activeLot.id : '');

  if (activeLot) {
    const progress = getLotProgress(activeLot);
    const status = getLotStatusText(activeLot, progress);
    lotOverviewCard.classList.toggle('is-active', !activeLot.isPaused && progress.target > progress.produced);
    if (lotOverviewName) {
      lotOverviewName.textContent = buildLotLabel(activeLot);
    }
    if (lotOverviewCount) {
      const targetLabel = progress.target > 0 ? progress.target : '--';
      lotOverviewCount.textContent = `${progress.produced} / ${targetLabel}`;
    }
    if (lotOverviewStatus) {
      lotOverviewStatus.textContent = status;
    }
    if (lotOverviewFill) {
      lotOverviewFill.style.width = `${progress.percent}%`;
    }
  } else {
    lotOverviewCard.classList.remove('is-active');
    if (lotOverviewName) {
      lotOverviewName.textContent = 'Aucun lot actif';
    }
    if (lotOverviewCount) {
      lotOverviewCount.textContent = '-- / --';
    }
    if (lotOverviewStatus) {
      lotOverviewStatus.textContent = 'Attente d\'un lot actif';
    }
    if (lotOverviewFill) {
      lotOverviewFill.style.width = '0%';
    }
  }

  if (!lotOverviewList) {
    return;
  }
  if (!lots.length) {
    lotOverviewList.innerHTML = '<li class="lot-overview-empty">Aucun lot configure.</li>';
    return;
  }

  const ranked = rankLots(lots, activeId).slice(0, 4);
  lotOverviewList.innerHTML = ranked
    .map((lot) => {
      const lotId = normalizeLotId(lot.id);
      const isActive = Boolean(activeId && lotId === activeId);
      const progress = getLotProgress(lot);
      const statusText = getLotStatusText(lot, progress);
      const targetLabel = progress.target > 0 ? progress.target : '--';
      const classes = ['lot-overview-item'];
      if (isActive) {
        classes.push('is-active');
      }
      if (lot.isPaused) {
        classes.push('is-paused');
      }
      return `
        <li class="${classes.join(' ')}">
          <div class="lot-overview-item-head">
            <strong>${escapeHtml(buildLotLabel(lot))}</strong>
            <span>${progress.produced}/${targetLabel}</span>
          </div>
          <div class="lot-overview-track" aria-hidden="true">
            <span style="width:${progress.percent}%"></span>
          </div>
          <p class="lot-overview-item-status">${escapeHtml(statusText)}</p>
        </li>
      `;
    })
    .join('');
}

function renderLotMetrics() {
  const lot = getActiveLot();
  renderLotOverview(lot);
  if (!statLotProgress || !statLotName || !statLotCard) {
    return;
  }
  if (!lot) {
    statLotCard.classList.remove('is-active');
    statLotProgress.textContent = '--';
    statLotName.textContent = 'Aucun lot actif';
    return;
  }
  const progress = getLotProgress(lot);
  const produced = progress.produced;
  const target = progress.target;
  const remaining = progress.remaining;
  statLotCard.classList.toggle('is-active', !lot.isPaused && produced < target);
  statLotProgress.textContent = `${produced}/${target > 0 ? target : '--'}`;
  const status = lot.isPaused ? 'PAUSE' : remaining > 0 ? `${remaining} restants` : 'objectif atteint';
  statLotName.textContent = `${buildLotLabel(lot)} (${status})`;
}

function renderReportZeroLotOptions() {
  if (!reportZeroLotSelect) {
    return;
  }
  const currentValue = String(reportZeroLotSelect.value || '').trim();
  const options = [
    '<option value="">Attribution automatique (priorite/assignation)</option>'
  ];
  const lots = Array.isArray(state.lots) ? state.lots : [];
  lots.forEach((lot) => {
    if (!lot || !lot.id) {
      return;
    }
    const label = buildLotLabel(lot);
    const produced = Number.isFinite(lot.producedCount) ? lot.producedCount : 0;
    const target = Number.isFinite(lot.targetCount) ? lot.targetCount : 0;
    const paused = lot.isPaused ? ' [PAUSE]' : '';
    options.push(
      `<option value="${escapeHtml(lot.id)}">${escapeHtml(label)} (${produced}/${target})${escapeHtml(
        paused
      )}</option>`
    );
  });
  reportZeroLotSelect.innerHTML = options.join('');
  if (currentValue) {
    reportZeroLotSelect.value = currentValue;
  }
}

function updateTechFilterButtons() {
  if (!techFiltersEl) {
    return;
  }
  techFiltersEl.classList.toggle('is-all', state.techFilter === 'all');
  const buttons = techFiltersEl.querySelectorAll('.tech-filter-btn');
  buttons.forEach((btn) => {
    const active = (btn.dataset.tech || 'all') === state.techFilter;
    btn.classList.toggle('active', active);
    btn.setAttribute('aria-pressed', active ? 'true' : 'false');
  });
  updateFilterDockState();
}

function renderTechFilters() {
  if (!techFiltersEl) {
    return;
  }
  const techMap = new Map();
  const techSource = Array.isArray(state.techOptions) && state.techOptions.length
    ? state.techOptions
    : state.machines.map((machine) => machine.technician);
  techSource.forEach((tech) => {
    const label = normalizeTech(tech);
    if (!label) {
      return;
    }
    const key = techKey(label);
    const entry = techMap.get(key);
    if (entry) {
      entry.labels.push(label);
    } else {
      techMap.set(key, { key, labels: [label] });
    }
  });

  const techList = Array.from(techMap.values())
    .map((entry) => [entry.key, pickTechLabel(entry.labels)])
    .filter((entry) => entry[1])
    .sort((a, b) => a[1].localeCompare(b[1], 'fr'));

  if (state.techFilter !== 'all' && !techMap.has(state.techFilter)) {
    state.techFilter = 'all';
    savePreferences();
  }

  const buttons = [];
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

function updateTagFilterButtons() {
  if (!tagFiltersEl) {
    return;
  }
  const selected = new Set(Array.isArray(state.tagFilter) ? state.tagFilter : []);
  tagFiltersEl.classList.toggle('has-selection', selected.size > 0);
  const toggle = tagFiltersEl.querySelector('.tag-select-toggle');
  const summary = tagFiltersEl.querySelector('.tag-select-summary');
  const count = tagFiltersEl.querySelector('.tag-select-count');
  if (count) {
    count.textContent = selected.size > 0 ? String(selected.size) : '';
  }
  if (toggle) {
    toggle.setAttribute(
      'aria-expanded',
      tagFiltersEl.classList.contains('is-open') ? 'true' : 'false'
    );
  }
  if (summary) {
    const labels = Array.from(tagFiltersEl.querySelectorAll('.tag-select-input:checked'))
      .map((input) => {
        const item = input.closest('.tag-select-item');
        const label = item ? item.querySelector('.tag-select-label') : null;
        return label ? label.textContent : '';
      })
      .filter(Boolean);
    summary.textContent = buildTagSummaryFromLabels(labels);
  }
  updateFilterDockState();
}

function renderTagFilters() {
  if (!tagFiltersEl) {
    return;
  }
  const wasOpen = tagFiltersEl.classList.contains('is-open');
  let selected = new Set(Array.isArray(state.tagFilter) ? state.tagFilter : []);
  const tags = Array.isArray(state.tags) ? state.tags : [];
  const tagList = tags
    .filter((tag) => {
      const tagId = normalizeTagId(tag.id || '');
      if (!tagId) {
        return false;
      }
      return true;
    })
    .sort((a, b) => String(a.name || '').localeCompare(String(b.name || ''), 'fr'));

  const validIds = new Set(tagList.map((tag) => normalizeTagId(tag.id || '')));
  if (selected.size > 0) {
    const nextSelected = Array.from(selected).filter((id) => validIds.has(id));
    if (nextSelected.length !== selected.size) {
      state.tagFilter = nextSelected;
      selected = new Set(nextSelected);
      savePreferences();
    }
  }

  const items = [];
  tagList.forEach((tag) => {
    const tagId = normalizeTagId(tag.id || '');
    if (!tagId) {
      return;
    }
    const label = tag.name || DEFAULT_TAG_LABEL;
    const checked = selected.has(tagId);
    items.push(
      `
        <label class="tag-select-item">
          <input class="tag-select-input" type="checkbox" value="${escapeHtml(tagId)}" ${
            checked ? 'checked' : ''
          } />
          <span class="tag-select-label">${escapeHtml(label)}</span>
        </label>
      `
    );
  });
  tagFiltersEl.innerHTML = `
    <div class="tag-select-header">
      <button class="tag-select-toggle" type="button" aria-expanded="${wasOpen ? 'true' : 'false'}">
        Tags
        <span class="tag-select-count">${selected.size ? selected.size : ''}</span>
      </button>
      <span class="tag-select-summary">${buildTagSummary(tagList, selected)}</span>
    </div>
    <div class="tag-select-panel">
      <div class="tag-select-panel-header">
        <span class="tag-select-title">Selection</span>
        <button class="tag-select-clear" type="button" data-action="clear-tags">Tout</button>
      </div>
      <div class="tag-select-list">
        ${items.join('')}
      </div>
    </div>
  `;
  if (wasOpen) {
    tagFiltersEl.classList.add('is-open');
  } else {
    tagFiltersEl.classList.remove('is-open');
  }
  updateTagFilterButtons();
}

function normalizeTagName(value) {
  if (!value) {
    return '';
  }
  return String(value)
    .trim()
    .toLowerCase()
    .normalize('NFD')
    .replace(/[\\u0300-\\u036f]/g, '');
}

function hydrateTagFilterFromNames() {
  if (!Array.isArray(state.tags) || !state.tags.length) {
    return;
  }
  let names = Array.isArray(state.tagFilterNames) ? state.tagFilterNames : [];
  if ((!names || !names.length) && window.localStorage) {
    try {
      const raw = localStorage.getItem(tagFilterNameStorageKey);
      const parsed = raw ? JSON.parse(raw) : null;
      if (Array.isArray(parsed)) {
        names = parsed;
      }
    } catch (error) {
      names = [];
    }
  }
  if (!names || !names.length) {
    return;
  }
  const nameMap = new Map();
  const validIds = new Set();
  state.tags.forEach((tag) => {
    const id = normalizeTagId(tag.id || '');
    if (!id || !tag.name) {
      return;
    }
    validIds.add(id);
    nameMap.set(normalizeTagName(tag.name), id);
  });
  const ids = names
    .map((name) => nameMap.get(normalizeTagName(name)))
    .filter(Boolean);
  if (!ids.length) {
    return;
  }
  const current = Array.isArray(state.tagFilter) ? state.tagFilter : [];
  const valid = current.filter((id) => validIds.has(normalizeTagId(id)));
  if (!valid.length || valid.length < ids.length) {
    state.tagFilter = Array.from(new Set([...valid, ...ids]));
    savePreferences();
  }
}

function buildTagSummary(tagList = [], selected = new Set()) {
  const selectedLabels = tagList
    .filter((tag) => selected.has(normalizeTagId(tag.id || '')))
    .map((tag) => tag.name || DEFAULT_TAG_LABEL);
  return buildTagSummaryFromLabels(selectedLabels);
}

function buildTagSummaryFromLabels(labels = []) {
  if (!labels.length) {
    return 'Tous';
  }
  const shown = labels.slice(0, 2);
  const extra = labels.length - shown.length;
  const suffix = extra > 0 ? ` +${extra}` : '';
  return `${shown.join(', ')}${suffix}`;
}

function updateTimeFilterLabel() {
  if (!statTimeLabel) {
    return;
  }
  statTimeLabel.textContent = dateFilterLabels[state.dateFilter] || 'Tous';
}

function getActiveFilterCount() {
  let count = 0;
  if (state.filter && state.filter !== 'all') {
    count += 1;
  }
  if (state.dateFilter && state.dateFilter !== 'all') {
    count += 1;
  }
  if (state.techFilter && state.techFilter !== 'all') {
    count += 1;
  }
  if (Array.isArray(state.tagFilter) && state.tagFilter.length) {
    count += state.tagFilter.length;
  }
  if (state.componentFilter && state.componentFilter !== 'all') {
    count += 1;
  }
  if (state.commentFilter && state.commentFilter !== 'all') {
    count += 1;
  }
  if (state.search && state.search.trim()) {
    count += 1;
  }
  if (state.quickFilter && state.quickFilter.value) {
    count += 1;
  }
  return count;
}

function updateFilterDockState() {
  const count = getActiveFilterCount();
  if (activeFiltersChip) {
    const plural = count > 1 ? 's' : '';
    activeFiltersChip.classList.toggle('is-active', count > 0);
    activeFiltersChip.textContent = count > 0 ? `${count} filtre${plural} actif${plural}` : 'Aucun filtre actif';
  }
  if (resetFiltersBtn) {
    resetFiltersBtn.disabled = count === 0;
  }
}

function updateSearchCollapse() {
  if (!searchWrap || !searchInput) {
    return;
  }
  const collapseEnabled = window.matchMedia('(max-width: 980px)').matches;
  const hasValue = Boolean(state.search && state.search.trim());
  searchWrap.classList.toggle('is-collapsed', collapseEnabled && !hasValue);
  updateFilterDockState();
}

function setActiveSidebarNav(targetId) {
  if (!sidebarNavButtons.length) {
    return;
  }
  sidebarNavButtons.forEach((button) => {
    const active = (button.dataset.scrollTarget || '') === targetId;
    button.classList.toggle('is-active', active);
    button.setAttribute('aria-pressed', active ? 'true' : 'false');
  });
}

function initSidebarNavigation() {
  if (!sidebarNavButtons.length) {
    return;
  }
  sidebarNavButtons.forEach((button) => {
    button.setAttribute('aria-pressed', button.classList.contains('is-active') ? 'true' : 'false');
    button.addEventListener('click', () => {
      const targetId = (button.dataset.scrollTarget || '').trim();
      if (!targetId) {
        return;
      }
      const target = document.getElementById(targetId);
      if (!target) {
        return;
      }
      setActiveSidebarNav(targetId);
      target.scrollIntoView({ behavior: 'smooth', block: 'start' });
    });
  });
}

function getStartOfToday() {
  const now = new Date();
  return new Date(now.getFullYear(), now.getMonth(), now.getDate());
}

function getEndOfToday() {
  const now = new Date();
  return new Date(now.getFullYear(), now.getMonth(), now.getDate(), 23, 59, 59, 999);
}

function getStartOfWeek() {
  const now = new Date();
  const day = now.getDay();
  const diff = (day + 6) % 7;
  const start = new Date(now);
  start.setDate(now.getDate() - diff);
  start.setHours(0, 0, 0, 0);
  return start;
}

function getEndOfWeek() {
  const start = getStartOfWeek();
  const end = new Date(start);
  end.setDate(start.getDate() + 6);
  end.setHours(23, 59, 59, 999);
  return end;
}

function passesDateFilter(dateValue) {
  if (state.dateFilter === 'all') {
    return true;
  }
  if (!dateValue) {
    return false;
  }
  const date = new Date(dateValue);
  if (Number.isNaN(date.getTime())) {
    return false;
  }
  const start = state.dateFilter === 'today' ? getStartOfToday() : getStartOfWeek();
  const end = state.dateFilter === 'today' ? getEndOfToday() : getEndOfWeek();
  return date >= start && date <= end;
}

function cycleDateFilter(render = true) {
  const currentIndex = dateFilterOrder.indexOf(state.dateFilter);
  const nextIndex = currentIndex === -1 ? 0 : (currentIndex + 1) % dateFilterOrder.length;
  state.dateFilter = dateFilterOrder[nextIndex];
  updateTimeFilterLabel();
  savePreferences();
  if (render) {
    reloadReports();
  }
}

function renderTechnicianOptions() {
  if (!technicianOptionsEl) {
    return;
  }
  const techMap = new Map();
  const source = Array.isArray(state.techOptions) && state.techOptions.length
    ? state.techOptions
    : state.machines.map((machine) => machine.technician);
  source.forEach((tech) => {
    const label = normalizeTech(tech);
    if (!label) {
      return;
    }
    const key = label.toLowerCase();
    if (!techMap.has(key)) {
      techMap.set(key, label);
    }
  });
  const techList = Array.from(techMap.values()).sort((a, b) => a.localeCompare(b, 'fr'));
  technicianOptionsEl.innerHTML = techList
    .map((label) => `<option value="${escapeHtml(label)}"></option>`)
    .join('');
}

function updateCommentFilterButtons() {
  if (!commentFilterButtons.length) {
    return;
  }
  if (commentFiltersEl) {
    commentFiltersEl.classList.toggle('is-all', state.commentFilter === 'all');
  }
  commentFilterButtons.forEach((btn) => {
    const active = btn.dataset.comment === state.commentFilter;
    btn.classList.toggle('active', active);
    btn.setAttribute('aria-pressed', active ? 'true' : 'false');
  });
  updateFilterDockState();
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

function setLotsLinkVisible(visible) {
  if (!lotsLink) {
    return;
  }
  lotsLink.hidden = !visible;
}

function canDeleteReportFromUser(user) {
  if (!user) {
    return false;
  }
  if (user.permissions && user.permissions.canDeleteReport) {
    return true;
  }
  if (user.type === 'local') {
    return true;
  }
  return Boolean(user.isHydraAdmin);
}

async function initAdminLink() {
  if (!adminLink && !lotsLink) {
    return;
  }
  setAdminLinkVisible(false);
  setLotsLinkVisible(false);
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
    if (data.user) {
      if (data.user.type === 'local') {
        setAdminLinkVisible(true);
      }
      const canDelete = canDeleteReportFromUser(data.user);
      if (canDelete) {
        state.canDeleteReport = true;
        state.canEditTags = true;
        setLotsLinkVisible(true);
        renderList();
      }
    }
  } catch (error) {
    setAdminLinkVisible(false);
    setLotsLinkVisible(false);
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

function normalizeBiosLanguageKey(value) {
  if (value == null) {
    return null;
  }
  const key = String(value).trim().toLowerCase();
  if (key === 'fr' || key.startsWith('fr-')) {
    return 'fr';
  }
  if (key === 'en' || key.startsWith('en-')) {
    return 'en';
  }
  return key === 'not_tested' ? 'not_tested' : null;
}

function normalizeBiosPasswordKey(value) {
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
  const normalized = normalizeStatusKey(cleaned);
  return normalized === 'ok' || normalized === 'nok' || normalized === 'not_tested' ? normalized : null;
}

function normalizeWifiStandardKey(value) {
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

  const normalized = normalizeStatusKey(cleaned);
  return normalized === 'ok' || normalized === 'nok' || normalized === 'not_tested' ? normalized : null;
}

function resolveComponentStatusDisplay(key, value) {
  if (key === 'biosLanguage') {
    const language = normalizeBiosLanguageKey(value);
    if (language === 'fr') {
      return { status: 'fr', label: 'FR' };
    }
    if (language === 'en') {
      return { status: 'en', label: 'EN' };
    }
    return { status: 'not_tested', label: statusLabels.not_tested };
  }

  if (key === 'biosPassword') {
    const normalized = normalizeBiosPasswordKey(value);
    if (normalized === 'ok') {
      return { status: 'ok', label: 'Non' };
    }
    if (normalized === 'nok') {
      return { status: 'nok', label: 'Oui' };
    }
    return { status: 'not_tested', label: statusLabels.not_tested };
  }

  if (key === 'wifiStandard') {
    const normalized = normalizeWifiStandardKey(value);
    if (normalized) {
      return { status: normalized, label: statusLabels[normalized] || '--' };
    }
    return { status: 'not_tested', label: statusLabels.not_tested };
  }

  const normalized = normalizeStatusKey(value);
  if (normalized) {
    return { status: normalized, label: statusLabels[normalized] || '--' };
  }
  return null;
}

function summarizeComponents(components, commentValue = '') {
  const summary = { ok: 0, nok: 0, other: 0, total: 0 };
  const hasComponents = components && typeof components === 'object' && !Array.isArray(components);
  if (hasComponents) {
    Object.entries(components).forEach(([componentKey, value]) => {
      if (componentKey === 'biosLanguage') {
        return;
      }
      const statusKey =
        componentKey === 'biosPassword'
          ? normalizeBiosPasswordKey(value)
          : componentKey === 'wifiStandard'
            ? normalizeWifiStandardKey(value)
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
  }

  const hasComment = typeof commentValue === 'string' && commentValue.trim();
  if (hasComment) {
    summary.nok += 1;
    summary.total += 1;
  }

  return summary;
}

function normalizeCategory(value) {
  if (value === 'laptop' || value === 'desktop' || value === 'unknown') {
    return value;
  }
  return 'unknown';
}

function buildCategoryBadge(category, id, extraClass = '') {
  const normalized = normalizeCategory(category);
  const label = categoryLabels[normalized] || categoryLabels.unknown;
  const className = extraClass ? `badge ${extraClass}` : 'badge';
  const idAttr = id ? ` data-id="${escapeHtml(String(id))}"` : '';
  const actionAttr = id ? ' data-action="cycle-category"' : '';
  const typeAttr = id ? ' type="button"' : '';
  return `<button class="${className}" data-category="${normalized}"${actionAttr}${idAttr}${typeAttr}>${escapeHtml(
    label
  )}</button>`;
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

function renderStatus(status, options = null, labelOverride = null) {
  const normalized = normalizeStatusKey(status) || String(status || '').trim().toLowerCase() || 'unknown';
  const label = labelOverride || statusLabels[normalized] || '--';
  if (options && options.id && options.key) {
    return `
      <button
        class="status-pill status-button"
        data-status="${normalized}"
        data-action="cycle-status"
        data-id="${escapeHtml(options.id)}"
        data-key="${escapeHtml(options.key)}"
        type="button"
      >
        ${label}
      </button>
    `;
  }
  return `<strong class="status-pill" data-status="${normalized}">${label}</strong>`;
}

function renderStatusValue(value, options = null) {
  if (options && options.key) {
    const componentDisplay = resolveComponentStatusDisplay(options.key, value);
    if (componentDisplay) {
      return renderStatus(componentDisplay.status, options, componentDisplay.label);
    }
    return renderStatus('not_tested', options, statusLabels.not_tested);
  }
  const statusKey = normalizeStatusKey(value);
  if (statusKey) {
    return renderStatus(statusKey, options);
  }
  if (options && options.id && options.key) {
    return renderStatus('not_tested', options);
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

function getUsbStatus(detail) {
  if (!detail || typeof detail !== 'object') {
    return null;
  }
  if (detail.usbStatus) {
    return detail.usbStatus;
  }
  if (detail.components && typeof detail.components === 'object') {
    return detail.components.usb || null;
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
  invalidateListCache();
}

function applyUsbStatusUpdate(id, status) {
  state.machines = state.machines.map((machine) => {
    if (machine.id !== id) {
      return machine;
    }
    const components = machine.components && typeof machine.components === 'object'
      ? { ...machine.components }
      : {};
    components.usb = status;
    return {
      ...machine,
      usbStatus: status,
      components
    };
  });

  if (state.details[id]) {
    const detail = state.details[id];
    const components = detail.components && typeof detail.components === 'object'
      ? { ...detail.components }
      : {};
    components.usb = status;
    state.details[id] = {
      ...detail,
      usbStatus: status,
      components
    };
  }
  invalidateListCache();
}

function setPadButtonsLoading(id, loading) {
  const buttons = listEl.querySelectorAll(`[data-action="set-pad"][data-id="${id}"]`);
  buttons.forEach((button) => {
    button.disabled = loading;
    button.classList.toggle('is-loading', loading);
  });
}

function setUsbButtonsLoading(id, loading) {
  const buttons = listEl.querySelectorAll(`[data-action="set-usb"][data-id="${id}"]`);
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

async function updateUsbStatus(id, status) {
  setUsbButtonsLoading(id, true);
  try {
    const response = await fetch(`/api/machines/${encodeURIComponent(id)}/usb`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ status })
    });
    if (response.status === 401) {
      window.location.href = '/login';
      return;
    }
    if (!response.ok) {
      throw new Error('usb_update_failed');
    }
    const data = await response.json();
    if (!data.ok) {
      throw new Error('usb_update_failed');
    }
    applyUsbStatusUpdate(id, data.status);
    renderList();
  } catch (error) {
    window.alert("Impossible d'enregistrer l'etat USB.");
  } finally {
    setUsbButtonsLoading(id, false);
  }
}

function nextCycleStatus(key, currentStatus) {
  const cycle = componentStatusCycles[key] || statusCycle;
  const normalized = (currentStatus || '').trim().toLowerCase() || 'not_tested';
  const index = cycle.indexOf(normalized);
  if (index === -1) {
    return cycle[0];
  }
  return cycle[(index + 1) % cycle.length];
}

function nextCategory(currentCategory) {
  const normalized = normalizeCategory(currentCategory);
  const index = categoryCycle.indexOf(normalized);
  if (index === -1) {
    return categoryCycle[0];
  }
  return categoryCycle[(index + 1) % categoryCycle.length];
}

function applyCategoryUpdate(id, category) {
  state.machines = state.machines.map((machine) => {
    if (machine.id !== id) {
      return machine;
    }
    return {
      ...machine,
      category
    };
  });

  if (state.details[id]) {
    state.details[id] = {
      ...state.details[id],
      category
    };
  }
  invalidateListCache();
}

async function updateCategory(id, category, button) {
  if (button) {
    button.disabled = true;
    button.classList.add('is-loading');
  }
  try {
    const response = await fetch(`/api/reports/${encodeURIComponent(id)}/category`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ category })
    });
    if (response.status === 401) {
      window.location.href = '/login';
      return;
    }
    if (!response.ok) {
      throw new Error('category_update_failed');
    }
    const data = await response.json();
    if (!data.ok) {
      throw new Error('category_update_failed');
    }
    applyCategoryUpdate(id, data.category || category);
    updateStats();
    renderList();
  } catch (error) {
    window.alert("Impossible d'enregistrer la categorie.");
  } finally {
    if (button) {
      button.disabled = false;
      button.classList.remove('is-loading');
    }
  }
}

function applyComponentStatusUpdate(id, key, status) {
  const statusFields = {
    camera: 'cameraStatus',
    usb: 'usbStatus',
    keyboard: 'keyboardStatus',
    pad: 'padStatus',
    badgeReader: 'badgeReaderStatus'
  };
  const statusField = statusFields[key] || null;

  state.machines = state.machines.map((machine) => {
    if (machine.id !== id) {
      return machine;
    }
    const components = machine.components && typeof machine.components === 'object'
      ? { ...machine.components }
      : {};
    components[key] = status;
    return {
      ...machine,
      ...(statusField ? { [statusField]: status } : {}),
      components
    };
  });

  if (state.details[id]) {
    const detail = state.details[id];
    const components = detail.components && typeof detail.components === 'object'
      ? { ...detail.components }
      : {};
    components[key] = status;
    state.details[id] = {
      ...detail,
      ...(statusField ? { [statusField]: status } : {}),
      components
    };
  }
  invalidateListCache();
}

async function updateComponentStatus(id, key, status, button) {
  if (button) {
    button.disabled = true;
    button.classList.add('is-loading');
  }
  try {
    const response = await fetch(`/api/reports/${encodeURIComponent(id)}/component`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ key, status })
    });
    if (response.status === 401) {
      window.location.href = '/login';
      return;
    }
    if (!response.ok) {
      throw new Error('component_update_failed');
    }
    const data = await response.json();
    if (!data.ok) {
      throw new Error('component_update_failed');
    }
    applyComponentStatusUpdate(id, key, data.status);
    renderList();
  } catch (error) {
    window.alert("Impossible d'enregistrer le statut.");
  } finally {
    if (button) {
      button.disabled = false;
      button.classList.remove('is-loading');
    }
  }
}

function setReportZeroLoading(loading) {
  if (reportZeroBtn) {
    reportZeroBtn.disabled = loading;
    reportZeroBtn.classList.toggle('is-loading', loading);
  }
  if (reportZeroSubmit) {
    reportZeroSubmit.disabled = loading;
  }
}

function showReportZeroError(message) {
  if (!reportZeroError) {
    return;
  }
  if (message) {
    reportZeroError.textContent = message;
    reportZeroError.hidden = false;
  } else {
    reportZeroError.textContent = '';
    reportZeroError.hidden = true;
  }
}

function setPatchnoteBody(body) {
  if (!patchnoteBodyEl) {
    return;
  }
  const safeBody = body ? escapeHtml(body).replace(/\n/g, '<br>') : '';
  patchnoteBodyEl.innerHTML = safeBody;
}

function openPatchnoteModal(patchnote) {
  if (!patchnoteModal) {
    return;
  }
  activePatchnoteId = patchnote && patchnote.id ? patchnote.id : null;
  setPatchnoteBody(patchnote && patchnote.body ? patchnote.body : '');
  patchnoteModal.hidden = false;
  if (patchnoteOkBtn) {
    patchnoteOkBtn.focus();
  }
}

function closePatchnoteModal() {
  if (!patchnoteModal) {
    return;
  }
  patchnoteModal.hidden = true;
  activePatchnoteId = null;
}

async function acknowledgePatchnote() {
  if (!activePatchnoteId) {
    closePatchnoteModal();
    return;
  }
  if (patchnoteOkBtn) {
    patchnoteOkBtn.disabled = true;
  }
  try {
    const response = await fetch('/api/patchnotes/ack', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ patchnoteId: activePatchnoteId })
    });
    if (response.status === 401) {
      window.location.href = '/login';
      return;
    }
  } catch (error) {
    // Fail silently so the UI isn't blocked.
  } finally {
    if (patchnoteOkBtn) {
      patchnoteOkBtn.disabled = false;
    }
  }
  closePatchnoteModal();
}

async function initPatchnote() {
  if (!patchnoteModal || !patchnoteBodyEl) {
    return;
  }
  try {
    const response = await fetch('/api/patchnotes/latest');
    if (response.status === 401) {
      window.location.href = '/login';
      return;
    }
    if (!response.ok) {
      return;
    }
    const data = await response.json();
    if (data.patchnote) {
      openPatchnoteModal(data.patchnote);
    }
  } catch (error) {
    // No-op.
  }
}

function openReportZeroModal() {
  if (!reportZeroModal || !reportZeroForm) {
    return;
  }
  reportZeroModal.hidden = false;
  reportZeroForm.reset();
  renderReportZeroLotOptions();
  showReportZeroError('');
  const firstInput = reportZeroForm.querySelector('input[name="hostname"]');
  if (firstInput) {
    firstInput.focus();
  }
}

function closeReportZeroModal() {
  if (!reportZeroModal) {
    return;
  }
  reportZeroModal.hidden = true;
  showReportZeroError('');
}

function showSuggestionError(message) {
  if (!suggestionError) {
    return;
  }
  if (message) {
    suggestionError.textContent = message;
    suggestionError.hidden = false;
  } else {
    suggestionError.textContent = '';
    suggestionError.hidden = true;
  }
}

function setSuggestionLoading(loading) {
  if (suggestionSubmitBtn) {
    suggestionSubmitBtn.disabled = loading;
  }
  if (suggestionAddBtn) {
    suggestionAddBtn.disabled = loading;
  }
}

function renderSuggestions(items) {
  if (!suggestionListEl || !suggestionEmptyEl) {
    return;
  }
  const list = Array.isArray(items) ? items : [];
  suggestionCache = list;
  if (!list.length) {
    suggestionListEl.innerHTML = '';
    suggestionEmptyEl.hidden = false;
    return;
  }
  suggestionEmptyEl.hidden = true;
  suggestionListEl.innerHTML = list
    .map((item) => {
      const title = escapeHtml(item.title || '');
      const body = escapeHtml(item.body || '');
      const createdAt = item.createdAt ? formatDateTime(item.createdAt) : '--';
      const createdBy = escapeHtml(item.createdBy || 'inconnu');
      return `
        <details class="suggestion-item">
          <summary class="suggestion-summary">
            <span>${title}</span>
            <span class="suggestion-meta">${escapeHtml(createdAt)}</span>
          </summary>
          <div class="suggestion-body">${body}</div>
          <div class="suggestion-meta">Par ${createdBy}</div>
        </details>
      `;
    })
    .join('');
}

async function loadSuggestions() {
  if (!suggestionListEl) {
    return;
  }
  try {
    const response = await fetch('/api/suggestions');
    if (response.status === 401) {
      window.location.href = '/login';
      return;
    }
    if (!response.ok) {
      throw new Error('suggestions_failed');
    }
    const data = await response.json();
    if (!data.ok) {
      throw new Error('suggestions_failed');
    }
    renderSuggestions(Array.isArray(data.suggestions) ? data.suggestions : []);
  } catch (error) {
    renderSuggestions([]);
  }
}

function openSuggestionModal() {
  if (!suggestionModal) {
    return;
  }
  suggestionModal.hidden = false;
  if (suggestionForm) {
    suggestionForm.hidden = true;
    suggestionForm.reset();
  }
  showSuggestionError('');
  loadSuggestions();
}

function closeSuggestionModal() {
  if (!suggestionModal) {
    return;
  }
  suggestionModal.hidden = true;
  showSuggestionError('');
}

function getSuggestionPayload() {
  if (!suggestionForm || !suggestionTitleInput || !suggestionBodyInput) {
    return null;
  }
  const title = String(suggestionTitleInput.value || '').trim();
  const body = String(suggestionBodyInput.value || '').trim();
  return {
    title,
    body,
    page: window && window.location ? window.location.pathname : ''
  };
}

async function submitSuggestion(payload) {
  setSuggestionLoading(true);
  try {
    const response = await fetch('/api/suggestions', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload)
    });
    if (response.status === 401) {
      window.location.href = '/login';
      return;
    }
    const data = await response.json();
    if (!response.ok || !data.ok) {
      throw new Error(data && data.error ? data.error : 'submit_failed');
    }
    if (suggestionForm) {
      suggestionForm.reset();
      suggestionForm.hidden = true;
    }
    showSuggestionError('');
    loadSuggestions();
  } catch (error) {
    showSuggestionError("Impossible d'envoyer la suggestion.");
  } finally {
    setSuggestionLoading(false);
  }
}

function getReportZeroPayload() {
  if (!reportZeroForm) {
    return null;
  }
  const formData = new FormData(reportZeroForm);
  const hostname = String(formData.get('hostname') || '').trim();
  const serialNumber = String(formData.get('serialNumber') || '').trim();
  const macAddress = String(formData.get('macAddress') || '').trim();
  const technician = String(formData.get('technician') || '').trim();
  const category = String(formData.get('category') || 'unknown').trim();
  const lotId = String(formData.get('lotId') || '').trim();
  const doubleCheck = String(formData.get('doubleCheck') || '').trim() !== '';

  return {
    hostname: hostname || null,
    serialNumber: serialNumber || null,
    macAddress: macAddress || null,
    technician: technician || null,
    category: category || 'unknown',
    lotId: lotId || null,
    doubleCheck
  };
}

async function createReportZero(payload) {
  setReportZeroLoading(true);
  try {
    const response = await fetch('/api/reports/report-zero', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload)
    });
    if (response.status === 401) {
      window.location.href = '/login';
      return;
    }
    if (!response.ok) {
      throw new Error('report_zero_failed');
    }
    const data = await response.json();
    if (!data.ok) {
      throw new Error('report_zero_failed');
    }
    if (data.reportId) {
      state.expandedId = String(data.reportId);
    }
    closeReportZeroModal();
    await loadMachines();
  } catch (error) {
    showReportZeroError("Impossible d'ajouter un report 0.");
  } finally {
    setReportZeroLoading(false);
  }
}

function setDeleteButtonsLoading(id, loading) {
  const buttons = listEl.querySelectorAll(`[data-action="delete-report"][data-id="${id}"]`);
  buttons.forEach((button) => {
    button.disabled = loading;
    button.classList.toggle('is-loading', loading);
  });
}

async function deleteReport(id) {
  const confirmed = window.confirm('Supprimer ce report ?');
  if (!confirmed) {
    return;
  }
  setDeleteButtonsLoading(id, true);
  try {
    const response = await fetch(`/api/reports/${encodeURIComponent(id)}`, {
      method: 'DELETE'
    });
    if (response.status === 401) {
      window.location.href = '/login';
      return;
    }
    if (response.status === 403) {
      window.alert("Pas les droits pour supprimer ce report.");
      return;
    }
    if (!response.ok) {
      throw new Error('report_delete_failed');
    }
    const data = await response.json();
    if (!data.ok) {
      throw new Error('report_delete_failed');
    }
    if (state.expandedId === id) {
      state.expandedId = null;
    }
    delete state.details[id];
    await loadMachines();
  } catch (error) {
    window.alert("Impossible de supprimer le report.");
  } finally {
    setDeleteButtonsLoading(id, false);
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
  invalidateListCache();
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

function applyTagRename(tagId, newLabel, activeTag) {
  if (!tagId || !newLabel) {
    return;
  }
  const normalizedId = normalizeTagId(tagId);
  const activeId = activeTag ? normalizeTagId(activeTag.id) : '';

  state.tags = (state.tags || []).map((tag) => {
    const currentId = normalizeTagId(tag.id || '');
    if (currentId === normalizedId) {
      return { ...tag, name: newLabel, is_active: false, isActive: false };
    }
    if (activeId && currentId === activeId) {
      return { ...tag, name: activeTag.name || tag.name, is_active: true, isActive: true };
    }
    if (activeId && (tag.is_active || tag.isActive)) {
      return { ...tag, is_active: false, isActive: false };
    }
    return tag;
  });

  if (activeId && !state.tags.some((tag) => normalizeTagId(tag.id || '') === activeId)) {
    state.tags.push({ id: activeTag.id, name: activeTag.name, is_active: true });
  }

  if (activeId) {
    state.activeTagId = activeId;
  }

  state.machines = state.machines.map((machine) => {
    if (normalizeTagId(machine.tagId) !== normalizedId) {
      return machine;
    }
    return { ...machine, tagName: newLabel, tag: newLabel };
  });

  savePreferences();
  renderTagFilters();
  renderList();
}

async function renameTag(tagId, newTag) {
  try {
    const response = await fetch('/api/tags/rename', {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ tagId, newTag })
    });
    if (!response.ok) {
      throw new Error('tag_rename_failed');
    }
    const data = await response.json();
    if (!data.ok || !data.tag) {
      throw new Error('tag_rename_failed');
    }
    applyTagRename(data.tag.id, data.tag.name || newTag, data.activeTag || null);
  } catch (error) {
    window.alert("Impossible de renommer le tag.");
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
  }, 15000);
  commentTimers.set(id, timeoutId);
}

function focusInlineComment(id) {
  if (!id) {
    return;
  }
  requestAnimationFrame(() => {
    const input = listEl.querySelector(`.comment-inline[data-comment-id="${id}"]`);
    if (input) {
      input.focus();
      input.selectionStart = input.value.length;
      input.selectionEnd = input.value.length;
    }
  });
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
  if (state.stats) {
    statTotal.textContent = state.stats.total || 0;
    statLaptop.textContent = state.stats.laptop || 0;
    statDesktop.textContent = state.stats.desktop || 0;
    statUnknown.textContent = state.stats.unknown || 0;
    renderLotMetrics();
    return;
  }
  const uniqueMachines = getUniqueMachines(getBaseFilteredMachines());
  const total = uniqueMachines.length;
  const laptop = uniqueMachines.filter((m) => normalizeCategory(m.category) === 'laptop').length;
  const desktop = uniqueMachines.filter((m) => normalizeCategory(m.category) === 'desktop').length;
  const unknown = uniqueMachines.filter((m) => normalizeCategory(m.category) === 'unknown').length;

  statTotal.textContent = total;
  statLaptop.textContent = laptop;
  statDesktop.textContent = desktop;
  statUnknown.textContent = unknown;
  renderLotMetrics();
}

function updateStatFilterCards() {
  if (!statFilterCards.length) {
    return;
  }
  statFilterCards.forEach((card) => {
    const filter = card.dataset.filter || 'all';
    const active = filter === state.filter;
    card.classList.toggle('is-active', active);
    card.setAttribute('aria-pressed', active ? 'true' : 'false');
  });
  updateFilterDockState();
}

function setCategoryFilter(filter) {
  state.filter = filter || 'all';
  updateStatFilterCards();
  savePreferences();
  reloadReports();
}

function resetAllFilters() {
  if (getActiveFilterCount() <= 0) {
    return;
  }
  state.filter = 'all';
  state.techFilter = 'all';
  state.tagFilter = [];
  state.tagFilterNames = [];
  state.componentFilter = 'all';
  state.commentFilter = 'all';
  state.dateFilter = 'all';
  state.quickFilter = null;
  state.activeToken = null;
  state.search = '';
  if (searchTimer) {
    window.clearTimeout(searchTimer);
    searchTimer = null;
  }
  if (searchInput) {
    searchInput.value = '';
  }
  if (tagFiltersEl) {
    tagFiltersEl.classList.remove('is-open');
  }
  updateSearchCollapse();
  updateStatFilterCards();
  updateTechFilterButtons();
  updateTestFilterButtons();
  updateCommentFilterButtons();
  renderTagFilters();
  renderTechFilters();
  updateTimeFilterLabel();
  savePreferences();
  reloadReports();
}

function getBaseFilteredMachines() {
  return state.machines.filter((machine) => {
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
      } else if (state.quickFilter.type === 'summary') {
        const summary = summarizeComponents(machine.components, machine.comment);
        if (state.quickFilter.value === 'ok' && summary.ok <= 0) {
          return false;
        }
        if (state.quickFilter.value === 'nok' && summary.nok <= 0) {
          return false;
        }
        if (state.quickFilter.value === 'nt' && summary.other <= 0) {
          return false;
        }
      }
    }
    return true;
  });
}

function applyFilters() {
  return sortMachines(getBaseFilteredMachines());
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
  const detailId = detail && detail.id != null ? String(detail.id) : '';
  const components =
    detail && detail.components && typeof detail.components === 'object' && !Array.isArray(detail.components)
      ? detail.components
      : {};

  const rows = [];

  function addRow(label, status, extra, componentKey) {
    const overrideStatus =
      componentKey && Object.prototype.hasOwnProperty.call(components, componentKey)
        ? components[componentKey]
        : status;
    const hasStatus = status !== undefined && status !== null && status !== '';
    const statusHtml =
      componentKey && detailId
        ? renderStatusValue(overrideStatus, { id: detailId, key: componentKey })
        : hasStatus
          ? renderStatusValue(overrideStatus)
          : '';
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
      addRow('Lecture disque', tests.diskRead, formatMbps(tests.diskReadMBps), 'diskReadTest');
    }
    if (tests.diskWrite || tests.diskWriteMBps != null) {
      addRow('Ecriture disque', tests.diskWrite, formatMbps(tests.diskWriteMBps), 'diskWriteTest');
    }
    if (tests.ramTest || tests.ramMBps != null) {
      addRow('RAM (WinSAT)', tests.ramTest, ramNote || formatMbps(tests.ramMBps), 'ramTest');
    }
    if (tests.cpuTest || tests.cpuMBps != null) {
      addRow('CPU (WinSAT)', tests.cpuTest, cpuNote || formatMbps(tests.cpuMBps), 'cpuTest');
    }
    const hasGpuWinSat = tests.gpuTest || tests.gpuScore != null || winSatGraphicsScore != null;
    if (hasGpuWinSat) {
      const gpuStatus = tests.gpuTest || (winSatGraphicsScore != null ? 'ok' : null);
      const gpuExtra = gpuNote || (tests.gpuScore != null ? formatScore(tests.gpuScore) : null);
      addRow('GPU (WinSAT)', gpuStatus, gpuExtra, 'gpuTest');
    }
    if (tests.cpuStress) {
      addRow('CPU (stress)', tests.cpuStress, null, 'cpuStress');
    }
    if (tests.gpuStress) {
      addRow('GPU (stress)', tests.gpuStress, null, 'gpuStress');
    }
    if (tests.networkPing || tests.networkPingTarget) {
      addRow('Ping', tests.networkPing, tests.networkPingTarget || null, 'networkPing');
    }
    if (tests.fsCheck) {
      addRow('Check disque', tests.fsCheck, null, 'fsCheck');
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

function buildReportHistory(detail) {
  const reports = Array.isArray(detail.relatedReports) ? detail.relatedReports : [];
  if (reports.length <= 1) {
    return '';
  }
  const items = reports
    .map((report) => {
      const id = report && report.id ? String(report.id) : '';
      if (!id) {
        return '';
      }
      const when = report.lastSeen || report.createdAt;
      const label = when ? formatDateTime(when) : '--';
      const active = detail.id && String(detail.id) === id ? ' is-active' : '';
      return `
        <button class="report-history-item${active}" type="button" data-action="open-report" data-id="${escapeHtml(
          id
        )}">
          <span>${escapeHtml(label)}</span>
          <span class="report-history-id">${escapeHtml(id.slice(0, 8))}</span>
        </button>
      `;
    })
    .filter(Boolean)
    .join('');

  if (!items) {
    return '';
  }

  return `
    <div class="report-history">
      <h3>Historique des rapports</h3>
      <div class="report-history-list">
        ${items}
      </div>
    </div>
  `;
}

function renderList(isScrollUpdate = false) {
  updateTimeFilterLabel();
  updateStats();
  updateStatFilterCards();
  updateFilterDockState();
  const useQuickFilter = Boolean(state.quickFilter && state.quickFilter.value);
  const cacheKey = JSON.stringify({
    length: state.machines.length,
    filter: state.filter,
    techFilter: state.techFilter,
    tagFilter: state.tagFilter,
    componentFilter: state.componentFilter,
    commentFilter: state.commentFilter,
    dateFilter: state.dateFilter,
    search: state.search,
    quickFilter: state.quickFilter,
    sort: state.sort
  });
  if (useQuickFilter && cacheKey !== state.listCacheKey) {
    state.listCache = applyFilters();
    state.listCacheKey = cacheKey;
  }
  const filtered = useQuickFilter ? state.listCache : [];
  const totalCount = useQuickFilter
    ? filtered.length
    : Number.isFinite(state.totalCount)
      ? state.totalCount
      : state.machines.length;

  if (!totalCount) {
    if (listEl) {
      listEl.style.paddingTop = '0px';
      listEl.style.paddingBottom = '0px';
      listEl.classList.remove('is-virtual');
    }
    listEl.innerHTML = '<div class="empty">Aucun poste ne correspond a ce filtre.</div>';
    return;
  }

  const columns = getColumnCount();
  const totalRows = Math.max(1, Math.ceil(totalCount / columns));
  const listTop = getListOffsetTop();
  const scrollTop = getScrollTop();
  const viewTop = Math.max(0, scrollTop - listTop);
  const viewBottom = viewTop + getViewportHeight();
  const rowHeight = Math.max(140, state.virtualRowHeight || 260);
  const overscan = Math.max(1, state.virtualOverscanRows || 2);
  const startRow = Math.max(0, findRowForOffset(viewTop, totalRows) - overscan);
  const endRow = Math.min(totalRows, findRowForOffset(viewBottom, totalRows) + overscan);
  const startIndex = startRow * columns;
  const endIndex = Math.min(totalCount, endRow * columns);
  const edgeThreshold = Math.max(1, columns * overscan);
  const nearStart = startIndex <= edgeThreshold;
  const nearEnd = endIndex >= Math.max(0, totalCount - edgeThreshold);

  const rangeUnchanged =
    isScrollUpdate &&
    state.virtualRange &&
    state.virtualRange.start === startIndex &&
    state.virtualRange.end === endIndex;
  if (rangeUnchanged) {
    if (!useQuickFilter) {
      ensurePagesForRange(startIndex, endIndex);
    }
    return;
  }

  state.virtualRange = { start: startIndex, end: endIndex };

  const topSpacer = getRowOffset(startRow);
  const bottomSpacer = Math.max(0, getTotalHeight(totalRows) - getRowOffset(endRow));
  listEl.style.paddingTop = `${Math.max(0, Math.round(topSpacer))}px`;
  listEl.style.paddingBottom = `${Math.max(0, Math.round(bottomSpacer))}px`;

  const visible = [];
  for (let idx = startIndex; idx < endIndex; idx += 1) {
    visible.push({ index: idx, item: useQuickFilter ? filtered[idx] : getItemAtIndex(idx) });
  }
  listEl.classList.add('is-virtual');
  const anchor = isScrollUpdate || state.skipAnchorRestore ? null : captureScrollAnchor();
  listEl.innerHTML = visible
    .map((entry, index) => {
      const machine = entry.item;
      if (!machine) {
        return `
          <article class="machine-card is-placeholder" aria-hidden="true" data-index="${entry.index}">
            <div class="card-top">
              <span class="category-pill">...</span>
            </div>
            <div class="card-main">
              <div class="card-left">
                <div class="skeleton-line"></div>
                <div class="skeleton-line short"></div>
                <div class="skeleton-line"></div>
              </div>
            </div>
          </article>
        `;
      }
      const category = normalizeCategory(machine.category);
      const categoryBadge = buildCategoryBadge(category, machine.id);
      const title = escapeHtml(formatPrimary(machine));
      const subtitle = escapeHtml(formatSubtitle(machine));
      const serialValue = machine.serialNumber || '';
      const macLabel = formatMacSummary(machine);
      const macValue =
        machine.macAddress ||
        (Array.isArray(machine.macAddresses) ? machine.macAddresses[0] : '') ||
        '';
      const technicianValue = machine.technician || '';
      const lastSeen = escapeHtml(timeAgo(machine.lastSeen));
      const tagLabel = getTagLabel(machine);
      const tagValue = escapeHtml(tagLabel);
      const tagIdValue = escapeHtml(getTagId(machine));
      const lotData = getMachineLot(machine);
      const rawLotLabel = lotData ? buildLotLabel(lotData) : '';
      const hasAssignedLot = Boolean(rawLotLabel && rawLotLabel !== DEFAULT_LOT_LABEL);
      const lotLabel = hasAssignedLot ? rawLotLabel : DEFAULT_LOT_LABEL;
      const lotValue = escapeHtml(lotLabel);
      const tagHtml = state.canEditTags && tagIdValue
        ? `<button class="tag-pill is-editable" type="button" title="${tagValue}" data-tag="${tagValue}" data-tag-id="${tagIdValue}">${tagValue}</button>`
        : `<span class="tag-pill" title="${tagValue}">${tagValue}</span>`;
      const lotClasses = ['lot-pill'];
      if (!hasAssignedLot) {
        lotClasses.push('is-empty');
      }
      if (lotData && lotData.isPaused) {
        lotClasses.push('is-paused');
      }
      const lotHtml = `
        <span class="${lotClasses.join(' ')}" title="${lotValue}">
          <span class="lot-pill-label">Lot</span>
          <span class="lot-pill-value">${lotValue}</span>
        </span>
      `;
      const lotInlineHtml = `
        <p class="machine-lot-line${hasAssignedLot ? '' : ' is-empty'}">
          <span>Lot en cours</span>
          <strong>${lotValue}</strong>
        </p>
      `;
      const commentValue = typeof machine.comment === 'string' ? machine.comment.trim() : '';
      const commentDisplay = commentValue || 'Ajouter un commentaire';
      const isEditingComment = state.quickCommentId === machine.id;
      const commentHtml = `
        <div
          class="card-comment${commentValue ? '' : ' is-empty'}${isEditingComment ? ' is-editing' : ''}"
          data-comment-card="${machine.id}"
          title="${escapeHtml(commentValue)}"
        >
          <div class="comment-view">
            <span class="comment-label">Commentaire</span>
            <span class="comment-text">${escapeHtml(commentDisplay)}</span>
          </div>
          <div class="comment-edit">
            <textarea
              class="comment-inline"
              data-comment-id="${machine.id}"
              maxlength="800"
              placeholder="Ajouter un commentaire"
            >${escapeHtml(commentValue)}</textarea>
          </div>
        </div>
      `;
      const expanded = state.expandedId === machine.id;
      const selected = expanded ? 'selected' : '';
      const absoluteIndex = startIndex + index;
      const delayClass = delayClasses[absoluteIndex % delayClasses.length];
      const summary = summarizeComponents(machine.components, machine.comment);
      const summaryActive = state.quickFilter && state.quickFilter.type === 'summary';
      const summaryHtml =
        summary.total > 0
          ? `
            <div class="machine-summary">
              <button class="summary-chip ok${summaryActive && state.quickFilter.value === 'ok' ? ' is-active' : ''}" type="button" data-summary="ok">OK ${summary.ok}</button>
              <button class="summary-chip nok${summaryActive && state.quickFilter.value === 'nok' ? ' is-active' : ''}" type="button" data-summary="nok">NOK ${summary.nok}</button>
              <button class="summary-chip nt${summaryActive && state.quickFilter.value === 'nt' ? ' is-active' : ''}" type="button" data-summary="nt">NT ${summary.other}</button>
            </div>
          `
          : `
            <div class="machine-summary">
              <button class="summary-chip nt" type="button" data-summary="nt">NT --</button>
            </div>
          `;
      const overrideId =
        expanded && state.detailOverrideId && state.expandedId === machine.id
          ? state.detailOverrideId
          : null;
      const detailData = expanded
        ? overrideId
          ? state.details[overrideId]
          : state.details[machine.id]
        : null;
      const detailHtml = expanded
        ? detailData && detailData.error
          ? '<div class="card-detail"><div class="empty">Impossible de charger les details.</div></div>'
          : detailData
            ? `<div class="card-detail">${buildDetailHtml(detailData)}</div>`
            : '<div class="card-detail"><div class="loading">Chargement des details...</div></div>'
        : '';
      const toggleLabel = expanded ? 'Masquer les details' : 'Afficher les details';

      return `
        <article class="machine-card ${delayClass} ${selected}" data-id="${machine.id}" data-page="${machine._page || ''}" data-index="${entry.index}" aria-expanded="${expanded}">
          <div class="card-top">
            ${categoryBadge}
            <div class="card-top-right">
              <div class="card-top-tags">
                ${tagHtml}
                ${lotHtml}
              </div>
              <span class="machine-meta"><span>${lastSeen}</span></span>
            </div>
          </div>
          <div class="card-main">
            <div class="card-left">
              <h3 class="machine-title">${title}</h3>
              <p class="machine-sub">${subtitle}</p>
              ${lotInlineHtml}
              <div class="machine-meta-row">
                ${buildMetaChip('SN', serialValue, serialValue, 'serial', state.activeToken, machine.id)}
                ${buildMetaChip('Tech', technicianValue, technicianValue, 'tech', state.activeToken, machine.id)}
              </div>
              <div class="machine-meta-row mac-row">
                ${buildMetaChip('MAC', macLabel || '', macValue, 'mac', state.activeToken, machine.id)}
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
  if (anchor) {
    restoreScrollAnchor(anchor);
  }
  if (state.skipAnchorRestore) {
    state.skipAnchorRestore = false;
  }
  if (state.scrollAnchorHold) {
    if (Date.now() > (state.scrollAnchorHold.until || 0)) {
      state.scrollAnchorHold = null;
    } else {
      scheduleAnchorHoldAdjustment();
    }
  }
  if (state.scrollHold && Number.isFinite(state.scrollHold.top)) {
    scheduleScrollHoldAdjustment();
  }

  if (!state.virtualCalibrated) {
    const firstCard = listEl.querySelector('.machine-card');
    if (firstCard) {
      const height = Math.round(firstCard.getBoundingClientRect().height);
      if (height > 0) {
        state.virtualRowHeight = height;
        state.virtualCalibrated = true;
        scheduleVirtualRender();
      }
    }
  }

  if (state.rowMeasureRaf) {
    window.cancelAnimationFrame(state.rowMeasureRaf);
  }
  state.rowMeasureRaf = window.requestAnimationFrame(() => {
    state.rowMeasureRaf = null;
    if (Date.now() - (state.lastScrollEventAt || 0) < 140) {
      return;
    }
    const cards = listEl.querySelectorAll('.machine-card:not(.is-placeholder)');
    if (!cards.length) {
      return;
    }
    let updated = false;
    const rowHeights = new Map();
    cards.forEach((card) => {
      const index = Number.parseInt(card.dataset.index || '', 10);
      if (!Number.isFinite(index)) {
        return;
      }
      const rowIndex = Math.floor(index / columns);
      const height = Math.round(card.getBoundingClientRect().height);
      if (!Number.isFinite(height) || height <= 0) {
        return;
      }
      const current = rowHeights.get(rowIndex) || 0;
      if (height > current) {
        rowHeights.set(rowIndex, height);
      }
    });
    rowHeights.forEach((height, rowIndex) => {
      if (updateRowHeight(rowIndex, height)) {
        updated = true;
      }
    });
    if (updated) {
      scheduleVirtualRender();
    }
  });

  if (!useQuickFilter) {
    ensurePagesForRange(startIndex, endIndex);
  } else if (nearEnd && state.hasMore && state.scrollDirection !== 'up') {
    loadReportsPage(state.pageStart + state.pages.length * state.pageSize);
  } else if (nearStart && state.pageStart > 0 && state.scrollDirection !== 'down') {
    loadReportsPage(state.pageStart - state.pageSize);
  }
}

function buildDetailHtml(detail) {
  const category = normalizeCategory(detail.category);
  const title = escapeHtml(formatPrimary(detail));
  const subtitle = escapeHtml(formatSubtitle(detail));
  const lot = getMachineLot(detail);
  const lotLabel = lot ? buildLotLabel(lot) : '';
  const technicianLine = detail.technician
    ? `<p class="detail-tech"><span>Technicien</span><strong>${escapeHtml(detail.technician)}</strong></p>`
    : '';
  const lotLine = lotLabel
    ? `<p class="detail-tech"><span>Lot</span><strong>${escapeHtml(lotLabel)}</strong></p>`
    : '';
  const detailId = detail && detail.id != null ? String(detail.id) : '';
  const padStatus = getPadStatus(detail);
  const usbStatus = getUsbStatus(detail);
  const componentDefaults = {
    biosBattery: 'not_tested',
    biosLanguage: 'not_tested',
    biosPassword: 'not_tested',
    wifiStandard: 'not_tested'
  };
  const rawComponents =
    detail && detail.components && typeof detail.components === 'object' && !Array.isArray(detail.components)
      ? detail.components
      : {};
  const components = { ...componentDefaults, ...rawComponents };
  const cameraStatus = detail.cameraStatus || components.camera || null;
  const keyboardStatus = detail.keyboardStatus || components.keyboard || null;
  const badgeStatus = detail.badgeReaderStatus || components.badgeReader || null;
  const deleteButton = state.canDeleteReport
    ? `
        <button class="detail-action danger" type="button" data-action="delete-report" data-id="${detailId}">
          Supprimer
        </button>
      `
    : '';
  const alcyoneLink =
    detail && detail.alcyoneUrl
      ? `
        <a class="detail-action" href="${escapeHtml(detail.alcyoneUrl)}" target="_blank" rel="noopener">
          Ouvrir Alcyone
        </a>
      `
      : '';
  const actionBar = detailId
    ? `
      <div class="detail-actions">
        <button class="detail-action" type="button" data-action="export-pdf" data-id="${detailId}">
          Telecharger PDF
        </button>
        ${alcyoneLink}
        ${deleteButton}
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
  const inventory =
    payload && payload.inventory && typeof payload.inventory === 'object' ? payload.inventory : null;
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
              ${renderStatusValue(value, { id: detailId, key })}
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
          ${renderStatusValue(cameraStatus, { id: detailId, key: 'camera' })}
        </div>
        <div class="detail-item">
          <span>Ports USB</span>
          ${renderStatusValue(usbStatus, { id: detailId, key: 'usb' })}
        </div>
        <div class="detail-item">
          <span>Clavier</span>
          ${renderStatusValue(keyboardStatus, { id: detailId, key: 'keyboard' })}
        </div>
        <div class="detail-item">
          <span>Pave tactile</span>
          ${renderStatusValue(padStatus, { id: detailId, key: 'pad' })}
        </div>
        <div class="detail-item">
          <span>Lecteur badge</span>
          ${renderStatusValue(badgeStatus, { id: detailId, key: 'badgeReader' })}
        </div>
        <div class="detail-item">
          <span>Pile BIOS</span>
          ${renderStatusValue(components.biosBattery, { id: detailId, key: 'biosBattery' })}
        </div>
        <div class="detail-item">
          <span>Langue BIOS</span>
          ${renderStatusValue(components.biosLanguage, { id: detailId, key: 'biosLanguage' })}
        </div>
        <div class="detail-item">
          <span>Mot de passe BIOS</span>
          ${renderStatusValue(components.biosPassword, { id: detailId, key: 'biosPassword' })}
        </div>
      </div>
    </div>
  `;

  let inventoryHtml = '';
  if (inventory) {
    const inventoryItems = [];

    const baseboard =
      inventory.baseboard && typeof inventory.baseboard === 'object' ? inventory.baseboard : null;
    const baseboardSerial = baseboard && baseboard.serialNumber ? String(baseboard.serialNumber) : '';
    if (baseboardSerial) {
      inventoryItems.push({ label: 'Carte mere', value: baseboardSerial });
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
      inventoryItems.push({ label: 'Batterie', value: batteryValues.join(' • ') });
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
      inventoryItems.push({ label: 'Disques', value: diskValues.join(' • ') });
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
      inventoryItems.push({ label: 'RAM', value: memoryValues.join(' • ') });
    }

    if (inventoryItems.length) {
      inventoryHtml = `
        <div class="hardware">
          <h3>Identifiants materiel</h3>
          <div class="detail-grid hardware-grid">
            ${inventoryItems
              .map(
                (item) => `
                  <div class="detail-item">
                    <span>${escapeHtml(item.label)}</span>
                    <strong>${escapeHtml(item.value)}</strong>
                  </div>
                `
              )
              .join('')}
          </div>
        </div>
      `;
    }
  }

  const diagnosticsHtml = buildDiagnosticsHtml(detail);
  const historyHtml = buildReportHistory(detail);

  return `
    <div class="detail-header">
      <h2 class="detail-title">${title}</h2>
      ${buildCategoryBadge(category, detailId, 'detail-category')}
      <p class="machine-sub">${subtitle}</p>
      ${technicianLine}
      ${lotLine}
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
    ${historyHtml}
    ${commentHtml}
    ${hardwareHtml}
    ${inventoryHtml}
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
  const lot = getMachineLot(detail);
  const lotLabel = lot ? buildLotLabel(lot) : '';
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
  const lotLine = lotLabel
    ? `<div class="report-meta-row"><span>Lot</span><strong>${escapeHtml(lotLabel)}</strong></div>`
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
  const componentDefaults = {
    biosBattery: 'not_tested',
    biosLanguage: 'not_tested',
    biosPassword: 'not_tested',
    wifiStandard: 'not_tested'
  };
  const rawComponents =
    detail && detail.components && typeof detail.components === 'object' && !Array.isArray(detail.components)
      ? detail.components
      : {};
  const components = { ...componentDefaults, ...rawComponents };

  const diagnosticsHtml = buildDiagnosticsHtml(detail);
  const diagnosticsSection = diagnosticsHtml
    ? diagnosticsHtml
    : `
      <div class="diagnostics">
        <h3>Diagnostics et performances</h3>
        <div class="empty">Aucun test disponible.</div>
      </div>
    `;

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
              ${renderStatusValue(value, { key })}
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
        <div class="detail-item">
          <span>Pile BIOS</span>
          ${renderStatusValue(components.biosBattery, { key: 'biosBattery' })}
        </div>
        <div class="detail-item">
          <span>Langue BIOS</span>
          ${renderStatusValue(components.biosLanguage, { key: 'biosLanguage' })}
        </div>
        <div class="detail-item">
          <span>Mot de passe BIOS</span>
          ${renderStatusValue(components.biosPassword, { key: 'biosPassword' })}
        </div>
      </div>
    </div>
  `;

  const payloadHtml = detail.payload
    ? `<pre>${escapeHtml(JSON.stringify(detail.payload, null, 2))}</pre>`
    : '<div class="empty">Payload non disponible.</div>';

  const summary = summarizeComponents(detail.components, detail.comment);
  const summaryActive = state.quickFilter && state.quickFilter.type === 'summary';
  const summaryHtml =
    summary.total > 0
      ? `
          <button class="summary-chip ok${summaryActive && state.quickFilter.value === 'ok' ? ' is-active' : ''}" type="button" data-summary="ok">OK ${summary.ok}</button>
          <button class="summary-chip nok${summaryActive && state.quickFilter.value === 'nok' ? ' is-active' : ''}" type="button" data-summary="nok">NOK ${summary.nok}</button>
          <button class="summary-chip nt${summaryActive && state.quickFilter.value === 'nt' ? ' is-active' : ''}" type="button" data-summary="nt">NT ${summary.other}</button>
        `
      : '<button class="summary-chip nt" type="button" data-summary="nt">NT --</button>';

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
            ${lotLine}
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
  resetPagination();
  if (Array.isArray(state.tagFilter)) {
    state.tagFilter = state.tagFilter.map((value) => normalizeTagId(value)).filter(Boolean);
  }
  await loadMeta();
  await Promise.all([loadStats(), loadReportsPage(0)]);
  renderTechnicianOptions();
  updateCommentFilterButtons();
  if (state.expandedId) {
    await ensureMachineDetail(state.expandedId);
  }
}

function updateExpandedDetailHtml(reportId, preserveScroll = false) {
  if (!listEl || !state.expandedId) {
    return false;
  }
  const holdTop = preserveScroll ? getScrollTop() : null;
  const restoreScroll = () => {
    if (!preserveScroll || holdTop == null) {
      return;
    }
    window.requestAnimationFrame(() => {
      window.requestAnimationFrame(() => {
        setScrollTop(holdTop);
      });
    });
  };
  const safeId =
    window.CSS && CSS.escape ? CSS.escape(state.expandedId) : String(state.expandedId).replace(/"/g, '\\"');
  const card = listEl.querySelector(`.machine-card[data-id="${safeId}"]`);
  if (!card) {
    return false;
  }
  const detailWrap = card.querySelector('.card-detail');
  if (!detailWrap) {
    return false;
  }
  const detailData = reportId != null ? state.details[String(reportId)] : null;
  if (detailData && detailData.error) {
    detailWrap.innerHTML = '<div class="empty">Impossible de charger les details.</div>';
    restoreScroll();
    return true;
  }
  if (detailData) {
    detailWrap.innerHTML = buildDetailHtml(detailData);
    restoreScroll();
    return true;
  }
  detailWrap.innerHTML = '<div class="loading">Chargement des details...</div>';
  restoreScroll();
  return true;
}

async function ensureMachineDetail(id, options = {}) {
  const detailId = id != null ? String(id) : '';
  if (!detailId) {
    return;
  }
  if (state.details[detailId]) {
    if (!options.skipRender) {
      renderList();
    }
    return;
  }
  if (!options.skipRender) {
    renderList();
  }
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
    if (!options.skipRender) {
      renderList();
    }
  } catch (error) {
    state.details[detailId] = { error: true };
    if (!options.skipRender) {
      renderList();
    }
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

statFilterCards.forEach((card) => {
  const activate = () => {
    const filter = card.dataset.filter || 'all';
    if (statTotalCard && card === statTotalCard) {
      cycleDateFilter(false);
      setCategoryFilter('all');
      return;
    }
    if (filter === state.filter) {
      setCategoryFilter('all');
      return;
    }
    setCategoryFilter(filter);
  };
  card.addEventListener('click', activate);
  card.addEventListener('keydown', (event) => {
    if (event.key === 'Enter' || event.key === ' ') {
      event.preventDefault();
      activate();
    }
  });
});

if (techFiltersEl) {
  techFiltersEl.addEventListener('click', (event) => {
    const button = event.target.closest('.tech-filter-btn');
    if (!button) {
      return;
    }
    const next = button.dataset.tech || 'all';
    state.techFilter = next === state.techFilter ? 'all' : next;
    updateTechFilterButtons();
    savePreferences();
    reloadReports();
  });
}

if (tagFiltersEl) {
  tagFiltersEl.addEventListener('click', (event) => {
    const toggle = event.target.closest('.tag-select-toggle');
    if (toggle) {
      event.preventDefault();
      tagFiltersEl.classList.toggle('is-open');
      updateTagFilterButtons();
      return;
    }
    const clear = event.target.closest('[data-action="clear-tags"]');
    if (clear) {
      event.preventDefault();
      state.tagFilter = [];
      savePreferences();
      renderTagFilters();
      renderTechFilters();
      reloadReports();
      return;
    }
  });
  tagFiltersEl.addEventListener('change', (event) => {
    const input = event.target.closest('.tag-select-input');
    if (!input) {
      return;
    }
    const id = normalizeTagId(input.value);
    const selected = new Set(Array.isArray(state.tagFilter) ? state.tagFilter : []);
    if (input.checked) {
      if (id) {
        selected.add(id);
      }
    } else {
      selected.delete(id);
    }
    state.tagFilter = Array.from(selected);
    savePreferences();
    renderTagFilters();
    renderTechFilters();
    reloadReports();
  });
}

document.addEventListener('click', (event) => {
  if (!tagFiltersEl || !tagFiltersEl.classList.contains('is-open')) {
    return;
  }
  if (!tagFiltersEl.contains(event.target)) {
    tagFiltersEl.classList.remove('is-open');
    updateTagFilterButtons();
  }
});

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
    savePreferences();
    updateLayoutButtons();
    applyLayout();
    scheduleVirtualRender();
  });
});

testFilterButtons.forEach((button) => {
  button.addEventListener('click', () => {
    const next = button.dataset.component || 'all';
    state.componentFilter = next === state.componentFilter ? 'all' : next;
    updateTestFilterButtons();
    savePreferences();
    reloadReports();
  });
});

commentFilterButtons.forEach((button) => {
  button.addEventListener('click', () => {
    const next = button.dataset.comment || 'all';
    state.commentFilter = next === state.commentFilter ? 'all' : next;
    updateCommentFilterButtons();
    savePreferences();
    reloadReports();
  });
});

updateLayoutButtons();
updateTestFilterButtons();
updateCommentFilterButtons();
updateTagFilterButtons();
applyLayout();
updateTimeFilterLabel();
updateStatFilterCards();
initSidebarNavigation();
updateFilterDockState();

if (searchToggle && searchWrap && searchInput) {
  searchToggle.addEventListener('click', () => {
    searchWrap.classList.remove('is-collapsed');
    searchInput.focus();
  });
  searchInput.addEventListener('focus', () => {
    searchWrap.classList.remove('is-collapsed');
  });
  searchInput.addEventListener('blur', () => {
    updateSearchCollapse();
  });
}

if (searchInput) {
  searchInput.addEventListener('input', (event) => {
    state.search = event.target.value;
    savePreferences();
    updateSearchCollapse();
    if (searchTimer) {
      clearTimeout(searchTimer);
    }
    searchTimer = setTimeout(() => {
      reloadReports();
    }, 300);
  });
}

if (resetFiltersBtn) {
  resetFiltersBtn.addEventListener('click', () => {
    resetAllFilters();
  });
}

refreshBtn.addEventListener('click', () => {
  loadMachines();
});

if (reportZeroBtn) {
  reportZeroBtn.addEventListener('click', () => {
    openReportZeroModal();
  });
}

if (reportZeroForm) {
  reportZeroForm.addEventListener('submit', (event) => {
    event.preventDefault();
    const payload = getReportZeroPayload();
    if (!payload) {
      return;
    }
    if (!payload.hostname && !payload.serialNumber && !payload.macAddress) {
      showReportZeroError('Renseigne au moins un identifiant.');
      return;
    }
    createReportZero(payload);
  });
}

if (reportZeroCloseButtons.length) {
  reportZeroCloseButtons.forEach((button) => {
    button.addEventListener('click', () => {
      closeReportZeroModal();
    });
  });
}

if (reportZeroModal) {
  reportZeroModal.addEventListener('click', (event) => {
    if (event.target === reportZeroModal) {
      closeReportZeroModal();
    }
  });
}

document.addEventListener('keydown', (event) => {
  if (event.key === 'Escape' && reportZeroModal && !reportZeroModal.hidden) {
    closeReportZeroModal();
  }
  if (event.key === 'Escape' && suggestionModal && !suggestionModal.hidden) {
    closeSuggestionModal();
  }
});

if (suggestionBtn) {
  suggestionBtn.addEventListener('click', () => {
    openSuggestionModal();
  });
}

if (suggestionAddBtn) {
  suggestionAddBtn.addEventListener('click', () => {
    if (suggestionForm) {
      suggestionForm.hidden = false;
      if (suggestionTitleInput) {
        suggestionTitleInput.focus();
      }
    }
  });
}

if (suggestionCancelBtn) {
  suggestionCancelBtn.addEventListener('click', () => {
    if (suggestionForm) {
      suggestionForm.reset();
      suggestionForm.hidden = true;
    }
    showSuggestionError('');
  });
}

if (suggestionForm) {
  suggestionForm.addEventListener('submit', (event) => {
    event.preventDefault();
    const payload = getSuggestionPayload();
    if (!payload) {
      return;
    }
    if (!payload.title || !payload.body) {
      showSuggestionError('Renseigne un titre et un detail.');
      return;
    }
    submitSuggestion(payload);
  });
}

if (suggestionCloseButtons.length) {
  suggestionCloseButtons.forEach((button) => {
    button.addEventListener('click', () => {
      closeSuggestionModal();
    });
  });
}

if (suggestionModal) {
  suggestionModal.addEventListener('click', (event) => {
    if (event.target === suggestionModal) {
      closeSuggestionModal();
    }
  });
}

listEl.addEventListener('click', (event) => {
  const commentInput = event.target.closest('.comment-inline');
  if (commentInput) {
    return;
  }
  const commentCard = event.target.closest('[data-comment-card]');
  if (commentCard) {
    event.preventDefault();
    event.stopPropagation();
    const id = commentCard.dataset.commentCard;
    if (!id) {
      return;
    }
    if (state.quickCommentId !== id) {
      state.quickCommentId = id;
      renderList();
      focusInlineComment(id);
    }
    return;
  }
  const tagPill = event.target.closest('.tag-pill');
  if (tagPill && tagPill.dataset.tagId) {
    event.preventDefault();
    event.stopPropagation();
    if (!state.canEditTags) {
      return;
    }
    const tagId = tagPill.dataset.tagId;
    const currentTag = tagPill.dataset.tag || DEFAULT_TAG_LABEL;
    const nextTag = window.prompt('Renommer le tag', currentTag);
    const trimmed = nextTag ? nextTag.trim() : '';
    if (!trimmed || trimmed === currentTag) {
      return;
    }
    renameTag(tagId, trimmed);
    return;
  }
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
    savePreferences();
    renderList();
    return;
  }
  const summaryBtn = event.target.closest('.summary-chip[data-summary]');
  if (summaryBtn) {
    event.preventDefault();
    event.stopPropagation();
    const value = summaryBtn.dataset.summary;
    if (!value) {
      return;
    }
    if (state.quickFilter && state.quickFilter.type === 'summary' && state.quickFilter.value === value) {
      state.quickFilter = null;
    } else {
      state.quickFilter = { type: 'summary', value };
    }
    state.activeToken = null;
    savePreferences();
    renderList();
    return;
  }
  const cycleBtn = event.target.closest('[data-action="cycle-status"]');
  if (cycleBtn) {
    event.preventDefault();
    event.stopPropagation();
    const id = cycleBtn.dataset.id;
    const key = cycleBtn.dataset.key;
    if (!id || !key) {
      return;
    }
    const nextStatus = nextCycleStatus(key, cycleBtn.dataset.status);
    updateComponentStatus(id, key, nextStatus, cycleBtn);
    return;
  }
  const categoryBtn = event.target.closest('[data-action="cycle-category"]');
  if (categoryBtn) {
    event.preventDefault();
    event.stopPropagation();
    const id = categoryBtn.dataset.id;
    if (!id) {
      return;
    }
    const nextValue = nextCategory(categoryBtn.dataset.category);
    updateCategory(id, nextValue, categoryBtn);
    return;
  }
  const reportLink = event.target.closest('[data-action="open-report"]');
  if (reportLink) {
    event.preventDefault();
    event.stopPropagation();
    const id = reportLink.dataset.id;
    if (!id) {
      return;
    }
    const targetId = String(id);
    const activeEl = document.activeElement;
    if (activeEl && typeof activeEl.blur === 'function') {
      activeEl.blur();
    }
    if (state.expandedId && state.expandedId !== targetId) {
      holdScrollTop(6, false);
    }
    const isLoaded = state.machines.some((machine) => String(machine.id) === targetId);
    if (!isLoaded && state.expandedId && state.expandedId !== targetId) {
      holdCardAnchor(state.expandedId);
    }
    if (isLoaded) {
      state.detailOverrideId = null;
      state.expandedId = targetId;
      renderList();
      ensureMachineDetail(targetId);
      return;
    }
    if (!state.expandedId) {
      state.expandedId = targetId;
      renderList();
      ensureMachineDetail(targetId);
      return;
    }
    state.detailOverrideId = targetId;
    updateExpandedDetailHtml(targetId, true);
    ensureMachineDetail(targetId, { skipRender: true }).then(() => {
      if (state.detailOverrideId === targetId) {
        if (!updateExpandedDetailHtml(targetId, true)) {
          renderList();
        }
      }
    });
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
  const usbBtn = event.target.closest('[data-action="set-usb"]');
  if (usbBtn) {
    event.preventDefault();
    event.stopPropagation();
    const id = usbBtn.dataset.id;
    const status = usbBtn.dataset.status;
    if (!id || !status) {
      return;
    }
    updateUsbStatus(id, status);
    return;
  }
  const deleteBtn = event.target.closest('[data-action="delete-report"]');
  if (deleteBtn) {
    event.preventDefault();
    event.stopPropagation();
    const id = deleteBtn.dataset.id;
    if (!id) {
      return;
    }
    deleteReport(id);
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
  state.detailOverrideId = null;
  if (state.expandedId === id) {
    state.expandedId = null;
    renderList();
    return;
  }
  state.expandedId = id;
  ensureMachineDetail(id);
});

listEl.addEventListener('input', (event) => {
  const input = event.target.closest('.comment-input, .comment-inline');
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
    const input = event.target.closest('.comment-input, .comment-inline');
    if (!input) {
      return;
    }
    const id = input.dataset.commentId;
    if (!id) {
      return;
    }
    scheduleCommentSave(id, input.value, true);
    if (input.classList.contains('comment-inline')) {
      state.quickCommentId = null;
      renderList();
    }
  },
  true
);

if (patchnoteOkBtn) {
  patchnoteOkBtn.addEventListener('click', () => {
    acknowledgePatchnote();
  });
}

initAdminLink();
initPatchnote();
initInfiniteScroll();
loadMachines();
