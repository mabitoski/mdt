const bodyView =
  typeof document !== 'undefined' && document.body && document.body.dataset
    ? String(document.body.dataset.view || '')
        .trim()
        .toLowerCase()
    : '';
const isLegacyView = Boolean(
  bodyView === 'legacy' ||
  (typeof window !== 'undefined' && window.__LEGACY_VIEW__) ||
  (typeof window !== 'undefined' && window.location && window.location.pathname.includes('legacy'))
);
const isServerView = bodyView === 'servers';
const storageSuffix = isLegacyView ? '-legacy' : isServerView ? '-servers' : '';

const legacyMode = isLegacyView ? 'legacy' : 'current';

const state = {
  machines: [],
  tags: [],
  activeTagId: null,
  lots: [],
  activeLotId: null,
  stats: null,
  currentUser: null,
  operatorScope: null,
  techOptions: [],
  filter: 'all',
  techFilter: 'all',
  tagFilter: [],
  tagFilterNames: [],
  componentFilter: 'all',
  commentFilter: 'all',
  dateFilter: 'all',
  dateFrom: '',
  dateTo: '',
  timelineGranularity: 'day',
  timelineBuckets: [],
  quickFilter: null,
  activeToken: null,
  quickCommentId: null,
  search: '',
  sort: 'activity',
  layout: '3',
  expandedId: null,
  detailOverrideId: null,
  details: {},
  lastUpdated: null,
  canDeleteReport: false,
  canEditTags: false,
  canAccessAdmin: false,
  canCreateReportZero: false,
  canEditReports: false,
  canEditBatteryHealth: false,
  canEditTechnician: false,
  canManageLots: false,
  canManagePallets: false,
  canManageLogistics: false,
  canRenameTags: false,
  pageSize: 24,
  pageStart: 0,
  pages: [],
  hasMore: true,
  isLoadingPage: false,
  maxPages: 4,
  loadedOffsets: new Set(),
  totalCount: null,
  filteredStatusCounts: null,
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
  reportsEpoch: 0,
  drawerTab: 'identifiants',
  boardView: 'workspace'
};

const listEl = document.getElementById('machine-list');
const listScroll = document.getElementById('list-scroll');
const searchInput = document.getElementById('search-input');
const searchWrap = document.getElementById('search-wrap');
const searchToggle = document.querySelector('.search-toggle');
const refreshBtn = document.getElementById('refresh-btn');
const reportZeroBtn = document.getElementById('report-zero-btn');
const purgeImportsBtn = document.getElementById('purge-imports-btn');
const reportZeroModal = document.getElementById('report-zero-modal');
const reportZeroForm = document.getElementById('report-zero-form');
const reportZeroError = document.getElementById('report-zero-error');
const reportZeroSubmit = document.getElementById('report-zero-submit');
const reportZeroLotSelect = document.getElementById('report-zero-lot');
const reportZeroTechnicianInput = reportZeroForm
  ? reportZeroForm.querySelector('input[name="technician"]')
  : null;
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
const statServer = document.getElementById('stat-server');
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
const boardTitleEl = document.getElementById('board-title');
const boardSubEl = document.getElementById('board-sub');
const boardTabsEl = document.getElementById('board-tabs');
const boardScopeBannerEl = document.getElementById('board-scope-banner');
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
const dateFilterButtons = document.querySelectorAll('.date-filter-btn[data-date-filter]');
const timelineGranularityButtons = document.querySelectorAll('.timeline-granularity-btn[data-granularity]');
const dateFromInput = document.getElementById('date-from-input');
const dateToInput = document.getElementById('date-to-input');
const applyDateRangeBtn = document.getElementById('apply-date-range-btn');
const timelineCustomRangeEl = document.getElementById('timeline-custom-range');
const timelineSummaryEl = document.getElementById('timeline-summary');
const timelineListEl = document.getElementById('timeline-list');
const resetFiltersBtn = document.getElementById('reset-filters-btn');
const activeFiltersChip = document.getElementById('active-filters-chip');
const resultsCountLabelEl = document.getElementById('results-count-label');
const resultsFiltersSummaryEl = document.getElementById('results-filters-summary');
const resultsCountInlineEl = document.getElementById('results-count-inline');
const resultsFiltersInlineEl = document.getElementById('results-filters-inline');
const sortSelect = document.getElementById('sort-select');
const filterHubEl = document.getElementById('section-filters');
const filterHubBodyEl = document.getElementById('filter-hub-body');
const filterToggleBtn = document.getElementById('filter-toggle-btn');
const summaryFilterButtons = document.querySelectorAll('.summary-filter-btn[data-summary]');
const categoryFilterButtons = document.querySelectorAll('.category-filter-btn[data-category]');
const signalFilterButtons = document.querySelectorAll('.signal-filter-btn[data-signal]');
const kpiTotalEl = document.getElementById('kpi-total');
const kpiActiveEl = document.getElementById('kpi-active');
const kpiOkEl = document.getElementById('kpi-ok');
const kpiNokEl = document.getElementById('kpi-nok');
const kpiNtEl = document.getElementById('kpi-nt');
const detailsDrawerShell = document.getElementById('details-drawer-shell');
const detailsDrawerPanel = document.getElementById('details-drawer-panel');
const detailsDrawerBody = document.getElementById('details-drawer-body');
const detailsDrawerTitle = document.getElementById('details-drawer-title');
const detailsDrawerSub = document.getElementById('details-drawer-sub');
const drawerPrevBtn = document.getElementById('drawer-prev-btn');
const drawerNextBtn = document.getElementById('drawer-next-btn');
const adminLink = document.getElementById('admin-link');
const lotsLink = document.getElementById('lots-link');
const palletsLink = document.getElementById('pallets-link');
const commentTimers = new Map();
let activePatchnoteId = null;
let infiniteObserver = null;
let topObserver = null;
let searchTimer = null;
let virtualRenderRaf = null;
let suggestionCache = [];
let reportLoadRecoveryTriggered = false;

const categoryLabels = {
  laptop: 'Portable',
  desktop: 'Tour',
  server: 'Serveur',
  unknown: 'Inconnu'
};
const categoryCycle = ['desktop', 'server', 'unknown', 'laptop'];
const DEFAULT_TAG_LABEL = 'En cours';
const DEFAULT_LOT_LABEL = 'Aucun lot';
const DEFAULT_PALLET_LABEL = 'Aucune palette';
const BATTERY_ALERT_THRESHOLD = 75;
const boardViewOptions = new Set(['workspace', 'battery-alerts']);

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
  diskSmart: 'SMART disques',
  serverRaid: 'RAID',
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
  'diskSmart',
  'serverRaid',
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
const componentStatusCycles = {
  biosLanguage: ['not_tested', 'fr', 'en'],
  biosPassword: ['not_tested', 'ok', 'nok']
};

const hiddenComponents = new Set(['networkTest', 'memDiag']);
const machineSummaryComponentKeys = Object.freeze([
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
const machineSummaryDiagnosticKeys = Object.freeze([
  'diskReadTest',
  'diskWriteTest',
  'ramTest',
  'cpuTest',
  'gpuTest',
  'networkPing'
]);
const serverSummaryKeys = Object.freeze([
  'networkPing',
  'fsCheck',
  'diskSmart',
  'serverRaid',
  'serverServices',
  'thermal'
]);

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
const sortOptions = new Set(['activity', 'lastSeen', 'status', 'technician', 'name', 'category']);
const layoutStorageKey = `mdt-layout${storageSuffix}`;
const storedLayout = window.localStorage ? localStorage.getItem(layoutStorageKey) : null;
if (storedLayout && layoutOptions.has(storedLayout)) {
  state.layout = storedLayout;
}
const datePresetCycleOrder = ['all', 'today', 'week', 'month', 'year'];
const dateFilterOrder = ['all', 'today', 'week', 'month', 'year', 'custom'];
const dateFilterLabels = {
  all: 'Tous',
  today: "Aujourd'hui",
  week: 'Cette semaine',
  month: 'Ce mois',
  year: 'Cette annee',
  custom: 'Periode perso'
};
const timelineGranularityOptions = new Set(['day', 'week', 'month']);

const prefsStorageKey = `mdt-ui-preferences${storageSuffix}`;
const tagFilterStorageKey = `mdt-tag-filter${storageSuffix}`;
const tagFilterNameStorageKey = `mdt-tag-filter-names${storageSuffix}`;
const filterCollapseStorageKey = `mdt-filter-collapse${storageSuffix}`;
const categoryFilterOptions = new Set(
  isServerView
    ? ['all']
    : isLegacyView
      ? ['all', 'laptop', 'desktop', 'server', 'unknown']
      : ['all', 'laptop', 'desktop', 'unknown']
);
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
if (!sortOptions.has(state.sort)) {
  state.sort = 'activity';
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
    dateFrom: state.dateFrom,
    dateTo: state.dateTo,
    timelineGranularity: state.timelineGranularity,
    boardView: state.boardView,
    sort: state.sort,
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
  if (boardViewOptions.has(prefs.boardView)) {
    state.boardView = prefs.boardView;
  }
  if (typeof prefs.sort === 'string' && prefs.sort.trim()) {
    state.sort = prefs.sort.trim();
  }
  if (dateFilterOrder.includes(prefs.dateFilter)) {
    state.dateFilter = prefs.dateFilter;
  }
  if (typeof prefs.dateFrom === 'string') {
    state.dateFrom = prefs.dateFrom;
  }
  if (typeof prefs.dateTo === 'string') {
    state.dateTo = prefs.dateTo;
  }
  if (timelineGranularityOptions.has(prefs.timelineGranularity)) {
    state.timelineGranularity = prefs.timelineGranularity;
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

function isBatteryAlertsView() {
  return state.boardView === 'battery-alerts';
}

function isTechnicianFilterLocked() {
  return Boolean(state.operatorScope && state.operatorScope.restricted);
}

function getOperatorScopePrimaryKey() {
  const scope = state.operatorScope;
  if (!scope || !scope.restricted) {
    return '';
  }
  return String(scope.primaryKey || '').trim();
}

function getOperatorScopePrimaryLabel() {
  const scope = state.operatorScope;
  if (!scope || !scope.restricted) {
    return '';
  }
  return String(scope.primaryLabel || '').trim();
}

function applyOperatorScope(scope) {
  const normalizedScope = scope && scope.restricted ? scope : null;
  state.operatorScope = normalizedScope;
  if (isTechnicianFilterLocked()) {
    state.techFilter = getOperatorScopePrimaryKey() || 'all';
  }
}

function applyCurrentUser(user) {
  state.currentUser = user && typeof user === 'object' ? user : null;
  applyOperatorScope(state.currentUser && state.currentUser.operatorScope ? state.currentUser.operatorScope : null);
}

function getReportScope() {
  if (isLegacyView) {
    return 'all';
  }
  return isServerView ? 'servers' : 'machines';
}

function getInventoryEntityLabel(count = 2) {
  if (isServerView) {
    return count === 1 ? 'serveur' : 'serveurs';
  }
  return count === 1 ? 'poste' : 'postes';
}

function getInventoryLoadLabel() {
  return isServerView ? 'serveurs' : 'postes';
}

function buildQueryParams({ includeCategory = true, includeTech = true } = {}) {
  const params = new URLSearchParams();
  params.set('scope', getReportScope());
  if (isBatteryAlertsView()) {
    params.set('alertMode', '1');
  }
  if (includeTech && !isTechnicianFilterLocked() && state.techFilter !== 'all') {
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
  if (state.dateFilter === 'custom') {
    const from = normalizeDateInputValue(state.dateFrom);
    const to = normalizeDateInputValue(state.dateTo);
    if (from) {
      params.set('dateFrom', from);
    }
    if (to) {
      params.set('dateTo', to);
    }
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

function updateDateFilterButtons() {
  if (!dateFilterButtons.length) {
    return;
  }
  dateFilterButtons.forEach((button) => {
    const filter = button.dataset.dateFilter || 'all';
    const active = filter === state.dateFilter;
    button.classList.toggle('active', active);
    button.setAttribute('aria-pressed', active ? 'true' : 'false');
  });
  if (timelineGranularityButtons.length) {
    timelineGranularityButtons.forEach((button) => {
      const active = (button.dataset.granularity || 'day') === state.timelineGranularity;
      button.classList.toggle('active', active);
      button.setAttribute('aria-pressed', active ? 'true' : 'false');
    });
  }
  if (timelineCustomRangeEl) {
    timelineCustomRangeEl.classList.toggle('is-active', state.dateFilter === 'custom');
  }
  if (dateFromInput) {
    dateFromInput.value = normalizeDateInputValue(state.dateFrom);
  }
  if (dateToInput) {
    dateToInput.value = normalizeDateInputValue(state.dateTo);
  }
  updateTimeFilterLabel();
  updateFilterDockState();
}

function syncDateFilterControls() {
  updateDateFilterButtons();
  updateSignalFilterButtons();
  renderTimeline();
}

function formatTimelineBucketLabel(value, granularity) {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return '--';
  }
  if (granularity === 'month') {
    return date.toLocaleDateString('fr-FR', { month: 'short', year: 'numeric' });
  }
  if (granularity === 'week') {
    const end = new Date(date);
    end.setDate(date.getDate() + 6);
    return `${date.toLocaleDateString('fr-FR', { day: '2-digit', month: '2-digit' })} -> ${end.toLocaleDateString('fr-FR', {
      day: '2-digit',
      month: '2-digit'
    })}`;
  }
  return date.toLocaleDateString('fr-FR', {
    day: '2-digit',
    month: '2-digit'
  });
}

function renderTimeline() {
  if (!timelineListEl || !timelineSummaryEl) {
    return;
  }
  if (!hasActiveDateFilter()) {
    timelineSummaryEl.textContent = `Active un filtre de periode pour compter les ${getInventoryEntityLabel(2)} par jour, semaine ou mois.`;
    timelineListEl.innerHTML = '<div class="timeline-empty">Aucune periode selectionnee.</div>';
    return;
  }
  const buckets = Array.isArray(state.timelineBuckets) ? state.timelineBuckets : [];
  const granularityLabel =
    state.timelineGranularity === 'month'
      ? 'mois'
      : state.timelineGranularity === 'week'
        ? 'semaines'
        : 'jours';
  timelineSummaryEl.textContent = `${buildDateFilterSummary()} · aggregation par ${granularityLabel}.`;
  if (!buckets.length) {
    timelineListEl.innerHTML = `<div class="timeline-empty">Aucun ${getInventoryEntityLabel(1)} sur cette periode.</div>`;
    return;
  }
  const maxCount = buckets.reduce((max, item) => Math.max(max, item.machineCount || 0), 0) || 1;
  timelineListEl.innerHTML = buckets
    .map((item) => {
      const machineCount = Number.parseInt(String(item.machineCount || 0), 10) || 0;
      const reportCount = Number.parseInt(String(item.reportCount || 0), 10) || 0;
      const width = Math.max(8, Math.round((machineCount / maxCount) * 100));
      return `
        <article class="timeline-bucket">
          <div class="timeline-bucket-head">
            <strong>${escapeHtml(formatTimelineBucketLabel(item.bucketStart, state.timelineGranularity))}</strong>
            <span>${escapeHtml(`${machineCount} ${getInventoryEntityLabel(machineCount)}`)}</span>
          </div>
          <div class="timeline-bucket-bar" aria-hidden="true"><span style="width:${width}%"></span></div>
          <p class="timeline-bucket-meta">${escapeHtml(`${reportCount} passage${reportCount > 1 ? 's' : ''}`)}</p>
        </article>
      `;
    })
    .join('');
}

async function loadTimeline() {
  if (!hasActiveDateFilter()) {
    state.timelineBuckets = [];
    renderTimeline();
    return;
  }
  if (timelineListEl) {
    timelineListEl.innerHTML = '<div class="timeline-empty">Chargement de la chronologie...</div>';
  }
  try {
    const params = buildQueryParams({ includeCategory: true, includeTech: true });
    params.set('granularity', state.timelineGranularity);
    const response = await fetch(`/api/stats/timeline?${params.toString()}`);
    if (response.status === 401) {
      window.location.href = '/login';
      return;
    }
    if (!response.ok) {
      throw new Error('timeline_failed');
    }
    const data = await response.json();
    if (!data.ok) {
      throw new Error('timeline_failed');
    }
    state.timelineBuckets = Array.isArray(data.buckets) ? data.buckets : [];
  } catch (error) {
    state.timelineBuckets = [];
  }
  renderTimeline();
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
  state.filteredStatusCounts = null;
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

function refreshActiveDrawerIfNeeded(id) {
  if (!state.expandedId) {
    return;
  }
  if (String(state.expandedId) !== String(id)) {
    return;
  }
  renderDetailsDrawerContent(String(id));
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
  async function fetchStatsSnapshot(params) {
    const response = await fetch(`/api/stats?${params.toString()}`);
    if (response.status === 401) {
      window.location.href = '/login';
      return null;
    }
    if (!response.ok) {
      throw new Error('stats_failed');
    }
    const data = await response.json();
    if (!data.ok) {
      throw new Error('stats_failed');
    }
    return data;
  }

  try {
    const data = await fetchStatsSnapshot(buildQueryParams({ includeCategory: false, includeTech: true }));
    if (!data) {
      return;
    }
    state.stats = {
      total: data.total || 0,
      laptop: data.laptop || 0,
      desktop: data.desktop || 0,
      server: data.server || 0,
      unknown: data.unknown || 0
    };
    state.techOptions = Array.isArray(data.techs) ? data.techs : [];
    renderTechFilters();
  } catch (error) {
    state.stats = null;
    state.techOptions = [];
    renderTechFilters();
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
    applyPermissions(data.permissions || null);
    applyOperatorScope(data.operatorScope || null);
    renderBoardTabs();
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

function wait(ms) {
  return new Promise((resolve) => {
    window.setTimeout(resolve, ms);
  });
}

async function fetchReportsPagePayload(requestQuery, { retryCount = 0 } = {}) {
  let lastError = null;
  for (let attempt = 0; attempt <= retryCount; attempt += 1) {
    try {
      const response = await fetch(`/api/reports?${requestQuery}`, {
        cache: 'no-store',
        headers: {
          'Cache-Control': 'no-store'
        }
      });
      if (response.status === 401) {
        return { unauthorized: true };
      }
      if (!response.ok) {
        throw new Error(`fetch_failed_${response.status}`);
      }
      const data = await response.json();
      if (!data || !data.ok) {
        throw new Error('fetch_failed');
      }
      return { data };
    } catch (error) {
      lastError = error;
      if (attempt < retryCount) {
        await wait(200 * (attempt + 1));
      }
    }
  }
  throw lastError || new Error('fetch_failed');
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
  let requestQuery = '';
  try {
    const params = buildQueryParams({ includeCategory: true });
    params.set('limit', String(state.pageSize));
    params.set('offset', String(offset));
    if (!Number.isFinite(state.totalCount)) {
      params.set('includeTotal', '1');
    }
    requestQuery = params.toString();
    const { data, unauthorized } = await fetchReportsPagePayload(requestQuery, {
      retryCount: offset === 0 ? 2 : 1
    });
    if (unauthorized) {
      window.location.href = '/login';
      return;
    }
    if (epoch !== state.reportsEpoch) {
      return;
    }
    if (data.total != null) {
      const totalValue = Number.parseInt(data.total, 10);
      if (Number.isFinite(totalValue)) {
        state.totalCount = totalValue;
      }
    }
    if (data.statusCounts) {
      state.filteredStatusCounts = normalizeStatusCountPayload(data.statusCounts);
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
    reportLoadRecoveryTriggered = false;
    trimPagesAround(offset);
    state.pageStart = state.pages.length ? state.pages[0].offset : 0;
    syncMachinesFromPages();
    renderList();
    updateLastUpdated();
    state.lastLoadScrollY = getScrollTop();
  } catch (error) {
    if (epoch === state.reportsEpoch) {
      console.error('Failed to load reports page', { offset, query: requestQuery, error });
      if (offset === 0 && state.machines.length > 0) {
        updateLastUpdated();
        return;
      }
      if (offset === 0 && !reportLoadRecoveryTriggered) {
        reportLoadRecoveryTriggered = true;
        if (listEl) {
          listEl.innerHTML = '<div class="loading">Recuperation de l affichage...</div>';
        }
        window.setTimeout(() => {
          if (getActiveFilterCount() > 0) {
            resetAllFilters();
            return;
          }
          reloadReports();
        }, 0);
        return;
      }
      const resetAction = getActiveFilterCount() > 0
        ? '<button class="card-action-btn" type="button" data-action="reset-filters-and-retry">Reinitialiser les filtres</button>'
        : '';
      listEl.innerHTML = `
        <div class="empty">
          Erreur lors du chargement.
          <div class="empty-actions">
            <button class="card-action-btn is-primary" type="button" data-action="retry-reports">Reessayer</button>
            ${resetAction}
          </div>
        </div>
      `;
    }
  } finally {
    if (epoch === state.reportsEpoch) {
      state.isLoadingPage = false;
      pumpOffsetQueue();
    }
  }
}

async function reloadReports() {
  listEl.innerHTML = `<div class="loading">Chargement des ${getInventoryLoadLabel()}...</div>`;
  resetPagination();
  reportLoadRecoveryTriggered = false;
  state.skipAnchorRestore = true;
  if (listScroll) {
    listScroll.scrollTop = 0;
  } else {
    window.scrollTo({ top: 0 });
  }
  await Promise.all([loadStats(), loadReportsPage(0), loadTimeline()]);
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

function updateSortSelect() {
  if (!sortSelect) {
    return;
  }
  const value = state.sort === 'lastSeen' ? 'activity' : state.sort;
  sortSelect.value = sortOptions.has(value) ? value : 'activity';
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

function updateSummaryFilterButtons() {
  if (!summaryFilterButtons.length) {
    return;
  }
  summaryFilterButtons.forEach((btn) => {
    const value = btn.dataset.summary || '';
    const active =
      state.quickFilter &&
      state.quickFilter.type === 'summary' &&
      state.quickFilter.value === value;
    btn.classList.toggle('active', Boolean(active));
    btn.setAttribute('aria-pressed', active ? 'true' : 'false');
  });
}

function updateCategoryFilterButtons() {
  if (!categoryFilterButtons.length) {
    return;
  }
  categoryFilterButtons.forEach((btn) => {
    const value = btn.dataset.category || 'all';
    const active = (state.filter || 'all') === value;
    btn.classList.toggle('active', active);
    btn.setAttribute('aria-pressed', active ? 'true' : 'false');
  });
}

function updateSignalFilterButtons() {
  if (!signalFilterButtons.length) {
    return;
  }
  signalFilterButtons.forEach((btn) => {
    const signal = btn.dataset.signal || '';
    let active = false;
    if (signal === 'battery-alerts') {
      active = isBatteryAlertsView();
    } else if (signal === 'recent') {
      active = state.dateFilter === 'today';
    } else if (signal === 'commented') {
      active = state.commentFilter === 'with';
    }
    btn.classList.toggle('active', active);
    btn.setAttribute('aria-pressed', active ? 'true' : 'false');
  });
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

function buildPalletLabel(pallet) {
  if (!pallet || typeof pallet !== 'object') {
    return DEFAULT_PALLET_LABEL;
  }
  const code = String(pallet.code || '').trim();
  const statusLabel = String(pallet.statusLabel || '').trim();
  if (code && statusLabel) {
    return `${code} - ${statusLabel}`;
  }
  if (code) {
    return code;
  }
  if (pallet.label) {
    return String(pallet.label).trim() || DEFAULT_PALLET_LABEL;
  }
  return DEFAULT_PALLET_LABEL;
}

function getMachinePallet(machine) {
  if (!machine || !machine.pallet || typeof machine.pallet !== 'object') {
    return null;
  }
  return machine.pallet;
}

function getMachineShipment(machine) {
  if (!machine || !machine.shipment || typeof machine.shipment !== 'object') {
    return null;
  }
  return machine.shipment;
}

function formatShipmentDate(value) {
  if (!value) {
    return '--';
  }
  const date = new Date(`${String(value).trim()}T00:00:00`);
  if (Number.isNaN(date.getTime())) {
    return '--';
  }
  return date.toLocaleDateString('fr-FR');
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

function buildLotAssignmentOptions(currentLot = null) {
  const currentLotId = normalizeLotId(currentLot && currentLot.id ? currentLot.id : '');
  const lotMap = new Map();
  if (currentLot && currentLotId) {
    lotMap.set(currentLotId, currentLot);
  }
  const lots = Array.isArray(state.lots) ? state.lots : [];
  lots.forEach((lot) => {
    const lotId = normalizeLotId(lot && lot.id ? lot.id : '');
    if (!lotId) {
      return;
    }
    if (!lotMap.has(lotId)) {
      lotMap.set(lotId, lot);
    }
  });
  const ranked = rankLots(Array.from(lotMap.values()), currentLotId || normalizeLotId(state.activeLotId || ''));
  const options = ['<option value="">Aucun lot</option>'];
  ranked.forEach((lot) => {
    if (!lot || !lot.id) {
      return;
    }
    const lotId = normalizeLotId(lot.id);
    const produced = Number.isFinite(lot.producedCount) ? lot.producedCount : 0;
    const target = Number.isFinite(lot.targetCount) ? lot.targetCount : 0;
    const paused = lot.isPaused ? ' [PAUSE]' : '';
    const selected = currentLotId && currentLotId === lotId ? ' selected' : '';
    options.push(
      `<option value="${escapeHtml(lot.id)}"${selected}>${escapeHtml(buildLotLabel(lot))} (${produced}/${target})${escapeHtml(
        paused
      )}</option>`
    );
  });
  return options.join('');
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
  if (isTechnicianFilterLocked()) {
    const lockedKey = getOperatorScopePrimaryKey();
    const lockedLabel = getOperatorScopePrimaryLabel() || 'Operateur connecte';
    if (lockedKey) {
      state.techFilter = lockedKey;
    }
    techFiltersEl.innerHTML = `
      <button class="filter-btn tech-filter-btn active is-locked" data-tech="${escapeHtml(
        lockedKey || 'all'
      )}" type="button" aria-pressed="true" disabled>
        ${escapeHtml(lockedLabel)}
      </button>
    `;
    updateTechFilterButtons();
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
  if (state.dateFilter === 'custom' && hasActiveDateFilter()) {
    statTimeLabel.textContent = buildDateFilterSummary();
    return;
  }
  statTimeLabel.textContent = dateFilterLabels[state.dateFilter] || 'Tous';
}

function getActiveFilterCount() {
  let count = 0;
  if (state.filter && state.filter !== 'all' && !(isServerView && state.filter === 'server')) {
    count += 1;
  }
  if (isBatteryAlertsView()) {
    count += 1;
  }
  if (hasActiveDateFilter()) {
    count += 1;
  }
  if (!isTechnicianFilterLocked() && state.techFilter && state.techFilter !== 'all') {
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

function buildActiveFilterLabels() {
  const labels = [];
  if (state.quickFilter && state.quickFilter.type === 'summary' && state.quickFilter.value) {
    const summaryMap = { ok: 'OK', nok: 'NOK', nt: 'NT' };
    labels.push(summaryMap[state.quickFilter.value] || state.quickFilter.value);
  }
  if (state.filter && state.filter !== 'all' && !(isServerView && state.filter === 'server')) {
    labels.push(categoryLabels[normalizeCategory(state.filter)] || state.filter);
  }
  if (isBatteryAlertsView()) {
    labels.push('Alertes batterie / RTC');
  }
  if (state.dateFilter === 'today') {
    labels.push('Activite recente');
  } else if (hasActiveDateFilter()) {
    labels.push(buildDateFilterSummary());
  }
  if (!isTechnicianFilterLocked() && state.techFilter && state.techFilter !== 'all') {
    const match = Array.isArray(state.techOptions)
      ? state.techOptions.find((value) => techKey(value) === state.techFilter)
      : null;
    labels.push(normalizeTech(match) || state.techFilter);
  }
  if (Array.isArray(state.tagFilter) && state.tagFilter.length) {
    const tagNames = state.tagFilter
      .map((id) => {
        const match = Array.isArray(state.tags)
          ? state.tags.find((tag) => normalizeTagId(tag.id || '') === normalizeTagId(id))
          : null;
        return match && match.name ? String(match.name) : null;
      })
      .filter(Boolean);
    if (tagNames.length) {
      labels.push(`Tags ${buildTagSummaryFromLabels(tagNames)}`);
    }
  }
  if (state.componentFilter && state.componentFilter !== 'all') {
    const componentLabel = componentLabels[state.componentFilter] || state.componentFilter;
    labels.push(componentLabel);
  }
  if (state.commentFilter === 'with') {
    labels.push('Avec commentaire');
  } else if (state.commentFilter === 'without') {
    labels.push('Sans commentaire');
  }
  if (state.search && state.search.trim()) {
    labels.push(`Recherche "${state.search.trim()}"`);
  }
  return labels;
}

function formatResultCountLabel(count) {
  const numeric = Number.isFinite(count) ? count : 0;
  return `${numeric} ${getInventoryEntityLabel(numeric)}`;
}

function updateResultsSummary(count = null) {
  const labels = buildActiveFilterLabels();
  const filtersText = labels.length ? `Filtres : ${labels.join(' + ')}` : 'Filtres : aucun';
  const countText = count == null ? `Resultats : -- ${getInventoryEntityLabel(1)}` : `Resultats : ${formatResultCountLabel(count)}`;
  if (resultsCountLabelEl) {
    resultsCountLabelEl.textContent = countText;
  }
  if (resultsFiltersSummaryEl) {
    resultsFiltersSummaryEl.textContent = filtersText;
  }
  if (resultsCountInlineEl) {
    resultsCountInlineEl.textContent = count == null ? `-- ${getInventoryEntityLabel(1)}` : formatResultCountLabel(count);
  }
  if (resultsFiltersInlineEl) {
    resultsFiltersInlineEl.textContent = filtersText;
  }
}

function loadFilterCollapsedPreference() {
  if (!window.localStorage) {
    return null;
  }
  try {
    const storedValue = localStorage.getItem(filterCollapseStorageKey);
    if (storedValue === null) {
      return null;
    }
    return storedValue === '1';
  } catch (error) {
    return null;
  }
}

function saveFilterCollapsedPreference(collapsed) {
  if (!window.localStorage) {
    return;
  }
  try {
    localStorage.setItem(filterCollapseStorageKey, collapsed ? '1' : '0');
  } catch (error) {
    // Ignore storage errors.
  }
}

function updateFilterToggleLabel(count = getActiveFilterCount()) {
  if (!filterToggleBtn || !filterHubEl) {
    return;
  }
  const collapsed = filterHubEl.classList.contains('is-collapsed');
  const plural = count > 1 ? 's' : '';
  const suffix = count > 0 ? ` (${count} actif${plural})` : '';
  const label = collapsed ? `Afficher les filtres${suffix}` : `Masquer les filtres${suffix}`;
  filterToggleBtn.textContent = label;
  filterToggleBtn.setAttribute('aria-expanded', collapsed ? 'false' : 'true');
}

function setFilterHubCollapsed(collapsed, { persist = true } = {}) {
  if (!filterHubEl) {
    return;
  }
  const next = Boolean(collapsed);
  filterHubEl.classList.toggle('is-collapsed', next);
  if (filterHubBodyEl) {
    filterHubBodyEl.setAttribute('aria-hidden', next ? 'true' : 'false');
  }
  updateFilterToggleLabel();
  if (persist) {
    saveFilterCollapsedPreference(next);
  }
}

function initFilterHub() {
  if (!filterHubEl) {
    return;
  }
  const storedCollapsed = loadFilterCollapsedPreference();
  const defaultCollapsed = storedCollapsed === null ? false : storedCollapsed;
  setFilterHubCollapsed(defaultCollapsed, { persist: false });
  if (filterToggleBtn) {
    filterToggleBtn.addEventListener('click', () => {
      const collapsed = filterHubEl.classList.contains('is-collapsed');
      setFilterHubCollapsed(!collapsed);
    });
  }
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
  updateFilterToggleLabel(count);
  updateResultsSummary();
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

function getStartOfMonth() {
  const now = new Date();
  return new Date(now.getFullYear(), now.getMonth(), 1);
}

function getEndOfMonth() {
  const now = new Date();
  return new Date(now.getFullYear(), now.getMonth() + 1, 0, 23, 59, 59, 999);
}

function getStartOfYear() {
  const now = new Date();
  return new Date(now.getFullYear(), 0, 1);
}

function getEndOfYear() {
  const now = new Date();
  return new Date(now.getFullYear(), 11, 31, 23, 59, 59, 999);
}

function normalizeDateInputValue(value) {
  const raw = String(value || '').trim();
  return /^\d{4}-\d{2}-\d{2}$/.test(raw) ? raw : '';
}

function getStateDateRange() {
  if (state.dateFilter === 'all') {
    return null;
  }
  if (state.dateFilter === 'today') {
    return { start: getStartOfToday(), end: getEndOfToday() };
  }
  if (state.dateFilter === 'week') {
    return { start: getStartOfWeek(), end: getEndOfWeek() };
  }
  if (state.dateFilter === 'month') {
    return { start: getStartOfMonth(), end: getEndOfMonth() };
  }
  if (state.dateFilter === 'year') {
    return { start: getStartOfYear(), end: getEndOfYear() };
  }
  if (state.dateFilter === 'custom') {
    const from = normalizeDateInputValue(state.dateFrom);
    const to = normalizeDateInputValue(state.dateTo);
    if (!from && !to) {
      return null;
    }
    const start = from ? new Date(`${from}T00:00:00`) : null;
    const end = to ? new Date(`${to}T23:59:59.999`) : null;
    if (start && Number.isNaN(start.getTime())) {
      return null;
    }
    if (end && Number.isNaN(end.getTime())) {
      return null;
    }
    if (start && end && start > end) {
      return { start: end, end: start };
    }
    return { start, end };
  }
  return null;
}

function hasActiveDateFilter() {
  if (state.dateFilter === 'all') {
    return false;
  }
  if (state.dateFilter !== 'custom') {
    return true;
  }
  return Boolean(normalizeDateInputValue(state.dateFrom) || normalizeDateInputValue(state.dateTo));
}

function formatDateFilterDate(value) {
  if (!value) {
    return '--';
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return '--';
  }
  return date.toLocaleDateString('fr-FR', {
    day: '2-digit',
    month: '2-digit',
    year: 'numeric'
  });
}

function buildDateFilterSummary() {
  const range = getStateDateRange();
  if (!range) {
    return 'Tous';
  }
  if (state.dateFilter !== 'custom') {
    return dateFilterLabels[state.dateFilter] || 'Tous';
  }
  const from = range.start ? formatDateFilterDate(range.start) : 'debut';
  const to = range.end ? formatDateFilterDate(range.end) : 'maintenant';
  return `${from} -> ${to}`;
}

function cycleDateFilter(render = true) {
  const currentIndex = datePresetCycleOrder.indexOf(state.dateFilter);
  const nextIndex = currentIndex === -1 ? 0 : (currentIndex + 1) % datePresetCycleOrder.length;
  state.dateFilter = datePresetCycleOrder[nextIndex];
  if (state.dateFilter !== 'custom') {
    state.dateFrom = '';
    state.dateTo = '';
  }
  syncDateFilterControls();
  savePreferences();
  if (render) {
    reloadReports();
  }
}

function renderTechnicianOptions() {
  if (!technicianOptionsEl) {
    return;
  }
  if (isTechnicianFilterLocked()) {
    const label = getOperatorScopePrimaryLabel();
    technicianOptionsEl.innerHTML = label
      ? `<option value="${escapeHtml(label)}"></option>`
      : '';
    syncReportZeroTechnicianField();
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
  syncReportZeroTechnicianField();
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
  updateSignalFilterButtons();
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

function sanitizeDownloadFilename(value, fallback = 'report') {
  const raw = value == null ? '' : String(value).trim();
  const normalized = raw.normalize('NFKD').replace(/[^\w.-]+/g, '-');
  const trimmed = normalized.replace(/-+/g, '-').replace(/^[-.]+|[-.]+$/g, '');
  return trimmed ? trimmed.slice(0, 80) : fallback;
}

function buildPdfDownloadFilename(detail) {
  const identityLabel = buildMachineIdentityLabel(detail, {
    includeSerial: true,
    fallback: (detail && (detail.hostname || detail.macAddress || detail.id)) || 'report'
  });
  const baseName = sanitizeDownloadFilename(
    identityLabel || '',
    'report'
  );
  return `rapport-atelier-${baseName}.pdf`;
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

function setPalletsLinkVisible(visible) {
  if (!palletsLink) {
    return;
  }
  palletsLink.hidden = !visible;
}

function setReportZeroVisible(visible) {
  if (!reportZeroBtn) {
    return;
  }
  reportZeroBtn.hidden = !visible;
}

function updatePurgeImportsVisibility() {
  if (!purgeImportsBtn) {
    return;
  }
  const visible = Boolean(state.legacyMode === 'legacy' && state.canDeleteReport);
  purgeImportsBtn.hidden = !visible;
}

function canDeleteReportFromUser(user) {
  if (!user) {
    return false;
  }
  return Boolean(user.permissions && user.permissions.canDeleteReport);
}

function applyPermissions(permissions) {
  const source = permissions && typeof permissions === 'object' ? permissions : {};
  state.canDeleteReport = source.canDeleteReport === true;
  state.canEditTags = source.canEditTags === true;
  state.canAccessAdmin = source.canAccessAdminPage === true;
  state.canCreateReportZero = source.canCreateReportZero === true;
  state.canEditReports = source.canEditReports === true;
  state.canEditBatteryHealth = source.canEditBatteryHealth === true;
  state.canEditTechnician = source.canEditTechnician === true;
  state.canManageLots = source.canManageLots === true;
  state.canManagePallets = source.canManagePallets === true;
  state.canManageLogistics = source.canManageLogistics === true;
  state.canRenameTags = source.canRenameTags === true;
  setAdminLinkVisible(state.canAccessAdmin);
  setLotsLinkVisible(state.canManageLots || state.canManageLogistics);
  setPalletsLinkVisible(state.canManagePallets || state.canManageLogistics);
  setReportZeroVisible(state.canCreateReportZero);
  updatePurgeImportsVisibility();
}

function syncReportZeroTechnicianField() {
  if (!reportZeroTechnicianInput) {
    return;
  }
  const locked = isTechnicianFilterLocked();
  const lockedLabel = getOperatorScopePrimaryLabel();
  reportZeroTechnicianInput.readOnly = locked;
  reportZeroTechnicianInput.classList.toggle('is-locked', locked);
  reportZeroTechnicianInput.setAttribute('aria-readonly', locked ? 'true' : 'false');
  if (locked) {
    reportZeroTechnicianInput.value = lockedLabel || '';
  }
}

function renderBoardTabs() {
  if (boardTabsEl) {
    const buttons = boardTabsEl.querySelectorAll('.board-tab-btn[data-board-view]');
    buttons.forEach((button) => {
      const active = (button.dataset.boardView || 'workspace') === state.boardView;
      button.classList.toggle('active', active);
      button.setAttribute('aria-selected', active ? 'true' : 'false');
      button.setAttribute('tabindex', active ? '0' : '-1');
    });
  }
  if (boardTitleEl) {
    boardTitleEl.textContent = isBatteryAlertsView()
      ? isServerView
        ? 'Alertes serveurs'
        : 'Alertes'
      : isServerView
        ? 'Liste des serveurs suivis'
        : 'Liste des postes en cours';
  }
  if (boardSubEl) {
    boardSubEl.textContent = isBatteryAlertsView()
      ? isServerView
        ? 'Vue dediee aux serveurs a traiter en priorite: services critiques, thermique ou alertes RTC.'
        : 'Vue dediee aux postes a traiter en priorite: batterie faible ou derive d horloge BIOS / RTC.'
      : isServerView
        ? 'Vue operationnelle en temps reel avec suivi clair de l infrastructure et des diagnostics serveurs.'
        : 'Vue operationnelle en temps reel avec suivi clair des machines et du lot prioritaire.';
  }
  if (boardScopeBannerEl) {
    const label = getOperatorScopePrimaryLabel();
    if (isTechnicianFilterLocked() && label) {
      boardScopeBannerEl.hidden = false;
      boardScopeBannerEl.textContent = `Vue restreinte au technicien ${label}.`;
    } else {
      boardScopeBannerEl.hidden = true;
      boardScopeBannerEl.textContent = '';
    }
  }
  syncReportZeroTechnicianField();
}

async function initAdminLink() {
  if (!adminLink && !lotsLink && !palletsLink) {
    return;
  }
  applyPermissions(null);
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
      applyCurrentUser(data.user);
      applyPermissions(data.user.permissions || null);
      renderBoardTabs();
      renderTechFilters();
      renderTechnicianOptions();
      renderList();
    }
  } catch (error) {
    applyPermissions(null);
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
  }
  const raw = String(value).trim().toLowerCase();
  if (!raw) {
    return null;
  }
  if (statusLabels[raw]) {
    return raw;
  }

  const cleaned = raw.normalize('NFD').replace(/[\u0300-\u036f]/g, '');
  if (statusLabels[cleaned]) {
    return cleaned;
  }
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
    cleaned.includes('good') ||
    cleaned.includes('present') ||
    cleaned.includes('working') ||
    cleaned.includes('fonction') ||
    cleaned.includes('disponible') ||
    cleaned.endsWith(' ok')
  ) {
    return 'ok';
  }
  return null;
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
  const status = resolved && resolved.status ? resolved.status : normalizeStatusKey(value);
  if (!status) {
    return null;
  }
  if (status === 'fr' || status === 'en') {
    return 'ok';
  }
  return status;
}

function getBatteryHealthValue(detail) {
  if (!detail || typeof detail !== 'object') {
    return null;
  }
  const telemetry = getBatteryTelemetry(detail);
  return parseBatteryHealthValue(detail.batteryHealth != null ? detail.batteryHealth : telemetry.healthPercent);
}

function getBatteryHealthStatus(detail) {
  const batteryHealth = getBatteryHealthValue(detail);
  if (batteryHealth == null) {
    return null;
  }
  return batteryHealth < BATTERY_ALERT_THRESHOLD ? 'nok' : 'ok';
}

function summarizeDetailForDrawer(detail) {
  const summary = { ok: 0, nok: 0, other: 0, total: 0 };
  if (!detail || typeof detail !== 'object') {
    return summary;
  }

  const payload = detail.payload && typeof detail.payload === 'object' ? detail.payload : null;
  const tests =
    payload && payload.tests && typeof payload.tests === 'object' && !Array.isArray(payload.tests)
      ? payload.tests
      : null;
  const components = resolveDetailComponents(detail);
  if (isServerDetail(detail)) {
    getServerStatusEntries(detail, components, tests).forEach((entry) => {
      const normalized = normalizeSummaryStatusForKey(entry.key, entry.status);
      if (normalized) {
        addSummaryStatus(summary, normalized);
      }
    });
  } else {
    machineSummaryComponentKeys.forEach((key) => {
      const raw = resolveUnifiedComponentStatus(key, components, tests);
      const normalized = normalizeSummaryStatusForKey(key, raw || 'not_tested');
      if (normalized) {
        addSummaryStatus(summary, normalized);
      }
    });

    const diagnosticCandidates = [];
    if (tests) {
      machineSummaryDiagnosticKeys.forEach((key) => {
        diagnosticCandidates.push(resolveUnifiedComponentStatus(key, components, tests));
      });
      if (tests.fsCheck || components.fsCheck) {
        diagnosticCandidates.push(resolveUnifiedComponentStatus('fsCheck', components, tests));
      }
    } else {
      machineSummaryDiagnosticKeys.forEach((key) => {
        diagnosticCandidates.push(resolveUnifiedComponentStatus(key, components));
      });
      if (components.fsCheck) {
        diagnosticCandidates.push(resolveUnifiedComponentStatus('fsCheck', components));
      }
    }
    diagnosticCandidates.forEach((value) => {
      const normalized = normalizeStatusKey(value);
      if (normalized) {
        addSummaryStatus(summary, normalized);
      }
    });

    const batteryStatus = getBatteryHealthStatus(detail);
    if (batteryStatus) {
      addSummaryStatus(summary, batteryStatus);
    }
  }

  const hasComment = typeof detail.comment === 'string' && detail.comment.trim();
  if (hasComment) {
    addSummaryStatus(summary, 'nok');
  }

  return summary;
}

function collectDetailNokEntries(detail) {
  if (!detail || typeof detail !== 'object') {
    return [];
  }

  const payload = detail.payload && typeof detail.payload === 'object' ? detail.payload : null;
  const tests =
    payload && payload.tests && typeof payload.tests === 'object' && !Array.isArray(payload.tests)
      ? payload.tests
      : null;
  const components = resolveDetailComponents(detail);
  const entries = [];
  const seen = new Set();

  function addEntry(label, key, tab, value) {
    const normalized = normalizeSummaryStatusForKey(key, value);
    if (normalized !== 'nok') {
      return;
    }
    const dedupeKey = `${tab}:${key}`;
    if (seen.has(dedupeKey)) {
      return;
    }
    seen.add(dedupeKey);
    entries.push({ label, key, tab });
  }

  if (isServerDetail(detail)) {
    getServerStatusEntries(detail, components, tests).forEach((entry) => {
      addEntry(entry.label, entry.key, entry.tab, entry.status);
    });
  } else {
    addEntry(
      'Lecture disque',
      'diskReadTest',
      'diagnostics',
      resolveUnifiedComponentStatus('diskReadTest', components, tests)
    );
    addEntry(
      'Ecriture disque',
      'diskWriteTest',
      'diagnostics',
      resolveUnifiedComponentStatus('diskWriteTest', components, tests)
    );
    addEntry('RAM (WinSAT)', 'ramTest', 'diagnostics', resolveUnifiedComponentStatus('ramTest', components, tests));
    addEntry('CPU (WinSAT)', 'cpuTest', 'diagnostics', resolveUnifiedComponentStatus('cpuTest', components, tests));
    addEntry('GPU (WinSAT)', 'gpuTest', 'diagnostics', resolveUnifiedComponentStatus('gpuTest', components, tests));
    addEntry(
      'Ping',
      'networkPing',
      'diagnostics',
      resolveUnifiedComponentStatus('networkPing', components, tests)
    );
    if ((tests && tests.fsCheck) || components.fsCheck) {
      addEntry('Check disque', 'fsCheck', 'diagnostics', resolveUnifiedComponentStatus('fsCheck', components, tests));
    }

    addEntry('Ports USB', 'usb', 'composants', resolveUnifiedComponentStatus('usb', components, tests));
    addEntry('Clavier', 'keyboard', 'composants', resolveUnifiedComponentStatus('keyboard', components, tests));
    addEntry('Camera', 'camera', 'composants', resolveUnifiedComponentStatus('camera', components, tests));
    addEntry('Pave tactile', 'pad', 'composants', resolveUnifiedComponentStatus('pad', components, tests));
    addEntry('Lecteur badge', 'badgeReader', 'composants', resolveUnifiedComponentStatus('badgeReader', components, tests));
    addEntry('CPU OK', 'cpu', 'composants', resolveUnifiedComponentStatus('cpu', components, tests));
    addEntry('GPU OK', 'gpu', 'composants', resolveUnifiedComponentStatus('gpu', components, tests));

    addEntry('Pile BIOS', 'biosBattery', 'bios_wifi', components.biosBattery || 'not_tested');
    addEntry('Langue BIOS', 'biosLanguage', 'bios_wifi', components.biosLanguage || 'not_tested');
    addEntry('Mot de passe BIOS', 'biosPassword', 'bios_wifi', components.biosPassword || 'not_tested');
    addEntry('Norme Wi-Fi', 'wifiStandard', 'bios_wifi', components.wifiStandard || 'not_tested');

    if (getBatteryHealthStatus(detail) === 'nok') {
      entries.push({ label: 'Sante batterie', key: 'batteryHealth', tab: 'identifiants' });
    }
  }

  const commentValue = typeof detail.comment === 'string' ? detail.comment.trim() : '';
  if (commentValue) {
    entries.push({ label: 'Commentaire operateur', key: 'comment', tab: 'commentaires' });
  }

  return entries;
}

function getPrimaryDetailNokEntry(detail) {
  const entries = collectDetailNokEntries(detail);
  return entries.length ? entries[0] : null;
}

function focusDrawerIssue(tab, key = '') {
  if (!detailsDrawerBody) {
    return;
  }
  const targetTab = typeof tab === 'string' && tab ? tab : 'composants';
  setDrawerTab(targetTab);
  if (!key) {
    return;
  }
  window.requestAnimationFrame(() => {
    const selector = `[data-tab-panel="${targetTab}"] [data-detail-key="${key}"]`;
    const target = detailsDrawerBody.querySelector(selector);
    if (!target) {
      return;
    }
    target.classList.add('is-focused');
    target.scrollIntoView({ behavior: 'smooth', block: 'center' });
    window.setTimeout(() => {
      target.classList.remove('is-focused');
    }, 1800);
  });
}

function getMachinePrimaryStatus(machine) {
  const summary = summarizeDetailForDrawer(machine);
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

function getMachinePrimaryStatusLabel(machine) {
  const status = getMachinePrimaryStatus(machine);
  if (status === 'nok') {
    return 'NOK';
  }
  if (status === 'ok') {
    return 'OK';
  }
  return 'NT';
}

function normalizeStatusCountPayload(value) {
  if (!value || typeof value !== 'object') {
    return null;
  }
  const total = Number.parseInt(String(value.total || 0), 10);
  const ok = Number.parseInt(String(value.ok || 0), 10);
  const nok = Number.parseInt(String(value.nok || 0), 10);
  const nt = Number.parseInt(String(value.nt || value.other || 0), 10);
  return {
    total: Number.isFinite(total) ? total : 0,
    ok: Number.isFinite(ok) ? ok : 0,
    nok: Number.isFinite(nok) ? nok : 0,
    nt: Number.isFinite(nt) ? nt : 0
  };
}

function adjustFilteredStatusCounts(previousMachine, nextMachine) {
  if (!state.filteredStatusCounts) {
    return;
  }
  const previousStatus = previousMachine ? getMachinePrimaryStatus(previousMachine) : null;
  const nextStatus = nextMachine ? getMachinePrimaryStatus(nextMachine) : null;
  if (!previousStatus || !nextStatus || previousStatus === nextStatus) {
    return;
  }
  const nextCounts = { ...state.filteredStatusCounts };
  if (Object.prototype.hasOwnProperty.call(nextCounts, previousStatus)) {
    nextCounts[previousStatus] = Math.max(0, (Number(nextCounts[previousStatus]) || 0) - 1);
  }
  if (Object.prototype.hasOwnProperty.call(nextCounts, nextStatus)) {
    nextCounts[nextStatus] = (Number(nextCounts[nextStatus]) || 0) + 1;
  }
  state.filteredStatusCounts = nextCounts;
}

function normalizeCategory(value) {
  if (value === 'laptop' || value === 'desktop' || value === 'server' || value === 'unknown') {
    return value;
  }
  return 'unknown';
}

function buildCategoryBadge(category, id, extraClass = '') {
  const normalized = normalizeCategory(category);
  const label = categoryLabels[normalized] || categoryLabels.unknown;
  const className = extraClass ? `badge ${extraClass}` : 'badge';
  if (!id || !state.canEditReports) {
    return `<span class="${className}" data-category="${normalized}">${escapeHtml(label)}</span>`;
  }
  return `<button class="${className}" data-category="${normalized}" data-action="cycle-category" data-id="${escapeHtml(
    String(id)
  )}" type="button">${escapeHtml(label)}</button>`;
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

function formatMachineSerialPreview(machine) {
  if (!machine || typeof machine !== 'object' || !machine.serialNumber) {
    return '';
  }
  const serial = String(machine.serialNumber).trim();
  if (!serial) {
    return '';
  }
  const primary = formatPrimary(machine);
  if (primary && String(primary).trim() === serial) {
    return '';
  }
  return `Serial ${serial}`;
}

function buildMachineIdentityLabel(machine, { includeSerial = true, fallback = '--' } = {}) {
  if (!machine || typeof machine !== 'object') {
    return fallback;
  }
  const vendor = machine.vendor ? String(machine.vendor).trim() : '';
  const model = machine.model ? String(machine.model).trim() : '';
  const serial = includeSerial && machine.serialNumber ? String(machine.serialNumber).trim() : '';
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

function formatMacSummary(machine) {
  const macs = Array.isArray(machine.macAddresses) ? machine.macAddresses.filter(Boolean) : [];
  const primary = machine.macAddress || macs[0] || null;
  const secondary = macs.find((mac) => mac && mac !== primary) || null;
  if (primary && secondary) {
    return `${primary} / ${secondary}`;
  }
  return primary || '--';
}

function normalizeDebugValueArray(value) {
  if (Array.isArray(value)) {
    return value
      .map((item) => (item == null ? '' : String(item).trim()))
      .filter(Boolean);
  }
  if (value == null) {
    return [];
  }
  const text = String(value).trim();
  if (!text) {
    return [];
  }
  return text
    .split(/\s*,\s*/)
    .map((item) => item.trim())
    .filter(Boolean);
}

function normalizeDebugRawArray(value) {
  if (Array.isArray(value)) {
    return value
      .map((item) => (item == null ? '' : String(item).trim()))
      .filter(Boolean);
  }
  if (value == null) {
    return [];
  }
  const text = String(value).trim();
  return text ? [text] : [];
}

function formatDebugValueList(values, separator = ' / ') {
  const list = normalizeDebugValueArray(values);
  return list.length ? list.join(separator) : '--';
}

function getRemoteAccessDebug(detail) {
  if (!detail || typeof detail !== 'object') {
    return null;
  }
  const payload =
    detail.payload && typeof detail.payload === 'object' && !Array.isArray(detail.payload)
      ? detail.payload
      : null;
  const debug =
    payload && payload.debug && typeof payload.debug === 'object' && !Array.isArray(payload.debug)
      ? payload.debug
      : null;
  const remoteAccess =
    debug &&
    debug.remoteAccess &&
    typeof debug.remoteAccess === 'object' &&
    !Array.isArray(debug.remoteAccess)
      ? debug.remoteAccess
      : null;
  if (!remoteAccess) {
    return null;
  }

  const adaptersRaw = Array.isArray(remoteAccess.adapters) ? remoteAccess.adapters : [];
  const adapters = adaptersRaw
    .map((adapter) => {
      if (!adapter || typeof adapter !== 'object' || Array.isArray(adapter)) {
        return null;
      }
      return {
        description: adapter.description ? String(adapter.description).trim() : '',
        mac: adapter.mac ? String(adapter.mac).trim() : '',
        dhcp: adapter.dhcp == null ? '' : String(adapter.dhcp).trim(),
        ipv4: normalizeDebugValueArray(adapter.ipv4),
        ipv6: normalizeDebugValueArray(adapter.ipv6),
        gateway: normalizeDebugValueArray(adapter.gateway),
        dns: normalizeDebugValueArray(adapter.dns)
      };
    })
    .filter(Boolean);

  const winrmRaw =
    remoteAccess.winrm &&
    typeof remoteAccess.winrm === 'object' &&
    !Array.isArray(remoteAccess.winrm)
      ? remoteAccess.winrm
      : null;

  return {
    hostnames: normalizeDebugValueArray(remoteAccess.hostnames),
    ipv4: normalizeDebugValueArray(remoteAccess.ipv4),
    ipv6: normalizeDebugValueArray(remoteAccess.ipv6),
    adapters,
    collectedAt:
      typeof remoteAccess.collectedAt === 'string' && remoteAccess.collectedAt.trim()
        ? remoteAccess.collectedAt.trim()
        : null,
    scriptVersion:
      typeof remoteAccess.scriptVersion === 'string' && remoteAccess.scriptVersion.trim()
        ? remoteAccess.scriptVersion.trim()
        : null,
    winrm: winrmRaw
      ? {
          bootstrapStatus: winrmRaw.bootstrapStatus ? String(winrmRaw.bootstrapStatus).trim() : '',
          bootstrapReason: winrmRaw.bootstrapReason ? String(winrmRaw.bootstrapReason).trim() : '',
          serviceStatus: winrmRaw.serviceStatus ? String(winrmRaw.serviceStatus).trim() : '',
          startMode: winrmRaw.startMode ? String(winrmRaw.startMode).trim() : '',
          localAccountTokenFilterPolicy:
            winrmRaw.localAccountTokenFilterPolicy == null
              ? ''
              : String(winrmRaw.localAccountTokenFilterPolicy).trim(),
          listeners: normalizeDebugRawArray(winrmRaw.listeners),
          testWsManStatus:
            winrmRaw.testWsMan && winrmRaw.testWsMan.status
              ? String(winrmRaw.testWsMan.status).trim()
              : '',
          testWsManVendor:
            winrmRaw.testWsMan && winrmRaw.testWsMan.vendor
              ? String(winrmRaw.testWsMan.vendor).trim()
              : '',
          testWsManVersion:
            winrmRaw.testWsMan && winrmRaw.testWsMan.version
              ? String(winrmRaw.testWsMan.version).trim()
              : ''
        }
      : null
  };
}

function formatRemoteAccessAdapters(remoteAccess) {
  if (!remoteAccess || !Array.isArray(remoteAccess.adapters) || !remoteAccess.adapters.length) {
    return '--';
  }
  const chunks = remoteAccess.adapters
    .map((adapter) => {
      const parts = [];
      if (adapter.description) {
        parts.push(adapter.description);
      }
      const ipv4 = formatDebugValueList(adapter.ipv4);
      if (ipv4 !== '--') {
        parts.push(`IPv4 ${ipv4}`);
      }
      const ipv6 = formatDebugValueList(adapter.ipv6);
      if (ipv6 !== '--') {
        parts.push(`IPv6 ${ipv6}`);
      }
      if (adapter.mac) {
        parts.push(`MAC ${adapter.mac}`);
      }
      return parts.join(' · ');
    })
    .filter(Boolean);
  return chunks.length ? chunks.join(' | ') : '--';
}

function formatRemoteAccessWinRm(remoteAccess) {
  const winrm = remoteAccess && remoteAccess.winrm ? remoteAccess.winrm : null;
  if (!winrm) {
    return '--';
  }
  const parts = [];
  if (winrm.bootstrapStatus) {
    parts.push(`bootstrap ${winrm.bootstrapStatus}`);
  }
  if (winrm.serviceStatus) {
    parts.push(`service ${winrm.serviceStatus}`);
  }
  if (winrm.startMode) {
    parts.push(`start ${winrm.startMode}`);
  }
  if (winrm.localAccountTokenFilterPolicy) {
    parts.push(`LATFP ${winrm.localAccountTokenFilterPolicy}`);
  }
  if (winrm.testWsManStatus) {
    parts.push(`WSMan ${winrm.testWsManStatus}`);
  }
  return parts.length ? parts.join(' · ') : '--';
}

function formatRemoteAccessWinRmListeners(remoteAccess) {
  const winrm = remoteAccess && remoteAccess.winrm ? remoteAccess.winrm : null;
  if (!winrm || !Array.isArray(winrm.listeners) || !winrm.listeners.length) {
    return '--';
  }
  return winrm.listeners.join(' | ');
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

function getAutopilotHash(detail) {
  if (!detail || typeof detail !== 'object') {
    return null;
  }
  const payload =
    detail.payload && typeof detail.payload === 'object' && !Array.isArray(detail.payload)
      ? detail.payload
      : null;
  const autopilot =
    payload &&
    payload.autopilot &&
    typeof payload.autopilot === 'object' &&
    !Array.isArray(payload.autopilot)
      ? payload.autopilot
      : null;
  const device =
    payload && payload.device && typeof payload.device === 'object' && !Array.isArray(payload.device)
      ? payload.device
      : null;
  const inventory =
    payload &&
    payload.inventory &&
    typeof payload.inventory === 'object' &&
    !Array.isArray(payload.inventory)
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
    detail.autopilotHash,
    payload && payload.autopilotHash,
    payload && payload.deviceHardwareData,
    payload && payload.device_hardware_data,
    payload && payload.hardwareHash
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

function formatAutopilotHashPreview(value, maxLength = 96) {
  const normalized = normalizeAutopilotHashValue(value);
  if (!normalized) {
    return '--';
  }
  if (normalized.length <= maxLength) {
    return normalized;
  }
  return `${normalized.slice(0, maxLength)}... (${normalized.length})`;
}

function copyTextToClipboard(text) {
  if (text == null) {
    return Promise.reject(new Error('empty_text'));
  }
  const value = String(text);
  if (!value.trim()) {
    return Promise.reject(new Error('empty_text'));
  }
  if (navigator.clipboard && typeof navigator.clipboard.writeText === 'function') {
    return navigator.clipboard.writeText(value);
  }
  return new Promise((resolve, reject) => {
    try {
      const textarea = document.createElement('textarea');
      textarea.value = value;
      textarea.setAttribute('readonly', '');
      textarea.style.position = 'fixed';
      textarea.style.top = '0';
      textarea.style.left = '-9999px';
      textarea.style.opacity = '0';
      document.body.appendChild(textarea);
      textarea.select();
      textarea.setSelectionRange(0, textarea.value.length);
      const copied = document.execCommand('copy');
      document.body.removeChild(textarea);
      if (!copied) {
        reject(new Error('copy_failed'));
        return;
      }
      resolve();
    } catch (error) {
      reject(error);
    }
  });
}

function setCopyButtonState(button, label, stateClass = '') {
  if (!button) {
    return;
  }
  if (!button.dataset.defaultLabel) {
    button.dataset.defaultLabel = button.textContent ? button.textContent.trim() : 'Copier';
  }
  button.textContent = label;
  button.classList.remove('is-success', 'is-error');
  if (stateClass) {
    button.classList.add(stateClass);
  }
}

function resetCopyButtonState(button) {
  if (!button) {
    return;
  }
  if (button._copyStateTimer) {
    window.clearTimeout(button._copyStateTimer);
  }
  setCopyButtonState(button, button.dataset.defaultLabel || 'Copier');
}

function copyAutopilotHashForReport(id, button) {
  const targetId = id != null ? String(id) : '';
  if (!targetId || !button) {
    return;
  }

  const detail = state.details[targetId] || getMachineById(targetId);
  const hash = getAutopilotHash(detail);
  if (!hash) {
    setCopyButtonState(button, 'Indispo', 'is-error');
    button._copyStateTimer = window.setTimeout(() => {
      resetCopyButtonState(button);
    }, 1500);
    return;
  }

  if (button._copyStateTimer) {
    window.clearTimeout(button._copyStateTimer);
  }
  button.disabled = true;
  setCopyButtonState(button, 'Copie...');

  copyTextToClipboard(hash)
    .then(() => {
      button.disabled = false;
      setCopyButtonState(button, 'Copie', 'is-success');
      button._copyStateTimer = window.setTimeout(() => {
        resetCopyButtonState(button);
      }, 1500);
    })
    .catch((error) => {
      console.error('Unable to copy autopilot hash', error);
      button.disabled = false;
      setCopyButtonState(button, 'Erreur', 'is-error');
      button._copyStateTimer = window.setTimeout(() => {
        resetCopyButtonState(button);
      }, 1500);
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

function parseBatteryHealthValue(value) {
  if (typeof value === 'number' && Number.isFinite(value)) {
    return value;
  }
  const parsed = Number.parseFloat(String(value || '').replace('%', '').trim());
  return Number.isFinite(parsed) ? parsed : null;
}

function parseNumericMetric(value) {
  if (typeof value === 'number' && Number.isFinite(value)) {
    return value;
  }
  const parsed = Number.parseFloat(String(value ?? '').replace(',', '.').trim());
  return Number.isFinite(parsed) ? parsed : null;
}

function formatBatteryCharge(value) {
  return formatBatteryHealth(value);
}

function formatWhCompact(value) {
  const numeric = parseNumericMetric(value);
  if (numeric == null) {
    return '--';
  }
  const rounded = Math.abs(numeric - Math.round(numeric)) < 0.05 ? Math.round(numeric) : numeric;
  return Number.isInteger(rounded) ? String(rounded) : rounded.toFixed(1).replace(/\.0$/, '');
}

function formatBatteryCapacitySummary(telemetry) {
  const fullWh = telemetry ? parseNumericMetric(telemetry.fullWh) : null;
  const designWh = telemetry ? parseNumericMetric(telemetry.designWh) : null;
  if (fullWh != null && designWh != null) {
    return `${formatWhCompact(fullWh)} / ${formatWhCompact(designWh)} Wh`;
  }
  if (fullWh != null) {
    return `${formatWhCompact(fullWh)} Wh`;
  }
  if (designWh != null) {
    return `Design ${formatWhCompact(designWh)} Wh`;
  }
  return '--';
}

function formatBatteryPowerSource(value) {
  const normalized = String(value || '').trim().toLowerCase();
  if (!normalized) {
    return '--';
  }
  if (['ac', 'mains', 'line', 'plugged', 'charging'].includes(normalized)) {
    return 'Secteur';
  }
  if (['battery', 'dc', 'discharging', 'unplugged'].includes(normalized)) {
    return 'Batterie';
  }
  return normalized.charAt(0).toUpperCase() + normalized.slice(1);
}

function getServerTelemetry(detail) {
  const payload =
    detail && detail.payload && typeof detail.payload === 'object' && !Array.isArray(detail.payload)
      ? detail.payload
      : null;
  const server =
    payload && payload.server && typeof payload.server === 'object' && !Array.isArray(payload.server)
      ? payload.server
      : null;
  const network =
    payload && payload.network && typeof payload.network === 'object' && !Array.isArray(payload.network)
      ? payload.network
      : null;
  const thermal =
    payload && payload.thermal && typeof payload.thermal === 'object' && !Array.isArray(payload.thermal)
      ? payload.thermal
      : null;
  const raid =
    server && server.raid && typeof server.raid === 'object' && !Array.isArray(server.raid)
      ? server.raid
      : null;
  const selectedServices = Array.isArray(server && server.selectedServices)
    ? server.selectedServices.filter((item) => item && typeof item === 'object' && !Array.isArray(item))
    : [];
  const failedServices = Array.isArray(server && server.failedServices)
    ? server.failedServices
        .map((item) => (item == null ? '' : String(item).trim()))
        .filter(Boolean)
    : [];
  const failingSelectedServices = selectedServices.filter((item) => {
    const activeState = String(item.activeState || '')
      .trim()
      .toLowerCase();
    return activeState && activeState !== 'active';
  });

  return {
    server,
    network,
    thermal,
    raid,
    selectedServices,
    failedServices,
    failingSelectedServices
  };
}

function isServerDetail(detail) {
  return normalizeCategory(detail && detail.category) === 'server';
}

function hasComponentStatus(components, key) {
  return Boolean(
    components &&
      typeof components === 'object' &&
      !Array.isArray(components) &&
      Object.prototype.hasOwnProperty.call(components, key)
  );
}

function deriveServerServicesStatus(serverTelemetry) {
  if (!serverTelemetry || typeof serverTelemetry !== 'object') {
    return null;
  }
  if (serverTelemetry.failingSelectedServices.length || serverTelemetry.failedServices.length) {
    return 'nok';
  }
  if (serverTelemetry.selectedServices.length) {
    return 'ok';
  }
  return null;
}

function applyServerTelemetryToComponents(detail, components) {
  if (!isServerDetail(detail)) {
    return components;
  }
  const next =
    components && typeof components === 'object' && !Array.isArray(components)
      ? { ...components }
      : {};
  const serverTelemetry = getServerTelemetry(detail);
  if (!hasComponentStatus(next, 'serverRaid') && serverTelemetry.raid && serverTelemetry.raid.status) {
    next.serverRaid = serverTelemetry.raid.status;
  }
  if (!hasComponentStatus(next, 'serverServices')) {
    const serverServicesStatus = deriveServerServicesStatus(serverTelemetry);
    if (serverServicesStatus) {
      next.serverServices = serverServicesStatus;
    }
  }
  if (!hasComponentStatus(next, 'thermal') && serverTelemetry.thermal && serverTelemetry.thermal.status) {
    next.thermal = serverTelemetry.thermal.status;
  }
  return next;
}

function getServerStatusEntries(detail, components, tests = null) {
  const entries = [];
  const serverTelemetry = getServerTelemetry(detail);
  const addEntry = (label, key, status, extra = null) => {
    if (status == null || status === '') {
      return;
    }
    entries.push({ label, key, status, extra, tab: 'diagnostics' });
  };

  const pingStatus = resolveUnifiedComponentStatus('networkPing', components, tests);
  if ((tests && tests.networkPing) || hasComponentStatus(components, 'networkPing')) {
    addEntry('Ping', 'networkPing', pingStatus, tests && tests.networkPingTarget ? tests.networkPingTarget : null);
  }

  const fsCheckStatus = resolveUnifiedComponentStatus('fsCheck', components, tests);
  if ((tests && tests.fsCheck) || hasComponentStatus(components, 'fsCheck')) {
    addEntry('Check disque', 'fsCheck', fsCheckStatus);
  }

  if (hasComponentStatus(components, 'diskSmart')) {
    addEntry('SMART disques', 'diskSmart', components.diskSmart || 'not_tested');
  }

  if (hasComponentStatus(components, 'serverRaid')) {
    addEntry('RAID', 'serverRaid', components.serverRaid || 'not_tested', formatServerRaidSummary(serverTelemetry.raid));
  }

  if (hasComponentStatus(components, 'serverServices')) {
    const servicesSummary = serverTelemetry.selectedServices.length
      ? formatServerSelectedServicesSummary(serverTelemetry.selectedServices)
      : serverTelemetry.failedServices.length
        ? formatServerFailedServicesSummary(serverTelemetry.failedServices)
        : null;
    addEntry('Services critiques', 'serverServices', components.serverServices || 'not_tested', servicesSummary);
  }

  if (hasComponentStatus(components, 'thermal')) {
    addEntry('Thermique', 'thermal', components.thermal || 'not_tested', formatServerThermalSummary(serverTelemetry.thermal));
  }

  return entries;
}

function formatDurationCompact(seconds) {
  const numeric = parseNumericMetric(seconds);
  if (numeric == null || numeric < 0) {
    return '--';
  }
  let remaining = Math.floor(numeric);
  const days = Math.floor(remaining / 86400);
  remaining -= days * 86400;
  const hours = Math.floor(remaining / 3600);
  remaining -= hours * 3600;
  const minutes = Math.floor(remaining / 60);
  const parts = [];
  if (days > 0) {
    parts.push(`${days} j`);
  }
  if (hours > 0) {
    parts.push(`${hours} h`);
  }
  if (minutes > 0 && parts.length < 2) {
    parts.push(`${minutes} min`);
  }
  if (!parts.length) {
    parts.push('< 1 min');
  }
  return parts.join(' ');
}

function formatServerLoadSummary(server) {
  if (!server || typeof server !== 'object') {
    return '--';
  }
  const values = [
    ['1m', parseNumericMetric(server.loadAverage1m)],
    ['5m', parseNumericMetric(server.loadAverage5m)],
    ['15m', parseNumericMetric(server.loadAverage15m)]
  ].filter(([, value]) => value != null);
  if (!values.length) {
    return '--';
  }
  return values
    .map(([label, value]) => `${label} ${value.toFixed(2).replace(/\.00$/, '')}`)
    .join(' / ');
}

function formatServerThermalSummary(thermal) {
  if (!thermal || typeof thermal !== 'object') {
    return '--';
  }
  const maxCelsius = parseNumericMetric(thermal.maxCelsius);
  if (maxCelsius == null) {
    return '--';
  }
  return `${maxCelsius.toFixed(1).replace(/\.0$/, '')} °C`;
}

function formatServerRaidSummary(raid) {
  if (!raid || typeof raid !== 'object') {
    return '--';
  }
  const mdstat = typeof raid.mdstat === 'string' ? raid.mdstat.trim() : '';
  if (!mdstat) {
    return '--';
  }
  const firstUsefulLine = mdstat
    .split('\n')
    .map((line) => line.trim())
    .find((line) => line && line !== 'Personalities :' && line !== 'unused devices: <none>');
  return firstUsefulLine || '--';
}

function formatServerSelectedServicesSummary(services) {
  if (!Array.isArray(services) || !services.length) {
    return '--';
  }
  return services
    .map((item) => {
      const name = item && item.name ? String(item.name).trim() : '--';
      const activeState = item && item.activeState ? String(item.activeState).trim() : 'unknown';
      const subState = item && item.subState ? String(item.subState).trim() : '';
      return subState ? `${name} (${activeState}/${subState})` : `${name} (${activeState})`;
    })
    .join(' • ');
}

function formatServerFailedServicesSummary(services) {
  if (!Array.isArray(services) || !services.length) {
    return '--';
  }
  return services.join(' • ');
}

function getBatteryTelemetry(source) {
  const payload = source && source.payload && typeof source.payload === 'object' ? source.payload : null;
  const device = payload && payload.device && typeof payload.device === 'object' ? payload.device : null;
  const batteryCapacity =
    device && device.batteryCapacity && typeof device.batteryCapacity === 'object'
      ? device.batteryCapacity
      : null;
  const chargePercent =
    source && source.batteryChargePercent != null
      ? source.batteryChargePercent
      : batteryCapacity && batteryCapacity.chargePercent != null
        ? batteryCapacity.chargePercent
        : null;
  const designWh =
    source && source.batteryDesignWh != null
      ? source.batteryDesignWh
      : batteryCapacity && batteryCapacity.designCapacityWh != null
        ? batteryCapacity.designCapacityWh
        : batteryCapacity && batteryCapacity.designWh != null
          ? batteryCapacity.designWh
          : null;
  const fullWh =
    source && source.batteryFullWh != null
      ? source.batteryFullWh
      : batteryCapacity && batteryCapacity.fullChargeCapacityWh != null
        ? batteryCapacity.fullChargeCapacityWh
        : batteryCapacity && batteryCapacity.fullWh != null
          ? batteryCapacity.fullWh
          : null;
  const remainingWh =
    source && source.batteryRemainingWh != null
      ? source.batteryRemainingWh
      : batteryCapacity && batteryCapacity.remainingCapacityWh != null
        ? batteryCapacity.remainingCapacityWh
        : batteryCapacity && batteryCapacity.remainingWh != null
          ? batteryCapacity.remainingWh
          : null;
  return {
    healthPercent: parseBatteryHealthValue(source && source.batteryHealth),
    chargePercent: parseBatteryHealthValue(chargePercent),
    designWh: parseNumericMetric(designWh),
    fullWh: parseNumericMetric(fullWh),
    remainingWh: parseNumericMetric(remainingWh),
    powerSource:
      (source && source.batteryPowerSource) || (batteryCapacity && batteryCapacity.powerSource) || null
  };
}

function formatReportDiagType(value) {
  const normalized = String(value || '').trim().toLowerCase();
  if (!normalized) {
    return '--';
  }
  if (normalized === 'double_check') {
    return 'Double check';
  }
  if (normalized === 'quick') {
    return 'Diagnostic';
  }
  return normalized.replace(/[_-]+/g, ' ');
}

function normalizeClockAlert(alert) {
  if (!alert || typeof alert !== 'object' || Array.isArray(alert)) {
    return null;
  }
  const reasonsSource = Array.isArray(alert.reasons)
    ? alert.reasons
    : typeof alert.reasons === 'string'
      ? alert.reasons.split(',')
      : [];
  const normalizedReasons = ['clock_backwards', 'clock_drift', 'delta_mismatch'].filter((reason) =>
    reasonsSource.map((item) => String(item || '').trim().toLowerCase()).includes(reason)
  );
  const driftSeconds = Number.parseInt(String(alert.driftSeconds ?? ''), 10);
  const deltaSeconds = Number.parseInt(String(alert.deltaSeconds ?? ''), 10);
  const normalized = {
    active: alert.active === true || normalizedReasons.length > 0,
    reasons: normalizedReasons,
    clientGeneratedAt: alert.clientGeneratedAt || null,
    firstClientGeneratedAt: alert.firstClientGeneratedAt || null,
    serverSeenAt: alert.serverSeenAt || null,
    firstServerSeenAt: alert.firstServerSeenAt || null,
    driftSeconds: Number.isFinite(driftSeconds) ? driftSeconds : null,
    deltaSeconds: Number.isFinite(deltaSeconds) ? deltaSeconds : null
  };
  if (
    !normalized.active &&
    !normalized.clientGeneratedAt &&
    !normalized.firstClientGeneratedAt &&
    normalized.driftSeconds == null &&
    normalized.deltaSeconds == null
  ) {
    return null;
  }
  return normalized;
}

function formatDurationSeconds(value) {
  const seconds = Number.parseInt(String(value ?? ''), 10);
  if (!Number.isFinite(seconds) || seconds < 0) {
    return '--';
  }
  if (seconds < 60) {
    return `${seconds} s`;
  }
  const hours = Math.floor(seconds / 3600);
  const minutes = Math.floor((seconds % 3600) / 60);
  const remainingSeconds = seconds % 60;
  if (hours > 0) {
    return minutes > 0 ? `${hours} h ${minutes} min` : `${hours} h`;
  }
  if (minutes > 0) {
    return remainingSeconds > 0 ? `${minutes} min ${remainingSeconds} s` : `${minutes} min`;
  }
  return `${seconds} s`;
}

function formatClockAlertSummary(alert) {
  const normalized = normalizeClockAlert(alert);
  if (!normalized) {
    return 'Aucune derive RTC detectee.';
  }
  const parts = [];
  if (normalized.reasons.includes('clock_backwards')) {
    parts.push('Horloge poste revenue en arriere');
  }
  if (normalized.reasons.includes('clock_drift')) {
    parts.push(
      normalized.driftSeconds != null
        ? `Ecart horloge ${formatDurationSeconds(normalized.driftSeconds)}`
        : 'Horloge poste decalee'
    );
  }
  if (normalized.reasons.includes('delta_mismatch')) {
    parts.push(
      normalized.deltaSeconds != null
        ? `Delta passages ${formatDurationSeconds(normalized.deltaSeconds)}`
        : 'Delta passages incoherent'
    );
  }
  if (parts.length) {
    return parts.join(' · ');
  }
  return normalized.active ? 'Controle pile BIOS recommande.' : 'Aucune derive RTC detectee.';
}

function buildClockAlertFields(alert, mode = 'detail') {
  const normalized = normalizeClockAlert(alert);
  if (!normalized) {
    return '';
  }
  const wrapperClass = mode === 'drawer' ? 'drawer-alert-cell' : 'detail-item';
  const wideClass = mode === 'drawer' ? ' drawer-alert-cell--wide' : ' detail-item--wide';
  const alertClass = normalized.active ? ' is-alert' : '';
  const statusLabel = normalized.active ? 'Controle requis' : 'RAS';
  const rows = [
    ['Alerte pile BIOS', statusLabel, false],
    ['Heure poste', formatDateTime(normalized.clientGeneratedAt), false],
    ['Heure serveur', formatDateTime(normalized.serverSeenAt), false],
    ['Ecart horloge', formatDurationSeconds(normalized.driftSeconds), false],
    ['1er passage poste', formatDateTime(normalized.firstClientGeneratedAt), false],
    ['1er passage serveur', formatDateTime(normalized.firstServerSeenAt), false],
    ['Delta passages', formatDurationSeconds(normalized.deltaSeconds), false],
    ['Diagnostic RTC', formatClockAlertSummary(normalized), true]
  ];
  return rows
    .map(([label, value, wide]) => {
      const classes = [wrapperClass];
      if (wide) {
        classes.push(wideClass.trim());
      }
      if (label === 'Alerte pile BIOS' && normalized.active) {
        classes.push('is-alert');
      }
      return `
        <div class="${classes.join(' ')}">
          <span>${escapeHtml(label)}</span>
          <strong>${escapeHtml(value || '--')}</strong>
        </div>
      `;
    })
    .join('');
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
  if (options && options.id && options.key && state.canEditReports) {
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
  let previousMachine = null;
  let nextMachine = null;
  state.machines = state.machines.map((machine) => {
    if (machine.id !== id) {
      return machine;
    }
    previousMachine = machine;
    const components = machine.components && typeof machine.components === 'object'
      ? { ...machine.components }
      : {};
    components.pad = status;
    nextMachine = {
      ...machine,
      padStatus: status,
      components
    };
    return nextMachine;
  });
  adjustFilteredStatusCounts(previousMachine, nextMachine);

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
  let previousMachine = null;
  let nextMachine = null;
  state.machines = state.machines.map((machine) => {
    if (machine.id !== id) {
      return machine;
    }
    previousMachine = machine;
    const components = machine.components && typeof machine.components === 'object'
      ? { ...machine.components }
      : {};
    components.usb = status;
    nextMachine = {
      ...machine,
      usbStatus: status,
      components
    };
    return nextMachine;
  });
  adjustFilteredStatusCounts(previousMachine, nextMachine);

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
  if (!state.canEditReports) {
    window.alert("Pas les droits pour modifier ce statut.");
    return;
  }
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
    refreshActiveDrawerIfNeeded(id);
    void loadStats();
  } catch (error) {
    window.alert("Impossible d'enregistrer le pavé tactile.");
  } finally {
    setPadButtonsLoading(id, false);
  }
}

async function updateUsbStatus(id, status) {
  if (!state.canEditReports) {
    window.alert("Pas les droits pour modifier ce statut.");
    return;
  }
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
    refreshActiveDrawerIfNeeded(id);
    void loadStats();
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

function applyLotMetaUpdate(lots, activeLotId) {
  if (Array.isArray(lots)) {
    state.lots = lots;
  }
  state.activeLotId = activeLotId ? normalizeLotId(activeLotId) : null;
  renderLotMetrics();
  renderReportZeroLotOptions();
}

function applyMachineLotUpdate(id, machineKey, lot) {
  const reportId = id != null ? String(id) : '';
  const normalizedMachineKey = machineKey ? String(machineKey) : '';
  const updatedLot = lot && typeof lot === 'object' ? lot : null;
  const matchesTarget = (item) => {
    if (!item || typeof item !== 'object') {
      return false;
    }
    if (normalizedMachineKey) {
      return String(item.machineKey || '') === normalizedMachineKey;
    }
    return String(item.id || '') === reportId;
  };

  state.machines = state.machines.map((machine) => {
    if (!matchesTarget(machine)) {
      return machine;
    }
    return {
      ...machine,
      lot: updatedLot
    };
  });

  Object.keys(state.details).forEach((detailId) => {
    const detail = state.details[detailId];
    if (!detail || detail.error || !matchesTarget(detail)) {
      return;
    }
    state.details[detailId] = {
      ...detail,
      lot: updatedLot
    };
  });

  invalidateListCache();
}

async function updateCategory(id, category, button) {
  if (!state.canEditReports) {
    window.alert("Pas les droits pour modifier la categorie.");
    return;
  }
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
    refreshActiveDrawerIfNeeded(id);
  } catch (error) {
    window.alert("Impossible d'enregistrer la categorie.");
  } finally {
    if (button) {
      button.disabled = false;
      button.classList.remove('is-loading');
    }
  }
}

async function updateMachineLot(id, lotId, button) {
  if (!state.canManageLots) {
    window.alert("Pas les droits pour modifier le lot.");
    return;
  }
  if (button) {
    button.disabled = true;
    button.classList.add('is-loading');
  }
  const safeId = window.CSS && CSS.escape ? CSS.escape(String(id)) : String(id).replace(/"/g, '\\"');
  const select = detailsDrawerShell
    ? detailsDrawerShell.querySelector(`[data-lot-select-for="${safeId}"]`)
    : null;
  if (select) {
    select.disabled = true;
  }
  try {
    const response = await fetch(`/api/machines/${encodeURIComponent(id)}/lot`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ lotId: lotId || null })
    });
    if (response.status === 401) {
      window.location.href = '/login';
      return;
    }
    if (response.status === 403) {
      window.alert("Pas les droits pour modifier le lot.");
      return;
    }
    if (!response.ok) {
      throw new Error('lot_update_failed');
    }
    const data = await response.json();
    if (!data.ok) {
      throw new Error('lot_update_failed');
    }
    applyLotMetaUpdate(Array.isArray(data.lots) ? data.lots : state.lots, data.activeLotId || null);
    applyMachineLotUpdate(id, data.machineKey || null, data.lot || null);
    renderList();
    refreshActiveDrawerIfNeeded(id);
  } catch (error) {
    window.alert("Impossible d'enregistrer le lot.");
  } finally {
    if (select) {
      select.disabled = false;
    }
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

  let previousMachine = null;
  let nextMachine = null;
  state.machines = state.machines.map((machine) => {
    if (machine.id !== id) {
      return machine;
    }
    previousMachine = machine;
    const components = machine.components && typeof machine.components === 'object'
      ? { ...machine.components }
      : {};
    components[key] = status;
    nextMachine = {
      ...machine,
      ...(statusField ? { [statusField]: status } : {}),
      components
    };
    return nextMachine;
  });
  adjustFilteredStatusCounts(previousMachine, nextMachine);

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
  if (!state.canEditReports) {
    window.alert("Pas les droits pour modifier ce statut.");
    return;
  }
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
    refreshActiveDrawerIfNeeded(id);
    void loadStats();
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

function syncModalOpenState() {
  const hasOpenModal = [patchnoteModal, suggestionModal, reportZeroModal].some(
    (modal) => modal && !modal.hidden
  );
  document.body.classList.toggle('modal-open', hasOpenModal);
}

function openPatchnoteModal(patchnote) {
  if (!patchnoteModal) {
    return;
  }
  activePatchnoteId = patchnote && patchnote.id ? patchnote.id : null;
  setPatchnoteBody(patchnote && patchnote.body ? patchnote.body : '');
  patchnoteModal.hidden = false;
  syncModalOpenState();
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
  syncModalOpenState();
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
  if (!state.canCreateReportZero) {
    window.alert("Pas les droits pour creer un rapport.");
    return;
  }
  reportZeroModal.hidden = false;
  syncModalOpenState();
  reportZeroForm.reset();
  renderReportZeroLotOptions();
  syncReportZeroTechnicianField();
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
  syncModalOpenState();
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
  syncModalOpenState();
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
  syncModalOpenState();
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
  const technician = isTechnicianFilterLocked()
    ? getOperatorScopePrimaryLabel()
    : String(formData.get('technician') || '').trim();
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
  if (!state.canCreateReportZero) {
    showReportZeroError("Pas les droits pour creer un report 0.");
    return;
  }
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
  const buttons = document.querySelectorAll(`[data-action="delete-report"][data-id="${id}"]`);
  buttons.forEach((button) => {
    button.disabled = loading;
    button.classList.toggle('is-loading', loading);
  });
}

function setPurgeImportsLoading(loading) {
  if (!purgeImportsBtn) {
    return;
  }
  purgeImportsBtn.disabled = loading;
  purgeImportsBtn.classList.toggle('is-loading', loading);
}

async function deleteLegacyImports() {
  if (!isLegacyView) {
    return;
  }
  if (!state.canDeleteReport) {
    window.alert("Pas les droits pour supprimer les imports.");
    return;
  }

  const confirmed = window.confirm(
    'Supprimer tous les rapports importes (CSV) ? Cette action est irreversible.'
  );
  if (!confirmed) {
    return;
  }

  const token = window.prompt('Tape SUPPRIMER IMPORTS pour confirmer la purge');
  if (token !== 'SUPPRIMER IMPORTS') {
    window.alert('Suppression annulee.');
    return;
  }

  setPurgeImportsLoading(true);
  try {
    const response = await fetch('/api/reports/imports/legacy', {
      method: 'DELETE'
    });
    if (response.status === 401) {
      window.location.href = '/login';
      return;
    }
    if (response.status === 403) {
      window.alert("Pas les droits pour supprimer les imports.");
      return;
    }
    if (!response.ok) {
      throw new Error('legacy_delete_failed');
    }
    const data = await response.json();
    if (!data.ok) {
      throw new Error('legacy_delete_failed');
    }

    closeDetailsDrawer();
    state.details = {};
    state.expandedId = null;
    state.quickCommentId = null;
    await loadMachines();
    window.alert(
      `Suppression terminee : ${data.deletedReports || 0} imports supprimes (${data.impactedMachines || 0} machines).`
    );
  } catch (error) {
    window.alert('Impossible de supprimer les imports.');
  } finally {
    setPurgeImportsLoading(false);
  }
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
      closeDetailsDrawer({ clearSelection: false });
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
  let previousMachine = null;
  let nextMachine = null;
  state.machines = state.machines.map((machine) => {
    if (machine.id !== id) {
      return machine;
    }
    previousMachine = machine;
    nextMachine = {
      ...machine,
      comment,
      commentedAt
    };
    return nextMachine;
  });
  adjustFilteredStatusCounts(previousMachine, nextMachine);

  if (state.details[id]) {
    state.details[id] = {
      ...state.details[id],
      comment,
      commentedAt
    };
  }
  invalidateListCache();
}

function applyBatteryHealthUpdate(id, batteryHealth) {
  const normalizedValue = parseBatteryHealthValue(batteryHealth);
  let previousMachine = null;
  let nextMachine = null;
  state.machines = state.machines.map((machine) => {
    if (machine.id !== id) {
      return machine;
    }
    previousMachine = machine;
    nextMachine = {
      ...machine,
      batteryHealth: normalizedValue
    };
    return nextMachine;
  });
  adjustFilteredStatusCounts(previousMachine, nextMachine);

  if (state.details[id]) {
    state.details[id] = {
      ...state.details[id],
      batteryHealth: normalizedValue
    };
  }
  invalidateListCache();
}

function applyTechnicianUpdate(id, technician) {
  const normalizedValue = normalizeTech(technician);
  state.machines = state.machines.map((machine) => {
    if (machine.id !== id) {
      return machine;
    }
    return {
      ...machine,
      technician: normalizedValue || null
    };
  });

  if (state.details[id]) {
    state.details[id] = {
      ...state.details[id],
      technician: normalizedValue || null,
      relatedReports: Array.isArray(state.details[id].relatedReports)
        ? state.details[id].relatedReports.map((report) => ({
            ...report,
            technician: normalizedValue || null
          }))
        : state.details[id].relatedReports
    };
  }

  if (normalizedValue) {
    const currentOptions = Array.isArray(state.techOptions) ? state.techOptions : [];
    const existingKeys = new Set(currentOptions.map((value) => techKey(value)).filter(Boolean));
    if (!existingKeys.has(techKey(normalizedValue))) {
      state.techOptions = [...currentOptions, normalizedValue].sort((a, b) => a.localeCompare(b, 'fr'));
      renderTechnicianOptions();
    }
  }
  invalidateListCache();
}

function setCommentButtonsLoading(id, loading) {
  const buttons = document.querySelectorAll(`[data-action="clear-comment"][data-id="${id}"]`);
  buttons.forEach((button) => {
    button.disabled = loading;
    button.classList.toggle('is-loading', loading);
  });
}

function setBatteryHealthButtonsLoading(id, loading) {
  const buttons = document.querySelectorAll(`[data-action="save-battery-health"][data-id="${id}"]`);
  buttons.forEach((button) => {
    button.disabled = loading;
    button.classList.toggle('is-loading', loading);
  });
  if (!detailsDrawerShell) {
    return;
  }
  const safeId = String(id).replace(/"/g, '\\"');
  const input = detailsDrawerShell.querySelector(`[data-battery-input-for="${safeId}"]`);
  if (input) {
    input.disabled = loading;
  }
}

function setTechnicianButtonsLoading(id, loading) {
  const buttons = document.querySelectorAll(`[data-action="save-technician"][data-id="${id}"]`);
  buttons.forEach((button) => {
    button.disabled = loading;
    button.classList.toggle('is-loading', loading);
  });
  if (!detailsDrawerShell) {
    return;
  }
  const safeId = String(id).replace(/"/g, '\\"');
  const input = detailsDrawerShell.querySelector(`[data-technician-input-for="${safeId}"]`);
  if (input) {
    input.disabled = loading;
  }
}

async function updateComment(id, comment) {
  if (!state.canEditReports) {
    window.alert("Pas les droits pour modifier le commentaire.");
    return;
  }
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
    if (isDrawerOpen() && String(state.expandedId || '') === String(id)) {
      renderDetailsDrawerContent(String(id));
    }
    void loadStats();
  } catch (error) {
    window.alert("Impossible d'enregistrer le commentaire.");
  } finally {
    setCommentButtonsLoading(id, false);
  }
}

async function updateBatteryHealth(id, rawValue) {
  if (!state.canEditBatteryHealth) {
    window.alert("Pas les droits pour modifier la batterie.");
    return;
  }

  const normalizedRaw = String(rawValue || '').trim();
  const batteryHealth = Number.parseInt(normalizedRaw, 10);
  if (!normalizedRaw || !Number.isFinite(batteryHealth) || batteryHealth < 0 || batteryHealth > 100) {
    window.alert('Le pourcentage batterie doit etre un entier entre 0 et 100.');
    return;
  }

  const currentMachine = state.details[id] && !state.details[id].error ? state.details[id] : getMachineById(id);
  const currentValue = parseBatteryHealthValue(currentMachine && currentMachine.batteryHealth);
  if (currentValue != null && currentValue === batteryHealth) {
    return;
  }

  setBatteryHealthButtonsLoading(id, true);
  try {
    const response = await fetch(`/api/machines/${encodeURIComponent(id)}/battery-health`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ batteryHealth })
    });
    if (response.status === 401) {
      window.location.href = '/login';
      return;
    }
    if (response.status === 403) {
      window.alert("Pas les droits pour modifier la batterie.");
      return;
    }
    if (!response.ok) {
      throw new Error('battery_health_update_failed');
    }
    const data = await response.json();
    if (!data.ok) {
      throw new Error('battery_health_update_failed');
    }
    applyBatteryHealthUpdate(id, data.batteryHealth);
    renderList();
    refreshActiveDrawerIfNeeded(id);
    void loadStats();
  } catch (error) {
    window.alert("Impossible d'enregistrer la batterie.");
  } finally {
    setBatteryHealthButtonsLoading(id, false);
  }
}

async function updateTechnician(id, rawValue) {
  if (!state.canEditTechnician) {
    window.alert("Pas les droits pour modifier le technicien.");
    return;
  }

  const technician = normalizeTech(rawValue);
  if (!technician) {
    window.alert('Le technicien doit etre renseigne.');
    return;
  }

  const currentMachine = state.details[id] && !state.details[id].error ? state.details[id] : getMachineById(id);
  const currentValue = normalizeTech(currentMachine && currentMachine.technician);
  if (currentValue && techKey(currentValue) === techKey(technician)) {
    return;
  }

  setTechnicianButtonsLoading(id, true);
  try {
    const response = await fetch(`/api/machines/${encodeURIComponent(id)}/technician`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ technician })
    });
    if (response.status === 401) {
      window.location.href = '/login';
      return;
    }
    if (response.status === 403) {
      window.alert("Pas les droits pour modifier le technicien.");
      return;
    }
    if (!response.ok) {
      throw new Error('technician_update_failed');
    }
    const data = await response.json();
    if (!data.ok) {
      throw new Error('technician_update_failed');
    }
    applyTechnicianUpdate(id, data.technician);
    renderList();
    refreshActiveDrawerIfNeeded(id);
  } catch (error) {
    window.alert("Impossible d'enregistrer le technicien.");
  } finally {
    setTechnicianButtonsLoading(id, false);
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
  if (!state.canRenameTags) {
    window.alert("Pas les droits pour renommer le tag.");
    return;
  }
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

function setTextContent(element, value) {
  if (!element) {
    return;
  }
  element.textContent = value;
}

function updateWorkspaceKpis(uniqueMachines = []) {
  let okCount = 0;
  let nokCount = 0;
  let ntCount = 0;
  const usesLocalScope = Boolean(state.quickFilter && state.quickFilter.value);

  if (!usesLocalScope && state.filteredStatusCounts) {
    okCount = Number.parseInt(String(state.filteredStatusCounts.ok || 0), 10) || 0;
    nokCount = Number.parseInt(String(state.filteredStatusCounts.nok || 0), 10) || 0;
    ntCount = Number.parseInt(String(state.filteredStatusCounts.nt || 0), 10) || 0;
  } else {
    uniqueMachines.forEach((machine) => {
      const primaryStatus = getMachinePrimaryStatus(machine);
      if (primaryStatus === 'nok') {
        nokCount += 1;
      } else if (primaryStatus === 'ok') {
        okCount += 1;
      } else {
        ntCount += 1;
      }
    });
  }

  const machineCount =
    usesLocalScope
      ? uniqueMachines.length
      : state.filteredStatusCounts
        ? Number.parseInt(String(state.filteredStatusCounts.total || 0), 10) || 0
        : Number.isFinite(state.totalCount)
        ? state.totalCount
        : uniqueMachines.length;
  setTextContent(kpiTotalEl, machineCount);
  setTextContent(kpiActiveEl, machineCount);
  setTextContent(kpiOkEl, okCount);
  setTextContent(kpiNokEl, nokCount);
  setTextContent(kpiNtEl, ntCount);
}

function updateStats() {
  const uniqueMachines = getUniqueMachines(getBaseFilteredMachines());
  updateWorkspaceKpis(uniqueMachines);

  if (state.stats) {
    setTextContent(statTotal, state.stats.total || 0);
    setTextContent(statLaptop, state.stats.laptop || 0);
    setTextContent(statDesktop, state.stats.desktop || 0);
    setTextContent(statServer, state.stats.server || 0);
    setTextContent(statUnknown, state.stats.unknown || 0);
  } else {
    const total = uniqueMachines.length;
    const laptop = uniqueMachines.filter((m) => normalizeCategory(m.category) === 'laptop').length;
    const desktop = uniqueMachines.filter((m) => normalizeCategory(m.category) === 'desktop').length;
    const server = uniqueMachines.filter((m) => normalizeCategory(m.category) === 'server').length;
    const unknown = uniqueMachines.filter((m) => normalizeCategory(m.category) === 'unknown').length;

    setTextContent(statTotal, total);
    setTextContent(statLaptop, laptop);
    setTextContent(statDesktop, desktop);
    setTextContent(statServer, server);
    setTextContent(statUnknown, unknown);
  }
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
  const nextFilter = categoryFilterOptions.has(filter) ? filter : 'all';
  state.filter = isServerView && nextFilter === 'server' ? 'all' : nextFilter || 'all';
  updateStatFilterCards();
  savePreferences();
  reloadReports();
}

function resetAllFilters() {
  if (getActiveFilterCount() <= 0) {
    return;
  }
  state.filter = 'all';
  state.boardView = 'workspace';
  state.techFilter = isTechnicianFilterLocked() ? getOperatorScopePrimaryKey() || 'all' : 'all';
  state.tagFilter = [];
  state.tagFilterNames = [];
  state.componentFilter = 'all';
  state.commentFilter = 'all';
  state.dateFilter = 'all';
  state.dateFrom = '';
  state.dateTo = '';
  state.timelineGranularity = 'day';
  state.timelineBuckets = [];
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
  renderBoardTabs();
  updateStatFilterCards();
  updateTechFilterButtons();
  updateTestFilterButtons();
  updateCommentFilterButtons();
  updateSummaryFilterButtons();
  updateCategoryFilterButtons();
  updateSignalFilterButtons();
  renderTagFilters();
  renderTechFilters();
  syncDateFilterControls();
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
        const summary = summarizeDetailForDrawer(machine);
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
  const sortMode = state.sort === 'lastSeen' ? 'activity' : state.sort;
  if (sortMode === 'status') {
    const order = { nok: 0, nt: 1, ok: 2 };
    sorted.sort((a, b) => {
      const statusA = getMachinePrimaryStatus(a);
      const statusB = getMachinePrimaryStatus(b);
      if (order[statusA] !== order[statusB]) {
        return order[statusA] - order[statusB];
      }
      return (b.lastSeen || '').localeCompare(a.lastSeen || '');
    });
    return sorted;
  }
  if (sortMode === 'technician') {
    sorted.sort((a, b) => {
      const techA = normalizeTech(a.technician || 'ZZZ');
      const techB = normalizeTech(b.technician || 'ZZZ');
      const techCompare = techA.localeCompare(techB, 'fr');
      if (techCompare !== 0) {
        return techCompare;
      }
      return (b.lastSeen || '').localeCompare(a.lastSeen || '');
    });
    return sorted;
  }
  if (sortMode === 'name') {
    sorted.sort((a, b) => formatPrimary(a).localeCompare(formatPrimary(b), 'fr'));
    return sorted;
  }

  if (sortMode === 'category') {
    const order = { laptop: 0, desktop: 1, server: 2, unknown: 3 };
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
  const detailId = detail && detail.id != null ? String(detail.id) : '';
  const components = resolveDetailComponents(detail);
  const serverTelemetry = getServerTelemetry(detail);
  const serverStatusEntries = isServerDetail(detail) ? getServerStatusEntries(detail, components, tests) : [];

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

  if (serverStatusEntries.length) {
    serverStatusEntries.forEach((entry) => {
      addRow(entry.label, entry.status, entry.extra, entry.key);
    });
  } else {
    if (tests) {
      const ramNote = tests.ramNote || formatWinSatNote(winSatMemScore);
      const cpuNote = tests.cpuNote || formatWinSatNote(winSatCpuScore);
      const gpuNote =
        tests.gpuNote || formatWinSatNote(winSatGraphicsScore != null ? winSatGraphicsScore : tests.gpuScore);
      addRow(
        'Lecture disque',
        resolveUnifiedComponentStatus('diskReadTest', components, tests),
        formatMbps(tests.diskReadMBps),
        'diskReadTest'
      );
      addRow(
        'Ecriture disque',
        resolveUnifiedComponentStatus('diskWriteTest', components, tests),
        formatMbps(tests.diskWriteMBps),
        'diskWriteTest'
      );
      addRow('RAM (WinSAT)', resolveUnifiedComponentStatus('ramTest', components, tests), ramNote || formatMbps(tests.ramMBps), 'ramTest');
      addRow('CPU (WinSAT)', resolveUnifiedComponentStatus('cpuTest', components, tests), cpuNote || formatMbps(tests.cpuMBps), 'cpuTest');
      const gpuStatusBase = resolveUnifiedComponentStatus('gpuTest', components, tests);
      const gpuStatus = gpuStatusBase !== 'not_tested' ? gpuStatusBase : (winSatGraphicsScore != null ? 'ok' : 'not_tested');
      const gpuExtra = gpuNote || (tests.gpuScore != null ? formatScore(tests.gpuScore) : null);
      addRow('GPU (WinSAT)', gpuStatus, gpuExtra, 'gpuTest');
      if (tests.cpuStress) {
        addRow('CPU (stress)', tests.cpuStress, null, 'cpuStress');
      }
      if (tests.gpuStress) {
        addRow('GPU (stress)', tests.gpuStress, null, 'gpuStress');
      }
      addRow('Ping', resolveUnifiedComponentStatus('networkPing', components, tests), tests.networkPingTarget || null, 'networkPing');
      if (tests.fsCheck) {
        addRow('Check disque', resolveUnifiedComponentStatus('fsCheck', components, tests), null, 'fsCheck');
      } else if (components.fsCheck) {
        addRow('Check disque', resolveUnifiedComponentStatus('fsCheck', components, tests), null, 'fsCheck');
      }
    } else {
      addRow('Lecture disque', resolveUnifiedComponentStatus('diskReadTest', components), null, 'diskReadTest');
      addRow('Ecriture disque', resolveUnifiedComponentStatus('diskWriteTest', components), null, 'diskWriteTest');
      addRow('RAM (WinSAT)', resolveUnifiedComponentStatus('ramTest', components), null, 'ramTest');
      addRow('CPU (WinSAT)', resolveUnifiedComponentStatus('cpuTest', components), null, 'cpuTest');
      addRow('GPU (WinSAT)', resolveUnifiedComponentStatus('gpuTest', components), null, 'gpuTest');
      addRow('Ping', resolveUnifiedComponentStatus('networkPing', components), null, 'networkPing');
      if (components.fsCheck) {
        addRow('Check disque', resolveUnifiedComponentStatus('fsCheck', components), null, 'fsCheck');
      }
    }
  }

  if (serverTelemetry.server) {
    const uptimeSummary = formatDurationCompact(serverTelemetry.server.uptimeSeconds);
    if (uptimeSummary !== '--') {
      addRow('Uptime', null, uptimeSummary, null);
    }
    const loadSummary = formatServerLoadSummary(serverTelemetry.server);
    if (loadSummary !== '--') {
      addRow('Charge systeme', null, loadSummary, null);
    }
  }
  if (serverTelemetry.thermal) {
    addRow(
      'Thermique',
      serverTelemetry.thermal.status || 'not_tested',
      formatServerThermalSummary(serverTelemetry.thermal),
      null
    );
  }
  if (serverTelemetry.raid) {
    addRow(
      'RAID',
      serverTelemetry.raid.status || 'not_tested',
      formatServerRaidSummary(serverTelemetry.raid),
      null
    );
  }
  if (serverTelemetry.selectedServices.length) {
    addRow(
      'Services critiques',
      serverTelemetry.failingSelectedServices.length ? 'nok' : 'ok',
      formatServerSelectedServicesSummary(serverTelemetry.selectedServices),
      null
    );
  }
  if (serverTelemetry.failedServices.length) {
    addRow(
      'Services en echec',
      'nok',
      formatServerFailedServicesSummary(serverTelemetry.failedServices),
      null
    );
  } else if (serverTelemetry.selectedServices.length) {
    addRow('Services en echec', 'ok', 'Aucun service en echec', null);
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
  const latestReport = reports[0] || null;
  const oldestReport = reports[reports.length - 1] || null;
  const latestBattery = latestReport ? getBatteryTelemetry(latestReport) : null;
  const oldestBattery = oldestReport ? getBatteryTelemetry(oldestReport) : null;
  const latestHealth = latestReport ? formatBatteryHealth(latestReport.batteryHealth ?? latestBattery?.healthPercent) : '--';
  const oldestHealth = oldestReport ? formatBatteryHealth(oldestReport.batteryHealth ?? oldestBattery?.healthPercent) : '--';
  const rangeStart = oldestReport ? oldestReport.lastSeen || oldestReport.diagCompletedAt || oldestReport.createdAt : null;
  const rangeEnd = latestReport ? latestReport.lastSeen || latestReport.diagCompletedAt || latestReport.createdAt : null;
  const overviewParts = [`${reports.length} passages traces`];
  if (rangeStart && rangeEnd) {
    overviewParts.push(`du ${formatDateTime(rangeStart)} au ${formatDateTime(rangeEnd)}`);
  }
  if (oldestHealth !== '--' && latestHealth !== '--') {
    overviewParts.push(`sante batterie ${oldestHealth} -> ${latestHealth}`);
  }
  const items = reports
    .map((report) => {
      const id = report && report.id ? String(report.id) : '';
      if (!id) {
        return '';
      }
      const when = report.lastSeen || report.diagCompletedAt || report.createdAt;
      const active = detail.id && String(detail.id) === id ? ' is-active' : '';
      const telemetry = getBatteryTelemetry(report);
      const batteryHealth = formatBatteryHealth(report.batteryHealth ?? telemetry.healthPercent);
      const batteryCharge = formatBatteryCharge(telemetry.chargePercent);
      const batteryCapacity = formatBatteryCapacitySummary(telemetry);
      const powerSource = formatBatteryPowerSource(telemetry.powerSource);
      const clockAlert = normalizeClockAlert(report.clockAlert);
      const metaParts = [
        report.technician ? `Tech ${report.technician}` : null,
        report.hostname ? report.hostname : null,
        batteryHealth !== '--' ? `Sante ${batteryHealth}` : null,
        batteryCharge !== '--' ? `Charge ${batteryCharge}` : null,
        batteryCapacity !== '--' ? `Capacite ${batteryCapacity}` : null,
        powerSource !== '--' ? powerSource : null
      ]
        .filter(Boolean)
        .map((part) => `<span class="report-history-chip">${escapeHtml(part)}</span>`)
        .join('');
      const footerParts = [
        `Rapport ${id.slice(0, 8)}`,
        report.appVersion ? `App ${report.appVersion}` : null,
        report.diagType ? formatReportDiagType(report.diagType) : null
      ]
        .filter(Boolean)
        .map((part) => `<span>${escapeHtml(part)}</span>`)
        .join('');
      const badges = [
        detail.id && String(detail.id) === id
          ? '<span class="report-history-badge is-current">Actuel</span>'
          : '<span class="report-history-badge">Archive</span>',
        clockAlert && clockAlert.active
          ? `<span class="report-history-badge is-alert">${escapeHtml(formatClockAlertSummary(clockAlert))}</span>`
          : ''
      ]
        .filter(Boolean)
        .join('');
      return `
        <button class="report-history-item${active}" type="button" data-action="open-report" data-id="${escapeHtml(
          id
        )}">
          <div class="report-history-top">
            <strong class="report-history-when">${escapeHtml(when ? formatDateTime(when) : '--')}</strong>
            <div class="report-history-badges">${badges}</div>
          </div>
          <div class="report-history-meta">${metaParts || '<span class="report-history-chip">Aucune metrique</span>'}</div>
          <div class="report-history-footer">${footerParts}</div>
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
      <p class="report-history-summary">${escapeHtml(overviewParts.join(' · '))}</p>
      <div class="report-history-list">
        ${items}
      </div>
    </div>
  `;
}

function resolveDetailComponents(detail) {
  const defaults = isServerDetail(detail)
    ? {}
    : {
        biosBattery: 'not_tested',
        biosLanguage: 'not_tested',
        biosPassword: 'not_tested',
        wifiStandard: 'not_tested'
      };
  const raw =
    detail && detail.components && typeof detail.components === 'object' && !Array.isArray(detail.components)
      ? detail.components
      : {};
  const merged = { ...defaults, ...raw };
  const topLevelFallbacks = {
    camera: detail && detail.cameraStatus,
    usb: detail && detail.usbStatus,
    keyboard: detail && detail.keyboardStatus,
    pad: detail && detail.padStatus,
    badgeReader: detail && detail.badgeReaderStatus
  };
  Object.entries(topLevelFallbacks).forEach(([key, value]) => {
    if (!Object.prototype.hasOwnProperty.call(merged, key) && value != null && value !== '') {
      merged[key] = value;
    }
  });
  if (!isServerDetail(detail)) {
    const clockAlert = normalizeClockAlert(detail && detail.clockAlert);
    if (clockAlert && clockAlert.active) {
      merged.biosBattery = 'nok';
    }
  }
  return applyServerTelemetryToComponents(detail, merged);
}

function resolveUnifiedComponentStatus(key, components, tests = null) {
  const source =
    components && typeof components === 'object' && !Array.isArray(components) ? components : {};
  if (key === 'diskReadTest') {
    return source.diskReadTest || (tests && tests.diskRead) || 'not_tested';
  }
  if (key === 'diskWriteTest') {
    return source.diskWriteTest || (tests && tests.diskWrite) || 'not_tested';
  }
  if (key === 'ramTest') {
    return source.ramTest || (tests && (tests.ramTest || tests.ram)) || 'not_tested';
  }
  if (key === 'cpuTest') {
    return source.cpuTest || (tests && (tests.cpuTest || tests.cpu)) || 'not_tested';
  }
  if (key === 'gpuTest') {
    return source.gpuTest || (tests && (tests.gpuTest || tests.gpu)) || 'not_tested';
  }
  if (key === 'networkPing') {
    return source.networkPing || (tests && tests.networkPing) || 'not_tested';
  }
  if (key === 'fsCheck') {
    return source.fsCheck || (tests && tests.fsCheck) || 'not_tested';
  }
  if (key === 'cpu') {
    return source.cpu || (tests && (tests.cpu || tests.cpuTest)) || source.cpuTest || 'not_tested';
  }
  if (key === 'gpu') {
    return source.gpu || (tests && (tests.gpu || tests.gpuTest)) || source.gpuTest || 'not_tested';
  }
  if (Object.prototype.hasOwnProperty.call(source, key)) {
    return source[key];
  }
  return 'not_tested';
}

function getDrawerMachineSequence() {
  const useQuickFilter = Boolean(state.quickFilter && state.quickFilter.value);
  const source = useQuickFilter ? applyFilters() : sortMachines(getBaseFilteredMachines());
  return getUniqueMachines(Array.isArray(source) ? source : [])
    .map((machine) => String(machine.id || ''))
    .filter(Boolean);
}

function getMachineById(id) {
  const targetId = id != null ? String(id) : '';
  if (!targetId) {
    return null;
  }
  return state.machines.find((machine) => String(machine.id) === targetId) || null;
}

function isDrawerOpen() {
  return Boolean(detailsDrawerShell && !detailsDrawerShell.hidden);
}

function updateDrawerNavButtons() {
  if (!drawerPrevBtn || !drawerNextBtn) {
    return;
  }
  const sequence = getDrawerMachineSequence();
  const currentId = state.expandedId ? String(state.expandedId) : '';
  const index = sequence.indexOf(currentId);
  const hasPrev = index > 0;
  const hasNext = index >= 0 && index < sequence.length - 1;
  drawerPrevBtn.disabled = !hasPrev;
  drawerNextBtn.disabled = !hasNext;
}

function setDrawerTab(nextTab) {
  if (!detailsDrawerBody) {
    return;
  }
  const safeTab = typeof nextTab === 'string' && nextTab ? nextTab : 'identifiants';
  state.drawerTab = safeTab;
  const tabButtons = detailsDrawerBody.querySelectorAll('.drawer-tab-btn[data-tab]');
  const tabPanels = detailsDrawerBody.querySelectorAll('.drawer-tab-panel[data-tab-panel]');
  tabButtons.forEach((button) => {
    const tab = button.dataset.tab || '';
    const active = tab === safeTab;
    button.classList.toggle('is-active', active);
    button.setAttribute('aria-selected', active ? 'true' : 'false');
  });
  tabPanels.forEach((panel) => {
    const tab = panel.dataset.tabPanel || '';
    const active = tab === safeTab;
    panel.classList.toggle('is-active', active);
    panel.hidden = !active;
  });
}

function closeDetailsDrawer({ clearSelection = true } = {}) {
  if (!detailsDrawerShell) {
    return;
  }
  detailsDrawerShell.classList.remove('is-open');
  document.body.classList.remove('drawer-open');
  window.setTimeout(() => {
    if (!detailsDrawerShell.classList.contains('is-open')) {
      detailsDrawerShell.hidden = true;
    }
  }, 170);
  if (clearSelection && state.expandedId) {
    state.expandedId = null;
    state.detailOverrideId = null;
    renderList();
  }
}

function buildDrawerDiagnosticsRows(detail) {
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
  const components = resolveDetailComponents(detail);
  const serverTelemetry = getServerTelemetry(detail);
  const serverStatusEntries = isServerDetail(detail) ? getServerStatusEntries(detail, components, tests) : [];
  const detailId = detail && detail.id != null ? String(detail.id) : '';
  const rows = [];

  function addRow(label, status, extra, key) {
    const statusValue =
      key && Object.prototype.hasOwnProperty.call(components, key) ? components[key] : status;
    const isNok = normalizeSummaryStatusForKey(key, statusValue) === 'nok';
    const statusHtml = key
      ? renderStatusValue(statusValue, { id: detailId, key })
      : renderStatusValue(statusValue);
    const metricHtml = extra ? `<span class="drawer-metric">${escapeHtml(extra)}</span>` : '';
    rows.push(`
      <div class="drawer-status-row${isNok ? ' is-nok' : ''}"${key ? ` data-detail-key="${escapeHtml(key)}"` : ''}>
        <span>${escapeHtml(label)}</span>
        <div class="drawer-status-stack">${statusHtml}${metricHtml}</div>
      </div>
    `);
  }

  if (serverStatusEntries.length) {
    serverStatusEntries.forEach((entry) => {
      addRow(entry.label, entry.status, entry.extra, entry.key);
    });
  } else {
    if (tests) {
      const cpuScore = winSpr && typeof winSpr.CpuScore === 'number' ? winSpr.CpuScore : null;
      const memScore = winSpr && typeof winSpr.MemoryScore === 'number' ? winSpr.MemoryScore : null;
      const gfxScore = winSpr
        ? typeof winSpr.GamingScore === 'number'
          ? winSpr.GamingScore
          : typeof winSpr.GraphicsScore === 'number'
            ? winSpr.GraphicsScore
            : null
        : null;

      addRow('Lecture disque', resolveUnifiedComponentStatus('diskReadTest', components, tests), formatMbps(tests.diskReadMBps), 'diskReadTest');
      addRow('Ecriture disque', resolveUnifiedComponentStatus('diskWriteTest', components, tests), formatMbps(tests.diskWriteMBps), 'diskWriteTest');
      addRow('RAM (WinSAT)', resolveUnifiedComponentStatus('ramTest', components, tests), tests.ramNote || formatWinSatNote(memScore), 'ramTest');
      addRow('CPU (WinSAT)', resolveUnifiedComponentStatus('cpuTest', components, tests), tests.cpuNote || formatWinSatNote(cpuScore), 'cpuTest');
      addRow('GPU (WinSAT)', resolveUnifiedComponentStatus('gpuTest', components, tests), tests.gpuNote || formatWinSatNote(gfxScore), 'gpuTest');
      addRow('Ping', resolveUnifiedComponentStatus('networkPing', components, tests), tests.networkPingTarget || null, 'networkPing');
      if (tests.fsCheck || components.fsCheck) {
        addRow('Check disque', resolveUnifiedComponentStatus('fsCheck', components, tests), null, 'fsCheck');
      }
    } else {
      addRow('Lecture disque', resolveUnifiedComponentStatus('diskReadTest', components), null, 'diskReadTest');
      addRow('Ecriture disque', resolveUnifiedComponentStatus('diskWriteTest', components), null, 'diskWriteTest');
      addRow('RAM (WinSAT)', resolveUnifiedComponentStatus('ramTest', components), null, 'ramTest');
      addRow('CPU (WinSAT)', resolveUnifiedComponentStatus('cpuTest', components), null, 'cpuTest');
      addRow('GPU (WinSAT)', resolveUnifiedComponentStatus('gpuTest', components), null, 'gpuTest');
      addRow('Ping', resolveUnifiedComponentStatus('networkPing', components), null, 'networkPing');
      if (components.fsCheck) {
        addRow('Check disque', resolveUnifiedComponentStatus('fsCheck', components), null, 'fsCheck');
      }
    }
  }

  if (serverTelemetry.server) {
    const uptimeSummary = formatDurationCompact(serverTelemetry.server.uptimeSeconds);
    if (uptimeSummary !== '--') {
      addRow('Uptime', null, uptimeSummary, null);
    }
    const loadSummary = formatServerLoadSummary(serverTelemetry.server);
    if (loadSummary !== '--') {
      addRow('Charge systeme', null, loadSummary, null);
    }
  }
  if (serverTelemetry.thermal) {
    addRow(
      'Thermique',
      serverTelemetry.thermal.status || 'not_tested',
      formatServerThermalSummary(serverTelemetry.thermal),
      null
    );
  }
  if (serverTelemetry.raid) {
    addRow(
      'RAID',
      serverTelemetry.raid.status || 'not_tested',
      formatServerRaidSummary(serverTelemetry.raid),
      null
    );
  }
  if (serverTelemetry.selectedServices.length) {
    addRow(
      'Services critiques',
      serverTelemetry.failingSelectedServices.length ? 'nok' : 'ok',
      formatServerSelectedServicesSummary(serverTelemetry.selectedServices),
      null
    );
  }
  if (serverTelemetry.failedServices.length) {
    addRow(
      'Services en echec',
      'nok',
      formatServerFailedServicesSummary(serverTelemetry.failedServices),
      null
    );
  } else if (serverTelemetry.selectedServices.length) {
    addRow('Services en echec', 'ok', 'Aucun service en echec', null);
  }

  return rows.join('');
}

function buildDrawerDetailHtml(detail) {
  const detailId = detail && detail.id != null ? String(detail.id) : '';
  const category = normalizeCategory(detail.category);
  const isServerCategory = category === 'server';
  const title = escapeHtml(formatPrimary(detail));
  const subtitle = escapeHtml(formatSubtitle(detail));
  const lot = getMachineLot(detail);
  const lotLabel = lot ? buildLotLabel(lot) : DEFAULT_LOT_LABEL;
  const pallet = getMachinePallet(detail);
  const rawPalletLabel = pallet ? buildPalletLabel(pallet) : '';
  const hasAssignedPallet = Boolean(rawPalletLabel && rawPalletLabel !== DEFAULT_PALLET_LABEL);
  const palletLabel = hasAssignedPallet ? rawPalletLabel : DEFAULT_PALLET_LABEL;
  const shipment = getMachineShipment(detail);
  const lotSelectOptions = buildLotAssignmentOptions(lot);
  const summary = summarizeDetailForDrawer(detail);
  const nokEntries = collectDetailNokEntries(detail);
  const primaryNokEntry = nokEntries.length ? nokEntries[0] : null;
  const components = resolveDetailComponents(detail);
  const clockAlert = normalizeClockAlert(detail.clockAlert);

  const payload =
    detail && detail.payload && typeof detail.payload === 'object' ? detail.payload : null;
  const tests =
    payload && payload.tests && typeof payload.tests === 'object' && !Array.isArray(payload.tests)
      ? payload.tests
      : null;
  const cpuInfo = payload && payload.cpu && typeof payload.cpu === 'object' ? payload.cpu : null;
  const gpuInfo = payload && payload.gpu && typeof payload.gpu === 'object' ? payload.gpu : null;
  const diskInfoRaw = payload ? payload.disks : null;
  const diskInfo = Array.isArray(diskInfoRaw) ? diskInfoRaw : diskInfoRaw ? [diskInfoRaw] : [];
  const volumeInfoRaw = payload ? payload.volumes : null;
  const volumeInfo = Array.isArray(volumeInfoRaw) ? volumeInfoRaw : volumeInfoRaw ? [volumeInfoRaw] : [];
  const batteryTelemetry = getBatteryTelemetry(detail);
  const batteryHealthValue = getBatteryHealthValue(detail);
  const isBatteryAlert = batteryHealthValue != null && batteryHealthValue < BATTERY_ALERT_THRESHOLD;
  const batteryHealthLabel = formatBatteryHealth(detail.batteryHealth ?? batteryTelemetry.healthPercent);
  const batteryChargeLabel = formatBatteryCharge(batteryTelemetry.chargePercent);
  const batteryCapacityLabel = formatBatteryCapacitySummary(batteryTelemetry);
  const batteryPowerSourceLabel = formatBatteryPowerSource(batteryTelemetry.powerSource);
  const remoteAccess = getRemoteAccessDebug(detail);
  const remoteHostnames = formatDebugValueList(remoteAccess && remoteAccess.hostnames);
  const remoteIpv4 = formatDebugValueList(remoteAccess && remoteAccess.ipv4);
  const remoteIpv6 = formatDebugValueList(remoteAccess && remoteAccess.ipv6);
  const remoteAdapters = formatRemoteAccessAdapters(remoteAccess);
  const remoteWinRm = formatRemoteAccessWinRm(remoteAccess);
  const remoteWinRmListeners = formatRemoteAccessWinRmListeners(remoteAccess);
  const remoteCollectedAt =
    remoteAccess && remoteAccess.collectedAt ? formatDateTime(remoteAccess.collectedAt) : '--';
  const serverTelemetry = getServerTelemetry(detail);
  const serverNetwork = serverTelemetry.network;
  const serverSummaryCards = [];
  if (serverTelemetry.server) {
    const uptimeSummary = formatDurationCompact(serverTelemetry.server.uptimeSeconds);
    if (uptimeSummary !== '--') {
      serverSummaryCards.push(
        `<div class="drawer-mini-card"><span>Uptime</span><strong>${escapeHtml(uptimeSummary)}</strong></div>`
      );
    }
    const loadSummary = formatServerLoadSummary(serverTelemetry.server);
    if (loadSummary !== '--') {
      serverSummaryCards.push(
        `<div class="drawer-mini-card"><span>Charge systeme</span><strong>${escapeHtml(loadSummary)}</strong></div>`
      );
    }
  }
  if (serverTelemetry.raid) {
    const raidStatus = String(serverTelemetry.raid.status || '')
      .trim()
      .toLowerCase();
    const raidLabel = raidStatus === 'nok' ? 'Alerte' : raidStatus === 'ok' ? 'OK' : '--';
    serverSummaryCards.push(
      `<div class="drawer-mini-card${
        raidStatus === 'nok' ? ' is-alert' : ''
      }"><span>RAID</span><strong>${escapeHtml(raidLabel)}</strong></div>`
    );
  }
  if (serverTelemetry.selectedServices.length) {
    serverSummaryCards.push(
      `<div class="drawer-mini-card${
        serverTelemetry.failingSelectedServices.length ? ' is-alert' : ''
      }"><span>Services critiques</span><strong>${escapeHtml(
        serverTelemetry.failingSelectedServices.length
          ? `${serverTelemetry.failingSelectedServices.length} en alerte`
          : 'OK'
      )}</strong></div>`
    );
  }
  if (serverTelemetry.thermal) {
    serverSummaryCards.push(
      `<div class="drawer-mini-card${
        String(serverTelemetry.thermal.status || '').trim().toLowerCase() === 'nok' ? ' is-alert' : ''
      }"><span>Temperature max</span><strong>${escapeHtml(
        formatServerThermalSummary(serverTelemetry.thermal)
      )}</strong></div>`
    );
  }

  if (detailsDrawerTitle) {
    detailsDrawerTitle.textContent = formatPrimary(detail);
  }
  if (detailsDrawerSub) {
    detailsDrawerSub.textContent = `${formatSubtitle(detail)} · ${timeAgo(detail.lastSeen)}`;
  }

  const deleteButton = state.canDeleteReport
    ? `<button class="drawer-action-btn is-danger" type="button" data-action="delete-report" data-id="${detailId}">Supprimer</button>`
    : '';

  const headerHtml = `
    <div class="drawer-card drawer-header-card">
      <div class="drawer-header-top">
        ${buildCategoryBadge(category, detailId, 'detail-category')}
        <span class="drawer-lot-pill">${escapeHtml(lotLabel)}</span>
        ${hasAssignedPallet ? `<span class="drawer-lot-pill drawer-pallet-pill">${escapeHtml(palletLabel)}</span>` : ''}
      </div>
      <h3>${title}</h3>
      <p>${subtitle}</p>
      <div class="drawer-header-meta">
        <span class="summary-chip ok">OK ${summary.ok}</span>
        ${
          primaryNokEntry
            ? `<button class="summary-chip nok drawer-summary-chip-action" type="button" data-action="focus-noks" data-tab="${escapeHtml(
                primaryNokEntry.tab
              )}" data-key="${escapeHtml(primaryNokEntry.key)}">NOK ${summary.nok}</button>`
            : `<span class="summary-chip nok">NOK ${summary.nok}</span>`
        }
        <span class="summary-chip nt">NT ${summary.other}</span>
        <span class="drawer-seen">${escapeHtml(formatDateTime(detail.lastSeen))}</span>
      </div>
      ${
        nokEntries.length
          ? `
            <div class="drawer-nok-summary">
              <span class="drawer-nok-summary-label">NOK detectes</span>
              <div class="drawer-nok-summary-list">
                ${nokEntries
                  .map(
                    (entry) => `
                      <button
                        class="drawer-nok-chip"
                        type="button"
                        data-action="focus-nok"
                        data-tab="${escapeHtml(entry.tab)}"
                        data-key="${escapeHtml(entry.key)}"
                      >
                        ${escapeHtml(entry.label)}
                      </button>
                    `
                  )
                  .join('')}
              </div>
            </div>
          `
          : ''
      }
      <div class="drawer-actions">
        <button class="drawer-action-btn" type="button" data-action="export-pdf" data-id="${detailId}">Telecharger PDF</button>
        ${deleteButton}
      </div>
    </div>
  `;

  const summaryCardsHtml = `
    <div class="drawer-summary-grid">
      <div class="drawer-mini-card"><span>CPU</span><strong>${escapeHtml((cpuInfo && cpuInfo.name) || '--')}</strong></div>
      <div class="drawer-mini-card"><span>RAM</span><strong>${escapeHtml(formatRam(detail.ramMb))}</strong></div>
      <div class="drawer-mini-card"><span>Stockage</span><strong>${escapeHtml(formatTotalStorage(diskInfo, volumeInfo))}</strong></div>
      <div class="drawer-mini-card"><span>GPU</span><strong>${escapeHtml((gpuInfo && gpuInfo.name) || '--')}</strong></div>
      ${
        isServerCategory
          ? ''
          : `
      <div class="drawer-mini-card${isBatteryAlert ? ' is-alert' : ''}"><span>Sante batterie</span><strong>${escapeHtml(
        batteryHealthLabel
      )}</strong></div>
      <div class="drawer-mini-card"><span>Charge batterie</span><strong>${escapeHtml(batteryChargeLabel)}</strong></div>
      <div class="drawer-mini-card"><span>Capacite utile</span><strong>${escapeHtml(batteryCapacityLabel)}</strong></div>
      <div class="drawer-mini-card"><span>Alimentation</span><strong>${escapeHtml(batteryPowerSourceLabel)}</strong></div>
      <div class="drawer-mini-card${clockAlert && clockAlert.active ? ' is-alert' : ''}"><span>Pile BIOS</span><strong>${escapeHtml(
        clockAlert && clockAlert.active ? 'Controle requis' : 'RAS'
      )}</strong></div>`
      }
      ${serverSummaryCards.join('')}
    </div>
  `;

  const autopilotHash = getAutopilotHash(detail);
  const autopilotHashTitle = autopilotHash || '--';
  const autopilotHashPreview = formatAutopilotHashPreview(autopilotHash);
  const canCopyAutopilotHash = Boolean(autopilotHash && detailId);
  const autopilotCopyButton = canCopyAutopilotHash
    ? `<button class="drawer-copy-btn" type="button" data-action="copy-autopilot-hash" data-id="${detailId}" title="Copier le hash complet">Copier</button>`
    : '';
  const serialValue = detail.serialNumber ? String(detail.serialNumber).trim() : '';
  const vendorValue = detail.vendor ? String(detail.vendor).trim() : '';
  const modelValue = detail.model ? String(detail.model).trim() : '';
  const barcodeLabel = buildMachineIdentityLabel(detail, { includeSerial: true, fallback: serialValue || '--' });
  const barcodeParams = [];
  if (vendorValue) {
    barcodeParams.push(`vendor=${encodeURIComponent(vendorValue)}`);
  }
  if (modelValue) {
    barcodeParams.push(`model=${encodeURIComponent(modelValue)}`);
  }
  const serialBarcodeSrc = serialValue
    ? `/api/barcode/serial/${encodeURIComponent(serialValue)}.png${barcodeParams.length ? `?${barcodeParams.join('&')}` : ''}`
    : '';
  const lotEditorHtml = state.canManageLots
    ? `
      <div class="drawer-lot-field">
        <span>Lot</span>
        <div class="drawer-lot-editor">
          <select class="drawer-lot-select" data-lot-select-for="${detailId}">
            ${lotSelectOptions}
          </select>
          <button class="drawer-action-btn" type="button" data-action="save-lot" data-id="${detailId}">Appliquer</button>
        </div>
      </div>
    `
    : `
      <div class="drawer-lot-field">
        <span>Lot</span>
        <strong>${escapeHtml(lotLabel)}</strong>
      </div>
    `;

  const technicianValue = normalizeTech(detail.technician);
  const technicianFieldHtml = state.canEditTechnician
    ? `
      <div class="drawer-lot-field" data-detail-key="technician">
        <span>Technicien</span>
        <div class="drawer-lot-editor">
          <input
            class="drawer-lot-select drawer-technician-input"
            type="text"
            list="technician-options"
            data-technician-input-for="${detailId}"
            value="${escapeHtml(technicianValue)}"
            placeholder="Technicien"
          />
          <button class="drawer-action-btn" type="button" data-action="save-technician" data-id="${detailId}">Appliquer</button>
        </div>
        <strong>${escapeHtml(technicianValue || '--')}</strong>
      </div>
    `
    : `
      <div data-detail-key="technician">
        <span>Technicien</span>
        <strong>${escapeHtml(technicianValue || '--')}</strong>
      </div>
    `;

  const batteryInputValue = batteryHealthValue != null ? String(batteryHealthValue) : '';
  const batteryFieldHtml = state.canEditBatteryHealth
    ? `
      <div class="drawer-lot-field${isBatteryAlert ? ' drawer-alert-cell is-alert' : ''}" data-detail-key="batteryHealth">
        <span>Sante batterie</span>
        <div class="drawer-lot-editor">
          <input
            class="drawer-lot-select drawer-battery-input"
            type="number"
            min="0"
            max="100"
            step="1"
            inputmode="numeric"
            data-battery-input-for="${detailId}"
            value="${escapeHtml(batteryInputValue)}"
            placeholder="0-100"
          />
          <button class="drawer-action-btn" type="button" data-action="save-battery-health" data-id="${detailId}">Appliquer</button>
        </div>
        <strong>${escapeHtml(batteryHealthLabel)}</strong>
      </div>
    `
    : `
      <div${isBatteryAlert ? ' class="drawer-alert-cell is-alert"' : ''} data-detail-key="batteryHealth">
        <span>Sante batterie</span>
        <strong>${escapeHtml(batteryHealthLabel)}</strong>
      </div>
    `;

  const identifiersPanel = `
    <div class="drawer-table">
      ${lotEditorHtml}
      ${technicianFieldHtml}
      <div><span>Palette</span><strong>${escapeHtml(palletLabel)}</strong></div>
      <div><span>Statut palette</span><strong>${escapeHtml((pallet && pallet.statusLabel) || '--')}</strong></div>
      <div><span>Commande</span><strong>${escapeHtml((shipment && shipment.orderNumber) || '--')}</strong></div>
      <div><span>Client</span><strong>${escapeHtml((shipment && shipment.client) || '--')}</strong></div>
      <div><span>Date expedition</span><strong>${escapeHtml(formatShipmentDate(shipment && shipment.date))}</strong></div>
      <div><span>Palette expedition</span><strong>${escapeHtml((shipment && shipment.palletCode) || '--')}</strong></div>
      <div><span>Serial</span><strong>${escapeHtml(detail.serialNumber || '--')}</strong></div>
      <div><span>MAC</span><strong>${escapeHtml(formatMacSummary(detail))}</strong></div>
      <div><span>OS</span><strong>${escapeHtml(detail.osVersion || '--')}</strong></div>
      ${batteryFieldHtml}
      <div><span>Charge batterie</span><strong>${escapeHtml(batteryChargeLabel)}</strong></div>
      <div><span>Capacite utile</span><strong>${escapeHtml(batteryCapacityLabel)}</strong></div>
      <div><span>Alimentation</span><strong>${escapeHtml(batteryPowerSourceLabel)}</strong></div>
      <div><span>Hash Autopilot</span><div class="drawer-long-row"><strong class="drawer-long-value" title="${escapeHtml(
        autopilotHashTitle
      )}">${escapeHtml(autopilotHashPreview)}</strong>${autopilotCopyButton}</div></div>
      <div class="drawer-barcode-row"><span>Code-barres serial</span>${
        serialBarcodeSrc
          ? `<div class="drawer-barcode-wrap"><img src="${serialBarcodeSrc}" alt="Code-barres ${escapeHtml(
              barcodeLabel
            )}" loading="lazy" /></div>`
          : '<strong>--</strong>'
      }</div>
      <div><span>Premier passage</span><strong>${escapeHtml(formatDateTime(detail.createdAt))}</strong></div>
      <div><span>Dernier passage</span><strong>${escapeHtml(formatDateTime(detail.lastSeen))}</strong></div>
      <div><span>IP serveur</span><strong>${escapeHtml(detail.lastIp || '--')}</strong></div>
      ${
        serverNetwork && serverNetwork.defaultGateway
          ? `<div><span>Passerelle</span><strong>${escapeHtml(serverNetwork.defaultGateway)}</strong></div>`
          : ''
      }
      ${
        serverNetwork && serverNetwork.primaryIpv4
          ? `<div><span>IPv4 primaire</span><strong>${escapeHtml(serverNetwork.primaryIpv4)}</strong></div>`
          : ''
      }
      ${
        serverNetwork && serverNetwork.primaryIpv6
          ? `<div><span>IPv6 primaire</span><strong>${escapeHtml(serverNetwork.primaryIpv6)}</strong></div>`
          : ''
      }
      ${
        serverNetwork && Array.isArray(serverNetwork.interfaces) && serverNetwork.interfaces.length
          ? `<div><span>Interfaces reseau</span><strong>${escapeHtml(String(serverNetwork.interfaces.length))}</strong></div>`
          : ''
      }
      ${
        remoteAccess
          ? `
      <div class="drawer-wide-row"><span>Hostnames debug</span><strong class="drawer-long-value">${escapeHtml(remoteHostnames)}</strong></div>
      <div class="drawer-wide-row"><span>IPv4 locales</span><strong class="drawer-long-value">${escapeHtml(remoteIpv4)}</strong></div>
      <div class="drawer-wide-row"><span>IPv6 locales</span><strong class="drawer-long-value">${escapeHtml(remoteIpv6)}</strong></div>
      <div class="drawer-wide-row"><span>WinRM</span><strong class="drawer-long-value">${escapeHtml(remoteWinRm)}</strong></div>
      <div class="drawer-wide-row"><span>Listeners WinRM</span><strong class="drawer-long-value">${escapeHtml(remoteWinRmListeners)}</strong></div>
      <div class="drawer-wide-row"><span>Adaptateurs debug</span><strong class="drawer-long-value">${escapeHtml(remoteAdapters)}</strong></div>
      <div><span>Collecte debug</span><strong>${escapeHtml(remoteCollectedAt)}</strong></div>
          `
          : ''
      }
      ${buildClockAlertFields(clockAlert, 'drawer')}
    </div>
  `;

  const diagnosticsPanel = `
    <div class="drawer-status-list">
      ${buildDrawerDiagnosticsRows(detail)}
    </div>
  `;

  const componentDefinitions = isServerCategory
    ? [
        ['Ping', 'networkPing'],
        ['Check disque', 'fsCheck'],
        ['SMART disques', 'diskSmart'],
        ['RAID', 'serverRaid'],
        ['Services critiques', 'serverServices'],
        ['Thermique', 'thermal']
      ]
    : [
        ['Ports USB', 'usb'],
        ['Clavier', 'keyboard'],
        ['Camera', 'camera'],
        ['Pave tactile', 'pad'],
        ['Lecteur badge', 'badgeReader'],
        ['CPU OK', 'cpu'],
        ['GPU OK', 'gpu']
      ];
  const componentRows = componentDefinitions
    .map(
      ([label, key]) => `
        <div class="drawer-status-row${
          normalizeSummaryStatusForKey(key, resolveUnifiedComponentStatus(key, components, tests)) === 'nok' ? ' is-nok' : ''
        }" data-detail-key="${escapeHtml(key)}">
          <span>${escapeHtml(label)}</span>
          ${renderStatusValue(resolveUnifiedComponentStatus(key, components, tests), { id: detailId, key })}
        </div>
      `
    )
    .join('');

  const biosRows = isServerCategory
    ? [
        ['Passerelle', serverNetwork && serverNetwork.defaultGateway ? serverNetwork.defaultGateway : '--'],
        ['IPv4 primaire', serverNetwork && serverNetwork.primaryIpv4 ? serverNetwork.primaryIpv4 : '--'],
        ['IPv6 primaire', serverNetwork && serverNetwork.primaryIpv6 ? serverNetwork.primaryIpv6 : '--'],
        [
          'Interfaces reseau',
          serverNetwork && Array.isArray(serverNetwork.interfaces) && serverNetwork.interfaces.length
            ? String(serverNetwork.interfaces.length)
            : '--'
        ],
        [
          'Services suivis',
          serverTelemetry.selectedServices.length
            ? formatServerSelectedServicesSummary(serverTelemetry.selectedServices)
            : '--'
        ],
        [
          'Services en echec',
          serverTelemetry.failedServices.length
            ? formatServerFailedServicesSummary(serverTelemetry.failedServices)
            : serverTelemetry.selectedServices.length
              ? 'Aucun service en echec'
              : '--'
        ]
      ]
        .map(
          ([label, value]) => `
            <div class="drawer-status-row">
              <span>${escapeHtml(label)}</span>
              <div class="drawer-status-stack"><span class="drawer-metric">${escapeHtml(value)}</span></div>
            </div>
          `
        )
        .join('')
    : [
        ['Pile BIOS', 'biosBattery'],
        ['Langue BIOS', 'biosLanguage'],
        ['Mot de passe BIOS', 'biosPassword'],
        ['Norme Wi-Fi', 'wifiStandard']
      ]
        .map(
          ([label, key]) => `
            <div class="drawer-status-row${
              normalizeSummaryStatusForKey(key, components[key] || 'not_tested') === 'nok' ? ' is-nok' : ''
            }" data-detail-key="${escapeHtml(key)}">
              <span>${escapeHtml(label)}</span>
              ${renderStatusValue(components[key] || 'not_tested', { id: detailId, key })}
            </div>
          `
        )
        .join('');

  const commentValue = typeof detail.comment === 'string' ? detail.comment : '';
  const commentMeta = detail.commentedAt
    ? `<p class="drawer-note-meta">Derniere modif: ${escapeHtml(formatDateTime(detail.commentedAt))}</p>`
    : '';
  const commentDisplay = commentValue || '--';
  const commentsPanel = state.canEditReports
    ? `
      <div class="drawer-comment-block" data-detail-key="comment">
        <textarea class="drawer-comment-input" data-comment-id="${detailId}" maxlength="800" placeholder="Ajouter un commentaire">${escapeHtml(
          commentValue
        )}</textarea>
        <div class="drawer-comment-actions">
          <button class="drawer-action-btn" type="button" data-action="clear-comment" data-id="${detailId}">Effacer</button>
        </div>
        ${commentMeta}
      </div>
      ${buildReportHistory(detail)}
    `
    : `
      <div class="drawer-comment-block" data-detail-key="comment">
        <div class="drawer-comment-readonly">${escapeHtml(commentDisplay)}</div>
        ${commentMeta}
      </div>
      ${buildReportHistory(detail)}
    `;

  const tabs = [
    { id: 'identifiants', title: 'Identifiants', content: identifiersPanel },
    { id: 'diagnostics', title: 'Diagnostics', content: diagnosticsPanel },
    {
      id: 'composants',
      title: isServerCategory ? 'Etat serveur' : 'Composants',
      content: `<div class="drawer-status-list">${componentRows}</div>`
    },
    {
      id: 'bios_wifi',
      title: isServerCategory ? 'Reseau / Infra' : 'BIOS / Wi-Fi',
      content: `<div class="drawer-status-list">${biosRows}</div>`
    },
    { id: 'commentaires', title: 'Commentaires', content: commentsPanel }
  ];

  const activeTab = tabs.some((tab) => tab.id === state.drawerTab) ? state.drawerTab : 'identifiants';
  const tabsNavHtml = tabs
    .map(
      (tab) => `
        <button class="drawer-tab-btn${tab.id === activeTab ? ' is-active' : ''}" type="button" data-tab="${tab.id}" aria-selected="${
          tab.id === activeTab ? 'true' : 'false'
        }">
          ${escapeHtml(tab.title)}
        </button>
      `
    )
    .join('');

  const tabPanelsHtml = tabs
    .map(
      (tab) => `
        <section class="drawer-tab-panel${tab.id === activeTab ? ' is-active' : ''}" data-tab-panel="${tab.id}"${
          tab.id === activeTab ? '' : ' hidden'
        }>
          ${tab.content}
        </section>
      `
    )
    .join('');

  return `
    ${headerHtml}
    ${summaryCardsHtml}
    <div class="drawer-tabs">
      <div class="drawer-tabs-nav">${tabsNavHtml}</div>
      <div class="drawer-tabs-content">${tabPanelsHtml}</div>
    </div>
  `;
}

function renderDetailsDrawerContent(reportId = null) {
  if (!detailsDrawerBody || !state.expandedId) {
    return false;
  }
  const targetId = reportId != null ? String(reportId) : String(state.expandedId);
  const detail = state.details[targetId];
  if (!detail) {
    detailsDrawerBody.innerHTML = '<div class="loading">Chargement des details...</div>';
    return false;
  }
  if (detail.error) {
    detailsDrawerBody.innerHTML = '<div class="empty">Impossible de charger les details.</div>';
    return false;
  }
  detailsDrawerBody.innerHTML = buildDrawerDetailHtml(detail);
  setDrawerTab(state.drawerTab || 'identifiants');
  return true;
}

function openDrawerRelative(step) {
  const direction = Number(step);
  if (!Number.isFinite(direction) || !direction || !state.expandedId) {
    return;
  }
  const sequence = getDrawerMachineSequence();
  const currentIndex = sequence.indexOf(String(state.expandedId));
  if (currentIndex < 0) {
    return;
  }
  const nextIndex = currentIndex + direction;
  if (nextIndex < 0 || nextIndex >= sequence.length) {
    return;
  }
  openDetailsDrawer(sequence[nextIndex], { resetTab: false });
}

function openDetailsDrawer(id, options = {}) {
  const targetId = id != null ? String(id) : '';
  if (!targetId || !detailsDrawerShell) {
    return;
  }
  const resetTab = options.resetTab !== false;
  if (resetTab) {
    state.drawerTab = 'identifiants';
  }
  state.expandedId = targetId;
  state.detailOverrideId = null;

  const machine = getMachineById(targetId);
  if (detailsDrawerTitle) {
    detailsDrawerTitle.textContent = machine ? formatPrimary(machine) : 'Chargement...';
  }
  if (detailsDrawerSub) {
    detailsDrawerSub.textContent = machine
      ? `${formatSubtitle(machine)} · ${timeAgo(machine.lastSeen)}`
      : 'Preparation des informations...';
  }
  detailsDrawerBody.innerHTML = '<div class="loading">Chargement des details...</div>';
  detailsDrawerShell.hidden = false;
  document.body.classList.add('drawer-open');
  window.requestAnimationFrame(() => {
    detailsDrawerShell.classList.add('is-open');
  });
  updateDrawerNavButtons();
  renderList();

  ensureMachineDetail(targetId, { skipRender: true })
    .then(() => {
      if (!isDrawerOpen() || String(state.expandedId || '') !== targetId) {
        return;
      }
      try {
        renderDetailsDrawerContent(targetId);
        updateDrawerNavButtons();
        renderList();
      } catch (error) {
        console.error('Failed to render details drawer after fetch', error);
        if (detailsDrawerBody) {
          detailsDrawerBody.innerHTML =
            '<div class="empty">Impossible de charger les details.</div>';
        }
      }
    })
    .catch((error) => {
      console.error('Failed to load machine detail', error);
      if (detailsDrawerBody) {
        detailsDrawerBody.innerHTML = '<div class="empty">Impossible de charger les details.</div>';
      }
    });
}

function renderList(isScrollUpdate = false) {
  updateTimeFilterLabel();
  updateStats();
  updateStatFilterCards();
  updateSummaryFilterButtons();
  updateCategoryFilterButtons();
  updateSignalFilterButtons();
  updateSortSelect();
  updateFilterDockState();
  const useQuickFilter = Boolean(state.quickFilter && state.quickFilter.value);
  const cacheKey = JSON.stringify({
    length: state.machines.length,
    boardView: state.boardView,
    filter: state.filter,
    techFilter: state.techFilter,
    tagFilter: state.tagFilter,
    componentFilter: state.componentFilter,
    commentFilter: state.commentFilter,
    dateFilter: state.dateFilter,
    dateFrom: state.dateFrom,
    dateTo: state.dateTo,
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
  updateResultsSummary(totalCount);

  if (!totalCount) {
    if (listEl) {
      listEl.style.paddingTop = '0px';
      listEl.style.paddingBottom = '0px';
      listEl.classList.remove('is-virtual');
    }
    listEl.innerHTML = isBatteryAlertsView()
      ? `<div class="empty">Aucune machine avec batterie inferieure a ${BATTERY_ALERT_THRESHOLD}% ou derive RTC detectee.</div>`
      : `<div class="empty">Aucun ${getInventoryEntityLabel(1)} ne correspond a ce filtre.</div>`;
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
      const serialPreview = escapeHtml(formatMachineSerialPreview(machine));
      const technicianValue = machine.technician || '';
      const batteryValue = parseBatteryHealthValue(machine.batteryHealth);
      const isBatteryAlert = batteryValue != null && batteryValue < BATTERY_ALERT_THRESHOLD;
      const clockAlert = normalizeClockAlert(machine.clockAlert);
      const isClockAlert = Boolean(clockAlert && clockAlert.active);
      const lastSeen = escapeHtml(timeAgo(machine.lastSeen));
      const tagLabel = getTagLabel(machine);
      const tagValue = escapeHtml(tagLabel);
      const tagIdValue = escapeHtml(getTagId(machine));
      const lotData = getMachineLot(machine);
      const rawLotLabel = lotData ? buildLotLabel(lotData) : '';
      const hasAssignedLot = Boolean(rawLotLabel && rawLotLabel !== DEFAULT_LOT_LABEL);
      const lotLabel = hasAssignedLot ? rawLotLabel : DEFAULT_LOT_LABEL;
      const lotValue = escapeHtml(lotLabel);
      const palletData = getMachinePallet(machine);
      const rawPalletLabel = palletData ? buildPalletLabel(palletData) : '';
      const hasAssignedPallet = Boolean(rawPalletLabel && rawPalletLabel !== DEFAULT_PALLET_LABEL);
      const palletLabel = hasAssignedPallet ? rawPalletLabel : DEFAULT_PALLET_LABEL;
      const palletValue = escapeHtml(palletLabel);
      const tagHtml = state.canRenameTags && tagIdValue
        ? `<button class="tag-pill is-editable" type="button" title="${tagValue}" data-tag="${tagValue}" data-tag-id="${tagIdValue}">${tagValue}</button>`
        : `<span class="tag-pill" title="${tagValue}">${tagValue}</span>`;
      const batteryHtml = batteryValue != null
        ? `<span class="card-meta-pill card-meta-pill--battery${isBatteryAlert ? ' is-alert' : ''}">Sante ${escapeHtml(
            formatBatteryHealth(batteryValue)
          )}</span>`
        : '';
      const biosAlertHtml = isClockAlert
        ? `<span class="card-meta-pill card-meta-pill--alert" title="${escapeHtml(
            formatClockAlertSummary(clockAlert)
          )}">RTC</span>`
        : '';
      const lotMetaHtml = hasAssignedLot
        ? `<span class="card-meta-pill">Lot ${lotValue}</span>`
        : '';
      const palletMetaHtml = hasAssignedPallet
        ? `<span class="card-meta-pill card-meta-pill--secondary">Palette ${palletValue}</span>`
        : '';
      const commentValue = typeof machine.comment === 'string' ? machine.comment.trim() : '';
      const isEditingComment = state.quickCommentId === machine.id;
      const commentEditHtml = state.canEditReports
        ? `
          <div class="card-quick-comment${isEditingComment ? ' is-open' : ''}">
            <textarea
              class="comment-inline"
              data-comment-id="${machine.id}"
              maxlength="800"
              placeholder="Ajouter un commentaire"
            >${escapeHtml(commentValue)}</textarea>
          </div>
        `
        : '';
      const selected = state.expandedId === machine.id ? 'selected' : '';
      const absoluteIndex = startIndex + index;
      const delayClass = delayClasses[absoluteIndex % delayClasses.length];
      const summary = summarizeDetailForDrawer(machine);
      const primaryStatus = getMachinePrimaryStatus(machine);
      const primaryStatusLabel = getMachinePrimaryStatusLabel(machine);
      const technicianLabel = escapeHtml(technicianValue || '--');
      const commentPreviewHtml = commentValue
        ? `<p class="card-comment-preview" title="${escapeHtml(commentValue)}">${escapeHtml(commentValue)}</p>`
        : '';
      const summaryActive = state.quickFilter && state.quickFilter.type === 'summary';
      const summaryHtml =
        summary.total > 0
          ? `
            <div class="machine-summary machine-summary--compact">
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
      const toggleLabel = 'Voir details';

      return `
        <article class="machine-card ${delayClass} ${selected} is-state-${primaryStatus}${isBatteryAlert ? ' is-battery-alert' : ''}${isClockAlert ? ' is-clock-alert' : ''}" data-id="${machine.id}" data-page="${machine._page || ''}" data-index="${entry.index}" aria-expanded="false">
          <div class="card-top">
            <div class="card-heading">
              <div class="card-heading-top">
                ${categoryBadge}
                <span class="machine-state machine-state--${primaryStatus}">
                  <span class="machine-state-dot" aria-hidden="true"></span>
                  ${primaryStatusLabel}
                </span>
                ${biosAlertHtml}
              </div>
              <h3 class="machine-title">${title}</h3>
              <p class="machine-sub">${subtitle}</p>
              ${serialPreview ? `<p class="machine-serial">${serialPreview}</p>` : ''}
            </div>
            <div class="card-activity">
              <span class="card-activity-label">Activite</span>
              <strong>${lastSeen}</strong>
            </div>
          </div>
          <div class="card-signals">
            ${summaryHtml}
            <div class="card-signal-pills">
              <span class="card-meta-pill card-meta-pill--tech">Tech ${technicianLabel}</span>
              ${batteryHtml}
              ${lotMetaHtml}
              ${palletMetaHtml}
              ${tagHtml}
            </div>
          </div>
          ${commentPreviewHtml}
          ${commentEditHtml}
          <div class="card-actions-inline">
            <button class="card-action-btn" type="button" data-action="open-drawer-tab" data-tab="composants" data-id="${machine.id}">
              Modifier statut
            </button>
            ${
              state.canEditReports
                ? `<button class="card-action-btn" type="button" data-action="toggle-comment" data-id="${machine.id}">${
                    commentValue ? 'Commentaire' : 'Commenter'
                  }</button>`
                : ''
            }
            <button class="card-action-btn is-primary" type="button" data-action="open-drawer" data-id="${machine.id}">${toggleLabel}</button>
          </div>
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
  const pallet = getMachinePallet(detail);
  const palletLabel = pallet ? buildPalletLabel(pallet) : '';
  const shipment = getMachineShipment(detail);
  const technicianLine = detail.technician
    ? `<p class="detail-tech"><span>Technicien</span><strong>${escapeHtml(detail.technician)}</strong></p>`
    : '';
  const lotLine = lotLabel
    ? `<p class="detail-tech"><span>Lot</span><strong>${escapeHtml(lotLabel)}</strong></p>`
    : '';
  const palletLine = palletLabel
    ? `<p class="detail-tech"><span>Palette</span><strong>${escapeHtml(palletLabel)}</strong></p>`
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
  const actionBar = detailId
    ? `
      <div class="detail-actions">
        <button class="detail-action" type="button" data-action="export-pdf" data-id="${detailId}">
          Telecharger PDF
        </button>
        ${deleteButton}
      </div>
    `
    : '';
  const commentValue = typeof detail.comment === 'string' ? detail.comment : '';
  const commentMeta = detail.commentedAt
    ? `<div class="comment-meta">Derniere modif : ${escapeHtml(formatDateTime(detail.commentedAt))}</div>`
    : '';
  const commentHtml = detailId
    ? state.canEditReports
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
      : `
        <div class="comment-block">
          <span class="comment-label">Commentaire</span>
          <div class="comment-readonly">${escapeHtml(commentValue || '--')}</div>
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
  const remoteAccess = getRemoteAccessDebug(detail);
  const remoteHostnames = formatDebugValueList(remoteAccess && remoteAccess.hostnames);
  const remoteIpv4 = formatDebugValueList(remoteAccess && remoteAccess.ipv4);
  const remoteIpv6 = formatDebugValueList(remoteAccess && remoteAccess.ipv6);
  const remoteAdapters = formatRemoteAccessAdapters(remoteAccess);
  const remoteWinRm = formatRemoteAccessWinRm(remoteAccess);
  const remoteWinRmListeners = formatRemoteAccessWinRmListeners(remoteAccess);
  const remoteCollectedAt =
    remoteAccess && remoteAccess.collectedAt ? formatDateTime(remoteAccess.collectedAt) : '--';
  const clockAlert = normalizeClockAlert(detail.clockAlert);

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
  const autopilotHash = getAutopilotHash(detail);
  const autopilotHashPreview = formatAutopilotHashPreview(autopilotHash, 120);
  const autopilotHashTitle = autopilotHash || '--';
  const canCopyAutopilotHash = Boolean(autopilotHash && detailId);
  const autopilotCopyButton = canCopyAutopilotHash
    ? `<button class="hash-copy-btn" type="button" data-action="copy-autopilot-hash" data-id="${detailId}" title="Copier le hash complet">Copier</button>`
    : '';
  const serialValue = detail.serialNumber ? String(detail.serialNumber).trim() : '';
  const vendorValue = detail.vendor ? String(detail.vendor).trim() : '';
  const modelValue = detail.model ? String(detail.model).trim() : '';
  const barcodeLabel = buildMachineIdentityLabel(detail, { includeSerial: true, fallback: serialValue || '--' });
  const barcodeParams = [];
  if (vendorValue) {
    barcodeParams.push(`vendor=${encodeURIComponent(vendorValue)}`);
  }
  if (modelValue) {
    barcodeParams.push(`model=${encodeURIComponent(modelValue)}`);
  }
  const serialBarcodeSrc = serialValue
    ? `/api/barcode/serial/${encodeURIComponent(serialValue)}.png${barcodeParams.length ? `?${barcodeParams.join('&')}` : ''}`
    : '';

  return `
    <div class="detail-header">
      <h2 class="detail-title">${title}</h2>
      ${buildCategoryBadge(category, detailId, 'detail-category')}
      <p class="machine-sub">${subtitle}</p>
      ${technicianLine}
      ${lotLine}
      ${palletLine}
      ${actionBar}
    </div>
    <div class="detail-grid">
      <div class="detail-item">
        <span>Palette</span>
        <strong>${escapeHtml(palletLabel || '--')}</strong>
      </div>
      <div class="detail-item">
        <span>Statut palette</span>
        <strong>${escapeHtml((pallet && pallet.statusLabel) || '--')}</strong>
      </div>
      <div class="detail-item">
        <span>N° commande</span>
        <strong>${escapeHtml((shipment && shipment.orderNumber) || '--')}</strong>
      </div>
      <div class="detail-item">
        <span>Client</span>
        <strong>${escapeHtml((shipment && shipment.client) || '--')}</strong>
      </div>
      <div class="detail-item">
        <span>Date expedition</span>
        <strong>${escapeHtml(formatShipmentDate(shipment && shipment.date))}</strong>
      </div>
      <div class="detail-item">
        <span>Palette expedition</span>
        <strong>${escapeHtml((shipment && shipment.palletCode) || '--')}</strong>
      </div>
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
        <span>Hash Autopilot</span>
        <div class="detail-hash-actions">
          <strong class="detail-long-value" title="${escapeHtml(autopilotHashTitle)}">${escapeHtml(
            autopilotHashPreview
          )}</strong>
          ${autopilotCopyButton}
        </div>
      </div>
      <div class="detail-item detail-barcode-row">
        <span>Code-barres serial</span>
        ${
          serialBarcodeSrc
            ? `<div class="detail-barcode-wrap"><img src="${serialBarcodeSrc}" alt="Code-barres ${escapeHtml(
                barcodeLabel
              )}" loading="lazy" /></div>`
            : '<strong>--</strong>'
        }
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
        <span>IP serveur</span>
        <strong>${escapeHtml(detail.lastIp || '--')}</strong>
      </div>
      ${
        remoteAccess
          ? `
      <div class="detail-item detail-item--wide">
        <span>Hostnames debug</span>
        <strong class="detail-long-value">${escapeHtml(remoteHostnames)}</strong>
      </div>
      <div class="detail-item detail-item--wide">
        <span>IPv4 locales</span>
        <strong class="detail-long-value">${escapeHtml(remoteIpv4)}</strong>
      </div>
      <div class="detail-item detail-item--wide">
        <span>IPv6 locales</span>
        <strong class="detail-long-value">${escapeHtml(remoteIpv6)}</strong>
      </div>
      <div class="detail-item detail-item--wide">
        <span>WinRM</span>
        <strong class="detail-long-value">${escapeHtml(remoteWinRm)}</strong>
      </div>
      <div class="detail-item detail-item--wide">
        <span>Listeners WinRM</span>
        <strong class="detail-long-value">${escapeHtml(remoteWinRmListeners)}</strong>
      </div>
      <div class="detail-item detail-item--wide">
        <span>Adaptateurs debug</span>
        <strong class="detail-long-value">${escapeHtml(remoteAdapters)}</strong>
      </div>
      <div class="detail-item">
        <span>Collecte debug</span>
        <strong>${escapeHtml(remoteCollectedAt)}</strong>
      </div>
          `
          : ''
      }
      ${buildClockAlertFields(clockAlert, 'detail')}
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
  const autopilotHash = getAutopilotHash(detail);
  const autopilotHashPreview = formatAutopilotHashPreview(autopilotHash, 120);
  const autopilotHashTitle = autopilotHash || '--';
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

  const summary = summarizeDetailForDrawer(detail);
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
            <span>Hash Autopilot</span>
            <strong class="detail-long-value" title="${escapeHtml(autopilotHashTitle)}">${escapeHtml(
              autopilotHashPreview
            )}</strong>
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
      link.download = buildPdfDownloadFilename(detail);
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
  listEl.innerHTML = `<div class="loading">Chargement des ${getInventoryLoadLabel()}...</div>`;
  resetPagination();
  reportLoadRecoveryTriggered = false;
  if (Array.isArray(state.tagFilter)) {
    state.tagFilter = state.tagFilter.map((value) => normalizeTagId(value)).filter(Boolean);
  }
  await loadMeta();
  await Promise.all([loadStats(), loadReportsPage(0), loadTimeline()]);
  renderTechnicianOptions();
  updateCommentFilterButtons();
  if (state.expandedId) {
    await ensureMachineDetail(state.expandedId, { skipRender: true });
    if (isDrawerOpen()) {
      renderDetailsDrawerContent();
      updateDrawerNavButtons();
    }
  }
}

function updateExpandedDetailHtml(reportId, preserveScroll = false) {
  return renderDetailsDrawerContent(reportId);
}

async function ensureMachineDetail(id, options = {}) {
  const detailId = id != null ? String(id) : '';
  if (!detailId) {
    return null;
  }
  const cachedDetail = state.details[detailId];
  if (cachedDetail && !cachedDetail.error) {
    if (!options.skipRender) {
      try {
        renderList();
      } catch (error) {
        console.error('Failed to render list from cached detail', error);
      }
    }
    if (isDrawerOpen() && String(state.expandedId || '') === detailId) {
      try {
        renderDetailsDrawerContent(detailId);
        updateDrawerNavButtons();
      } catch (error) {
        console.error('Failed to render drawer from cached detail', error);
      }
    }
    return cachedDetail;
  }
  if (cachedDetail && cachedDetail.error) {
    delete state.details[detailId];
  }
  if (!options.skipRender) {
    try {
      renderList();
    } catch (error) {
      console.error('Failed to render list before detail fetch', error);
    }
  }
  let fetchedMachine = null;
  try {
    const detailUrl = `/api/machines/${encodeURIComponent(detailId)}`;
    const controller = typeof AbortController !== 'undefined' ? new AbortController() : null;
    const timeoutId = controller
      ? window.setTimeout(() => controller.abort(), 15000)
      : null;
    let response;
    try {
      response = await fetch(detailUrl, controller ? { signal: controller.signal } : undefined);
    } finally {
      if (timeoutId != null) {
        window.clearTimeout(timeoutId);
      }
    }
    if (response.status === 401) {
      window.location.href = '/login';
      return;
    }
    if (!response.ok) {
      throw new Error('detail_failed');
    }
    const data = await response.json();
    fetchedMachine = data && data.machine ? data.machine : null;
    if (!fetchedMachine) {
      throw new Error('detail_empty');
    }
    state.details[detailId] = fetchedMachine;
  } catch (error) {
    state.details[detailId] = { error: true };
    if (!options.skipRender) {
      try {
        renderList();
      } catch (renderError) {
        console.error('Failed to render list after detail fetch error', renderError);
      }
    }
    if (isDrawerOpen() && String(state.expandedId || '') === detailId) {
      try {
        renderDetailsDrawerContent(detailId);
        updateDrawerNavButtons();
      } catch (renderError) {
        console.error('Failed to render drawer after detail fetch error', renderError);
      }
    }
    return state.details[detailId];
  }
  if (!options.skipRender) {
    try {
      renderList();
    } catch (error) {
      console.error('Failed to render list after detail fetch success', error);
    }
  }
  if (isDrawerOpen() && String(state.expandedId || '') === detailId) {
    try {
      renderDetailsDrawerContent(detailId);
      updateDrawerNavButtons();
    } catch (error) {
      console.error('Failed to render drawer after detail fetch success', error);
    }
  }
  return fetchedMachine;
}

function updateLastUpdated() {
  if (!lastUpdatedEl) {
    return;
  }
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

if (boardTabsEl) {
  boardTabsEl.addEventListener('click', (event) => {
    const button = event.target.closest('.board-tab-btn[data-board-view]');
    if (!button) {
      return;
    }
    const nextView = button.dataset.boardView || 'workspace';
    if (!boardViewOptions.has(nextView) || nextView === state.boardView) {
      return;
    }
    state.boardView = nextView;
    savePreferences();
    renderBoardTabs();
    updateSignalFilterButtons();
    reloadReports();
  });
}

if (techFiltersEl) {
  techFiltersEl.addEventListener('click', (event) => {
    if (isTechnicianFilterLocked()) {
      return;
    }
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

summaryFilterButtons.forEach((button) => {
  button.addEventListener('click', () => {
    const value = button.dataset.summary || '';
    if (!summaryFilterValues.has(value)) {
      return;
    }
    if (state.quickFilter && state.quickFilter.type === 'summary' && state.quickFilter.value === value) {
      state.quickFilter = null;
    } else {
      state.quickFilter = { type: 'summary', value };
    }
    state.activeToken = null;
    updateSummaryFilterButtons();
    savePreferences();
    renderList();
  });
});

categoryFilterButtons.forEach((button) => {
  button.addEventListener('click', () => {
    const next = button.dataset.category || 'all';
    if (!categoryFilterOptions.has(next)) {
      return;
    }
    state.filter = next;
    updateCategoryFilterButtons();
    updateStatFilterCards();
    savePreferences();
    reloadReports();
  });
});

signalFilterButtons.forEach((button) => {
  button.addEventListener('click', () => {
    const signal = button.dataset.signal || '';
    if (signal === 'battery-alerts') {
      state.boardView = isBatteryAlertsView() ? 'workspace' : 'battery-alerts';
      savePreferences();
      renderBoardTabs();
      updateSignalFilterButtons();
      reloadReports();
      return;
    }
    if (signal === 'recent') {
      state.dateFilter = state.dateFilter === 'today' ? 'all' : 'today';
      state.dateFrom = '';
      state.dateTo = '';
      syncDateFilterControls();
      updateSignalFilterButtons();
      savePreferences();
      reloadReports();
      return;
    }
    if (signal === 'commented') {
      state.commentFilter = state.commentFilter === 'with' ? 'all' : 'with';
      updateCommentFilterButtons();
      updateSignalFilterButtons();
      savePreferences();
      reloadReports();
    }
  });
});

dateFilterButtons.forEach((button) => {
  button.addEventListener('click', () => {
    const next = button.dataset.dateFilter || 'all';
    if (!dateFilterOrder.includes(next)) {
      return;
    }
    state.dateFilter = next;
    if (next !== 'custom') {
      state.dateFrom = '';
      state.dateTo = '';
    }
    syncDateFilterControls();
    savePreferences();
    if (next !== 'custom') {
      reloadReports();
    }
  });
});

timelineGranularityButtons.forEach((button) => {
  button.addEventListener('click', () => {
    const next = button.dataset.granularity || 'day';
    if (!timelineGranularityOptions.has(next) || next === state.timelineGranularity) {
      return;
    }
    state.timelineGranularity = next;
    syncDateFilterControls();
    savePreferences();
    loadTimeline();
  });
});

if (dateFromInput) {
  dateFromInput.addEventListener('change', (event) => {
    state.dateFrom = normalizeDateInputValue(event.target.value);
    if (state.dateFilter !== 'custom') {
      state.dateFilter = 'custom';
    }
    syncDateFilterControls();
    savePreferences();
  });
}

if (dateToInput) {
  dateToInput.addEventListener('change', (event) => {
    state.dateTo = normalizeDateInputValue(event.target.value);
    if (state.dateFilter !== 'custom') {
      state.dateFilter = 'custom';
    }
    syncDateFilterControls();
    savePreferences();
  });
}

if (applyDateRangeBtn) {
  applyDateRangeBtn.addEventListener('click', () => {
    state.dateFrom = normalizeDateInputValue(dateFromInput ? dateFromInput.value : state.dateFrom);
    state.dateTo = normalizeDateInputValue(dateToInput ? dateToInput.value : state.dateTo);
    state.dateFilter = 'custom';
    syncDateFilterControls();
    savePreferences();
    reloadReports();
  });
}

if (sortSelect) {
  sortSelect.addEventListener('change', (event) => {
    const next = String(event.target.value || '').trim();
    state.sort = sortOptions.has(next) ? next : 'activity';
    updateSortSelect();
    savePreferences();
    renderList();
  });
}

updateLayoutButtons();
updateTestFilterButtons();
updateCommentFilterButtons();
updateTagFilterButtons();
updateSummaryFilterButtons();
updateCategoryFilterButtons();
updateSignalFilterButtons();
updateSortSelect();
applyLayout();
syncDateFilterControls();
updateStatFilterCards();
initSidebarNavigation();
initFilterHub();
updateFilterDockState();
renderBoardTabs();

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

if (refreshBtn) {
  refreshBtn.addEventListener('click', () => {
    loadMachines();
  });
}

if (purgeImportsBtn) {
  purgeImportsBtn.addEventListener('click', () => {
    deleteLegacyImports();
  });
}

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
  const retryReportsBtn = event.target.closest('[data-action="retry-reports"]');
  if (retryReportsBtn) {
    event.preventDefault();
    event.stopPropagation();
    reportLoadRecoveryTriggered = false;
    reloadReports();
    return;
  }
  const resetAndRetryBtn = event.target.closest('[data-action="reset-filters-and-retry"]');
  if (resetAndRetryBtn) {
    event.preventDefault();
    event.stopPropagation();
    reportLoadRecoveryTriggered = false;
    resetAllFilters();
    return;
  }
  const commentInput = event.target.closest('.comment-inline');
  if (commentInput) {
    return;
  }
  const commentCard = event.target.closest('[data-comment-card]');
  if (commentCard) {
    event.preventDefault();
    event.stopPropagation();
    if (!state.canEditReports) {
      return;
    }
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
  const toggleCommentBtn = event.target.closest('[data-action="toggle-comment"]');
  if (toggleCommentBtn) {
    event.preventDefault();
    event.stopPropagation();
    if (!state.canEditReports) {
      return;
    }
    const id = toggleCommentBtn.dataset.id;
    if (!id) {
      return;
    }
    state.quickCommentId = state.quickCommentId === id ? null : id;
    renderList();
    if (state.quickCommentId === id) {
      focusInlineComment(id);
    }
    return;
  }
  const tagPill = event.target.closest('.tag-pill');
  if (tagPill && tagPill.dataset.tagId) {
    event.preventDefault();
    event.stopPropagation();
    if (!state.canRenameTags) {
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
    openDetailsDrawer(String(id), { resetTab: false });
    return;
  }
  const openDrawerBtn = event.target.closest('[data-action="open-drawer"]');
  if (openDrawerBtn) {
    event.preventDefault();
    event.stopPropagation();
    const id = openDrawerBtn.dataset.id;
    if (!id) {
      return;
    }
    openDetailsDrawer(id);
    return;
  }
  const openDrawerTabBtn = event.target.closest('[data-action="open-drawer-tab"]');
  if (openDrawerTabBtn) {
    event.preventDefault();
    event.stopPropagation();
    const id = openDrawerTabBtn.dataset.id;
    const tab = openDrawerTabBtn.dataset.tab;
    if (!id) {
      return;
    }
    if (tab) {
      state.drawerTab = tab;
      openDetailsDrawer(id, { resetTab: false });
      return;
    }
    openDetailsDrawer(id);
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
  const copyHashBtn = event.target.closest('[data-action="copy-autopilot-hash"]');
  if (copyHashBtn) {
    event.preventDefault();
    event.stopPropagation();
    const id = copyHashBtn.dataset.id;
    if (!id) {
      return;
    }
    copyAutopilotHashForReport(id, copyHashBtn);
    return;
  }
  const card = event.target.closest('.machine-card');
  if (!card) {
    return;
  }
  const id = card.dataset.id;
  if (!id) {
    return;
  }
  openDetailsDrawer(id);
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

if (detailsDrawerShell) {
  detailsDrawerShell.addEventListener('click', (event) => {
    const closeBtn = event.target.closest('[data-action="close-drawer"]');
    if (closeBtn) {
      event.preventDefault();
      closeDetailsDrawer();
      return;
    }

    const tabBtn = event.target.closest('.drawer-tab-btn[data-tab]');
    if (tabBtn) {
      event.preventDefault();
      const tab = tabBtn.dataset.tab || 'identifiants';
      setDrawerTab(tab);
      return;
    }

    const focusNokBtn = event.target.closest('[data-action="focus-noks"], [data-action="focus-nok"]');
    if (focusNokBtn) {
      event.preventDefault();
      const tab = focusNokBtn.dataset.tab || 'composants';
      const key = focusNokBtn.dataset.key || '';
      focusDrawerIssue(tab, key);
      return;
    }

    const reportLink = event.target.closest('[data-action="open-report"]');
    if (reportLink) {
      event.preventDefault();
      const id = reportLink.dataset.id;
      if (id) {
        openDetailsDrawer(id, { resetTab: false });
      }
      return;
    }

    const cycleBtn = event.target.closest('[data-action="cycle-status"]');
    if (cycleBtn) {
      event.preventDefault();
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
      const id = categoryBtn.dataset.id;
      if (!id) {
        return;
      }
      const nextValue = nextCategory(categoryBtn.dataset.category);
      updateCategory(id, nextValue, categoryBtn);
      return;
    }

    const exportBtn = event.target.closest('[data-action="export-pdf"]');
    if (exportBtn) {
      event.preventDefault();
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

    const saveBatteryBtn = event.target.closest('[data-action="save-battery-health"]');
    if (saveBatteryBtn) {
      event.preventDefault();
      const id = saveBatteryBtn.dataset.id;
      if (!id || !detailsDrawerShell) {
        return;
      }
      const safeId = String(id).replace(/"/g, '\\"');
      const input = detailsDrawerShell.querySelector(`[data-battery-input-for="${safeId}"]`);
      if (!input) {
        return;
      }
      updateBatteryHealth(id, input.value);
      return;
    }

    const saveTechnicianBtn = event.target.closest('[data-action="save-technician"]');
    if (saveTechnicianBtn) {
      event.preventDefault();
      const id = saveTechnicianBtn.dataset.id;
      if (!id || !detailsDrawerShell) {
        return;
      }
      const safeId = String(id).replace(/"/g, '\\"');
      const input = detailsDrawerShell.querySelector(`[data-technician-input-for="${safeId}"]`);
      if (!input) {
        return;
      }
      updateTechnician(id, input.value);
      return;
    }

    const saveLotBtn = event.target.closest('[data-action="save-lot"]');
    if (saveLotBtn) {
      event.preventDefault();
      const id = saveLotBtn.dataset.id;
      if (!id || !detailsDrawerShell) {
        return;
      }
      const detail = state.details[id];
      const safeId = String(id).replace(/"/g, '\\"');
      const select = detailsDrawerShell.querySelector(`[data-lot-select-for="${safeId}"]`);
      if (!detail || detail.error || !select) {
        return;
      }
      const currentLot = getMachineLot(detail);
      const currentLotId = normalizeLotId(currentLot && currentLot.id ? currentLot.id : '');
      const nextLotId = normalizeLotId(select.value || '');
      if (currentLotId === nextLotId) {
        return;
      }
      updateMachineLot(id, select.value || null, saveLotBtn);
      return;
    }

    const copyHashBtn = event.target.closest('[data-action="copy-autopilot-hash"]');
    if (copyHashBtn) {
      event.preventDefault();
      const id = copyHashBtn.dataset.id;
      if (!id) {
        return;
      }
      copyAutopilotHashForReport(id, copyHashBtn);
      return;
    }

    const clearCommentBtn = event.target.closest('[data-action="clear-comment"]');
    if (clearCommentBtn) {
      event.preventDefault();
      const id = clearCommentBtn.dataset.id;
      if (!id) {
        return;
      }
      const input = detailsDrawerBody
        ? detailsDrawerBody.querySelector(`.drawer-comment-input[data-comment-id="${id}"]`)
        : null;
      if (input) {
        input.value = '';
      }
      updateComment(id, '');
      return;
    }

    const deleteBtn = event.target.closest('[data-action="delete-report"]');
    if (deleteBtn) {
      event.preventDefault();
      const id = deleteBtn.dataset.id;
      if (id) {
        deleteReport(id);
      }
      return;
    }
  });

  detailsDrawerShell.addEventListener('input', (event) => {
    const input = event.target.closest('.drawer-comment-input');
    if (!input) {
      return;
    }
    const id = input.dataset.commentId;
    if (!id) {
      return;
    }
    scheduleCommentSave(id, input.value);
  });

  detailsDrawerShell.addEventListener('keydown', (event) => {
    const input = event.target.closest('.drawer-battery-input');
    if (!input || event.key !== 'Enter') {
      const technicianInput = event.target.closest('.drawer-technician-input');
      if (!technicianInput || event.key !== 'Enter') {
        return;
      }
      event.preventDefault();
      const id = technicianInput.dataset.technicianInputFor;
      if (!id) {
        return;
      }
      updateTechnician(id, technicianInput.value);
      return;
    }
    event.preventDefault();
    const id = input.dataset.batteryInputFor;
    if (!id) {
      return;
    }
    updateBatteryHealth(id, input.value);
  });

  detailsDrawerShell.addEventListener(
    'blur',
    (event) => {
      const input = event.target.closest('.drawer-comment-input');
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
}

if (drawerPrevBtn) {
  drawerPrevBtn.addEventListener('click', () => {
    openDrawerRelative(-1);
  });
}

if (drawerNextBtn) {
  drawerNextBtn.addEventListener('click', () => {
    openDrawerRelative(1);
  });
}

document.addEventListener('keydown', (event) => {
  if (event.key === 'Escape' && isDrawerOpen()) {
    closeDetailsDrawer();
  }
});

if (patchnoteOkBtn) {
  patchnoteOkBtn.addEventListener('click', () => {
    acknowledgePatchnote();
  });
}

initAdminLink();
initPatchnote();
initInfiniteScroll();
loadMachines();
