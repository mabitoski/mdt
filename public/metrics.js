const grafanaFrame = document.getElementById('grafana-frame');
const grafanaHint = document.getElementById('grafana-hint');
const serviceStatus = document.getElementById('metrics-service-status');
const openGrafanaBtn = document.getElementById('open-grafana-btn');
const openGrafanaInline = document.getElementById('open-grafana-inline');
const reloadGrafanaBtn = document.getElementById('reload-grafana-btn');
const dashboardTabs = Array.from(
  document.querySelectorAll('.metrics-dashboard-tab[data-dashboard]')
);

const protocol = window.location.protocol === 'https:' ? 'https:' : 'http:';
const host = window.location.hostname || '127.0.0.1';
const grafanaBaseUrl = `${protocol}//${host}:3002`;
const commonParams = {
  orgId: '1',
  refresh: '30s',
  theme: 'light'
};

const dashboards = {
  overview: {
    path: '/d/mdt-atelier-overview/atelier-qualite-parc',
    from: 'now-30d',
    to: 'now'
  },
  lots: {
    path: '/d/mdt-atelier-lots/atelier-lots-production',
    from: 'now-30d',
    to: 'now'
  },
  types: {
    path: '/d/mdt-atelier-types/atelier-types-poste',
    from: 'now-30d',
    to: 'now'
  },
  weekly: {
    path: '/d/mdt-atelier-weekly/atelier-hebdomadaire',
    from: 'now-7d',
    to: 'now'
  }
};

let currentDashboard = 'overview';
let loadTimeout = null;

function setStatus(message, isOk = true) {
  if (!serviceStatus) {
    return;
  }
  serviceStatus.textContent = message;
  serviceStatus.dataset.state = isOk ? 'ok' : 'error';
}

function setHint(message) {
  if (!grafanaHint) {
    return;
  }
  grafanaHint.textContent = message;
}

function buildDashboardUrl(key, force = false) {
  const config = dashboards[key] || dashboards.overview;
  const params = new URLSearchParams(commonParams);
  params.set('from', config.from);
  params.set('to', config.to);
  if (force) {
    params.set('v', Date.now().toString());
  }
  return `${grafanaBaseUrl}${config.path}?${params.toString()}`;
}

function updateLinks(url) {
  if (openGrafanaBtn) {
    openGrafanaBtn.href = url;
  }
  if (openGrafanaInline) {
    openGrafanaInline.href = url;
  }
}

function updateTabState() {
  dashboardTabs.forEach((tab) => {
    const isActive = tab.dataset.dashboard === currentDashboard;
    tab.classList.toggle('is-active', isActive);
    tab.setAttribute('aria-pressed', isActive ? 'true' : 'false');
  });
}

function armLoadTimeout() {
  if (loadTimeout) {
    clearTimeout(loadTimeout);
    loadTimeout = null;
  }
  loadTimeout = setTimeout(() => {
    setStatus('Connexion impossible', false);
    setHint(
      "L'integration Grafana ne repond pas. Ouvre le dashboard en plein ecran pour verifier la connectivite."
    );
  }, 12000);
}

function loadDashboard({ force = false } = {}) {
  if (!grafanaFrame) {
    return;
  }
  const url = buildDashboardUrl(currentDashboard, force);
  updateTabState();
  updateLinks(url);
  setStatus('Connexion en cours...', true);
  setHint('Connexion au dashboard...');
  armLoadTimeout();
  grafanaFrame.src = url;
}

if (grafanaFrame) {
  grafanaFrame.addEventListener('load', () => {
    if (loadTimeout) {
      clearTimeout(loadTimeout);
      loadTimeout = null;
    }
    setStatus('Dashboard actif', true);
    setHint(`Dashboard charge depuis ${grafanaBaseUrl}`);
  });
}

dashboardTabs.forEach((tab) => {
  tab.addEventListener('click', () => {
    const next = tab.dataset.dashboard;
    if (!next || !dashboards[next]) {
      return;
    }
    currentDashboard = next;
    loadDashboard({ force: false });
  });
});

if (reloadGrafanaBtn) {
  reloadGrafanaBtn.addEventListener('click', () => loadDashboard({ force: true }));
}

updateTabState();
loadDashboard();
