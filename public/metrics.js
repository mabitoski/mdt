const grafanaFrame = document.getElementById('grafana-frame');
const grafanaHint = document.getElementById('grafana-hint');
const serviceStatus = document.getElementById('metrics-service-status');
const openGrafanaBtn = document.getElementById('open-grafana-btn');
const openGrafanaInline = document.getElementById('open-grafana-inline');
const reloadGrafanaBtn = document.getElementById('reload-grafana-btn');

const protocol = window.location.protocol === 'https:' ? 'https:' : 'http:';
const host = window.location.hostname || '127.0.0.1';
const grafanaPort = '3002';
const dashboardPath = '/d/mdt-atelier-overview/atelier-qualite-parc';
const dashboardParams = new URLSearchParams({
  orgId: '1',
  refresh: '30s',
  theme: 'light',
  kiosk: 'tv'
});

const grafanaBaseUrl = `${protocol}//${host}:${grafanaPort}`;
const grafanaDashboardUrl = `${grafanaBaseUrl}${dashboardPath}?${dashboardParams.toString()}`;

function setStatus(message, isOk = true) {
  if (!serviceStatus) {
    return;
  }
  serviceStatus.textContent = message;
  serviceStatus.dataset.state = isOk ? 'ok' : 'error';
}

function updateGrafanaLinks() {
  if (openGrafanaBtn) {
    openGrafanaBtn.href = grafanaDashboardUrl;
  }
  if (openGrafanaInline) {
    openGrafanaInline.href = grafanaDashboardUrl;
  }
}

function loadGrafanaFrame({ force = false } = {}) {
  if (!grafanaFrame) {
    return;
  }
  const suffix = force ? `&v=${Date.now()}` : '';
  grafanaFrame.src = `${grafanaDashboardUrl}${suffix}`;
  if (grafanaHint) {
    grafanaHint.textContent = 'Connexion au dashboard...';
  }
  setStatus('Connexion en cours...', true);
}

if (grafanaFrame) {
  grafanaFrame.addEventListener('load', () => {
    if (grafanaHint) {
      grafanaHint.textContent = `Dashboard charge depuis ${grafanaBaseUrl}`;
    }
    setStatus('Dashboard actif', true);
  });
}

if (reloadGrafanaBtn) {
  reloadGrafanaBtn.addEventListener('click', () => loadGrafanaFrame({ force: true }));
}

updateGrafanaLinks();
loadGrafanaFrame();
