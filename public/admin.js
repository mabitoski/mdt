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
