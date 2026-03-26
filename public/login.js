const params = new URLSearchParams(window.location.search);
const errorEl = document.getElementById('login-error');
const ssoBlock = document.getElementById('login-sso');
const microsoftBtn = document.getElementById('login-microsoft-btn');
const dividerEl = document.getElementById('login-divider');
const formEl = document.getElementById('login-form');
const usernameEl = document.getElementById('login-username');
const passwordEl = document.getElementById('login-password');
const togglePasswordBtn = document.getElementById('toggle-password');
const submitBtn = document.getElementById('login-submit');

const errorMessages = {
  '1': 'Connexion Microsoft impossible. Reessayez.',
  group_required: "Votre compte Microsoft n'appartient a aucun groupe MDT autorise.",
  sso_only: 'Le login par mot de passe LDAP est desactive. Utilise Microsoft ou le compte admin local.',
  local_invalid: 'Compte admin local invalide. Reessayez.'
};

function showError(message) {
  if (!errorEl || !message) {
    return;
  }
  errorEl.textContent = message;
  errorEl.classList.add('visible');
}

const errorCode = params.get('error');
if (errorCode) {
  showError(errorMessages[errorCode] || errorMessages['1']);
}

if (microsoftBtn) {
  microsoftBtn.addEventListener('click', () => {
    microsoftBtn.setAttribute('aria-disabled', 'true');
    microsoftBtn.textContent = 'Redirection Microsoft...';
  });
}

if (togglePasswordBtn && passwordEl) {
  togglePasswordBtn.addEventListener('click', () => {
    const reveal = passwordEl.type === 'password';
    passwordEl.type = reveal ? 'text' : 'password';
    togglePasswordBtn.textContent = reveal ? 'Masquer' : 'Afficher';
    togglePasswordBtn.setAttribute('aria-pressed', reveal ? 'true' : 'false');
    passwordEl.focus();
  });
}

if (formEl && submitBtn) {
  formEl.addEventListener('submit', () => {
    submitBtn.disabled = true;
    submitBtn.textContent = 'Connexion locale...';
  });
}

if (ssoBlock) {
  fetch('/api/auth/providers')
    .then((response) => (response.ok ? response.json() : null))
    .then((data) => {
      if (!data || !data.microsoft || data.microsoft.enabled !== true) {
        if (microsoftBtn) {
          microsoftBtn.removeAttribute('href');
          microsoftBtn.setAttribute('aria-disabled', 'true');
          microsoftBtn.textContent = 'SSO Microsoft indisponible';
        }
        if (dividerEl) {
          dividerEl.hidden = true;
        }
        showError("Le SSO Microsoft n'est pas configure sur cette instance. Le compte admin local reste disponible.");
        return;
      }
      if (microsoftBtn && !errorCode) {
        window.requestAnimationFrame(() => {
          microsoftBtn.focus();
        });
      }
    })
    .catch(() => {
      if (microsoftBtn) {
        microsoftBtn.removeAttribute('href');
        microsoftBtn.setAttribute('aria-disabled', 'true');
        microsoftBtn.textContent = 'SSO Microsoft indisponible';
      }
      if (dividerEl) {
        dividerEl.hidden = true;
      }
      showError("Impossible de verifier la configuration Microsoft. Le compte admin local reste disponible.");
    });
}

if (usernameEl && (!errorCode || errorCode === 'local_invalid')) {
  window.requestAnimationFrame(() => {
    usernameEl.focus();
  });
}
