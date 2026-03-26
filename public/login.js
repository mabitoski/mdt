const params = new URLSearchParams(window.location.search);
const errorEl = document.getElementById('login-error');
const ssoBlock = document.getElementById('login-sso');
const microsoftBtn = document.getElementById('login-microsoft-btn');

const errorMessages = {
  '1': 'Connexion Microsoft impossible. Reessayez.',
  group_required: "Votre compte Microsoft n'appartient a aucun groupe MDT autorise.",
  group_resolution_failed:
    "Impossible de verifier vos groupes Microsoft. Il faut probablement activer l'acces Graph pour l'application.",
  sso_only: 'Le login par mot de passe est desactive. Utilise Se connecter avec Microsoft.'
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
        showError("Le SSO Microsoft n'est pas configure sur cette instance.");
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
      showError("Impossible de verifier la configuration Microsoft.");
    });
}
