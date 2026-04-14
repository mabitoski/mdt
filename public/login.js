const params = new URLSearchParams(window.location.search);
const errorEl = document.getElementById('login-error');
const ssoBlock = document.getElementById('login-sso');
const microsoftBtn = document.getElementById('login-microsoft-btn');
const confidentialityModal = document.getElementById('login-confidentiality-modal');
const confidentialityCloseBtn = document.getElementById('login-confidentiality-close');

const errorMessages = {
  '1': 'Connexion Microsoft impossible. Reessayez.',
  group_required: "Votre compte Microsoft n'appartient a aucun groupe MDT autorise.",
  group_resolution_failed:
    "Impossible de verifier vos groupes Microsoft. Il faut probablement activer l'acces Graph pour l'application.",
  sso_only: 'Le login par mot de passe est desactive. Utilise Se connecter avec Microsoft.'
};
let confidentialityModalOpen = false;

function focusMicrosoftButton() {
  if (!microsoftBtn || errorCode || confidentialityModalOpen) {
    return;
  }
  if (microsoftBtn.getAttribute('aria-disabled') === 'true') {
    return;
  }
  window.requestAnimationFrame(() => {
    microsoftBtn.focus();
  });
}

function closeConfidentialityModal() {
  if (!confidentialityModal) {
    return;
  }
  confidentialityModal.hidden = true;
  confidentialityModalOpen = false;
  focusMicrosoftButton();
}

function openConfidentialityModal() {
  if (!confidentialityModal) {
    return;
  }
  confidentialityModal.hidden = false;
  confidentialityModalOpen = true;
  if (confidentialityCloseBtn) {
    window.requestAnimationFrame(() => {
      confidentialityCloseBtn.focus();
    });
  }
}

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

if (confidentialityModal) {
  openConfidentialityModal();
}

if (confidentialityCloseBtn) {
  confidentialityCloseBtn.addEventListener('click', () => {
    closeConfidentialityModal();
  });
}

if (confidentialityModal) {
  confidentialityModal.addEventListener('click', (event) => {
    if (event.target === confidentialityModal) {
      closeConfidentialityModal();
    }
  });
}

document.addEventListener('keydown', (event) => {
  if (event.key === 'Escape' && confidentialityModalOpen) {
    closeConfidentialityModal();
  }
});

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
      focusMicrosoftButton();
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
