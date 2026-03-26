const params = new URLSearchParams(window.location.search);
const formEl = document.getElementById('login-form');
const errorEl = document.getElementById('login-error');
const usernameEl = document.getElementById('login-username');
const passwordEl = document.getElementById('login-password');
const togglePasswordBtn = document.getElementById('toggle-password');
const submitBtn = document.getElementById('login-submit');
const ssoBlock = document.getElementById('login-sso');
const dividerEl = document.getElementById('login-divider');
const microsoftBtn = document.getElementById('login-microsoft-btn');

if (params.get('error') === '1' && errorEl) {
  errorEl.classList.add('visible');
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
    submitBtn.textContent = 'Connexion...';
  });
}

if (microsoftBtn) {
  microsoftBtn.addEventListener('click', () => {
    microsoftBtn.setAttribute('aria-disabled', 'true');
    microsoftBtn.textContent = 'Redirection Microsoft...';
  });
}

if (ssoBlock && dividerEl) {
  fetch('/api/auth/providers')
    .then((response) => (response.ok ? response.json() : null))
    .then((data) => {
      if (!data || !data.microsoft || data.microsoft.enabled !== true) {
        ssoBlock.hidden = true;
        dividerEl.hidden = true;
      }
    })
    .catch(() => {
      ssoBlock.hidden = true;
      dividerEl.hidden = true;
    });
}

if (usernameEl && params.get('error') !== '1') {
  window.requestAnimationFrame(() => {
    usernameEl.focus();
  });
}
