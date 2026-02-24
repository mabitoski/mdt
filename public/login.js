const params = new URLSearchParams(window.location.search);
const formEl = document.getElementById('login-form');
const errorEl = document.getElementById('login-error');
const usernameEl = document.getElementById('login-username');
const passwordEl = document.getElementById('login-password');
const togglePasswordBtn = document.getElementById('toggle-password');
const submitBtn = document.getElementById('login-submit');

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

if (usernameEl && params.get('error') !== '1') {
  window.requestAnimationFrame(() => {
    usernameEl.focus();
  });
}
