const params = new URLSearchParams(window.location.search);
const errorEl = document.getElementById('login-error');

if (params.get('error') === '1' && errorEl) {
  errorEl.classList.add('visible');
}
