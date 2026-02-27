const listEl = document.getElementById('patchnote-list');

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

function formatDate(value) {
  if (!value) {
    return '--';
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return '--';
  }
  return date.toLocaleDateString('fr-FR', {
    day: '2-digit',
    month: 'short',
    year: 'numeric'
  });
}

function renderPatchnotes(items) {
  if (!listEl) {
    return;
  }
  if (!items || !items.length) {
    listEl.innerHTML = '<div class="patchnote-empty">Aucune patchnote disponible.</div>';
    return;
  }
  listEl.innerHTML = items
    .map((note, index) => {
      const version = escapeHtml(note.version || 'Version');
      const dateLabel = formatDate(note.createdAt);
      const content = escapeHtml(note.body || '').replace(/\n/g, '<br>');
      const openAttr = index === 0 ? ' open' : '';
      return `
        <details class="patchnote-item"${openAttr}>
          <summary class="patchnote-summary">
            <span class="patchnote-title">${version}</span>
            <span class="patchnote-date">${escapeHtml(dateLabel)}</span>
          </summary>
          <div class="patchnote-content">${content}</div>
        </details>
      `;
    })
    .join('');
}

async function loadPatchnotes() {
  if (!listEl) {
    return;
  }
  try {
    const response = await fetch('/api/patchnotes');
    if (response.status === 401) {
      window.location.href = '/login';
      return;
    }
    if (!response.ok) {
      throw new Error('patchnotes_fetch_failed');
    }
    const data = await response.json();
    renderPatchnotes(data.patchnotes || []);
  } catch (error) {
    listEl.innerHTML = '<div class="patchnote-empty">Impossible de charger les patchnotes.</div>';
  }
}

loadPatchnotes();
