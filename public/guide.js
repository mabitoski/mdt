const manualCsvImportForm = document.getElementById('manual-csv-import-form');
const manualCsvFileInput = document.getElementById('manual-csv-file');
const manualCsvSubmit = document.getElementById('manual-csv-submit');
const manualCsvFeedback = document.getElementById('manual-csv-feedback');
let canImportManualCsv = false;

function setFeedback(target, state, message) {
  if (!target) {
    return;
  }
  if (state) {
    target.dataset.state = state;
  } else {
    delete target.dataset.state;
  }
  target.textContent = message || '';
}

function readFileAsText(file) {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => resolve(typeof reader.result === 'string' ? reader.result : '');
    reader.onerror = () => reject(new Error('file_read_failed'));
    reader.readAsText(file);
  });
}

function setManualImportLoading(loading) {
  if (manualCsvFileInput) {
    manualCsvFileInput.disabled = loading || !canImportManualCsv;
  }
  if (manualCsvSubmit) {
    manualCsvSubmit.disabled = loading || !canImportManualCsv;
  }
}

async function initManualCsvPermissions() {
  if (!manualCsvImportForm) {
    return;
  }
  try {
    const response = await fetch('/api/me');
    if (response.status === 401) {
      window.location.href = '/login';
      return;
    }
    if (!response.ok) {
      return;
    }
    const data = await response.json();
    canImportManualCsv = Boolean(data && data.user && data.user.permissions && data.user.permissions.canImportManualCsv);
    if (!canImportManualCsv) {
      setFeedback(
        manualCsvFeedback,
        'info',
        "Import manuel reserve aux profils atelier autorises."
      );
    } else {
      setFeedback(manualCsvFeedback, '', '');
    }
  } catch (error) {
    canImportManualCsv = false;
  } finally {
    setManualImportLoading(false);
  }
}

function formatErrorList(errors) {
  const items = Array.isArray(errors) ? errors : [];
  if (!items.length) {
    return '';
  }
  return items
    .slice(0, 5)
    .map((item) => {
      const line = item && item.line ? `L${item.line}` : 'L?';
      const message = item && item.error ? item.error : 'Erreur inconnue';
      return `${line}: ${message}`;
    })
    .join(' | ');
}

async function submitManualCsvImport(event) {
  event.preventDefault();
  if (!canImportManualCsv) {
    setFeedback(manualCsvFeedback, 'error', "Pas les droits pour importer ce CSV.");
    return;
  }
  if (!manualCsvFileInput || !manualCsvFileInput.files || !manualCsvFileInput.files.length) {
    setFeedback(manualCsvFeedback, 'error', 'Selectionne un fichier CSV.');
    return;
  }

  const file = manualCsvFileInput.files[0];
  setManualImportLoading(true);
  setFeedback(manualCsvFeedback, 'info', `Lecture du fichier ${file.name}...`);

  try {
    const csvText = await readFileAsText(file);
    const response = await fetch('/api/reports/import-manual-csv', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        fileName: file.name,
        csvText
      })
    });
    if (response.status === 401) {
      window.location.href = '/login';
      return;
    }
    if (response.status === 403) {
      setFeedback(manualCsvFeedback, 'error', "Pas les droits pour importer ce CSV.");
      return;
    }

    let data = null;
    try {
      data = await response.json();
    } catch (error) {
      data = null;
    }

    if (!response.ok || !data || !data.ok) {
      const detail = formatErrorList(data && data.errors);
      setFeedback(
        manualCsvFeedback,
        'error',
        detail || "Import impossible. Verifie le format et les valeurs du CSV."
      );
      return;
    }

    const applied = Number.isFinite(data.appliedCount) ? data.appliedCount : 0;
    const skipped = Number.isFinite(data.skippedCount) ? data.skippedCount : 0;
    const detail = formatErrorList(data.errors);
    const message = skipped
      ? `${applied} ligne(s) importee(s), ${skipped} rejetee(s). ${detail}`
      : `${applied} ligne(s) importee(s) avec succes.`;
    setFeedback(manualCsvFeedback, 'success', message.trim());
    manualCsvImportForm.reset();
  } catch (error) {
    setFeedback(manualCsvFeedback, 'error', "Lecture ou import impossible pour ce fichier.");
  } finally {
    setManualImportLoading(false);
  }
}

if (manualCsvImportForm) {
  manualCsvImportForm.addEventListener('submit', submitManualCsvImport);
  initManualCsvPermissions();
}
