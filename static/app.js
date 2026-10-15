/**
 * app.js — B2B Lead Extractor Frontend Logic
 * Vanilla JS: SSE streaming, sortable table, copy-to-clipboard, CSV/Excel export.
 * Zero external JS dependencies.
 */

'use strict';

// ─── State ────────────────────────────────────────────────────────────────────

const state = {
  leads:      [],        // Array<Lead>
  streaming:  false,
  startTime:  0,
  reqId:      '',
  sortCol:    null,
  sortDir:    'asc',
  elapsedTmr: null,
};

// ─── DOM refs ─────────────────────────────────────────────────────────────────

const $ = id => document.getElementById(id);

const inputCategory = $('input-category');
const inputLocation = $('input-location');
const inputVolume   = $('input-volume');
const inputStar     = $('input-star');
const btnExtract    = $('btn-extract');
const btnStop       = $('btn-stop');
const statusBar     = $('status-bar');
const progressTrack = $('progress-track');
const progressFill  = $('progress-fill');
const resultsSection= $('results-section');
const leadsTable    = $('leads-table');
const leadsBody     = $('leads-tbody');
const emptyState    = $('empty-state');
const exportBtns    = $('export-btns');
const valLeads      = $('val-leads');
const valEmails     = $('val-emails');
const valElapsed    = $('val-elapsed');
const statSpinner   = $('stat-spinner');
const modeHint      = $('mode-hint');
const toast         = $('toast');

// ─── Rating mode segmented toggle ─────────────────────────────────────────────

let ratingMode = 'exact';

document.querySelectorAll('.seg-btn').forEach(btn => {
  btn.addEventListener('click', () => {
    document.querySelectorAll('.seg-btn').forEach(b => b.classList.remove('active'));
    btn.classList.add('active');
    ratingMode = btn.dataset.mode;
    updateModeHint();
  });
});

function updateModeHint() {
  const s = parseInt(inputStar.value, 10);
  if (ratingMode === 'exact') {
    modeHint.textContent = s === 0
      ? 'All ratings accepted'
      : `Exact: round(rating) == ${s}.0`;
  } else {
    if (s === 0) {
      modeHint.textContent = 'All ratings accepted';
    } else {
      const minR = Math.max(1.0, s - 0.5).toFixed(1);
      const maxR = Math.min(5.0, s + 0.5).toFixed(1);
      modeHint.textContent = `Approx: ${minR} ≤ rating ≤ ${maxR}`;
    }
  }
}

inputStar.addEventListener('change', updateModeHint);
updateModeHint();

// ─── Preset chips ──────────────────────────────────────────────────────────────

document.querySelectorAll('.preset-chip').forEach(chip => {
  chip.addEventListener('click', () => {
    const targetId = chip.dataset.target;
    const targetInput = document.getElementById(targetId);
    if (targetInput) {
      targetInput.value = chip.textContent.trim();
      targetInput.focus();
    }
  });
});

// ─── Extract ────────────────────────────────────────────────────────────────────

btnExtract.addEventListener('click', startExtraction);
btnStop.addEventListener('click', stopExtraction);

function generateReqId() {
  return Math.random().toString(36).slice(2, 10);
}

function startExtraction() {
  const category = inputCategory.value.trim();
  const location = inputLocation.value.trim();
  const volume   = Math.min(500, Math.max(10, parseInt(inputVolume.value, 10) || 50));
  const star     = parseInt(inputStar.value, 10);

  if (!category) { showToast('Please enter a category.'); inputCategory.focus(); return; }
  if (!location) { showToast('Please enter a location.');  inputLocation.focus(); return; }

  // Reset UI
  state.leads     = [];
  state.streaming = true;
  state.reqId     = generateReqId();
  state.startTime = Date.now();

  leadsBody.innerHTML = '';
  emptyState.style.display = 'none';
  resultsSection.classList.add('visible');
  statusBar.classList.remove('hidden');
  progressTrack.classList.add('visible');
  progressFill.style.width = '0%';
  exportBtns.style.display = 'none';
  valLeads.textContent   = '0';
  valEmails.textContent  = '0';
  valElapsed.textContent = '0.0s';
  statSpinner.style.display = '';

  btnExtract.disabled = true;
  btnStop.style.display = '';

  // Elapsed timer
  clearInterval(state.elapsedTmr);
  state.elapsedTmr = setInterval(() => {
    if (!state.streaming) { clearInterval(state.elapsedTmr); return; }
    valElapsed.textContent = ((Date.now() - state.startTime) / 1000).toFixed(1) + 's';
  }, 200);

  // Build SSE URL
  const params = new URLSearchParams({
    category,
    location,
    volume,
    star,
    mode:   ratingMode,
    req_id: state.reqId,
  });
  const url = `/api/extract?${params.toString()}`;

  const es = new EventSource(url);

  es.onmessage = (event) => {
    try {
      const msg = JSON.parse(event.data);
      handleMessage(msg, volume, es);
    } catch (e) {
      console.error('SSE parse error', e);
    }
  };

  es.onerror = (err) => {
    console.error('SSE error', err);
    finishStream(es);
  };
}

function handleMessage(msg, volume, es) {
  if (msg.type === 'lead') {
    const lead = msg.data;
    state.leads.push(lead);
    appendRow(lead);

    const emailCount = state.leads.filter(l => l.email && l.email.length > 0).length;
    valLeads.textContent  = state.leads.length;
    valEmails.textContent = emailCount;

    // Progress
    const pct = Math.min(100, Math.round((state.leads.length / volume) * 100));
    progressFill.style.width = pct + '%';

  } else if (msg.type === 'done' || msg.type === 'error') {
    if (msg.type === 'error') {
      showToast('Error: ' + (msg.data?.message || 'Unknown'));
    }
    if (msg.data?.elapsed) {
      valElapsed.textContent = msg.data.elapsed + 's';
    }
    finishStream(es);
  }
}

function finishStream(es) {
  if (es) { try { es.close(); } catch (_) {} }
  state.streaming = false;
  clearInterval(state.elapsedTmr);

  btnExtract.disabled = false;
  btnStop.style.display = 'none';
  statSpinner.style.display = 'none';
  progressFill.style.width = '100%';

  if (state.leads.length > 0) {
    exportBtns.style.display = '';
  } else {
    emptyState.style.display = '';
  }

  showToast(`Extraction complete · ${state.leads.length} verified leads`);
}

async function stopExtraction() {
  if (state.reqId) {
    try {
      await fetch(`/api/stop/${state.reqId}`, { method: 'POST' });
    } catch (_) {}
  }
  state.streaming = false;
  finishStream(null);
}

// ─── Table rendering ──────────────────────────────────────────────────────────

function appendRow(lead) {
  const tr = document.createElement('tr');
  tr.dataset.name    = (lead.name    || '').toLowerCase();
  tr.dataset.address = (lead.address || '').toLowerCase();
  tr.dataset.rating  = lead.rating != null ? lead.rating : '';
  tr.dataset.email   = (lead.email   || '').toLowerCase();

  const ratingHTML = lead.rating != null
    ? `<span class="star-icon">★</span><span class="rating-val">${parseFloat(lead.rating).toFixed(1)}</span>`
    : `<span class="rating-empty">—</span>`;

  const emailHTML = lead.email
    ? `<a href="mailto:${esc(lead.email)}" style="color:var(--text-muted)">${esc(lead.email)}</a>`
    : '—';

  const websiteHTML = lead.website
    ? `<a class="website-link" href="${esc(lead.website)}" target="_blank" rel="noopener"
         title="${esc(lead.website)}">
         <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor"
              stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
           <path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6"/>
           <polyline points="15 3 21 3 21 9"/>
           <line x1="10" y1="14" x2="21" y2="3"/>
         </svg>
       </a>`
    : '—';

  tr.innerHTML = `
    <td class="col-index">${lead.index}</td>
    <td class="col-name">${esc(lead.name)}</td>
    <td class="col-address" title="${esc(lead.address)}">${esc(lead.address) || '—'}</td>
    <td class="col-rating">${ratingHTML}</td>
    <td class="col-phone">
      <span class="phone-val">${esc(lead.phone)}</span>
      <button class="phone-copy-btn" title="Copy number" data-phone="${esc(lead.phone)}">
        <svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor"
             stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round">
          <rect x="9" y="9" width="13" height="13" rx="2"/>
          <path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"/>
        </svg>
      </button>
    </td>
    <td class="col-email">${emailHTML}</td>
    <td class="col-website">${websiteHTML}</td>
  `;

  // Copy phone button
  tr.querySelector('.phone-copy-btn').addEventListener('click', function () {
    copyText(this.dataset.phone);
    this.classList.add('copied');
    setTimeout(() => this.classList.remove('copied'), 1500);
  });

  leadsBody.appendChild(tr);
}

function esc(str) {
  if (!str) return '';
  return String(str)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#039;');
}

// ─── Sorting ──────────────────────────────────────────────────────────────────

document.querySelectorAll('th.sortable').forEach(th => {
  th.addEventListener('click', () => {
    const col = th.dataset.col;
    if (state.sortCol === col) {
      state.sortDir = state.sortDir === 'asc' ? 'desc' : 'asc';
    } else {
      state.sortCol = col;
      state.sortDir = 'asc';
    }

    document.querySelectorAll('th.sortable').forEach(h => {
      h.classList.remove('sort-asc', 'sort-desc');
    });
    th.classList.add(state.sortDir === 'asc' ? 'sort-asc' : 'sort-desc');

    sortTable(col, state.sortDir);
  });
});

function sortTable(col, dir) {
  const rows = Array.from(leadsBody.querySelectorAll('tr'));
  rows.sort((a, b) => {
    const av = a.dataset[col] || '';
    const bv = b.dataset[col] || '';

    const an = parseFloat(av);
    const bn = parseFloat(bv);
    if (!isNaN(an) && !isNaN(bn)) {
      return dir === 'asc' ? an - bn : bn - an;
    }
    return dir === 'asc'
      ? av.localeCompare(bv)
      : bv.localeCompare(av);
  });

  rows.forEach((row, i) => {
    row.querySelector('.col-index').textContent = i + 1;
    leadsBody.appendChild(row);
  });
}

// ─── Exports ──────────────────────────────────────────────────────────────────

function leadsToTSV(textSafePhones = false) {
  const headers = ['#', 'Business Name', 'Address', 'Rating', 'Phone Number', 'Email', 'Website'];
  const rows = state.leads.map((l, i) => {
    let phoneVal = l.phone || '';
    if (textSafePhones && phoneVal.startsWith('+')) {
      phoneVal = "'" + phoneVal;
    }
    return [
      i + 1,
      l.name    || '',
      l.address || '',
      l.rating  != null ? parseFloat(l.rating).toFixed(1) : '',
      phoneVal,
      l.email   || '',
      l.website || '',
    ];
  });
  return [headers, ...rows].map(r => r.join('\t')).join('\n');
}

function leadsToCSV() {
  const headers = ['#', 'Business Name', 'Address', 'Rating', 'Phone Number', 'Email', 'Website'];
  const csvEsc = v => `"${String(v).replace(/"/g, '""')}"`;
  const rows = state.leads.map((l, i) => [
    i + 1,
    l.name    || '',
    l.address || '',
    l.rating  != null ? parseFloat(l.rating).toFixed(1) : '',
    l.phone   || '',
    l.email   || '',
    l.website || '',
  ]);
  return '\uFEFF' + [headers, ...rows].map(r => r.map(csvEsc).join(',')).join('\r\n');
}

// 1. Open in Google Sheets
$('btn-open-sheets')?.addEventListener('click', () => {
  if (!state.leads.length) {
    showToast('No leads to open');
    return;
  }
  const tsv = leadsToTSV(true);
  copyText(tsv);
  showToast('Leads copied! Press Ctrl+V in the new Google Sheet', 3500);
  window.open('https://sheets.new', '_blank');
});

// 2. Download Google Sheets (.xlsx)
$('btn-sheets-xlsx')?.addEventListener('click', async () => {
  if (!state.leads.length) {
    showToast('No leads to export');
    return;
  }
  try {
    showToast('Generating Google Sheets .xlsx...');
    const resp = await fetch('/api/export/sheets', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(state.leads),
    });
    if (!resp.ok) throw new Error('Export request failed');
    const blob = await resp.blob();
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = 'leads_export.xlsx';
    a.click();
    URL.revokeObjectURL(url);
    showToast('Downloaded leads_export.xlsx (Ready for Google Sheets)');
  } catch (err) {
    console.error('Export error', err);
    showToast('Failed to export .xlsx');
  }
});

// 3. Copy TSV
$('btn-copy-tsv')?.addEventListener('click', () => {
  copyText(leadsToTSV(false));
  showToast('TSV copied — paste directly into Google Sheets');
});

// 4. Download CSV
$('btn-csv')?.addEventListener('click', () => {
  downloadFile(leadsToCSV(), 'leads.csv', 'text/csv;charset=utf-8;');
  showToast('CSV downloaded');
});

// 5. Download Excel (.csv with UTF-8 BOM)
$('btn-excel')?.addEventListener('click', () => {
  downloadFile(leadsToCSV(), 'leads_excel.csv', 'text/csv;charset=utf-8;');
  showToast('Excel-compatible CSV downloaded');
});

function downloadFile(content, filename, mime) {
  const blob = new Blob([content], { type: mime });
  const url  = URL.createObjectURL(blob);
  const a    = document.createElement('a');
  a.href     = url;
  a.download = filename;
  a.click();
  URL.revokeObjectURL(url);
}

// ─── Utilities ────────────────────────────────────────────────────────────────

function copyText(text) {
  if (navigator.clipboard?.writeText) {
    navigator.clipboard.writeText(text).catch(() => legacyCopy(text));
  } else {
    legacyCopy(text);
  }
}

function legacyCopy(text) {
  const ta = document.createElement('textarea');
  ta.value = text;
  ta.style.position = 'fixed';
  ta.style.opacity  = '0';
  document.body.appendChild(ta);
  ta.select();
  document.execCommand('copy');
  document.body.removeChild(ta);
}

let toastTimer = null;
function showToast(message, duration = 2800) {
  toast.textContent = message;
  toast.classList.add('show');
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => toast.classList.remove('show'), duration);
}
