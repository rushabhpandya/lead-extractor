/**
 * app.js — AI Based Location Scraper Frontend Logic
 * Clean Enterprise Interface
 * Vanilla JS: SSE streaming, sortable table, copy-to-clipboard, CSV/Excel/Sheets export.
 * Zero external JS dependencies.
 */

'use strict';

// ─── State ────────────────────────────────────────────────────────────────────

let activeRunId = 0;
let activeEventSource = null;

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
const tableSearch   = $('table-search');
const gridCounter   = $('grid-counter');

// ─── Active Preset Chips Highlighting ─────────────────────────────────────────

function updateChips() {
  const cat = (inputCategory.value || '').trim();
  const loc = (inputLocation.value || '').trim();

  document.querySelectorAll('#category-presets .preset-chip').forEach(chip => {
    if (chip.textContent.trim().toLowerCase() === cat.toLowerCase()) {
      chip.classList.add('active-chip');
    } else {
      chip.classList.remove('active-chip');
    }
  });

  document.querySelectorAll('#location-presets .preset-chip').forEach(chip => {
    if (chip.textContent.trim().toLowerCase() === loc.toLowerCase()) {
      chip.classList.add('active-chip');
    } else {
      chip.classList.remove('active-chip');
    }
  });
}

inputCategory.addEventListener('input', updateChips);
inputLocation.addEventListener('input', updateChips);

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
      updateChips();
    }
  });
});

// ─── Demo Pills ────────────────────────────────────────────────────────────────

document.querySelectorAll('.demo-pill').forEach(pill => {
  pill.addEventListener('click', () => {
    inputCategory.value = pill.dataset.cat;
    inputLocation.value = pill.dataset.loc;
    inputVolume.value   = pill.dataset.vol;
    updateChips();
    startExtraction();
  });
});

// Initial chip sync
updateChips();

// ─── Live Table Search ─────────────────────────────────────────────────────────

if (tableSearch) {
  tableSearch.addEventListener('input', () => {
    const query = tableSearch.value.trim().toLowerCase();
    const rows = leadsBody.querySelectorAll('tr');
    let visibleCount = 0;
    rows.forEach(row => {
      const match = !query || row.textContent.toLowerCase().includes(query);
      row.style.display = match ? '' : 'none';
      if (match) visibleCount++;
    });
    if (gridCounter) {
      gridCounter.textContent = query
        ? `${visibleCount} of ${state.leads.length} matching`
        : `${state.leads.length} loaded`;
    }
  });
}

// ─── Extract ────────────────────────────────────────────────────────────────────

btnExtract.addEventListener('click', startExtraction);
btnStop.addEventListener('click', stopExtraction);

function generateReqId() {
  return Math.random().toString(36).slice(2, 10);
}

function startExtraction() {
  const category = inputCategory.value.trim();
  const location = inputLocation.value.trim();
  const volume   = Math.min(200, Math.max(10, parseInt(inputVolume.value, 10) || 50));
  const star     = parseInt(inputStar.value, 10);

  if (!category) { showToast('Please enter a category.'); inputCategory.focus(); return; }
  if (!location) { showToast('Please enter a location.');  inputLocation.focus(); return; }

  updateChips();

  // Increment run ID and lock locally to prevent interleaved streams
  activeRunId++;
  const myRunId = activeRunId;

  // Immediately abort/close previous active stream
  if (activeEventSource) {
    try { activeEventSource.close(); } catch (_) {}
    activeEventSource = null;
  }

  // Reset UI and state
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
  valLeads.textContent   = '0';
  valEmails.textContent  = '0';
  valElapsed.textContent = '0.0s';
  statSpinner.style.display = '';

  if (gridCounter) gridCounter.textContent = 'Extracting...';
  if (tableSearch) tableSearch.value = '';

  btnExtract.disabled = true;
  btnStop.style.display = '';

  // Elapsed timer
  clearInterval(state.elapsedTmr);
  state.elapsedTmr = setInterval(() => {
    if (!state.streaming || myRunId !== activeRunId) { clearInterval(state.elapsedTmr); return; }
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
  activeEventSource = es;

  es.onmessage = (event) => {
    if (myRunId !== activeRunId) {
      try { es.close(); } catch (_) {}
      return;
    }
    try {
      const msg = JSON.parse(event.data);
      handleMessage(msg, volume, es, myRunId);
    } catch (e) {
      console.error('SSE parse error', e);
    }
  };

  es.onerror = (err) => {
    if (myRunId !== activeRunId) return;
    console.error('SSE error', err);
    finishStream(es);
  };
}

function handleMessage(msg, volume, es, myRunId) {
  if (myRunId !== activeRunId) return;

  if (msg.type === 'lead') {
    const lead = msg.data;
    state.leads.push(lead);
    appendRow(lead);

    const emailCount = state.leads.filter(l => l.email && l.email.length > 0).length;
    valLeads.textContent  = state.leads.length;
    valEmails.textContent = emailCount;

    if (gridCounter) {
      gridCounter.textContent = `${state.leads.length} loaded`;
    }

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
  if (activeEventSource === es) { activeEventSource = null; }
  state.streaming = false;
  clearInterval(state.elapsedTmr);

  btnExtract.disabled = false;
  btnStop.style.display = 'none';
  statSpinner.style.display = 'none';
  progressFill.style.width = '100%';

  if (state.leads.length > 0) {
    if (emptyState) emptyState.style.display = 'none';
    if (gridCounter) gridCounter.textContent = `${state.leads.length} loaded`;
  } else {
    if (emptyState) emptyState.style.display = '';
    if (gridCounter) gridCounter.textContent = `0 loaded`;
  }

  showToast(`Extraction complete · ${state.leads.length} verified leads`);
}

async function stopExtraction() {
  activeRunId++; // invalidate any active stream callbacks immediately
  if (activeEventSource) {
    try { activeEventSource.close(); } catch (_) {}
    activeEventSource = null;
  }
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
  const cleanName = (lead.name || '').replace(/\s*\(Branch\s*\d+\)/gi, '');
  const rowNum = leadsBody.rows.length + 1;
  const tr = document.createElement('tr');
  tr.dataset.name    = cleanName.toLowerCase();
  tr.dataset.address = (lead.address || '').toLowerCase();
  tr.dataset.rating  = lead.rating != null ? lead.rating : '';
  tr.dataset.email   = (lead.email   || '').toLowerCase();

  // Monogram initials (2 letters)
  const words = cleanName.trim().split(/\s+/).filter(Boolean);
  let initials = 'BZ';
  if (words.length >= 2) {
    initials = (words[0][0] + words[1][0]).toUpperCase();
  } else if (words.length === 1 && words[0].length >= 2) {
    initials = words[0].slice(0, 2).toUpperCase();
  }

  const ratingVal = lead.rating != null ? parseFloat(lead.rating) : null;
  const ratingPct = ratingVal != null ? Math.min(100, Math.round((ratingVal / 5.0) * 100)) : 0;

  const ratingHTML = ratingVal != null
    ? `<div class="rating-cell-wrap">
         <div class="rating-pill">
           <span class="star-icon">★</span>
           <span class="rating-val">${ratingVal.toFixed(1)}</span>
         </div>
         <div class="rating-mini-bar" title="${ratingVal.toFixed(1)} / 5.0">
           <div class="rating-mini-fill" style="width: ${ratingPct}%"></div>
         </div>
       </div>`
    : `<span class="rating-empty">—</span>`;

  const emailHTML = lead.email
    ? `<div class="email-wrap">
         <svg class="email-icon" width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
           <path d="M4 4h16c1.1 0 2 .9 2 2v12c0 1.1-.9 2-2 2H4c-1.1 0-2-.9-2-2V6c0-1.1.9-2 2-2z"/>
           <polyline points="22,6 12,13 2,6"/>
         </svg>
         <a class="email-link" href="mailto:${esc(lead.email)}" title="${esc(lead.email)}">${esc(lead.email)}</a>
       </div>`
    : '<span class="empty-field">—</span>';

  const websiteHTML = lead.website
    ? `<a class="website-link" href="${esc(lead.website)}" target="_blank" rel="noopener" title="${esc(lead.website)}">
         <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
           <path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6"/>
           <polyline points="15 3 21 3 21 9"/>
           <line x1="10" y1="14" x2="21" y2="3"/>
         </svg>
       </a>`
    : '<span class="empty-field">—</span>';

  tr.innerHTML = `
    <td class="col-index">${rowNum}</td>
    <td class="col-name">
      <div class="lead-biz-wrap">
        <div class="biz-avatar">${esc(initials)}</div>
        <div class="biz-info">
          <div class="biz-title">
            <span>${esc(cleanName)}</span>
            <span class="verified-badge" title="Verified Phone">✓</span>
          </div>
        </div>
      </div>
    </td>
    <td class="col-address" title="${esc(lead.address)}">
      <div class="address-wrap">
        <span class="loc-pin">📍</span>
        <span class="address-text">${esc(lead.address) || '—'}</span>
      </div>
    </td>
    <td class="col-rating">${ratingHTML}</td>
    <td class="col-phone">
      <div class="phone-wrap">
        <span class="phone-badge">+91</span>
        <span class="phone-val">${esc(lead.phone)}</span>
        <button class="phone-copy-btn" title="Copy number" data-phone="${esc(lead.phone)}">
          <svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round">
            <rect x="9" y="9" width="13" height="13" rx="2"/>
            <path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"/>
          </svg>
        </button>
      </div>
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
    const cleanName = (l.name || '').replace(/\s*\(Branch\s*\d+\)/gi, '');
    return [
      i + 1,
      cleanName,
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
  const rows = state.leads.map((l, i) => {
    const cleanName = (l.name || '').replace(/\s*\(Branch\s*\d+\)/gi, '');
    return [
      i + 1,
      cleanName,
      l.address || '',
      l.rating  != null ? parseFloat(l.rating).toFixed(1) : '',
      l.phone   || '',
      l.email   || '',
      l.website || '',
    ];
  });
  return '\uFEFF' + [headers, ...rows].map(r => r.map(csvEsc).join(',')).join('\r\n');
}

// 1. Open in Google Sheets
$('btn-open-sheets')?.addEventListener('click', () => {
  if (!state.leads.length) {
    showToast('No leads to open — run an extraction first');
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
    showToast('No leads to export — run an extraction first');
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
    showToast('Downloaded leads_export.xlsx');
  } catch (err) {
    console.error('Export error', err);
    showToast('Failed to export .xlsx');
  }
});

// 3. Copy TSV
$('btn-copy-tsv')?.addEventListener('click', () => {
  if (!state.leads.length) {
    showToast('No leads to copy — run an extraction first');
    return;
  }
  copyText(leadsToTSV(false));
  showToast('TSV copied — paste directly into Google Sheets');
});

// 4. Download CSV
$('btn-csv')?.addEventListener('click', () => {
  if (!state.leads.length) {
    showToast('No leads to export — run an extraction first');
    return;
  }
  downloadFile(leadsToCSV(), 'leads.csv', 'text/csv;charset=utf-8;');
  showToast('CSV downloaded');
});

// 5. Download Excel (.csv with UTF-8 BOM)
$('btn-excel')?.addEventListener('click', () => {
  if (!state.leads.length) {
    showToast('No leads to export — run an extraction first');
    return;
  }
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
