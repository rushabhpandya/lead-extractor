/**
 * app.js — AI Based Location Scraper Frontend Logic
 * Outbound Intelligence Edition (v=outbound2026)
 * Vanilla JS: SSE streaming, live telemetry, high-intent filter pills,
 * zero-request marketing signal badges, 1-click WhatsApp, and filtered exports.
 * Zero external JS dependencies.
 */

'use strict';

// ─── State ────────────────────────────────────────────────────────────────────

let activeRunId = 0;
let activeEventSource = null;

const state = {
  leads:        [],        // Array<Lead>
  streaming:    false,
  startTime:    0,
  reqId:        '',
  sortCol:      'score',   // Default sort by Lead Score
  sortDir:      'desc',    // Highest value first
  elapsedTmr:   null,
  activeFilter: 'all',     // 'all' | 'mx' | 'ig' | 'wa' | 'ads' | 'noads'
};

// ─── DOM refs ─────────────────────────────────────────────────────────────────

const $ = id => document.getElementById(id);

const inputCategory   = $('input-category');
const inputLocation   = $('input-location');
const inputVolume     = $('input-volume');
const inputStar       = $('input-star');
const chkBypassCache  = $('chk-bypass-cache');
const btnExtract      = $('btn-extract');
const btnStop         = $('btn-stop');
const btnStopKeep     = $('btn-stop-keep');
const telemetryBar    = $('telemetry-bar');
const telemetryStage  = $('telemetry-stage');
const telemetryMsg    = $('telemetry-msg');
const statusBar       = $('status-bar');
const progressTrack   = $('progress-track');
const progressFill    = $('progress-fill');
const resultsSection  = $('results-section');
const leadsTable      = $('leads-table');
const leadsBody       = $('leads-tbody');
const emptyState      = $('empty-state');
const exportBtns      = $('export-btns');
const valLeads        = $('val-leads');
const valEmails       = $('val-emails');
const valPhones       = $('val-phones');
const valPhoneTag     = $('val-phone-tag');
const valPhoneSub     = $('val-phone-sub');
const valElapsed      = $('val-elapsed');
const statSpinner     = $('stat-spinner');
const modeHint        = $('mode-hint');
const toast           = $('toast');
const tableSearch     = $('table-search');
const gridCounter     = $('grid-counter');
const btnCopyEmails   = $('btn-copy-emails');

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

// ─── High-Intent Filter Pills & Search ────────────────────────────────────────

function leadMatchesFilter(lead, filter) {
  if (!filter || filter === 'all') return true;
  if (filter === 'mx') return Boolean(lead.email_mx_verified && lead.email);
  if (filter === 'ig') return Boolean(lead.instagram_handle);
  if (filter === 'wa') return Boolean(lead.whatsapp_url || lead.is_whatsapp_ready);
  if (filter === 'ads') return Boolean(lead.has_meta_pixel || lead.has_google_ads);
  if (filter === 'noads') return Boolean(!lead.has_meta_pixel && !lead.has_google_ads);
  return true;
}

function leadMatchesSearch(lead, query) {
  if (!query) return true;
  const q = query.toLowerCase();
  const searchStr = [
    lead.name,
    lead.address,
    lead.phone,
    lead.email,
    lead.instagram_handle ? '@' + lead.instagram_handle : '',
    lead.website,
    lead.cms_platform,
  ].filter(Boolean).join(' ').toLowerCase();
  return searchStr.includes(q);
}

function getFilteredLeads() {
  const query = (tableSearch?.value || '').trim();
  return state.leads.filter(l =>
    leadMatchesFilter(l, state.activeFilter) && leadMatchesSearch(l, query)
  );
}

function applyFiltersAndSearch() {
  const query = (tableSearch?.value || '').trim().toLowerCase();
  const rows = leadsBody.querySelectorAll('tr');
  let visibleCount = 0;

  rows.forEach(row => {
    const isMx = row.dataset.mx === '1';
    const isIg = row.dataset.ig === '1';
    const isWa = row.dataset.wa === '1';
    const hasAds = row.dataset.ads === '1';

    let filterMatch = true;
    if (state.activeFilter === 'mx') filterMatch = isMx;
    else if (state.activeFilter === 'ig') filterMatch = isIg;
    else if (state.activeFilter === 'wa') filterMatch = isWa;
    else if (state.activeFilter === 'ads') filterMatch = hasAds;
    else if (state.activeFilter === 'noads') filterMatch = !hasAds;

    const searchMatch = !query || row.textContent.toLowerCase().includes(query);
    const visible = filterMatch && searchMatch;

    row.style.display = visible ? '' : 'none';
    if (visible) {
      visibleCount++;
      row.querySelector('.col-index').textContent = visibleCount;
    }
  });

  if (gridCounter) {
    if (query || state.activeFilter !== 'all') {
      gridCounter.textContent = `${visibleCount} of ${state.leads.length} filtered`;
    } else {
      gridCounter.textContent = `${state.leads.length} loaded`;
    }
  }
}

if (tableSearch) {
  tableSearch.addEventListener('input', applyFiltersAndSearch);
}

document.querySelectorAll('.filter-pill').forEach(btn => {
  btn.addEventListener('click', () => {
    document.querySelectorAll('.filter-pill').forEach(b => b.classList.remove('active'));
    btn.classList.add('active');
    state.activeFilter = btn.dataset.filter || 'all';
    applyFiltersAndSearch();
  });
});

// ─── Extract Pipeline ──────────────────────────────────────────────────────────

btnExtract.addEventListener('click', startExtraction);
btnStop.addEventListener('click', () => stopExtraction(false));
btnStopKeep?.addEventListener('click', () => stopExtraction(true));

function generateReqId() {
  return Math.random().toString(36).slice(2, 10);
}

function startExtraction() {
  const category = inputCategory.value.trim();
  const location = inputLocation.value.trim();
  const volume   = Math.min(1000, Math.max(10, parseInt(inputVolume.value, 10) || 50));
  const star     = parseInt(inputStar.value, 10);
  const bypassCache = chkBypassCache ? chkBypassCache.checked : false;

  if (!category) { showToast('Please enter a category.'); inputCategory.focus(); return; }
  if (!location) { showToast('Please enter a location.');  inputLocation.focus(); return; }

  updateChips();

  activeRunId++;
  const myRunId = activeRunId;

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
  if (valPhones)   valPhones.textContent   = '100%';
  if (valPhoneTag) valPhoneTag.textContent = '100%';
  if (valPhoneSub) valPhoneSub.textContent = '100% Verified';
  valElapsed.textContent = '0.0s';
  statSpinner.style.display = '';

  if (telemetryBar) {
    telemetryBar.style.display = 'flex';
    if (telemetryStage) telemetryStage.textContent = 'INIT';
    if (telemetryMsg) telemetryMsg.textContent = bypassCache
      ? 'Fresh Live Crawl requested — connecting to pipeline...'
      : 'Querying persistent SQLite cache and discovery engines...';
  }

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
    mode:         ratingMode,
    req_id:       state.reqId,
    bypass_cache: bypassCache ? 'true' : 'false',
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

  if (msg.type === 'status') {
    if (telemetryBar) telemetryBar.style.display = 'flex';
    if (telemetryStage) telemetryStage.textContent = (msg.stage || 'PROGRESS').toUpperCase();
    if (telemetryMsg) telemetryMsg.textContent = msg.message || 'Processing...';

    if (msg.verified != null && msg.verified > 0) {
      valLeads.textContent = msg.verified;
      const pct = Math.min(100, Math.round((msg.verified / volume) * 100));
      progressFill.style.width = pct + '%';
    }

  } else if (msg.type === 'lead') {
    const lead = msg.data;
    state.leads.push(lead);
    appendRow(lead);

    const emailCount = state.leads.filter(l => l.email && l.email.length > 0).length;
    const phoneCount = state.leads.filter(l => l.phone && l.phone.trim().length > 0).length;
    const phonePct = state.leads.length > 0 ? Math.round((phoneCount / state.leads.length) * 100) : 100;
    valLeads.textContent  = state.leads.length;
    valEmails.textContent = emailCount;
    if (valPhones)   valPhones.textContent   = `${phonePct}%`;
    if (valPhoneTag) valPhoneTag.textContent = `${phonePct}%`;
    if (valPhoneSub) valPhoneSub.textContent = `${phoneCount}/${state.leads.length} Callable`;

    applyFiltersAndSearch();

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
  if (telemetryBar) telemetryBar.style.display = 'none';

  if (state.leads.length > 0) {
    if (emptyState) emptyState.style.display = 'none';
    applyFiltersAndSearch();
  } else {
    if (emptyState) emptyState.style.display = '';
    if (gridCounter) gridCounter.textContent = `0 loaded`;
  }

  showToast(`Extraction complete · ${state.leads.length} verified leads`);
}

async function stopExtraction(keepLeads = true) {
  activeRunId++;
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
  clearInterval(state.elapsedTmr);

  btnExtract.disabled = false;
  btnStop.style.display = 'none';
  statSpinner.style.display = 'none';
  if (telemetryBar) telemetryBar.style.display = 'none';

  if (keepLeads && state.leads.length > 0) {
    showToast(`Stopped · Retained ${state.leads.length} leads in terminal`);
    applyFiltersAndSearch();
  } else {
    finishStream(null);
  }
}

// ─── Table Rendering with Signal Badges & 1-Click WhatsApp ──────────────────

function appendRow(lead) {
  const cleanName = (lead.name || '').replace(/\s*\(Branch\s*\d+\)/gi, '');
  const rowNum = leadsBody.rows.length + 1;
  const tr = document.createElement('tr');

  const leadScore = lead.lead_score != null ? parseInt(lead.lead_score, 10) : 0;
  tr.dataset.name    = cleanName.toLowerCase();
  tr.dataset.score   = leadScore;
  tr.dataset.address = (lead.address || '').toLowerCase();
  tr.dataset.rating  = lead.rating != null ? lead.rating : '';
  tr.dataset.email   = (lead.email   || '').toLowerCase();
  tr.dataset.mx      = lead.email_mx_verified ? '1' : '0';
  tr.dataset.ig      = lead.instagram_handle ? '1' : '0';
  tr.dataset.wa      = (lead.whatsapp_url || lead.is_whatsapp_ready) ? '1' : '0';
  tr.dataset.ads     = (lead.has_meta_pixel || lead.has_google_ads) ? '1' : '0';

  // Monogram initials (2 letters)
  const words = cleanName.trim().split(/\s+/).filter(Boolean);
  let initials = 'BZ';
  if (words.length >= 2) {
    initials = (words[0][0] + words[1][0]).toUpperCase();
  } else if (words.length === 1 && words[0].length >= 2) {
    initials = words[0].slice(0, 2).toUpperCase();
  }

  // 1. Signals Column
  const scoreClass = leadScore >= 70 ? 'score-high' : leadScore >= 40 ? 'score-med' : 'score-low';
  const metaBadgeHTML = lead.has_meta_pixel
    ? `<span class="signal-tag meta-badge" title="Meta Pixel Detected">META</span>`
    : '';
  const gadsBadgeHTML = lead.has_google_ads
    ? `<span class="signal-tag gads-badge" title="Google Ads / GTM Detected">G-ADS</span>`
    : '';
  const cmsPlatform = lead.cms_platform && lead.cms_platform !== 'Custom'
    ? `<span class="signal-tag cms-badge" title="CMS: ${esc(lead.cms_platform)}">${esc(lead.cms_platform)}</span>`
    : '';

  const signalsHTML = `
    <div class="signals-cell-wrap">
      <span class="score-badge ${scoreClass}" title="Outreach Readiness Score: ${leadScore}/100">${leadScore}</span>
      ${metaBadgeHTML}
      ${gadsBadgeHTML}
      ${cmsPlatform}
    </div>`;

  // 2. Rating Column
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

  // 3. Email Column with MX verification dot
  const mxDotHTML = lead.email_mx_verified
    ? `<span class="mx-check-dot" title="MX Deliverability Verified"></span>`
    : '';
  const emailHTML = lead.email
    ? `<div class="email-wrap">
         <svg class="email-icon" width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
           <path d="M4 4h16c1.1 0 2 .9 2 2v12c0 1.1-.9 2-2 2H4c-1.1 0-2-.9-2-2V6c0-1.1.9-2 2-2z"/>
           <polyline points="22,6 12,13 2,6"/>
         </svg>
         <a class="email-link" href="mailto:${esc(lead.email)}" title="${esc(lead.email)}">${esc(lead.email)}</a>
         ${mxDotHTML}
       </div>`
    : '<span class="empty-field">—</span>';

  // 4. Instagram Column
  const igHandle = lead.instagram_handle || null;
  const igUrl    = lead.instagram_url    || (igHandle ? `https://www.instagram.com/${igHandle}/` : null);
  const igHTML = igHandle && igUrl
    ? `<a class="instagram-pill" href="${esc(igUrl)}" target="_blank" rel="noopener" title="${esc(igUrl)}">
         <svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
           <rect x="2" y="2" width="20" height="20" rx="5" ry="5"/>
           <path d="M16 11.37A4 4 0 1 1 12.63 8 4 4 0 0 1 16 11.37z"/>
           <line x1="17.5" y1="6.5" x2="17.51" y2="6.5"/>
         </svg>
         @${esc(igHandle)}
       </a>`
    : '<span class="empty-field">—</span>';

  // 5. Website Column
  const websiteHTML = lead.website
    ? `<a class="website-link" href="${esc(lead.website)}" target="_blank" rel="noopener" title="${esc(lead.website)}">
         <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
           <path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6"/>
           <polyline points="15 3 21 3 21 9"/>
           <line x1="10" y1="14" x2="21" y2="3"/>
         </svg>
       </a>`
    : '<span class="empty-field">—</span>';

  // 6. Phone Column with WhatsApp 1-Click Button & Copy
  const rawPhone = (lead.phone || '').trim();
  let phoneCellHTML = '<span class="empty-field">—</span>';
  if (rawPhone) {
    const phoneStr = esc(rawPhone);
    const countryBadge = (() => {
      if (rawPhone.startsWith('+91')) return 'IN';
      if (rawPhone.startsWith('+44')) return 'GB';
      if (rawPhone.startsWith('+39')) return 'IT';
      if (rawPhone.startsWith('+33')) return 'FR';
      if (rawPhone.startsWith('+49')) return 'DE';
      if (rawPhone.startsWith('+34')) return 'ES';
      if (rawPhone.startsWith('+1'))  return 'US';
      if (rawPhone.startsWith('+971')) return 'AE';
      if (rawPhone.startsWith('+41')) return 'CH';
      if (rawPhone.startsWith('+81')) return 'JP';
      if (rawPhone.startsWith('+65')) return 'SG';
      if (rawPhone.startsWith('+61')) return 'AU';
      if (rawPhone.startsWith('+31')) return 'NL';
      if (lead.country_code) return esc(lead.country_code.toUpperCase());
      return '';
    })();

    const waBtnHTML = lead.whatsapp_url
      ? `<a class="wa-btn" href="${esc(lead.whatsapp_url)}" target="_blank" rel="noopener" title="1-Click WhatsApp Chat">
           <svg width="11" height="11" viewBox="0 0 24 24" fill="currentColor">
             <path d="M12.04 2C6.58 2 2.13 6.45 2.13 11.91c0 1.75.46 3.45 1.32 4.95L2.05 22l5.25-1.38c1.45.79 3.08 1.21 4.74 1.21 5.46 0 9.91-4.45 9.91-9.91 0-2.65-1.03-5.14-2.9-7.01A9.816 9.816 0 0 0 12.04 2m.01 1.67c2.2 0 4.26.86 5.82 2.42a8.225 8.225 0 0 1 2.41 5.83c0 4.54-3.7 8.24-8.24 8.24-1.48 0-2.93-.4-4.2-1.15l-.3-.18-3.12.82.83-3.04-.2-.31a8.196 8.196 0 0 1-1.26-4.38c0-4.54 3.7-8.24 8.24-8.24m4.52 11.66c-.25-.13-1.47-.72-1.7-.81-.23-.08-.39-.13-.56.13-.17.25-.64.81-.79.97-.14.17-.29.19-.54.06-.25-.13-1.06-.39-2.02-1.25-.75-.67-1.26-1.5-1.41-1.75-.15-.25-.02-.39.11-.51.11-.11.25-.29.38-.44.13-.14.17-.25.25-.42.08-.17.04-.31-.02-.44-.06-.13-.56-1.34-.76-1.84-.2-.48-.4-.42-.56-.43h-.48c-.17 0-.44.06-.67.31-.23.25-.88.86-.88 2.1s.9 2.43 1.02 2.6c.13.17 1.77 2.7 4.29 3.79.6.26 1.07.41 1.44.53.6.19 1.15.16 1.58.1.48-.07 1.47-.6 1.68-1.18.21-.58.21-1.07.15-1.18-.06-.11-.23-.17-.48-.3"/>
           </svg>
         </a>`
      : '';

    phoneCellHTML = `
      <div class="phone-wrap">
        ${countryBadge ? `<span class="phone-badge">${countryBadge}</span>` : ''}
        <span class="phone-val">${phoneStr}</span>
        ${waBtnHTML}
        <button class="phone-copy-btn" title="Copy number" data-phone="${phoneStr}">
          <svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round">
            <rect x="9" y="9" width="13" height="13" rx="2"/>
            <path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"/>
          </svg>
        </button>
      </div>`;
  }

  tr.innerHTML = `
    <td class="col-index">${rowNum}</td>
    <td class="col-name">
      <div class="lead-biz-wrap">
        <div class="biz-avatar">${esc(initials)}</div>
        <div class="biz-info">
          <div class="biz-title">
            <span>${esc(cleanName)}</span>
            <span class="verified-badge" title="Verified Callable Phone">✓</span>
          </div>
        </div>
      </div>
    </td>
    <td class="col-signals">${signalsHTML}</td>
    <td class="col-address" title="${esc(lead.address)}">
      <div class="address-wrap">
        <span class="loc-pin">📍</span>
        <span class="address-text">${esc(lead.address) || '—'}</span>
      </div>
    </td>
    <td class="col-rating">${ratingHTML}</td>
    <td class="col-phone">${phoneCellHTML}</td>
    <td class="col-email">${emailHTML}</td>
    <td class="col-instagram">${igHTML}</td>
    <td class="col-website">${websiteHTML}</td>
  `;

  const copyBtn = tr.querySelector('.phone-copy-btn');
  if (copyBtn) {
    copyBtn.addEventListener('click', function () {
      copyText(this.dataset.phone);
      this.classList.add('copied');
      setTimeout(() => this.classList.remove('copied'), 1500);
    });
  }

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
      state.sortDir = (col === 'score' || col === 'rating') ? 'desc' : 'asc';
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
    if (col === 'score') {
      const an = parseInt(a.dataset.score || '0', 10);
      const bn = parseInt(b.dataset.score || '0', 10);
      return dir === 'asc' ? an - bn : bn - an;
    }

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
    leadsBody.appendChild(row);
  });
  applyFiltersAndSearch();
}

// ─── Filtered Exports & Bulk Copy ─────────────────────────────────────────────

function leadsToTSV(textSafePhones = false) {
  const headers = [
    '#', 'Business Name', 'Address', 'Rating', 'Lead Score',
    'Phone Number', 'Phone (E.164)', 'WhatsApp URL',
    'Email', 'Email MX Verified', 'Instagram Handle', 'LinkedIn URL',
    'Website', 'CMS Platform', 'Meta Pixel', 'Google Ads', 'Category', 'Country'
  ];
  const targetLeads = getFilteredLeads();
  const rows = targetLeads.map((l, i) => {
    let phoneVal = l.phone || '';
    if (textSafePhones && phoneVal.startsWith('+')) {
      phoneVal = "'" + phoneVal;
    }
    const cleanName = (l.name || '').replace(/\s*\(Branch\s*\d+\)/gi, '');
    return [
      i + 1,
      cleanName,
      l.address || '',
      l.rating != null ? parseFloat(l.rating).toFixed(1) : '',
      l.lead_score || 0,
      phoneVal,
      l.phone_e164 || l.phone || '',
      l.whatsapp_url || '',
      l.email || '',
      l.email_mx_verified ? 'YES' : 'NO',
      l.instagram_handle ? '@' + l.instagram_handle : '',
      l.linkedin_url || '',
      l.website || '',
      l.cms_platform || 'Custom',
      l.has_meta_pixel ? 'YES' : 'NO',
      l.has_google_ads ? 'YES' : 'NO',
      l.category || '',
      l.country_code || '',
    ];
  });
  return [headers, ...rows].map(r => r.join('\t')).join('\n');
}

function leadsToCSV() {
  const headers = [
    '#', 'Business Name', 'Address', 'Rating', 'Lead Score',
    'Phone Number', 'Phone (E.164)', 'WhatsApp URL',
    'Email', 'Email MX Verified', 'Instagram Handle', 'LinkedIn URL',
    'Website', 'CMS Platform', 'Meta Pixel', 'Google Ads', 'Category', 'Country'
  ];
  const csvEsc = v => `"${String(v).replace(/"/g, '""')}"`;
  const targetLeads = getFilteredLeads();
  const rows = targetLeads.map((l, i) => {
    const cleanName = (l.name || '').replace(/\s*\(Branch\s*\d+\)/gi, '');
    return [
      i + 1,
      cleanName,
      l.address || '',
      l.rating != null ? parseFloat(l.rating).toFixed(1) : '',
      l.lead_score || 0,
      l.phone || '',
      l.phone_e164 || l.phone || '',
      l.whatsapp_url || '',
      l.email || '',
      l.email_mx_verified ? 'YES' : 'NO',
      l.instagram_handle ? '@' + l.instagram_handle : '',
      l.linkedin_url || '',
      l.website || '',
      l.cms_platform || 'Custom',
      l.has_meta_pixel ? 'YES' : 'NO',
      l.has_google_ads ? 'YES' : 'NO',
      l.category || '',
      l.country_code || '',
    ];
  });
  return '\uFEFF' + [headers, ...rows].map(r => r.map(csvEsc).join(',')).join('\r\n');
}

// 1. Open in Google Sheets (Filtered Leads Only)
$('btn-open-sheets')?.addEventListener('click', () => {
  const filtered = getFilteredLeads();
  if (!filtered.length) {
    showToast('No leads match the current view to export');
    return;
  }
  const tsv = leadsToTSV(true);
  copyText(tsv);
  showToast(`Copied ${filtered.length} filtered leads! Press Ctrl+V in Google Sheets`, 3500);
  window.open('https://sheets.new', '_blank');
});

// 2. Download Google Sheets (.xlsx) (Filtered Leads Only)
$('btn-sheets-xlsx')?.addEventListener('click', async () => {
  const filtered = getFilteredLeads();
  if (!filtered.length) {
    showToast('No leads match the current view to export');
    return;
  }
  try {
    showToast('Generating Google Sheets .xlsx...');
    const resp = await fetch('/api/export/sheets', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(filtered),
    });
    if (!resp.ok) throw new Error('Export request failed');
    const blob = await resp.blob();
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = 'leads_export.xlsx';
    a.click();
    URL.revokeObjectURL(url);
    showToast(`Downloaded ${filtered.length} leads in leads_export.xlsx`);
  } catch (err) {
    console.error('Export error', err);
    showToast('Failed to export .xlsx');
  }
});

// 3. Copy TSV
$('btn-copy-tsv')?.addEventListener('click', () => {
  const filtered = getFilteredLeads();
  if (!filtered.length) {
    showToast('No leads match the current view to export');
    return;
  }
  copyText(leadsToTSV(false));
  showToast(`TSV copied for ${filtered.length} leads — paste into Sheets/Excel`);
});

// 4. Download CSV
$('btn-csv')?.addEventListener('click', () => {
  const filtered = getFilteredLeads();
  if (!filtered.length) {
    showToast('No leads match the current view to export');
    return;
  }
  downloadFile(leadsToCSV(), 'leads.csv', 'text/csv;charset=utf-8;');
  showToast(`CSV downloaded with ${filtered.length} leads`);
});

// 5. Download Excel (.csv)
$('btn-excel')?.addEventListener('click', () => {
  const filtered = getFilteredLeads();
  if (!filtered.length) {
    showToast('No leads match the current view to export');
    return;
  }
  downloadFile(leadsToCSV(), 'leads_excel.csv', 'text/csv;charset=utf-8;');
  showToast(`Excel-compatible CSV downloaded with ${filtered.length} leads`);
});

// 6. Copy Emails Utility Button
btnCopyEmails?.addEventListener('click', () => {
  const filtered = getFilteredLeads();
  // Prefer MX-verified emails; fallback to all visible emails
  let emails = filtered
    .filter(l => l.email_mx_verified && l.email && l.email.includes('@'))
    .map(l => l.email.trim());

  if (emails.length === 0) {
    emails = filtered
      .filter(l => l.email && l.email.includes('@'))
      .map(l => l.email.trim());
  }

  // Deduplicate
  const uniqueEmails = Array.from(new Set(emails));
  if (uniqueEmails.length === 0) {
    showToast('No emails found in the current filtered view');
    return;
  }

  copyText(uniqueEmails.join('\n'));
  showToast(`Copied ${uniqueEmails.length} emails to clipboard (newline-separated)`);
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
