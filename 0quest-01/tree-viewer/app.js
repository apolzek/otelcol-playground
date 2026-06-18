const DATA_URL = '../tree-domain.json';
const OTYPE_ORDER = ['topdomains', 'domains', 'subdomains', 'topchannels', 'channels'];
const OTYPE_LABEL = {
  topdomains: 'TOP DOM',
  domains: 'DOMAIN',
  subdomains: 'SUBDOM',
  topchannels: 'TOP CH',
  channels: 'CHANNEL',
};

const state = {
  data: [],
  activeOtypes: new Set(OTYPE_ORDER),
  query: '',
  totals: { nodes: 0, byType: {} },
};

const $ = (id) => document.getElementById(id);
const $tree = $('tree');
const $search = $('search');
const $filters = $('filters');
const $stats = $('stats');
const $empty = $('empty');
const $error = $('error');

// ---------- Load & totals ----------
async function load() {
  try {
    const res = await fetch(DATA_URL);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const json = await res.json();
    state.data = Array.isArray(json) ? json : [json];
    computeTotals(state.data);
    renderFilters();
    render();
  } catch (err) {
    showError(`Could not load tree-domain.json: ${err.message}. Serve this folder with an HTTP server (e.g.: "python3 -m http.server" at the project root).`);
  }
}

function computeTotals(nodes) {
  state.totals = { nodes: 0, byType: {} };
  const walk = (arr) => {
    for (const n of arr) {
      state.totals.nodes++;
      state.totals.byType[n.otype] = (state.totals.byType[n.otype] || 0) + 1;
      if (n.subs?.length) walk(n.subs);
    }
  };
  walk(nodes);
}

// ---------- Filters ----------
function renderFilters() {
  $filters.innerHTML = '';
  for (const otype of OTYPE_ORDER) {
    if (!state.totals.byType[otype]) continue;
    const chip = document.createElement('button');
    chip.className = 'chip active';
    chip.dataset.otype = otype;
    chip.innerHTML = `
      <span class="dot" style="background: var(--color-${otype})"></span>
      ${otype}
      <span class="n">${state.totals.byType[otype]}</span>
    `;
    chip.addEventListener('click', () => toggleFilter(otype, chip));
    $filters.appendChild(chip);
  }
}

function toggleFilter(otype, chip) {
  if (state.activeOtypes.has(otype)) {
    state.activeOtypes.delete(otype);
    chip.classList.remove('active');
  } else {
    state.activeOtypes.add(otype);
    chip.classList.add('active');
  }
  render();
}

// ---------- Search & filter tree ----------
function matchesQuery(node, q) {
  if (!q) return true;
  const ql = q.toLowerCase();
  return (
    (node.name || '').toLowerCase().includes(ql) ||
    (node.key || '').toLowerCase().includes(ql) ||
    (node.prefix || '').toLowerCase().includes(ql) ||
    String(node.id || '').includes(ql)
  );
}

// Clone a full subtree, keeping all children regardless of otype filter.
function cloneFull(node) {
  return {
    ...node,
    _selfMatch: false,
    _filteredSubs: (node.subs || []).map(cloneFull),
  };
}

// Recursive search match — ignores otype filter for descendants so the user
// can still see the full children tree when searching. If the node itself
// matches the query, we keep its entire subtree (so expanding shows all
// children, even ones that don't match). Otherwise we keep only the matching
// descendants so the path to the match is preserved.
function findMatchesDeep(node, q) {
  const selfMatch = matchesQuery(node, q);
  if (selfMatch) {
    return { ...node, _selfMatch: true, _filteredSubs: (node.subs || []).map(cloneFull) };
  }
  const childMatches = (node.subs || []).map((s) => findMatchesDeep(s, q)).filter(Boolean);
  if (childMatches.length) {
    return { ...node, _selfMatch: false, _filteredSubs: childMatches };
  }
  return null;
}

// Top-level filter: otype chips affect only the visibility of root cards.
// When no search is active, we clone full subtrees so expanding always shows
// the complete tree (domains + subdomains) regardless of chip selection.
function filterTree(nodes, q) {
  const result = [];
  for (const n of nodes) {
    if (!state.activeOtypes.has(n.otype)) continue;
    if (!q) {
      result.push(cloneFull(n));
    } else {
      const matched = findMatchesDeep(n, q);
      if (matched) result.push(matched);
    }
  }
  return result;
}

// ---------- Utilities ----------
function countDescendants(node) {
  if (!node.subs?.length) return 0;
  let total = node.subs.length;
  for (const s of node.subs) total += countDescendants(s);
  return total;
}

function escapeHtml(s) {
  return String(s ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function highlight(text, q) {
  if (text == null) return '';
  if (!q) return escapeHtml(text);
  const s = String(text);
  const ql = q.toLowerCase();
  const idx = s.toLowerCase().indexOf(ql);
  if (idx === -1) return escapeHtml(s);
  return (
    escapeHtml(s.slice(0, idx)) +
    '<span class="highlight">' + escapeHtml(s.slice(idx, idx + q.length)) + '</span>' +
    escapeHtml(s.slice(idx + q.length))
  );
}

// ---------- Rendering ----------
function renderTopNode(node, q) {
  const el = document.createElement('div');
  el.className = 'node';
  el.setAttribute('role', 'treeitem');

  const hasChildren = (node._filteredSubs || []).length > 0;
  const descendants = countDescendants(node);

  const row = document.createElement('div');
  row.className = 'node-row' + (node._selfMatch && q ? ' match' : '');

  row.innerHTML = `
    <span class="toggle${hasChildren ? '' : ' empty'}" data-action="toggle">&gt;</span>
    <span class="badge ${node.otype}">${OTYPE_LABEL[node.otype] || node.otype}</span>
    <span class="name">${highlight(node.name, q)}</span>
    <span class="meta-key">${node.key ? highlight(node.key, q) : '<span class="meta-dim">-</span>'}</span>
    <span class="meta-id">${node.id ? highlight(String(node.id), q) : '<span class="meta-dim">-</span>'}</span>
    <span class="meta-prefix">${node.prefix ? highlight(node.prefix, q) : '<span class="meta-dim">-</span>'}</span>
    <span class="count${descendants === 0 ? ' count-zero' : ''}">${descendants}</span>
  `;

  el.appendChild(row);

  let children = null;
  if (hasChildren) {
    children = document.createElement('div');
    children.className = 'children';
    for (const child of node._filteredSubs) {
      children.appendChild(renderNestedNode(child, q, 1));
    }
    el.appendChild(children);
  }

  row.addEventListener('click', (e) => {
    if (e.target.closest('[data-action="toggle"]') && hasChildren) {
      const willExpand = !el.classList.contains('expanded');
      el.classList.toggle('expanded');
      if (willExpand) {
        children.querySelectorAll('.node').forEach((n) => n.classList.add('expanded'));
      }
    } else {
      openModal(node);
    }
  });

  return el;
}

function renderNestedNode(node, q, depth) {
  const el = document.createElement('div');
  el.className = 'node';
  el.setAttribute('role', 'treeitem');
  el.style.setProperty('--depth', depth);

  const hasChildren = (node._filteredSubs || []).length > 0;
  const descendants = countDescendants(node);

  const row = document.createElement('div');
  row.className = 'node-row' + (node._selfMatch && q ? ' match' : '');
  row.style.setProperty('--depth', depth);

  const metaParts = [];
  if (node.key) metaParts.push(`<span>key: ${highlight(node.key, q)}</span>`);
  if (node.id) metaParts.push(`<span>id: ${highlight(String(node.id), q)}</span>`);
  if (node.prefix) metaParts.push(`<span>prefix: ${highlight(node.prefix, q)}</span>`);

  row.innerHTML = `
    <span class="toggle${hasChildren ? '' : ' empty'}" data-action="toggle">&gt;</span>
    <span class="badge ${node.otype}">${OTYPE_LABEL[node.otype] || node.otype}</span>
    <span class="name">${highlight(node.name, q)}</span>
    <span class="meta">${metaParts.join('')}</span>
    ${hasChildren ? `<span class="count">${descendants}</span>` : ''}
  `;

  el.appendChild(row);

  if (hasChildren) {
    const children = document.createElement('div');
    children.className = 'children';
    for (const child of node._filteredSubs) {
      children.appendChild(renderNestedNode(child, q, depth + 1));
    }
    el.appendChild(children);
  }

  row.addEventListener('click', (e) => {
    e.stopPropagation();
    if (e.target.closest('[data-action="toggle"]') && hasChildren) {
      el.classList.toggle('expanded');
    } else {
      openModal(node);
    }
  });

  return el;
}

function countVisible(nodes) {
  let n = 0;
  for (const node of nodes) {
    n++;
    n += countVisible(node._filteredSubs || []);
  }
  return n;
}

function render() {
  const q = state.query.trim();
  const filtered = filterTree(state.data, q);
  $tree.innerHTML = '';

  if (!filtered.length) {
    $empty.classList.remove('hidden');
    $stats.textContent = `0 of ${state.totals.nodes} nodes`;
    return;
  }
  $empty.classList.add('hidden');

  for (const n of filtered) $tree.appendChild(renderTopNode(n, q));

  // When searching, auto-expand matches
  if (q) {
    document.querySelectorAll('.node').forEach((n) => n.classList.add('expanded'));
  }

  const visible = countVisible(filtered);
  $stats.textContent = `${visible} of ${state.totals.nodes} nodes${q ? ` | search: "${q}"` : ''}`;
}

function showError(msg) {
  $error.textContent = msg;
  $error.classList.remove('hidden');
}

// ---------- Modal ----------
const $modal = $('modal');
const $modalTitle = $('modal-title');
const $modalBadge = $('modal-badge');
const $modalBody = $('modal-body');

function findOriginalNode(target, nodes = state.data) {
  for (const n of nodes) {
    if (n.id === target.id && n.key === target.key && n.name === target.name) return n;
    const found = findOriginalNode(target, n.subs || []);
    if (found) return found;
  }
  return null;
}

function openModal(node) {
  // Use original (unfiltered) node so JSON view shows full subtree
  const original = findOriginalNode(node) || node;
  const descendants = countDescendants(original);
  const directSubs = original.subs || [];

  $modalTitle.textContent = original.name || '-';
  $modalBadge.className = `badge ${original.otype}`;
  $modalBadge.textContent = OTYPE_LABEL[original.otype] || original.otype;

  const field = (label, value, mono = true) => {
    if (value == null || value === '') {
      return `
        <div class="field-label">${label}</div>
        <div class="field-value empty">empty</div>
      `;
    }
    const safe = escapeHtml(String(value));
    return `
      <div class="field-label">${label}</div>
      <div class="field-value">
        <span>${safe}</span>
        <button class="copy-btn" data-copy="${safe.replace(/"/g, '&quot;')}">copy</button>
      </div>
    `;
  };

  const subsListHtml = directSubs.length
    ? directSubs.map((s) => `
        <div class="subs-row" data-sub-key="${escapeHtml(s.key)}" data-sub-id="${escapeHtml(s.id)}">
          <span class="badge ${s.otype}">${OTYPE_LABEL[s.otype] || s.otype}</span>
          <span class="name">${escapeHtml(s.name)}</span>
          <span class="sub-count">${countDescendants(s)} desc.</span>
        </div>
      `).join('')
    : '<div class="subs-empty">No direct children.</div>';

  $modalBody.innerHTML = `
    <div class="field-grid">
      ${field('Name', original.name, false)}
      ${field('Key', original.key)}
      ${field('ID', original.id)}
      ${field('Prefix', original.prefix)}
      <div class="field-label">Type</div>
      <div class="field-value">${original.otype}</div>
      <div class="field-label">Descendants</div>
      <div class="field-value">${descendants} total | ${directSubs.length} direct</div>
    </div>

    <div>
      <p class="modal-section-title">Direct children (${directSubs.length})</p>
      <div class="subs-list">${subsListHtml}</div>
    </div>

    <details class="json-toggle">
      <summary>View full JSON</summary>
      <pre>${escapeHtml(JSON.stringify(original, null, 2))}</pre>
    </details>
  `;

  // copy buttons
  $modalBody.querySelectorAll('.copy-btn').forEach((btn) => {
    btn.addEventListener('click', async (e) => {
      e.stopPropagation();
      const val = btn.dataset.copy;
      try {
        await navigator.clipboard.writeText(val);
        btn.textContent = 'copied';
        btn.classList.add('copied');
        setTimeout(() => {
          btn.textContent = 'copy';
          btn.classList.remove('copied');
        }, 1200);
      } catch {
        btn.textContent = 'failed';
      }
    });
  });

  // drill into a direct sub
  $modalBody.querySelectorAll('.subs-row').forEach((r) => {
    r.addEventListener('click', () => {
      const key = r.dataset.subKey;
      const id = r.dataset.subId;
      const sub = directSubs.find((s) => s.key === key && String(s.id) === id);
      if (sub) openModal(sub);
    });
    r.style.cursor = 'pointer';
  });

  $modal.classList.remove('hidden');
}

function closeModal() {
  $modal.classList.add('hidden');
}

$modal.addEventListener('click', (e) => {
  if (e.target.closest('[data-close]')) closeModal();
});

// ---------- Events ----------
let searchTimer;
$search.addEventListener('input', (e) => {
  clearTimeout(searchTimer);
  const val = e.target.value;
  searchTimer = setTimeout(() => {
    state.query = val;
    render();
  }, 120);
});

$('expand-all').addEventListener('click', () => {
  document.querySelectorAll('.node').forEach((n) => n.classList.add('expanded'));
});
$('collapse-all').addEventListener('click', () => {
  document.querySelectorAll('.node').forEach((n) => n.classList.remove('expanded'));
});

document.addEventListener('keydown', (e) => {
  if (e.key === 'Escape') {
    if (!$modal.classList.contains('hidden')) { closeModal(); return; }
    if (document.activeElement === $search) {
      $search.value = '';
      state.query = '';
      render();
      $search.blur();
      return;
    }
  }
  if (e.target.tagName === 'INPUT' || e.target.tagName === 'TEXTAREA') return;
  if (e.key === '/') {
    e.preventDefault();
    $search.focus();
    $search.select();
  } else if (e.key.toLowerCase() === 'e') {
    document.querySelectorAll('.node').forEach((n) => n.classList.add('expanded'));
  } else if (e.key.toLowerCase() === 'c') {
    document.querySelectorAll('.node').forEach((n) => n.classList.remove('expanded'));
  }
});

load().then(() => {
  // dev helper: #modal=NAME opens the modal for that node (useful for testing)
  const m = location.hash.match(/modal=([^&]+)/);
  if (m) {
    const name = decodeURIComponent(m[1]);
    const findByName = (arr) => {
      for (const n of arr) {
        if (n.name === name) return n;
        const f = findByName(n.subs || []);
        if (f) return f;
      }
      return null;
    };
    const n = findByName(state.data);
    if (n) openModal(n);
  }
});
