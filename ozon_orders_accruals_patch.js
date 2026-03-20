(() => {
  'use strict';

  const __p = String(location.pathname || '').toLowerCase();
  if (!(__p.includes('orders') || __p.includes('transactions'))) return;



  window.__ACCRUALS_PATCH_V12 = window.__ACCRUALS_PATCH_V12 || 'accruals_patch_v12';
  console.info('[ACCRUALS_PATCH]', window.__ACCRUALS_PATCH_V12);

window.__ACCRUALS_PATCH_V12 = 'accruals_patch_v12';

  const LS_STATE_KEY = 'ozon_accruals_state_v8';
  const MAX_SEEN = 12000;

  const DEBUG = localStorage.getItem('accrual_debug') === '1';
  const dbg = (...a) => { try { if (DEBUG) console.log('[ACCR]', ...a); } catch {} };

  const round2 = (n) => Math.round((Number(n) || 0) * 100) / 100;

  function toNum(x) {
    try {
      if (x == null) return 0;

      // objects like {value:..} / {amount:..}
      if (typeof x === 'object') {
        if ('value' in x) x = x.value;
        else if ('amount' in x) x = x.amount;
      }

      if (typeof x === 'string') {
        let s = x;

        // normalize spaces / nbsp
        s = s.replace(/\u00A0/g, ' ').trim();

        // normalize unicode minus/dash to '-'
        s = s.replace(/[−–—]/g, '-');

        // parentheses as negative
        let neg = false;
        if (s.startsWith('(') && s.endsWith(')')) {
          neg = true;
          s = s.slice(1, -1);
        }

        // remove currency/letters, keep digits, dot, comma, minus
        s = s.replace(/руб\.?/gi, '')
             .replace(/₽/g, '')
             .replace(/\s+/g, '')
             .replace(',', '.');

        // extract first number (keeps leading '-')
        const m = s.match(/-?\d+(?:\.\d+)?/);
        if (!m) return 0;

        const n = Number(m[0]);
        if (!Number.isFinite(n)) return 0;
        return neg ? -n : n;
      }

      const n = Number(x);
      return Number.isFinite(n) ? n : 0;
    } catch { return 0; }
  }

  const normId = (x) => String(x ?? '').trim().replace(/\s+/g, '').replace(/^№/g, '');
  const baseOrderId = (id) => normId(id).replace(/-\d{1,3}$/, '');

    function loadState() {
    let st = null;
    try { st = JSON.parse(localStorage.getItem(LS_STATE_KEY) || 'null'); } catch { st = null; }
    if (!st || typeof st !== 'object') st = {};
    if (!st.map || typeof st.map !== 'object') st.map = {};
    if (!st.seen || typeof st.seen !== 'object') st.seen = {};
    if (!Array.isArray(st.seenList)) st.seenList = [];
    // ограничим seenList, чтобы localStorage не разрастался
    const LIM = 20000;
    if (st.seenList.length > LIM) {
      const drop = st.seenList.splice(0, st.seenList.length - LIM);
      for (const k of drop) delete st.seen[k];
    }
    return st;
  }

function saveState(st) {
    try {
      if (st.seenList.length > MAX_SEEN) {
        const drop = st.seenList.splice(0, st.seenList.length - MAX_SEEN);
        drop.forEach(k => { try { delete st.seen[k]; } catch {} });
      }
      localStorage.setItem(LS_STATE_KEY, JSON.stringify(st));
    } catch {}
  }

  function addToMap(st, id, delta) {
    const key = normId(id);
    if (!key) return;
    const cur = toNum(st.map[key]);
    st.map[key] = round2(cur + toNum(delta));
  }

  function opId(op) {
    return normId(op?.operation_id ?? op?.operationId ?? op?.id ?? op?.uid ?? '');
  }

  function opKey(op, id, amt) {
    const oid = opId(op);
    if (oid) return 'op:' + oid;
    const t = normId(op?.operation_type ?? op?.operationType ?? op?.type ?? op?.name ?? '');
    const d = normId(op?.operation_date ?? op?.date ?? op?.created_at ?? op?.createdAt ?? '');
    return ['k', normId(id), t, d, String(round2(toNum(amt)))].join('|');
  }

  function extractOps(json) {
    const direct =
      json?.result?.operations ??
      json?.operations ??
      json?.result?.items ??
      json?.items ??
      json?.result?.rows ??
      json?.rows ??
      json?.result?.data ??
      json?.data ??
      null;

    if (Array.isArray(direct)) return direct;

    // heuristic: find array of objects containing id+amount-ish fields
    try {
      const root = (json && typeof json === 'object')
        ? (json.result && typeof json.result === 'object' ? json.result : json)
        : {};
      for (const v of Object.values(root)) {
        if (!Array.isArray(v) || !v.length || typeof v[0] !== 'object') continue;
        const o = v[0] || {};
        const hasId = ('posting_number' in o) || ('postingNumber' in o) || ('order_id' in o) || ('orderId' in o) || ('shipment_number' in o);
        const hasAmt = ('seller_amount' in o) || ('amount' in o) || ('accrual_amount' in o) || ('payout' in o) || ('sum' in o);
        if (hasId && hasAmt) return v;
      }
    } catch {}
    return [];
  }

  function opPostingId(op) {
    return normId(
      op?.posting_number ?? op?.postingNumber ?? op?.posting ?? op?.posting_id ?? op?.postingId ??
      op?.shipment_number ?? op?.shipmentNumber ??
      op?.order_id ?? op?.orderId ?? op?.order_number ?? op?.orderNumber ??
      ''
    );
  }

        function opAmount(op) {
      // localStorage.accrual_field:
      //   - "amount" (default)       -> ориентируемся на amount/accrual_amount
      //   - "seller_amount"         -> ориентируемся на seller_amount
      //   - "first_nonzero"         -> первое ненулевое из всех кандидатов
      const mode = (localStorage.getItem('accrual_field') || 'amount').toLowerCase();
      const sets = {
        amount: [
          op?.amount,
          op?.accrual_amount,
          op?.sum,
          op?.total,
          op?.payout,
          op?.seller_amount,
        ],
        seller_amount: [
          op?.seller_amount,
          op?.amount,
          op?.accrual_amount,
          op?.sum,
          op?.total,
          op?.payout,
        ],
        first_nonzero: [
          op?.amount,
          op?.seller_amount,
          op?.accrual_amount,
          op?.sum,
          op?.total,
          op?.payout,
        ],
      };
      const cands = sets[mode] || sets.amount;
      for (const v of cands) {
        const n = toNum(v);
        if (Number.isFinite(n) && n !== 0) return n;
      }
      return 0;
    }

function captureAccrualsFromJson(json, urlHint) {
    try {
      const ops = extractOps(json);
      if (!ops.length) return;

      const st = loadState();
      let added = 0;

      for (const op of ops) {
        const id = opPostingId(op);
        if (!id) continue;

        const amt = opAmount(op);
        if (!Number.isFinite(amt) || amt === 0) continue; // keep negatives, drop only true 0

        const k = opKey(op, id, amt);
        if (st.seen[k]) continue;

        st.seen[k] = 1;
        st.seenList.push(k);
        addToMap(st, id, amt);
        added++;
      }

      if (added) saveState(st);
      dbg('captured:', added, 'url:', urlHint || '');
    } catch (e) {
      dbg('capture failed', e);
    }
  }

  function shouldInspectReq(url, body) {
    const u = String(url || '').toLowerCase();
    const b = (typeof body === 'string') ? body.toLowerCase() : '';
    if (!u && !b) return false;

    const hit = (s) => (
      s.includes('finance') ||
      s.includes('transaction') ||
      s.includes('transactions') ||
      s.includes('accrual') ||
      s.includes('%d0%bd%d0%b0%d1%87%d0%b8%d1%81%d0%bb') ||  // "начисл" urlencoded
      s.includes('начисл') ||
      s.includes('api-seller.ozon.ru')
    );

    return hit(u) || hit(b);
  }

  async function tryParseAndCapture(text, urlHint) {
    try {
      if (!text) return;
      const t = String(text).trim();
      if (!(t.startsWith('{') || t.startsWith('['))) return;
      const j = JSON.parse(t);
      const ops = extractOps(j);
      if (!ops.length) return;
      captureAccrualsFromJson(j, urlHint);
    } catch {}
  }

  // ===== APPLY =====

  function isDelivered(row) {
    const s = String(
      row?.status ?? row?.state ?? row?.posting_status ?? row?.postingStatus ?? row?.delivery_status ?? ''
    ).toLowerCase();
    return s.includes('delivered') || s.includes('достав') || s.includes('вручен') || s.includes('completed');
  }

  function rowPostingId(row) {
    return normId(
      row?.posting_number ?? row?.postingNumber ?? row?.posting ?? row?.shipment_number ?? row?.shipmentNumber ??
      row?.order_id ?? row?.orderId ?? row?.order_number ?? row?.orderNumber ??
      row?.postingId ?? ''
    );
  }

  function rowQty(row) {
    const cands = [row?.qty, row?.quantity, row?.count, row?.itemsCount, row?.productsCount];
    for (const v of cands) {
      const n = toNum(v);
      if (Number.isFinite(n) && n > 0) return n;
    }
    return 1;
  }

  function rowWeight(row) {
    const q = rowQty(row);
    const cands = [row?.price, row?.sale, row?.sale_amount, row?.amount, row?.total, row?.currentPrice, row?.yourPrice];
    for (const v of cands) {
      const n = toNum(v);
      if (Number.isFinite(n) && n > 0) return n * q;
    }
    return 1 * q;
  }

  function rowCost(row) {
    const cands = [row?.costPrice, row?.cost_price, row?.netPrice, row?.net_price];
    for (const v of cands) {
      const n = toNum(v);
      if (Number.isFinite(n) && n > 0) return n;
    }
    return 0;
  }

  function packagingCost(row) {
    // ВАЖНО: по умолчанию фиксированная упаковка 40 ₽ (внутренняя себестоимость).
    // Если нужно изменить — задайте: localStorage.setItem('packaging_cost','50')
    const ov = toNum(localStorage.getItem('packaging_cost'));
    if (Number.isFinite(ov) && ov > 0) return ov;
    return 40;
  }

  function applyAccrualsToRows(rows) {
    try {
      if (!Array.isArray(rows) || !rows.length) return;

      const APPLY_ALL = localStorage.getItem('accrual_apply_all') === '1';
      const st = loadState();
      const mp = (st && st.map && typeof st.map === 'object') ? st.map : {};

      // base groups for distribution
      const groups = new Map(); // base -> { wsum, cnt }
      for (const r of rows) {
        const pid = rowPostingId(r);
        if (!pid) continue;
        const base = baseOrderId(pid);
        const g = groups.get(base) || { wsum: 0, cnt: 0 };
        g.wsum += rowWeight(r);
        g.cnt += 1;
        groups.set(base, g);
      }

      let applied = 0;

      for (const r of rows) {
        if (!r) continue;
        if (!APPLY_ALL && !isDelivered(r)) continue;

        const pid = rowPostingId(r);
        if (!pid) continue;

        const base = baseOrderId(pid);

        const exact = toNum(mp[pid] ?? 0);
        const baseTotal = (base && base !== pid) ? toNum(mp[base] ?? 0) : 0;

        const baseMode = localStorage.getItem('accrual_base_mode') || 'no_exact'; // no_exact|always|off
        let basePart = 0;
        if (baseTotal && baseMode !== 'off' && (baseMode === 'always' || !exact)) {
          const g = groups.get(base);
          if (g && g.cnt) {
            const w = rowWeight(r);
            const denom = (g.wsum && g.wsum > 0) ? g.wsum : g.cnt;
            basePart = baseTotal * (w / denom);
          }
        }

        const accrTotal = round2(exact + basePart);
        if (!accrTotal) continue;

        const cost = rowCost(r);
        const pack = packagingCost(r);
        const baseCost = round2(cost + pack);

        // MODEL: payout = начисления - упаковка
        const payout = round2(accrTotal);
        const margin = round2(payout - cost - pack);               // == accrTotal - (cost+pack)
        const markupPct = baseCost > 0 ? round2((margin / baseCost) * 100) : 0;

        r.__accrual_total = accrTotal;
        r.__accrual_payout = payout;
        r.__accrual_margin = margin;
        r.__accrual_markup = markupPct;
        r.__accrual_baseCost = baseCost;

        r.payout = payout;
        r.margin = margin;
        r.markup = markupPct;
        r.markupPct = markupPct;

        applied++;
      }

      dbg('apply rows:', applied, 'rows:', rows.length, 'mapKeys:', Object.keys(mp).length);
    } catch (e) {
      dbg('apply failed', e);
    }
  }

  // Debug helpers
  window.__accrState = () => { try { return loadState(); } catch { return null; } };
  window.accrState = window.__accrState;
  window.accrMap = window.__accrMap;

  window.__accrMap = (id) => {
    try {
      const st = loadState();
      const k = normId(id);
      const b = baseOrderId(k);
      console.log('ID:', k, 'map=', st.map[k], 'BASE:', b, 'baseMap=', st.map[b]);
      return { id: k, val: st.map[k], base: b, baseVal: st.map[b] };
    } catch { return null; }
  };

  function fmtRub(x) {
    const n = Number(x || 0);
    return n.toLocaleString('ru-RU', { minimumFractionDigits: 2, maximumFractionDigits: 2 }) + ' ₽';
  }
  function fmtPct(x) { return (Number(x || 0)).toFixed(2) + '%'; }

  function patchOrdersTableDom() {
    try {
      const rows = window.state?.rows;
      if (!Array.isArray(rows) || !rows.length) return;

      const tables = Array.from(document.querySelectorAll('table'));
      let target = null;
      let headers = null;

      for (const t of tables) {
        const ths = Array.from(t.querySelectorAll('thead th')).map(x => String(x.textContent || '').trim());
        if (ths.some(h => h.includes('Отправление')) && ths.some(h => h.includes('Наценка'))) {
          target = t; headers = ths; break;
        }
      }
      if (!target || !headers || !target.tBodies || !target.tBodies[0]) return;

      const iPost = headers.findIndex(h => h.includes('Отправление'));
      const iPayout = headers.findIndex(h => h.includes('К выплате'));
      const iMarkup = headers.findIndex(h => h.includes('Наценка'));
      const iMargin = headers.findIndex(h => h.includes('Маржа'));

      if (iPost < 0) return;

      const byPid = new Map();
      for (const r of rows) {
        const pid = rowPostingId(r);
        if (pid) byPid.set(pid, r);
      }

      for (const tr of Array.from(target.tBodies[0].rows)) {
        const tdPost = tr.cells[iPost];
        if (!tdPost) continue;
        const pid = normId(tdPost.innerText || tdPost.textContent || '');
        if (!pid) continue;

        const r = byPid.get(pid);
        if (!r || !r.__accrual_total) continue;

        if (iPayout >= 0 && tr.cells[iPayout]) tr.cells[iPayout].textContent = fmtRub(r.__accrual_payout);
        if (iMarkup >= 0 && tr.cells[iMarkup]) tr.cells[iMarkup].textContent = fmtPct(r.__accrual_markup);
        if (iMargin >= 0 && tr.cells[iMargin]) tr.cells[iMargin].textContent = fmtRub(r.__accrual_margin);
      }
    } catch {}
  }

  window.__applyAccrualsNow = () => {
    try { applyAccrualsToRows(window.state?.rows || []); } catch {}
    try { patchOrdersTableDom(); } catch {}
    try {
      if (typeof window.renderTable === 'function') window.renderTable();
      else if (typeof window.renderOrdersTable === 'function') window.renderOrdersTable();
    } catch {}
  };

  function tryWrap(name) {
    try {
      const fn = window[name];
      if (typeof fn !== 'function') return;
      if (fn.__accrualWrapped) return;

      const wrapped = function (...args) {
        try { applyAccrualsToRows(window.state?.rows || []); } catch {}
        const res = fn.apply(this, args);
        try { patchOrdersTableDom(); } catch {}
        return res;
      };
      wrapped.__accrualWrapped = true;
      window[name] = wrapped;
    } catch {}
  }

  function wrapRenders() {
    tryWrap('renderTable');
    tryWrap('renderOrdersTable');
    tryWrap('renderOrders');
    tryWrap('updateTable');
    tryWrap('updateOrdersTable');
  }

  function ensureFetchWrapped() {
    try {
      const cur = window.fetch;
      if (typeof cur !== 'function') return;
      if (cur.__accrualCaptureWrapped) return;

      const orig = cur;
      const wrapped = async (input, init) => {
        const resp = await orig(input, init);
        try {
          const url = (typeof input === 'string') ? input :
                      (input && typeof input.url === 'string') ? input.url : '';
          const body = (init && typeof init.body === 'string') ? init.body : '';
          if (shouldInspectReq(url, body)) {
            const c = resp.clone();
            const txt = await c.text().catch(() => '');
            await tryParseAndCapture(txt, url);
          }
        } catch {}
        return resp;
      };
      wrapped.__accrualCaptureWrapped = true;
      window.fetch = wrapped;
      dbg('fetch wrapped');
    } catch {}
  }

  function ensureXHRWrapped() {
    try {
      const XHR = window.XMLHttpRequest;
      if (!XHR || !XHR.prototype) return;
      if (XHR.prototype.__accrualCaptureWrapped) return;

      const origOpen = XHR.prototype.open;
      const origSend = XHR.prototype.send;

      XHR.prototype.open = function (method, url, ...rest) {
        try { this.__accr_url = String(url || ''); } catch {}
        return origOpen.call(this, method, url, ...rest);
      };

      XHR.prototype.send = function (...args) {
        try { this.__accr_body = (args && typeof args[0] === 'string') ? args[0] : ''; } catch {}
        try {
          this.addEventListener('load', () => {
            try {
              const url = this.__accr_url || '';
              const body = this.__accr_body || '';
              if (!shouldInspectReq(url, body)) return;
              tryParseAndCapture(this.responseText, url);
            } catch {}
          });
        } catch {}
        return origSend.call(this, ...args);
      };

      XHR.prototype.__accrualCaptureWrapped = true;
      dbg('xhr wrapped');
    } catch {}
  }

  // ===== DOM CAPTURE (transactions.html shows cached data, not API) =====

  function setToMap(st, id, val) {
    const key = normId(id);
    if (!key) return;
    st.map[key] = round2(toNum(val));
  }

  function isTransactionsPage() {
    try {
      const p = String(location.pathname || '').toLowerCase();
      return p.includes('transactions');
    } catch { return false; }
  }

  
  
  function captureAccrualsFromTransactionsDom() {
    try {
      if (!isTransactionsPage()) return 0;
      if (localStorage.getItem('accrual_dom_capture') === '0') return 0;

      const tables = Array.from(document.querySelectorAll('table'));
      let target = null, headers = null;

      for (const t of tables) {
        const ths = Array.from(t.querySelectorAll('thead th')).map(x => String(x.textContent || '').trim());
        if (ths.some(h => h.includes('Отправление')) && ths.some(h => h.includes('Сумма'))) {
          target = t; headers = ths; break;
        }
      }
      if (!target || !headers || !target.tBodies || !target.tBodies[0]) return 0;

      const iPost = headers.findIndex(h => h.includes('Отправление'));
      const iDate = headers.findIndex(h => h.includes('Дата'));
      const iSum  = headers.findIndex(h => h.includes('Сумма'));
      if (iPost < 0 || iSum < 0) return 0;

      const st = loadState();
      let updated = 0;

      for (const tr of Array.from(target.tBodies[0].rows)) {
        const tdP = tr.cells[iPost];
        const tdS = tr.cells[iSum];
        if (!tdP || !tdS) continue;

        const idsRaw = String(tdP.innerText || tdP.textContent || '').trim();
        if (!idsRaw || idsRaw === '-' || idsRaw === '—') continue;

        const dtRaw = (iDate >= 0 && tr.cells[iDate]) ? String(tr.cells[iDate].innerText || tr.cells[iDate].textContent || '').trim() : '';
        const amt = toNum(tdS.innerText || tdS.textContent || '');
        if (!Number.isFinite(amt) || amt === 0) continue; // отрицательные оставляем

        const parts = idsRaw.split(/[,;]\s*|\s+/g).map(s => s.trim()).filter(Boolean);

        // если в ячейке есть и base, и base-1 — считаем это одним отправлением -> берём только суффиксные id
        const suff = parts.filter(x => /-\d{1,3}$/.test(normId(x)));
        const ids = suff.length ? suff : parts;

        for (const id of ids) {
          const k = normId(id);
          if (!k) continue;

          const sk = 'dom|' + k + '|' + dtRaw + '|' + String(round2(amt));
          if (st.seen[sk]) continue;
          st.seen[sk] = 1;
          st.seenList.push(sk);

          // DOM-значение считаем авторитетным: ПЕРЕзатираем, а не суммируем
          st.map[k] = round2(amt);
          updated++;
        }
      }

      if (updated) {
        saveState(st);
        console.info('[ACCR][DOM] captured', updated, 'keys', Object.keys(st.map || {}).length);
      }
      return updated;
    } catch (e) {
      console.warn('[ACCR][DOM] capture failed', e);
      return 0;
    }
  }

window.__captureAccrualsFromDomNow = () => captureAccrualsFromTransactionsDom();
window.__captureAccrualsFromDomNow = () => captureAccrualsFromTransactionsDom();
  // aliases for удобства в консоли
  try {
    window.accrState = window.__accrState;
    window.accrMap = window.__accrMap;
    window.captureAccrualsDomNow = window.__captureAccrualsFromDomNow;
    window.applyAccrualsNow = window.__applyAccrualsNow;
  } catch {}

  // init + recheck
  
  function autoDomCaptureLoop() {
    if (!isTransactionsPage()) return;
    let n = 0;
    const t = setInterval(() => {
      n++;
      try { captureAccrualsFromTransactionsDom(); } catch {}
      if (n >= 40) clearInterval(t);
    }, 500);
  }

  wrapRenders();
  ensureFetchWrapped();
  ensureXHRWrapped();

  try { autoDomCaptureLoop(); } catch {}


  let n = 0;
  const t = setInterval(() => {
    n++;
    wrapRenders();
    ensureFetchWrapped();
    ensureXHRWrapped();
    if (n >= 40) clearInterval(t);
  }, 500);

})();
