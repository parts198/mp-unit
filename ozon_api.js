(() => {
  const MS_DAY = 86400000;

  // Лимиты Ozon (минимум, который точно нужен вам сейчас)
  // Транзакции: "максимальный период в одном запросе — 1 месяц"
  const LIMITS = [
    { re: /\/v3\/finance\/transaction\/list/i, maxDays: 30, mode: 'transactions' },
  ];

  function toIso(d){ return (d instanceof Date ? d : new Date(d)).toISOString(); }

  function splitByDays(fromIso, toIso, maxDays){
    const a = new Date(fromIso), b = new Date(toIso);
    if (!isFinite(a) || !isFinite(b) || b <= a) return [{ from: fromIso, to: toIso }];
    const out = [];
    let cur = new Date(a.getTime());
    while (cur < b){
      let nxt = new Date(cur.getTime() + maxDays * MS_DAY);
      if (nxt > b) nxt = new Date(b.getTime());
      out.push({ from: cur.toISOString(), to: nxt.toISOString() });
      cur = nxt;
    }
    return out;
  }

  function normalizeStore(s){
    if (!s || typeof s !== 'object') return null;
    return {
      client_id: String(s.client_id ?? s.clientId ?? s['Client-Id'] ?? '').trim(),
      api_key:   String(s.api_key ?? s.apiKey ?? s['Api-Key'] ?? '').trim(),
      name:      String(s.name ?? '').trim(),
      vat:       s.vat ?? s.nds ?? s.vat_rate ?? null,
    };
  }

  function loadStores(){
    try { if (window.ozonLoadStores) return window.ozonLoadStores() || []; } catch {}
    try { if (Array.isArray(window.OZON_STORES)) return window.OZON_STORES; } catch {}
    try { if (Array.isArray(window.STORES)) return window.STORES; } catch {}
    try {
      const j = localStorage.getItem('ozon_stores') || localStorage.getItem('stores') || localStorage.getItem('OZON_STORES');
      if (j) {
        const arr = JSON.parse(j);
        if (Array.isArray(arr)) return arr;
      }
    } catch {}
    return [];
  }

  function guessStoreFromUI(){
    const stores = loadStores().map(normalizeStore).filter(Boolean);
    if (!stores.length) return null;

    const sel =
      document.querySelector('#storeSelect') ||
      document.querySelector('#loadStore') ||
      document.querySelector('select[data-store-select="1"]') ||
      document.querySelector('select');

    if (sel) {
      const v = String(sel.value || '').trim();
      if (v && v !== '__all__') {
        const parts = v.split('|');
        const cid = (parts.length > 1 ? parts[1] : parts[0]).trim();
        const hit = stores.find(x => String(x.client_id) === cid);
        if (hit) return hit;
      }
    }
    return (stores.length === 1) ? stores[0] : null;
  }

  // Поддерживаем оба варианта: fetchOzon(url, {store, body}) и fetchOzon(url, store, body)
  function parseArgs(url, a1, a2){
    let store = null, body = null;

    if (a1 && typeof a1 === 'object' && ('store' in a1 || 'body' in a1)) {
      store = normalizeStore(a1.store);
      body  = a1.body ?? null;
    } else {
      store = normalizeStore(a1);
      body  = a2 ?? null;
    }

    if (!store) store = guessStoreFromUI();
    if (!body) body = {};

    if (!store || !store.client_id || !store.api_key) {
      throw new Error('Не выбран магазин или не заданы Client-Id / Api-Key.');
    }

    return { url, store, body };
  }

  async function proxyPost(url, store, body, cache){
    const r = await fetch('/api/ozon_proxy.php', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ url, store, body, cache: cache || { mode: 'use' } }),
    });
    const t = await r.text();
    let j = null;
    try { j = t ? JSON.parse(t) : null; } catch {}
    if (!r.ok) {
      const msg = (j && (j.message || j.error)) ? (j.message || j.error) : (t || ('HTTP ' + r.status));
      throw new Error(msg);
    }
    return j;
  }

  function stripVatIfNeeded(url, body){
    // вы договорились "VAT не отправлять" — делаем это жёстко для import/prices
    if (!/\/v1\/product\/import\/prices/i.test(url)) return body;
    if (!body || typeof body !== 'object') return body;
    if (!Array.isArray(body.prices)) return body;
    for (const p of body.prices) {
      if (p && typeof p === 'object') delete p.vat;
    }
    return body;
  }

  async function fetchTransactionsAll(store, body){
    const base = JSON.parse(JSON.stringify(body || {}));
    base.page_size = Math.min(1000, Number(base.page_size || 1000) || 1000);

    const f = base.filter || {};
    const df = f.date || null;

    // если нет date-фильтра — просто страницы
    if (!df || !df.from || !df.to) {
      let page = 1;
      const ops = [];
      for (;;) {
        const req = Object.assign({}, base, { page });
        const j = await proxyPost('/v3/finance/transaction/list', store, req, { mode: 'use' });
        ops.push(...(j?.result?.operations || []));
        const pc = Number(j?.result?.page_count || 0) || 0;
        if (pc === 0 || page >= pc) break;
        page++;
      }
      return { result: { operations: ops, page_count: 0, row_count: ops.length } };
    }

    // режем на куски <= 30 дней, внутри каждого куска забираем все страницы
    const segs = splitByDays(df.from, df.to, 30);
    const allOps = [];

    for (const seg of segs) {
      let page = 1;
      for (;;) {
        const req = JSON.parse(JSON.stringify(base));
        req.filter = req.filter || {};
        req.filter.date = { from: seg.from, to: seg.to };
        req.page = page;

        const j = await proxyPost('/v3/finance/transaction/list', store, req, { mode: 'use' });
        allOps.push(...(j?.result?.operations || []));
        const pc = Number(j?.result?.page_count || 0) || 0;
        if (pc === 0 || page >= pc) break;
        page++;
      }
    }
    return { result: { operations: allOps, page_count: 0, row_count: allOps.length } };
  }

  async function fetchOzon(url, a1, a2){
    const { store, body } = parseArgs(url, a1, a2);

    // спец-режимы по эндпоинтам
    if (/\/v3\/finance\/transaction\/list/i.test(url)) {
      return await fetchTransactionsAll(store, body);
    }

    // общий режим: прокси + кэш + зачистка vat где нужно
    const cleanBody = stripVatIfNeeded(url, body);
    return await proxyPost(url, store, cleanBody, { mode: 'use' });
  }

  window.fetchOzon = fetchOzon;
  window.ozonFetch = window.ozonFetch || fetchOzon;
})();
