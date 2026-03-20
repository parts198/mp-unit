(() => {
  const API_BASE = 'https://api-seller.ozon.ru';
  const MS_DAY = 86400000;

  // Ограничение Озона для /v3/finance/transaction/list: максимум 1 месяц.
  // Берём безопасные 30 дней (в UTC).
  const LIMITS_BY_PATH = {
    '/v3/finance/transaction/list': 30,
  };

  // --- русификация услуг (можно расширять) ---
  const RU_SERVICE_NAME = {
    MarketplaceServiceItemDelivToCustomer: 'Последняя миля',
    MarketplaceServiceItemPickup: 'Pick-up (забор отправлений)',
    MarketplaceServiceItemDirectFlowTrans: 'Магистраль',
    MarketplaceServiceItemReturnFlowTrans: 'Обратная магистраль',
    MarketplaceServiceItemDirectFlowLogistic: 'Логистика',
    MarketplaceServiceItemReturnFlowLogistic: 'Обратная логистика',
    MarketplaceServiceItemFulfillment: 'Сборка заказа',
    MarketplaceServiceItemDropoffFF: 'Обработка отправления (FF)',
    MarketplaceServiceItemDropoffPVZ: 'Обработка отправления (ПВЗ)',
    MarketplaceServiceItemDropoffSC: 'Обработка отправления (SC)',
    MarketplaceServiceItemDeliveryKGT: 'Доставка КГТ',
    MarketplaceRedistributionOfAcquiringOperation: 'Оплата эквайринга',
    MarketplaceMarketingActionCostItem: 'Продвижение товаров',
    OperationMarketplaceServiceStorage: 'Хранение/размещение',
  };

  function safeJsonParse(s) { try { return JSON.parse(s); } catch { return null; } }

  function isStore(o) {
    if (!o || typeof o !== 'object') return false;
    const cid = o.client_id ?? o.clientId ?? o['Client-Id'];
    const key = o.api_key ?? o.apiKey ?? o['Api-Key'];
    return !!String(cid || '').trim() && !!String(key || '').trim();
  }

  function normalizeStore(s) {
    return {
      client_id: String(s.client_id ?? s.clientId ?? s['Client-Id'] ?? '').trim(),
      api_key: String(s.api_key ?? s.apiKey ?? s['Api-Key'] ?? '').trim(),
      name: String(s.name ?? '').trim(),
      vat: s.vat ?? s.nds ?? s.vat_rate,
    };
  }

  function loadStoresRuntime() {
    try { if (window.ozonLoadStores) return window.ozonLoadStores() || []; } catch {}
    try { if (Array.isArray(window.OZON_STORES)) return window.OZON_STORES; } catch {}
    try { if (Array.isArray(window.STORES)) return window.STORES; } catch {}
    return [];
  }

  function guessClientIdFromUI() {
    const sel =
      document.querySelector('#storeSelect') ||
      document.querySelector('#loadStore') ||
      document.querySelector('#store') ||
      document.querySelector('select[data-store-select="1"]') ||
      document.querySelector('select');

    if (!sel) return null;
    const v = String(sel.value || '').trim();
    if (!v || v === '__all__') return null;

    const parts = v.split('|');
    const maybeId = (parts.length > 1 ? parts[1] : parts[0]).trim();
    return /^\d+$/.test(maybeId) ? maybeId : null;
  }

  function getActiveStoreFallback() {
    const stores = loadStoresRuntime().map(normalizeStore);
    if (!stores.length) return null;

    const cid = guessClientIdFromUI();
    if (cid) {
      const hit = stores.find(s => String(s.client_id) === String(cid));
      if (hit) return hit;
    }
    if (stores.length === 1) return stores[0];
    return null;
  }

  function extractUrl(args) {
    for (const a of args) if (typeof a === 'string' && a.trim()) return a.trim();
    if (args[0] && typeof args[0].url === 'string') return args[0].url;
    return '';
  }

  function looksLikeBody(o) {
    return !!o && typeof o === 'object' && (
      'filter' in o || 'page' in o || 'page_size' in o ||
      'from' in o || 'to' in o
    );
  }

  function extractStoreAndBody(args) {
    let store = null;
    let body = null;

    for (const a of args) {
      if (!a || typeof a !== 'object') continue;

      // store передали напрямую
      if (!store && isStore(a)) store = normalizeStore(a);

      // store передали как {store: {...}}
      if (!store && a.store && isStore(a.store)) store = normalizeStore(a.store);

      // body передали напрямую
      if (!body && looksLikeBody(a)) body = a;

      // body передали как {body: {...}}
      if (!body && a.body && looksLikeBody(a.body)) body = a.body;
    }

    if (!store) store = getActiveStoreFallback();
    if (!body) body = {};

    return { store, body };
  }

  function authHeaders(store) {
    return {
      'Client-Id': store.client_id,
      'Api-Key': store.api_key,
      'Content-Type': 'application/json',
    };
  }

  async function postJson(url, store, body) {
    const r = await fetch(url, { method: 'POST', headers: authHeaders(store), body: JSON.stringify(body || {}) });
    const t = await r.text();
    const j = t ? safeJsonParse(t) : null;

    if (!r.ok) {
      const msg = (j && (j.message || j.error)) ? (j.message || j.error) : (t || ('HTTP ' + r.status));
      throw new Error(msg);
    }
    return j;
  }

  function splitRangeByDays(fromISO, toISO, maxDays) {
    const a = new Date(fromISO);
    const b = new Date(toISO);
    if (!Number.isFinite(a.getTime()) || !Number.isFinite(b.getTime()) || b <= a) {
      return [{ from: fromISO, to: toISO }];
    }

    const out = [];
    let cur = new Date(a.getTime());
    while (cur < b) {
      let nxt = new Date(cur.getTime() + maxDays * MS_DAY);
      if (nxt > b) nxt = new Date(b.getTime());
      out.push({ from: cur.toISOString(), to: nxt.toISOString() });
      cur = nxt;
    }
    return out;
  }

  function russifyServices(ops) {
    for (const op of ops || []) {
      if (Array.isArray(op.services)) {
        for (const s of op.services) {
          if (!s || typeof s !== 'object') continue;
          if (s.name && RU_SERVICE_NAME[s.name]) s.name = RU_SERVICE_NAME[s.name];
        }
      }
      // некоторые поля/названия Озон уже отдаёт русскими — не трогаем
    }
    return ops;
  }

  async function loadFinanceTransactionsAll(store, body) {
    const base = JSON.parse(JSON.stringify(body || {}));
    base.page_size = Math.min(1000, Number(base.page_size || 1000) || 1000);

    const filter = base.filter || {};
    const date = filter.date;

    // Если нет date-фильтра — просто пагинация как есть
    if (!date || !date.from || !date.to) {
      let page = Number(base.page || 1) || 1;
      const allOps = [];
      for (let guard = 0; guard < 20000; guard++) {
        const req = Object.assign({}, base, { page });
        const j = await postJson(API_BASE + '/v3/finance/transaction/list', store, req);
        const got = j?.result?.operations || [];
        allOps.push(...got);
        const pc = Number(j?.result?.page_count || 0) || 0;
        if (pc === 0 || page >= pc) break;
        page++;
      }
      russifyServices(allOps);
      return { result: { operations: allOps, page_count: 0, row_count: allOps.length } };
    }

    // Нарезка диапазона (<= 30 дней) + пагинация внутри куска
    const maxDays = LIMITS_BY_PATH['/v3/finance/transaction/list'] || 30;
    const segs = splitRangeByDays(date.from, date.to, maxDays);

    const allOps = [];
    for (const seg of segs) {
      let page = 1;
      for (let guard = 0; guard < 20000; guard++) {
        const req = JSON.parse(JSON.stringify(base));
        req.filter = req.filter || {};
        req.filter.date = { from: seg.from, to: seg.to };
        req.page = page;

        const j = await postJson(API_BASE + '/v3/finance/transaction/list', store, req);
        const got = j?.result?.operations || [];
        allOps.push(...got);

        const pc = Number(j?.result?.page_count || 0) || 0;
        if (pc === 0 || page >= pc) break;
        page++;
      }
    }

    russifyServices(allOps);
    return { result: { operations: allOps, page_count: 0, row_count: allOps.length } };
  }

  async function fetchOzon(...args) {
    const url0 = extractUrl(args);
    const { store, body } = extractStoreAndBody(args);

    if (!store || !store.client_id || !store.api_key) {
      throw new Error('Не выбран магазин или не заданы Client-Id / Api-Key.');
    }

    const url = url0.startsWith('http')
      ? url0
      : (API_BASE + (url0.startsWith('/') ? url0 : '/' + url0));

    // Спец-обработка финансовых транзакций
    if (url.includes('/v3/finance/transaction/list')) {
      return await loadFinanceTransactionsAll(store, body);
    }

    // Обычный POST
    return await postJson(url, store, body);
  }

  window.fetchOzon = fetchOzon;
  if (!window.ozonFetch) window.ozonFetch = fetchOzon;
})();
