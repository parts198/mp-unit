(() => {
  const ORIG = window.fetch;
  if (!ORIG) return;

  const API_HOST = 'api-seller.ozon.ru';


  function loadStores() {
    try {
      const j =
        localStorage.getItem('ozon_stores') ||
        localStorage.getItem('stores') ||
        localStorage.getItem('OZON_STORES');
      const arr = j ? JSON.parse(j) : [];
      return Array.isArray(arr) ? arr : ((arr && typeof arr === 'object') ? Object.values(arr) : []);
    } catch { return []; }
  }

  function findStoreByClientId(client_id) {
    const token = String(client_id || '').trim();
    const stores = loadStores();

    function normClientId(x){
      return String(x?.client_id ?? x?.clientId ?? x?.['Client-Id'] ?? '').trim();
    }
    function normApiKey(x){
      return String(x?.api_key ?? x?.apiKey ?? x?.['Api-Key'] ?? '').trim();
    }
    function normName(x){
      return String(x?.name ?? x?.title ?? x?.store ?? x?.shop ?? x?.label ?? '').trim();
    }

    let s = null;

    if (token) {
      s = stores.find(x => normClientId(x) === token) || null;
      if (!s) {
        const tl = token.toLowerCase();
        s = stores.find(x => normName(x).toLowerCase() === tl) || null;
      }
    }

    if (!s && stores.length === 1) s = stores[0];

    if (!s) return null;

    const out_cid = normClientId(s);
    const out_key = normApiKey(s);
    if (!out_cid || !out_key) return null;

    return { client_id: out_cid, api_key: out_key };
  }


  function getSelectedClientIdFallback() {
    const sel =
      document.querySelector('#storeSelect') ||
      document.querySelector('#loadStore') ||
      document.querySelector('#store') ||
      document.querySelector('select[data-store-select="1"]') ||
      document.querySelector('select');
    if (!sel) return '';
    const v = String(sel.value || '').trim();
    if (!v || v === '__all__') return '';
    const parts = v.split('|');
    const cid = (parts.length > 1 ? parts[1] : parts[0]).trim();
    return cid;
  }

  function getClientIdFromHeaders(init) {
    try {
      const h = init?.headers;
      if (!h) return '';
      if (typeof h.get === 'function') return String(h.get('Client-Id') || '').trim();
      // plain object
      return String(h['Client-Id'] || h['client-id'] || h['Client-id'] || '').trim();
    } catch { return ''; }
  }

  function getApiKeyFromHeaders(init) {
    try {
      const h = init?.headers;
      if (!h) return '';
      if (typeof h.get === 'function') return String(h.get('Api-Key') || h.get('Api-key') || '').trim();
      return String(h['Api-Key'] || h['api-key'] || h['Api-key'] || '').trim();
    } catch { return ''; }
  }


  function stripToPath(url) {
    try {
      const u = new URL(url, location.href);
      // перехватываем только api-seller.ozon.ru
      if (!u.host || u.host !== API_HOST) return null;
      return u.pathname + u.search;
    } catch {
      // если прилетел уже /v3/...
      if (typeof url === 'string' && url.startsWith('/v')) return url;
      return null;
    }
  }

  window.fetch = async (input, init = {}) => {
    const url =
      (typeof input === 'string') ? input :
      (input && typeof input.url === 'string') ? input.url : '';

    const path = stripToPath(url);
    if (!path) return ORIG(input, init);

    const method = String(init.method || 'GET').toUpperCase();
    if (method !== 'POST') return ORIG(input, init); // в вашем проекте OZON в основном POST

    // client_id: сначала из заголовков, иначе из селекта
    const client_id = getClientIdFromHeaders(init) || getSelectedClientIdFallback();
    if (!client_id) {
      // пусть отработает старое поведение, чтобы не ломать UI
      return ORIG(input, init);
    }

    // body (если нет — пустой объект)
    let bodyObj = {};
    try {
      if (typeof init.body === 'string' && init.body.trim()) bodyObj = JSON.parse(init.body);
    } catch {}

    const api_key = getApiKeyFromHeaders(init);
    let store = null;
    if (client_id && api_key) {
      store = { client_id, api_key };
    } else {
      store = findStoreByClientId(client_id);
    }

    if (!store) {
      return new Response(JSON.stringify({ error: 'missing_store' }), {
        status: 400,
        headers: { 'Content-Type': 'application/json; charset=utf-8' },
      });
    }

    const proxyResp = await ORIG('/api/ozon_proxy.php', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ url: path, store, body: bodyObj, cache: { mode: 'use' } }),
    });
const txt = await proxyResp.text();
    // Возвращаем как будто это ответ OZON
    return new Response(txt, {
      status: proxyResp.status,
      headers: { 'Content-Type': 'application/json; charset=utf-8' },
    });
  };
})();
