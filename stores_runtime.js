(() => {
  // Ищем магазины в localStorage по нескольким ключам (на случай разных версий страниц)
  const LS_KEYS = [
    'ozon_stores',
    'ozonStores',
    'stores',
    'OZON_STORES_LOCAL',
    'ozon_stores_v1',
    'ozon_stores_v2'
  ];

  function safeParse(s) {
    try { return JSON.parse(s); } catch { return null; }
  }

  function normStr(x) {
    const s = (x === null || x === undefined) ? '' : String(x);
    return s.trim();
  }

  function normalizeStore(s) {
    if (!s || typeof s !== 'object') return null;
    const name = normStr(s.name || s.title || s.store_name);
    const client_id = normStr(s.client_id || s.clientId || s.client || s.id);
    const api_key = normStr(s.api_key || s.apiKey || s.key || s.token);
    const vat = normStr(s.vat ?? s.nds ?? s.vat_rate ?? '');

    // client_id и api_key нужны для реальных запросов; но для отображения в списке оставляем даже частично заполненные
    return {
      name,
      client_id,
      api_key,
      vat
    };
  }

  function uniqKey(st) {
    if (st.client_id) return 'cid:' + st.client_id;
    if (st.name) return 'name:' + st.name.toLowerCase();
    return null;
  }

  function mergeStores(secretsArr, localArr) {
    const out = [];
    const map = new Map();

    const push = (raw) => {
      const st = normalizeStore(raw);
      if (!st) return;
      const k = uniqKey(st);
      if (!k) return;

      if (!map.has(k)) {
        map.set(k, st);
        out.push(st);
      } else {
        // local переопределяет secrets (api_key/vat и т.п.)
        const cur = map.get(k);
        const merged = Object.assign({}, cur, st);
        map.set(k, merged);
        const idx = out.findIndex(x => uniqKey(x) === k);
        if (idx >= 0) out[idx] = merged;
      }
    };

    (secretsArr || []).forEach(push);
    (localArr || []).forEach(push);

    // чуть стабильнее сортировка (по имени, потом client_id)
    out.sort((a, b) => {
      const an = (a.name || '').toLowerCase();
      const bn = (b.name || '').toLowerCase();
      if (an < bn) return -1;
      if (an > bn) return 1;
      return String(a.client_id || '').localeCompare(String(b.client_id || ''));
    });

    return out;
  }

  function readSecrets() {
    try {
      if (Array.isArray(window.OZON_STORES)) return window.OZON_STORES;
      if (Array.isArray(window.STORES)) return window.STORES;
    } catch (e) {}
    return [];
  }

  function readLocal() {
    try {
      for (const k of LS_KEYS) {
        const v = localStorage.getItem(k);
        if (!v) continue;
        const j = safeParse(v);
        if (Array.isArray(j) && j.length) return j;
      }
    } catch (e) {}
    return [];
  }

  function writeLocal(arr) {
    try {
      const s = JSON.stringify(arr || []);
      for (const k of LS_KEYS) localStorage.setItem(k, s);
    } catch (e) {}
  }

  function computeMerged() {
    const secrets = readSecrets();
    const local = readLocal();
    const merged = mergeStores(secrets, local);

    // если local пустой, но secrets есть — сохраняем в local, чтобы на всех страницах было одинаково
    if ((!local || !local.length) && merged.length) writeLocal(merged);

    window.OZON_STORES = merged;
    window.STORES = merged;
    return merged;
  }

  window.ozonLoadStores = function() {
    return computeMerged();
  };

  window.ozonSaveStores = function(arr) {
    const merged = mergeStores(readSecrets(), arr || []);
    writeLocal(merged);
    window.OZON_STORES = merged;
    window.STORES = merged;
    return merged;
  };

  // Инициализация
  computeMerged();

  // Если localStorage меняется в другой вкладке — обновляем список
  try {
    window.addEventListener('storage', (e) => {
      if (!e || !e.key) return;
      if (LS_KEYS.includes(e.key)) computeMerged();
    });
  } catch (e) {}
})();
