(() => {
  const LS_STORES   = 'ozon_stores_v1';     // ЕДИНЫЙ ключ магазинов для всех страниц
  const LS_CLUSTERS = 'ozon_clusters_v1';   // ЕДИНЫЙ ключ кластеров (список строк)

  function normStore(st){
    if(!st) return null;
    const name = String(st.name ?? st.title ?? '').trim();
    const client_id = String(st.client_id ?? st.clientId ?? st['Client-Id'] ?? '').trim();
    const api_key = String(st.api_key ?? st.apiKey ?? st['Api-Key'] ?? '').trim();
    if(!name || !client_id || !api_key) return null;
    return { name, client_id, api_key };
  }
  function storeKey(st){ return `${st.name}|${st.client_id}`; }

  function dedupeStores(stores){
    const mp = new Map();
    for(const s of (stores || [])){
      const n = normStore(s);
      if(!n) continue;
      mp.set(storeKey(n), n);
    }
    return Array.from(mp.values());
  }

  function loadStoresLS(){
    try{
      const raw = localStorage.getItem(LS_STORES);
      const arr = raw ? JSON.parse(raw) : [];
      if(Array.isArray(arr)) return arr.map(normStore).filter(Boolean);
    }catch(e){}
    return [];
  }

  function saveStoresLS(stores){
    try{ localStorage.setItem(LS_STORES, JSON.stringify(dedupeStores(stores))); }catch(e){}
  }

  // Миграция: если где-то раньше были другие ключи — подхватим один раз
  function migrateLegacyToNew(){
    const cur = loadStoresLS();
    if(cur.length) return cur;

    const legacyKeys = [
      'ozon_cluster_demand_stores_v1',
      'ozonStores',
      'stores',
      'ozon_stores'
    ];
    for(const k of legacyKeys){
      try{
        const raw = localStorage.getItem(k);
        if(!raw) continue;
        const arr = JSON.parse(raw);
        if(!Array.isArray(arr)) continue;
        const tmp = arr.map(normStore).filter(Boolean);
        if(tmp.length){
          saveStoresLS(tmp);
          return tmp;
        }
      }catch(e){}
    }
    return [];
  }

  function loadStores({seedFromSecrets=true}={}){
    let stores = migrateLegacyToNew();

    // secrets — только ДОБАВЛЯЕМ недостающие, ничего не удаляем
    if(seedFromSecrets){
      try{
        const sec = window.OZON_STORES || window.stores || window.ozonStores || window.OzonStores || [];
        if(Array.isArray(sec) && sec.length){
          const tmp = sec.map(normStore).filter(Boolean);
          stores = dedupeStores([...(stores||[]), ...(tmp||[])]);
          saveStoresLS(stores);
        }
      }catch(e){}
    }
    return stores;
  }

  // Кластеры
  function loadClusters(){
    try{
      const raw = localStorage.getItem(LS_CLUSTERS);
      const arr = raw ? JSON.parse(raw) : [];
      if(Array.isArray(arr)){
        return arr.map(x => String(x||'').trim()).filter(Boolean);
      }
    }catch(e){}
    return [];
  }

  function saveClusters(list){
    const uniq = Array.from(new Set((list||[]).map(x => String(x||'').trim()).filter(Boolean)))
      .sort((a,b)=>a.localeCompare(b,'ru'));
    try{ localStorage.setItem(LS_CLUSTERS, JSON.stringify(uniq)); }catch(e){}
    return uniq;
  }

  function mergeClusters(list){
    return saveClusters([ ...loadClusters(), ...(list||[]) ]);
  }

  window.OZON_LS_STORES_KEY = LS_STORES;
  window.OZON_LS_CLUSTERS_KEY = LS_CLUSTERS;

  window.ozonLoadStores = loadStores;
  window.ozonSaveStores = saveStoresLS;
  window.ozonStoreKey = storeKey;

  window.ozonLoadClusters = loadClusters;
  window.ozonMergeClusters = mergeClusters;
})();
