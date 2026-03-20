(() => {
  const LS_TO   = 'ozon_clusters_to_v1';
  const LS_FROM = 'ozon_clusters_from_v1';

  function safeJsonParse(raw, fallback){
    try { return JSON.parse(raw); } catch(e){ return fallback; }
  }

  function load(key){
    const raw = localStorage.getItem(key);
    const arr = raw ? safeJsonParse(raw, []) : [];
    return Array.isArray(arr) ? arr : [];
  }

  function save(key, arr){
    const set = new Set();
    for(const x of (arr || [])){
      const v = String(x || '').trim();
      if(v) set.add(v);
    }
    const out = Array.from(set.values()).sort((a,b)=>a.localeCompare(b,'ru'));
    localStorage.setItem(key, JSON.stringify(out));
    return out;
  }

  window.ozonLoadClustersTo   = () => load(LS_TO);
  window.ozonLoadClustersFrom = () => load(LS_FROM);

  window.ozonSaveClustersTo   = (arr) => save(LS_TO, arr);
  window.ozonSaveClustersFrom = (arr) => save(LS_FROM, arr);
})();
