(() => {
  const LS_KEY = 'ozon_clusters_v1';
  const BASE = [
    'Москва','Санкт-Петербург','Воронеж','Краснодар','Екатеринбург','Казань','Самара',
    'Новосибирск','Пермь','Уфа','Омск','Ростов-на-Дону','Тюмень','Тверь','Саратов',
    'Невинномысск','Дальний Восток'
  ];

  function uniq(arr){
    const out=[]; const set=new Set();
    for(const x of (arr||[])){
      const s=String(x||'').trim();
      if(!s) continue;
      const k=s.toLowerCase();
      if(set.has(k)) continue;
      set.add(k); out.push(s);
    }
    return out;
  }
  function loadLS(){
    try{
      const v=JSON.parse(localStorage.getItem(LS_KEY)||'[]');
      return Array.isArray(v) ? v : [];
    }catch(e){ return []; }
  }
  function saveLS(arr){
    try{ localStorage.setItem(LS_KEY, JSON.stringify(arr)); }catch(e){}
  }
  function loadAll(){
    return uniq([...BASE, ...loadLS()]).sort((a,b)=>a.localeCompare(b,'ru'));
  }
  function remember(list){
    const cur=loadAll();
    const next=uniq([...cur, ...(list||[])]).sort((a,b)=>a.localeCompare(b,'ru'));
    saveLS(next);
    window.OZON_CLUSTERS = next;
    return next;
  }

  window.ozonLoadClusters = loadAll;
  window.ozonRememberClusters = remember;
  window.OZON_CLUSTERS = loadAll();
})();
