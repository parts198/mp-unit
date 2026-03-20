(() => {
  const LS_KEY = 'ozon_stores_v1';

  function normStore(st){
    if(!st) return null;
    const name = String(st.name ?? st.title ?? '').trim();
    const client_id = String(st.client_id ?? st.clientId ?? st['Client-Id'] ?? '').trim();
    const api_key = String(st.api_key ?? st.apiKey ?? st['Api-Key'] ?? '').trim();
    if(!name || !client_id || !api_key) return null;
    return { name, client_id, api_key };
  }

  function getFromLS(){
    try{
      const raw = localStorage.getItem(LS_KEY);
      const arr = raw ? JSON.parse(raw) : [];
      if(!Array.isArray(arr)) return [];
      return arr.map(normStore).filter(Boolean);
    }catch(e){ return []; }
  }

  function setToLS(stores){
    const out = (stores || []).map(normStore).filter(Boolean);
    try{ localStorage.setItem(LS_KEY, JSON.stringify(out)); }catch(e){}
    return out;
  }

  function getFromSecrets(){
    try{
      const arr = window.OZON_STORES || window.stores || window.ozonStores || [];
      if(!Array.isArray(arr)) return [];
      return arr.map(normStore).filter(Boolean);
    }catch(e){ return []; }
  }

  function get(){
    const ls = getFromLS();
    if(ls.length) return ls;
    const sec = getFromSecrets();
    if(sec.length) return setToLS(sec);
    return [];
  }

  function set(stores){ return setToLS(stores); }

  window.OzonStores = { LS_KEY, get, set, normStore };
})();
