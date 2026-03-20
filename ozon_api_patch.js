(() => {
  const ORIG_FETCH = window.fetch;
  if(!ORIG_FETCH) return;

  // Настройки по умолчанию (можете менять при необходимости)
  const CFG = window.OZON_PATCH_CONFIG = Object.assign({
    defaultMaxDays: 31,     // дефолт для date_from/date_to и неизвестных since/to
    postingMaxDays: 7,      // для /posting/*/list — безопасно дробим по 7 дней
    sleepMs: 120,           // небольшая пауза между чанками/страницами (чтобы не ловить лимиты)
    maxRetries: 2,          // ретраи на 429/5xx
    retryBaseMs: 600
  }, window.OZON_PATCH_CONFIG || {});

  const sleep = (ms) => new Promise(r => setTimeout(r, ms));

  function isOzonApiUrl(url){
    return typeof url === 'string' && url.includes('api-seller.ozon.ru');
  }
  function pickMaxDays(url){
    // “листинги” лучше дробить коротко (часто так стабильнее)
    if(/\/v[0-9]+\/posting\/(fbo|fbs)\/list/i.test(url)) return CFG.postingMaxDays;
    return CFG.defaultMaxDays;
  }

  function safeJsonParse(s){
    try{ return JSON.parse(s); }catch(e){ return null; }
  }

  function extractListShape(j){
    // Возвращает { getItems(j)->array, setItems(sample, items)->mergedJson } или null (если не “список”)
    if(!j) return null;

    if(Array.isArray(j.result)){
      return {
        getItems: (x)=> Array.isArray(x?.result) ? x.result : [],
        setItems: (_sample, items)=> ({ result: items })
      };
    }
    if(Array.isArray(j?.result?.postings)){
      return {
        getItems: (x)=> Array.isArray(x?.result?.postings) ? x.result.postings : [],
        setItems: (sample, items)=> {
          const out = Object.assign({}, sample);
          out.result = Object.assign({}, sample.result, { postings: items, has_next: false });
          return out;
        }
      };
    }
    if(Array.isArray(j?.result?.items)){
      return {
        getItems: (x)=> Array.isArray(x?.result?.items) ? x.result.items : [],
        setItems: (sample, items)=> {
          const out = Object.assign({}, sample);
          out.result = Object.assign({}, sample.result, { items: items });
          return out;
        }
      };
    }
    if(Array.isArray(j?.items)){
      return {
        getItems: (x)=> Array.isArray(x?.items) ? x.items : [],
        setItems: (sample, items)=> Object.assign({}, sample, { items })
      };
    }
    return null;
  }

  function findDateFilter(body){
    // Возвращает { obj, fromKey, toKey, type } где type: 'iso'|'ymd' или null
    if(!body || typeof body !== 'object') return null;

    const f = body.filter && typeof body.filter === 'object' ? body.filter : null;
    if(f){
      if(f.since && f.to) return { obj: f, fromKey:'since', toKey:'to', type:'iso' };
      if(f.date_from && f.date_to) return { obj: f, fromKey:'date_from', toKey:'date_to', type:'ymd' };
    }
    if(body.since && body.to) return { obj: body, fromKey:'since', toKey:'to', type:'iso' };
    if(body.date_from && body.date_to) return { obj: body, fromKey:'date_from', toKey:'date_to', type:'ymd' };

    return null;
  }

  function rangeDays(d){
    if(!d) return 0;
    if(d.type === 'iso'){
      const a = new Date(String(d.obj[d.fromKey]||''));
      const b = new Date(String(d.obj[d.toKey]||''));
      if(!isFinite(a)||!isFinite(b)) return 0;
      const from = a.toISOString().slice(0,10);
      const to   = b.toISOString().slice(0,10);
      return window.ozonDiffDaysInclusive ? window.ozonDiffDaysInclusive(from,to) : 0;
    }
    if(d.type === 'ymd'){
      const from = String(d.obj[d.fromKey]||'').slice(0,10);
      const to   = String(d.obj[d.toKey]||'').slice(0,10);
      return window.ozonDiffDaysInclusive ? window.ozonDiffDaysInclusive(from,to) : 0;
    }
    return 0;
  }

  function splitRange(d, maxDays){
    const max = Math.max(1, Number(maxDays||31)||31);
    if(d.type === 'iso'){
      return window.ozonSplitIsoRange ? window.ozonSplitIsoRange(d.obj[d.fromKey], d.obj[d.toKey], max) : [];
    }
    // ymd
    return window.ozonSplitYmdRange ? window.ozonSplitYmdRange(d.obj[d.fromKey], d.obj[d.toKey], max) : [];
  }

  function stripVatFromPriceImport(bodyObj){
    if(!bodyObj || typeof bodyObj !== 'object') return bodyObj;
    if(!Array.isArray(bodyObj.prices)) return bodyObj;
    for(const p of bodyObj.prices){
      if(p && typeof p === 'object' && 'vat' in p) delete p.vat;
    }
    return bodyObj;
  }

  async function fetchWithRetry(url, init){
    let lastErr = null;
    for(let attempt=0; attempt<=CFG.maxRetries; attempt++){
      try{
        const r = await ORIG_FETCH(url, init);
        // ретраим 429 и 5xx
        if(r.status === 429 || (r.status >= 500 && r.status <= 599)){
          lastErr = new Error('HTTP ' + r.status);
          if(attempt < CFG.maxRetries){
            await sleep(CFG.retryBaseMs * Math.pow(2, attempt));
            continue;
          }
        }
        return r;
      }catch(e){
        lastErr = e;
        if(attempt < CFG.maxRetries){
          await sleep(CFG.retryBaseMs * Math.pow(2, attempt));
          continue;
        }
        throw lastErr;
      }
    }
    throw lastErr || new Error('fetch failed');
  }

  async function runChunkedList(url, init, bodyObj, dateInfo){
    const shape0 = null; // определим по первому ответу
    const maxDays = pickMaxDays(url);
    const chunks = splitRange(dateInfo, maxDays);
    if(!chunks.length) return null;

    // Пагинация: если есть limit/offset — собираем всё в один массив
    const hasPaging = (typeof bodyObj.limit !== 'undefined') || (typeof bodyObj.offset !== 'undefined');
    const limit = Math.max(1, Number(bodyObj.limit || 1000) || 1000);

    let sample = null;
    let shape = null;
    const all = [];

    for(let ci=0; ci<chunks.length; ci++){
      const ch = chunks[ci];

      // выставляем диапазон на текущий чанк
      if(dateInfo.type === 'iso'){
        dateInfo.obj[dateInfo.fromKey] = ch.sinceISO;
        dateInfo.obj[dateInfo.toKey]   = ch.toISO;
      }else{
        dateInfo.obj[dateInfo.fromKey] = ch.fromYmd;
        dateInfo.obj[dateInfo.toKey]   = ch.toYmd;
      }

      if(hasPaging){
        let offset = 0;
        for(let pi=0; pi<20000; pi++){
          bodyObj.limit = limit;
          bodyObj.offset = offset;

          const r = await fetchWithRetry(url, Object.assign({}, init, { body: JSON.stringify(bodyObj) }));
          const t = await r.text();
          const j = safeJsonParse(t);

          if(!r.ok){
            const msg = (j && (j.message || j.error)) ? (j.message || j.error) : (t || ('HTTP ' + r.status));
            throw new Error(msg);
          }
          if(!sample) sample = j;
          if(!shape) shape = extractListShape(j);
          if(!shape){
            // не список -> не трогаем
            return null;
          }

          const items = shape.getItems(j) || [];
          all.push(...items);

          if(items.length < limit) break;
          offset += limit;

          if(CFG.sleepMs) await sleep(CFG.sleepMs);
        }
      }else{
        const r = await fetchWithRetry(url, Object.assign({}, init, { body: JSON.stringify(bodyObj) }));
        const t = await r.text();
        const j = safeJsonParse(t);

        if(!r.ok){
          const msg = (j && (j.message || j.error)) ? (j.message || j.error) : (t || ('HTTP ' + r.status));
          throw new Error(msg);
        }
        if(!sample) sample = j;
        if(!shape) shape = extractListShape(j);
        if(!shape){
          return null;
        }
        const items = shape.getItems(j) || [];
        all.push(...items);
      }

      if(CFG.sleepMs) await sleep(CFG.sleepMs);
    }

    if(!sample || !shape) return null;
    return shape.setItems(sample, all);
  }

  window.fetch = async (input, init) => {
    const url = (typeof input === 'string') ? input : (input && typeof input.url === 'string' ? input.url : '');
    const method =
      (init && init.method) ? String(init.method).toUpperCase() :
      (input && input.method) ? String(input.method).toUpperCase() : 'GET';

    // нас интересуют POST в API + JSON body
    if(method === 'POST' && isOzonApiUrl(url) && init && typeof init.body === 'string'){
      // 1) price import: убираем vat
      if(url.includes('/v1/product/import/prices')){
        try{
          const b = safeJsonParse(init.body);
          if(b){
            stripVatFromPriceImport(b);
            init = Object.assign({}, init, { body: JSON.stringify(b) });
          }
        }catch(e){}
        return ORIG_FETCH(input, init);
      }

      // 2) авто-дробление диапазона
      try{
        const b = safeJsonParse(init.body);
        const d = findDateFilter(b);
        if(b && d){
          const days = rangeDays(d);
          const maxDays = pickMaxDays(url);

          // дробим только если реально превышено
          if(days && days > maxDays){
            const merged = await runChunkedList(url, init, b, d);
            if(merged){
              const txt = JSON.stringify(merged);
              return new Response(txt, { status: 200, headers: { 'Content-Type':'application/json' } });
            }
          }
        }
      }catch(e){
        // если что-то пошло не так — просто отдаём оригинальный fetch
      }
    }

    return ORIG_FETCH(input, init);
  };
})();
