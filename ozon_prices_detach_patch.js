(() => {
  const FLAG = '__ozon_prices_detach_patch_v1__';
  if (window[FLAG]) return;
  window[FLAG] = true;

  function patchPriceBody(body){
    try{
      if(!body || !Array.isArray(body.prices)) return body;
      for(const p of body.prices){
        // VAT вообще не отправляем
        if(p && Object.prototype.hasOwnProperty.call(p,'vat')) delete p.vat;

        // Снять из стратегии ценообразования
        p.price_strategy_enabled = 'DISABLED';

        // Выключить автомеханику акций, чтобы Ozon не “держал” цену
        p.auto_action_enabled = 'DISABLED';
        p.auto_add_to_ozon_actions_list_enabled = 'DISABLED';

        // На всякий случай: изменение price не должно автоматически добавлять в “эластичный бустинг”
        if(Object.prototype.hasOwnProperty.call(p,'manage_elastic_boosting_through_price')){
          p.manage_elastic_boosting_through_price = false;
        }
      }
    }catch(e){}
    return body;
  }

  function wrapOzonFetch(){
    if(typeof window.ozonFetch !== 'function' || window.ozonFetch.__patched_prices_detach) return false;
    const orig = window.ozonFetch;
    const w = async function(path, store, body){
      try{
        const p = String(path || '');
        if(p.includes('/v1/product/import/prices')) body = patchPriceBody(body);
      }catch(e){}
      return orig.call(this, path, store, body);
    };
    w.__patched_prices_detach = true;
    window.ozonFetch = w;
    return true;
  }

  function wrapPostJson(){
    if(typeof window.postJson !== 'function' || window.postJson.__patched_prices_detach) return false;
    const orig = window.postJson;
    const w = async function(url, body, store){
      try{
        const u = String(url || '');
        if(u.includes('api-seller.ozon.ru') && u.includes('/v1/product/import/prices')) body = patchPriceBody(body);
      }catch(e){}
      return orig.call(this, url, body, store);
    };
    w.__patched_prices_detach = true;
    window.postJson = w;
    return true;
  }

  // пытаемся применить сразу и повторяем, пока страница не догрузит нужные функции
  function boot(){
    const ok1 = wrapOzonFetch();
    const ok2 = wrapPostJson();
    if(!(ok1 || ok2)) setTimeout(boot, 200);
  }
  boot();
})();
