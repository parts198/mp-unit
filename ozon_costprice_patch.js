(() => {
  const LS_KEY = 'ozon_costprice_map_v1';

  const ORIG_FETCH = window.fetch;
  if (!ORIG_FETCH) return;

  function loadMap() {
    try {
      const raw = localStorage.getItem(LS_KEY);
      const m = raw ? JSON.parse(raw) : {};
      return (m && typeof m === 'object') ? m : {};
    } catch { return {}; }
  }
  function saveMap(m) {
    try { localStorage.setItem(LS_KEY, JSON.stringify(m || {})); } catch {}
  }

  function normNumStr(v) {
    if (v === null || v === undefined) return '';
    let s = String(v).trim();
    if (!s) return '';
    s = s.replace(/\s+/g,'').replace(',','.');
    // оставить только число
    if (!/^-?\d+(\.\d+)?$/.test(s)) return '';
    return s;
  }

  function toNum(v) {
    const s = normNumStr(v);
    if (!s) return NaN;
    const n = Number(s);
    return Number.isFinite(n) ? n : NaN;
  }

  function round2(n) {
    return Math.round(n * 100) / 100;
  }

  function isCostInput(el) {
    if (!el || el.tagName !== 'INPUT') return false;
    const t = (el.type || '').toLowerCase();
    if (t !== 'text' && t !== 'number') return false;

    const s = [
      el.name, el.id, el.className,
      el.placeholder, el.getAttribute('data-field'),
      el.getAttribute('data-col'), el.getAttribute('aria-label')
    ].filter(Boolean).join(' ').toLowerCase();

    // ключевые слова для себестоимости
    return s.includes('себест') || s.includes('cost') || s.includes('net_price') || s.includes('netprice');
  }

  function findRow(el) {
    return el.closest('tr') || el.closest('.row') || null;
  }

  function guessOfferIdFromRow(row) {
    if (!row) return '';
    // 1) явные атрибуты
    const d = row.dataset || {};
    const direct = d.offerId || d.offer_id || d.offer || '';
    if (direct) return String(direct).trim();

    // 2) элемент с data-offer-id
    const x = row.querySelector('[data-offer-id]');
    if (x) {
      const v = x.getAttribute('data-offer-id');
      if (v) return String(v).trim();
    }

    // 3) первый <code> в строке (часто offer_id)
    const c = row.querySelector('code');
    if (c && c.textContent) {
      const v = c.textContent.trim();
      if (v) return v;
    }

    // 4) эвристика по тексту строки
    const txt = (row.textContent || '').trim();
    // ищем "похожее на артикул": буквы/цифры/подчёркивания/дефисы
    const m = txt.match(/\b[0-9A-ZА-Я][0-9A-ZА-Я_\-\.]{4,}\b/i);
    return m ? m[0] : '';
  }

  function findPriceInput(row) {
    if (!row) return null;
    const inputs = Array.from(row.querySelectorAll('input'));
    // ищем "price" но не old/min
    return inputs.find(i => {
      const s = [i.name,i.id,i.className,i.placeholder,i.getAttribute('data-field'),i.getAttribute('data-col')]
        .filter(Boolean).join(' ').toLowerCase();
      if (!s.includes('price') && !s.includes('цена')) return false;
      if (s.includes('old') || s.includes('стар') || s.includes('min') || s.includes('миним')) return false;
      return true;
    }) || null;
  }

  function findMarkupInput(row) {
    if (!row) return null;
    const inputs = Array.from(row.querySelectorAll('input'));
    return inputs.find(i => {
      const s = [i.name,i.id,i.className,i.placeholder,i.getAttribute('data-field'),i.getAttribute('data-col')]
        .filter(Boolean).join(' ').toLowerCase();
      return s.includes('markup') || s.includes('margin') || s.includes('нацен') || s.includes('марж') || s.includes('pct') || s.includes('%');
    }) || null;
  }

  function dispatchInput(el) {
    try { el.dispatchEvent(new Event('input', { bubbles: true })); } catch {}
    try { el.dispatchEvent(new Event('change', { bubbles: true })); } catch {}
  }

  function recalcRowFromCost(costInput) {
    const row = findRow(costInput);
    if (!row) return;

    const cost = toNum(costInput.value);
    if (!Number.isFinite(cost) || cost <= 0) return;

    const priceInput = findPriceInput(row);
    const markupInput = findMarkupInput(row);

    // Правило:
    // - если задана наценка% → пересчитываем цену = себест * (1 + %/100)
    // - иначе если задана цена → пересчитываем наценку% = (цена/себест - 1)*100
    if (markupInput) {
      const pct = toNum(markupInput.value);
      if (Number.isFinite(pct)) {
        if (priceInput) {
          const newPrice = round2(cost * (1 + pct / 100));
          priceInput.value = String(newPrice);
          dispatchInput(priceInput);
        }
        return;
      }
    }

    if (priceInput && markupInput) {
      const price = toNum(priceInput.value);
      if (Number.isFinite(price) && price > 0) {
        const pct = round2(((price / cost) - 1) * 100);
        markupInput.value = String(pct);
        dispatchInput(markupInput);
      }
    }
  }

  function refreshMapFromDom() {
    const m = loadMap();
    const inputs = Array.from(document.querySelectorAll('input')).filter(isCostInput);
    for (const inp of inputs) {
      const row = findRow(inp);
      const offer = guessOfferIdFromRow(row);
      const val = normNumStr(inp.value);
      if (offer && val) m[offer] = val;
    }
    saveMap(m);
    return m;
  }

  // Сохраняем себестоимость при вводе + пересчёт
  /* COSTPRICE_DEBOUNCE_INPUT_V1 */
    let __cpTimer = null;
    document.addEventListener('input', (e) => {
      try {
        if (e && e.isComposing) return;
        const el = e.target;
        if (!isCostInput(el)) return;

        clearTimeout(__cpTimer);
        __cpTimer = setTimeout(() => {
          try {
            const row = findRow(el);
            const offer = guessOfferIdFromRow(row);
            const val = normNumStr(el.value);

            if (offer && val) {
              const m = loadMap();
              m[offer] = val;
              saveMap(m);
            }

            recalcRowFromCost(el);
          } catch {}
        }, 250);
      } catch {}
    }, true);

document.addEventListener('change', (e) => {
    const el = e.target;
    if (!isCostInput(el)) return;

    const row = findRow(el);
    const offer = guessOfferIdFromRow(row);
    const val = normNumStr(el.value);

    if (offer && val) {
      const m = loadMap();
      m[offer] = val;
      saveMap(m);
    }

    recalcRowFromCost(el);
  }, true);

  // Подмешивание net_price + удаление vat при отправке цен
  window.fetch = async (input, init) => {
    try {
      const url =
        (typeof input === 'string') ? input :
        (input && typeof input.url === 'string') ? input.url : '';

      const method =
        (init && init.method) ? String(init.method).toUpperCase() :
        (input && input.method) ? String(input.method).toUpperCase() : 'GET';

      if (
        method === 'POST' &&
        url.includes('api-seller.ozon.ru') &&
        url.includes('/v1/product/import/prices') &&
        init && typeof init.body === 'string'
      ) {
        // обновим карту из DOM, чтобы не зависеть от того, "трогали" ли себестоимость сейчас
        const m = refreshMapFromDom();

        const j = JSON.parse(init.body);
        if (j && Array.isArray(j.prices)) {
          for (const p of j.prices) {
            // 1) VAT не отправляем
            if ('vat' in p) delete p.vat;

            // 2) net_price подмешиваем по offer_id
            const offer = String(p.offer_id || '').trim();
            if (offer && m[offer]) {
              p.net_price = String(m[offer]);
            }
          }
          init = Object.assign({}, init, { body: JSON.stringify(j) });
        }
      }
    } catch {}
    return ORIG_FETCH(input, init);
  };
})();
