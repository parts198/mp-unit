(() => {
  const PATH = '/v3/finance/transaction/list';

  const RE_POSTING = /\b(\d{6,}-\d{3,}-\d+)\b/; // пример: 0213371939-0034-1
  const money = (n) => {
    const x = Number(n);
    if (!Number.isFinite(x)) return '0 ₽';
    const abs = Math.round(Math.abs(x) * 100) / 100;
    const s = abs.toFixed(2).replace(/\.00$/, '').replace('.', ',');
    // пробелы как разделитель тысяч
    return s.replace(/\B(?=(\d{3})+(?!\d))/g, ' ') + ' ₽';
  };

  const toZ = (ymd, end) => {
    // трактуем выбранную дату как UTC, чтобы не “съедало” сутки из-за локального TZ
    return end ? `${ymd}T23:59:59.999Z` : `${ymd}T00:00:00.000Z`;
  };

  function getRange() {
    const dates = Array.from(document.querySelectorAll('input[type="date"]'))
      .map(i => String(i.value || '').trim())
      .filter(Boolean);

    if (dates.length < 2) return null;
    const from = dates[0];
    const to = dates[1];
    return { from: toZ(from, false), to: toZ(to, true) };
  }

  function getStoreFromUI() {
    // fetchOzon сам умеет брать магазин из UI/ozonLoadStores, но если на странице есть store — передадим его явно.
    try {
      const sel =
        document.querySelector('#storeSelect') ||
        document.querySelector('#loadStore') ||
        document.querySelector('#store') ||
        document.querySelector('select[data-store-select="1"]') ||
        document.querySelector('select');

      const stores = (window.ozonLoadStores ? (window.ozonLoadStores() || []) : (window.OZON_STORES || window.STORES || [])) || [];
      if (!sel || !stores.length) return null;

      const v = String(sel.value || '').trim();
      if (!v) return null;

      const parts = v.split('|');
      const cid = (parts.length > 1 ? parts[1] : parts[0]).trim();
      const hit = stores.find(s => String(s.client_id) === cid) || stores.find(s => String(s.clientId) === cid);
      return hit || null;
    } catch {
      return null;
    }
  }

  function serviceSum(op, names) {
    let s = 0;
    const arr = op?.services;
    if (!Array.isArray(arr)) return 0;
    for (const x of arr) {
      const nm = String(x?.name || '').trim();
      if (!nm) continue;
      if (names.includes(nm)) s += Number(x.price || 0) || 0;
    }
    return s;
  }

  function buildPostingAgg(operations) {
    const m = {}; // posting_number -> agg
    for (const op of operations || []) {
      const pn = op?.posting?.posting_number || op?.posting_number || null;
      if (!pn) continue;

      const a = m[pn] || (m[pn] = {
        commission: 0,
        logistics: 0,
        delivery: 0,
        first_mile: 0,
        packaging: 0,
      });

      // Комиссия (часто отрицательная) — показываем как расход (плюсом)
      a.commission += Math.abs(Number(op.sale_commission || 0) || 0);

      // Разносим услуги по колонкам.
      // Важно: названия могут быть как кодами, так и уже русскими (после russify в ozon_fetch.js).
      // Поэтому учитываем оба варианта.
      const pack = [
        'MarketplaceServiceItemFulfillment', 'Сборка заказа',
        'MarketplaceServiceItemDropoffFF', 'Обработка отправления (FF)',
        'MarketplaceServiceItemDropoffPVZ', 'Обработка отправления (ПВЗ)',
        'MarketplaceServiceItemDropoffSC', 'Обработка отправления (SC)',
      ];
      const deliv = [
        'MarketplaceServiceItemDelivToCustomer', 'Последняя миля',
        'MarketplaceDeliveryCostItem', 'Доставка товара до покупателя',
        'MarketplaceServiceItemDeliveryKGT', 'Доставка КГТ',
      ];
      const first = [
        'MarketplaceServiceItemPickup', 'Pick-up (забор отправлений)',
      ];
      const logi = [
        'MarketplaceServiceItemDirectFlowLogistic', 'Логистика',
        'MarketplaceServiceItemReturnFlowLogistic', 'Обратная логистика',
        'MarketplaceServiceItemDirectFlowTrans', 'Магистраль',
        'MarketplaceServiceItemReturnFlowTrans', 'Обратная магистраль',
        'MarketplaceServiceItemDirectFlowLogisticVDC', 'Логистика вРЦ',
      ];

      a.packaging += Math.abs(serviceSum(op, pack));
      a.delivery  += Math.abs(serviceSum(op, deliv));
      a.first_mile += Math.abs(serviceSum(op, first));
      a.logistics += Math.abs(serviceSum(op, logi));
    }
    return m;
  }

  function findDetailTables() {
    const tables = Array.from(document.querySelectorAll('table'));
    return tables.filter(t => {
      const th = Array.from(t.querySelectorAll('th')).map(x => (x.textContent || '').trim().toLowerCase());
      return th.includes('комиссия') && th.includes('логистика') && th.includes('упаковка');
    });
  }

  function headerIndexMap(table) {
    const ths = Array.from(table.querySelectorAll('th'));
    const map = {};
    ths.forEach((th, i) => {
      const k = (th.textContent || '').trim().toLowerCase();
      map[k] = i;
    });
    return map;
  }

  function guessPostingForTable(table) {
    // ищем posting_number в ближайшем контейнере выше (включая предыдущие строки)
    let p = table;
    for (let step = 0; step < 10 && p; step++) {
      const txt = (p.textContent || '');
      const m = txt.match(RE_POSTING);
      if (m) return m[1];
      p = p.parentElement;
    }
    return null;
  }

  function setCell(tr, idx, value) {
    const tds = tr ? Array.from(tr.querySelectorAll('td')) : [];
    if (!tds[idx]) return;
    // Не затираем, если там уже не ноль (на случай, если где-то посчитано правильно)
    const cur = (tds[idx].textContent || '').replace(/\s+/g,' ').trim();
    if (cur && cur !== '0 ₽' && cur !== '0' && cur !== '0,0 ₽' && cur !== '0,00 ₽') return;
    tds[idx].textContent = value;
  }

  async function apply() {
    if (typeof window.fetchOzon !== 'function') {
      console.warn('fetchOzon missing');
      return;
    }

    const range = getRange();
    if (!range) {
      console.warn('date range missing');
      return;
    }

    const store = getStoreFromUI(); // может быть null — fetchOzon возьмёт сам
    const body = {
      filter: { date: { from: range.from, to: range.to } },
      page: 1,
      page_size: 1000,
      transaction_type: 'all',
    };

    const json = store
      ? await window.fetchOzon(PATH, { store, body })
      : await window.fetchOzon(PATH, body);

    const ops = json?.result?.operations || json?.operations || [];
    const agg = buildPostingAgg(ops);

    const tables = findDetailTables();
    for (const t of tables) {
      const pn = guessPostingForTable(t);
      if (!pn || !agg[pn]) continue;

      const idx = headerIndexMap(t);
      const iCommission = idx['комиссия'];
      const iLogistics = idx['логистика'];
      const iDelivery = idx['доставка клиенту'];
      const iFirstMile = idx['первая миля'];
      const iPackaging = idx['упаковка'];

      if ([iCommission,iLogistics,iDelivery,iFirstMile,iPackaging].some(x => typeof x !== 'number')) continue;

      const rows = Array.from(t.querySelectorAll('tbody tr'));
      for (const r of rows) {
        // обычно в детальной таблице одна позиция; если несколько — пока ставим одинаково (лучше, чем 0).
        setCell(r, iCommission, money(agg[pn].commission));
        setCell(r, iLogistics,  money(agg[pn].logistics));
        setCell(r, iDelivery,   money(agg[pn].delivery));
        setCell(r, iFirstMile,  money(agg[pn].first_mile));
        setCell(r, iPackaging,  money(agg[pn].packaging));
      }
    }
  }

  function hookButtons() {
    // кнопка "Загрузить заказы" (по тексту)
    const btns = Array.from(document.querySelectorAll('button, a')).filter(x => {
      const t = (x.textContent || '').toLowerCase();
      return t.includes('загруз') && t.includes('заказ');
    });

    for (const b of btns) {
      if (b.__financeHooked) continue;
      b.__financeHooked = true;
      b.addEventListener('click', () => {
        // дать странице дорендерить таблицу
        setTimeout(() => apply().catch(e => console.error(e)), 1200);
        setTimeout(() => apply().catch(e => console.error(e)), 3000);
      }, true);
    }

    // отдельная кнопка “Обновить комиссии/логистику”
    if (!document.getElementById('btnFinanceFill')) {
      const el = document.createElement('button');
      el.id = 'btnFinanceFill';
      el.type = 'button';
      el.textContent = 'Обновить комиссии/логистику';
      el.style.cssText = 'position:fixed;right:12px;bottom:12px;z-index:9999;padding:8px 12px;';
      el.addEventListener('click', () => apply().catch(e => alert(e.message || String(e))));
      document.body.appendChild(el);
    }
  }

  document.addEventListener('DOMContentLoaded', () => {
    hookButtons();
    // на всякий случай: если таблица уже на странице
    setTimeout(() => apply().catch(() => {}), 1500);
    // и периодически цепляем кнопки, если DOM перерисовывается
    setInterval(hookButtons, 2000);
  });
})();
