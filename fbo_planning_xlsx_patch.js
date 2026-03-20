(() => {
  function sleep(ms){ return new Promise(r => setTimeout(r, ms)); }

  function loadXlsxLib() {
    if (window.XLSX) return Promise.resolve();
    return new Promise((resolve, reject) => {
      const s = document.createElement('script');
      s.src = 'https://cdn.jsdelivr.net/npm/xlsx@0.18.5/dist/xlsx.full.min.js';
      s.onload = () => resolve();
      s.onerror = () => reject(new Error('Не удалось загрузить библиотеку XLSX (SheetJS)'));
      document.head.appendChild(s);
    });
  }

  function findBtnCsvReco() {
    const btns = Array.from(document.querySelectorAll('button, a'));
    return btns.find(b => /CSV/i.test(b.textContent || '') && /рекомендац/i.test(b.textContent || '')) || null;
  }

  function findClusterSelect() {
    return document.querySelector('#planCluster')
        || document.querySelector('#clusterSelect')
        || Array.from(document.querySelectorAll('select')).find(s => /кластер/i.test((s.parentElement?.innerText || '')) || /cluster/i.test(s.id || ''))
        || null;
  }

  function findRecoTable() {
    // 1) по заголовку блока
    const header = Array.from(document.querySelectorAll('h1,h2,h3,div'))
      .find(el => /Рекомендации\s+на\s+поставку\s+FBO/i.test(el.textContent || ''));
    if (header) {
      let n = header;
      for (let i=0; i<30 && n; i++) {
        if (n.tagName === 'TABLE') return n;
        const t = n.querySelector && n.querySelector('table');
        if (t) return t;
        n = n.nextElementSibling;
      }
    }
    // 2) fallback: первая таблица где есть "offer" и "рекоменд"
    const tables = Array.from(document.querySelectorAll('table'));
    return tables.find(t => /offer/i.test(t.innerText || '') && /рекоменд/i.test(t.parentElement?.innerText || t.innerText || '')) || null;
  }

  function tableToAOA(tbl) {
    const aoa = [];
    const rows = Array.from(tbl.querySelectorAll('tr'));
    for (const tr of rows) {
      const cells = Array.from(tr.querySelectorAll('th,td'));
      aoa.push(cells.map(c => (c.innerText || '').trim()));
    }
    return aoa.length ? aoa : [['Нет данных']];
  }

  function safeSheetName(name, existing) {
    let s = String(name || 'Sheet').trim();
    s = s.replace(/[:\\/?*\[\]]/g, '_');
    if (!s) s = 'Sheet';
    s = s.slice(0, 31);
    let uniq = s, k = 2;
    while (existing.includes(uniq)) {
      uniq = (s.slice(0, 28) + '_' + k).slice(0, 31);
      k++;
    }
    return uniq;
  }

  async function exportXlsxAllClusters() {
    await loadXlsxLib();
    const XLSX = window.XLSX;

    const sel = findClusterSelect();
    const tbl0 = findRecoTable();
    if (!tbl0) throw new Error('Не найдена таблица рекомендаций на странице.');

    const wb = XLSX.utils.book_new();

    const clusters = sel
      ? Array.from(sel.options).map(o => o.value).filter(v => v && v !== '__all__')
      : ['Рекомендации'];

    const original = sel ? sel.value : null;

    for (const cl of clusters) {
      if (sel) {
        sel.value = cl;
        sel.dispatchEvent(new Event('change', { bubbles: true }));
        // даём странице перерисоваться (если она пересчитывает таблицу)
        await sleep(200);
      }

      const tbl = findRecoTable();
      const aoa = tbl ? tableToAOA(tbl) : [['Нет данных (таблица не найдена)']];
      const ws = XLSX.utils.aoa_to_sheet(aoa);

      const sheetName = safeSheetName(cl, wb.SheetNames);
      XLSX.utils.book_append_sheet(wb, ws, sheetName);
    }

    if (sel && original !== null) {
      sel.value = original;
      sel.dispatchEvent(new Event('change', { bubbles: true }));
    }

    const fname = 'fbo_recommendations_' + new Date().toISOString().slice(0,10) + '.xlsx';
    XLSX.writeFile(wb, fname);
  }

  document.addEventListener('DOMContentLoaded', () => {
    const btn = findBtnCsvReco();
    if (!btn) return;

    // меняем подпись
    try { btn.textContent = btn.textContent.replace(/CSV/i, 'XLSX'); } catch(e){}

    // перехватываем клик
    btn.addEventListener('click', (e) => {
      e.preventDefault();
      e.stopImmediatePropagation();
      exportXlsxAllClusters().catch(err => {
        console.error(err);
        alert('XLSX экспорт: ' + (err?.message || err));
      });
    }, true);
  });
})();
