(() => {
  const STYLE_ID = 'table-sort-style-v1';

  function injectStyle(){
    if(document.getElementById(STYLE_ID)) return;
    const st = document.createElement('style');
    st.id = STYLE_ID;
    st.textContent = `
      th[data-sortable="1"]{ cursor:pointer; user-select:none; }
      th[data-sortable="1"]::after{ content:""; margin-left:6px; opacity:.55; }
      th[data-sort-dir="asc"]::after{ content:"▲"; }
      th[data-sort-dir="desc"]::after{ content:"▼"; }
    `;
    document.head.appendChild(st);
  }

  function normText(s){
    return String(s ?? '').trim().toLowerCase().replace(/\s+/g,' ');
  }

  function parseValue(raw){
    const s0 = String(raw ?? '').trim();
    if(!s0) return {t:'text', v:''};

    // числовые: "1 234", "1 234,56", "853 279 ₽"
    const s = s0
      .replace(/\u00A0/g,' ')
      .replace(/₽/g,'')
      .replace(/%/g,'')
      .replace(/\s+/g,' ')
      .trim();

    const numLike = s.replace(/\s/g,'').replace(',', '.');
    if(/^[-+]?\d+(\.\d+)?$/.test(numLike)){
      return {t:'num', v: Number(numLike)};
    }

    return {t:'text', v: normText(s0)};
  }

  function getCell(tr, idx){
    const tds = tr.children;
    if(!tds || idx < 0 || idx >= tds.length) return '';
    return tds[idx].innerText ?? tds[idx].textContent ?? '';
  }

  function makeSortable(table){
    if(!table || table.dataset.sortReady === '1') return;

    const thead = table.querySelector('thead');
    const tbody = table.querySelector('tbody');
    if(!thead || !tbody) return;

    const ths = Array.from(thead.querySelectorAll('th'));
    if(!ths.length) return;

    ths.forEach((th, idx) => {
      th.setAttribute('data-sortable','1');
      th.addEventListener('click', () => {
        // toggle dir
        const curDir = th.getAttribute('data-sort-dir') || '';
        const dir = (curDir === 'asc') ? 'desc' : 'asc';

        // reset other headers
        ths.forEach(x => { if(x !== th) x.removeAttribute('data-sort-dir'); });
        th.setAttribute('data-sort-dir', dir);

        const rows = Array.from(tbody.querySelectorAll('tr'));
        const enriched = rows.map((tr, i) => {
          const val = parseValue(getCell(tr, idx));
          return {tr, i, val};
        });

        enriched.sort((a,b) => {
          const da = a.val, db = b.val;
          let cmp = 0;
          if(da.t === 'num' && db.t === 'num'){
            cmp = (da.v - db.v);
          } else {
            cmp = String(da.v).localeCompare(String(db.v), 'ru');
          }
          if(cmp === 0) cmp = a.i - b.i; // стабильность
          return (dir === 'asc') ? cmp : -cmp;
        });

        const frag = document.createDocumentFragment();
        enriched.forEach(x => frag.appendChild(x.tr));
        tbody.innerHTML = '';
        tbody.appendChild(frag);
      });
    });

    table.dataset.sortReady = '1';
  }

  function scan(){
    injectStyle();
    document.querySelectorAll('table').forEach(makeSortable);
  }

  document.addEventListener('DOMContentLoaded', scan);

  // Автоподхват таблиц, которые генерируются позже
  const mo = new MutationObserver(() => scan());
  mo.observe(document.documentElement, {subtree:true, childList:true});

  // экспорт на всякий
  window.makeSortableTable = makeSortable;
})();
