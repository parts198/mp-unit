(() => {
  const ITEMS = [
    ['index.html','Цены'],
    ['actions.html','Акции'],
    ['orders.html','Заказы'],
    ['returns.html','Возвраты'],
    ["orders_returns.html","Заказы и возвраты"],
    ['transactions.html','Начисления'],
    ['dashboard.html','Дашборд'],
    ['cluster_flow.html','Кластеры'],
    ['stores.html','Магазины'],
    ['cluster_demand.html','Кластерный спрос'],
    ['fbo_acceptance.html','Акты приёмки'],
    ['fbo_planning.html','Планирование FBO'],
  ];

  const cur = (location.pathname.split('/').pop() || 'index.html').split('?')[0].split('#')[0];
  const nav = document.querySelector('nav.nav');
  if(!nav) return;

  nav.innerHTML = ITEMS.map(([href,title]) => {
    const a = document.createElement('a');
    a.href = href;
    a.textContent = title;
    if(href === cur) a.setAttribute('aria-current','page');
    return a.outerHTML;
  }).join('\n');
})();
