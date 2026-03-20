(() => {
  const NAV = [
    ["index.html", "Цены"],
    ["actions.html", "Акции"],
    ["orders.html", "Заказы"],
    ["returns.html", "Возвраты"],
    ["orders_returns.html","Заказы и возвраты"],
    ["transactions.html", "Начисления"],
    ["dashboard.html", "Дашборд"],
    ["cluster_flow.html", "Кластеры"],
    ["stores.html", "Магазины"],
    ["cluster_demand.html", "Кластерный спрос"],
    ["fbo_acceptance.html", "Акты приёмки"],
    ["fbo_planning.html", "Планирование FBO"],
  ];

  function curFile(){
    const p = (location.pathname || "").split("/").pop();
    return p || "index.html";
  }

  function apply(){
    const nav = document.querySelector("header nav.nav");
    if(!nav) return;
    const cur = curFile();

    nav.innerHTML = NAV.map(([href, text]) => {
      const isCur = (href === cur);
      return `<a href="${href}" ${isCur ? 'aria-current="page"' : ""}>${text}</a>`;
    }).join("");
  }

  document.addEventListener("DOMContentLoaded", apply);
})();
