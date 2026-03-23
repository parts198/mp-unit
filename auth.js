const API_BASE = "https://api.mp-unit.ru";

function setTokens({ access, refresh }) {
  localStorage.setItem("access", access || "");
  localStorage.setItem("refresh", refresh || "");
}

function getAccess() {
  return localStorage.getItem("access") || "";
}

function getRefresh() {
  return localStorage.getItem("refresh") || "";
}

function setProfile(profile) {
  localStorage.setItem("profile", JSON.stringify(profile || {}));
}

function getProfile() {
  try { return JSON.parse(localStorage.getItem("profile") || "{}"); }
  catch { return {}; }
}

// для админ-режима (чтобы вернуться обратно)
function stashAdminSession() {
  localStorage.setItem("admin_access", getAccess());
  localStorage.setItem("admin_refresh", getRefresh());
  localStorage.setItem("admin_profile", localStorage.getItem("profile") || "{}");
}

function hasAdminStash() {
  return !!localStorage.getItem("admin_access");
}

function restoreAdminSession() {
  const a = localStorage.getItem("admin_access") || "";
  const r = localStorage.getItem("admin_refresh") || "";
  const p = localStorage.getItem("admin_profile") || "{}";
  localStorage.setItem("access", a);
  localStorage.setItem("refresh", r);
  localStorage.setItem("profile", p);
  localStorage.removeItem("admin_access");
  localStorage.removeItem("admin_refresh");
  localStorage.removeItem("admin_profile");
}

function logout() {
  localStorage.removeItem("access");
  localStorage.removeItem("refresh");
  localStorage.removeItem("profile");
  localStorage.removeItem("admin_access");
  localStorage.removeItem("admin_refresh");
  localStorage.removeItem("admin_profile");
  window.location.href = "/login.html";
}

async function apiFetch(path, { method="GET", headers={}, body=null, auth=true } = {}) {
  const h = { "Content-Type": "application/json", ...headers };
  if (auth) {
    const token = getAccess();
    if (token) h["Authorization"] = `Bearer ${token}`;
  }

  const res = await fetch(`${API_BASE}${path}`, {
    method,
    headers: h,
    body: body ? JSON.stringify(body) : null,
  });

  if (res.status === 401 && auth && getRefresh()) {
    const ok = await refreshToken();
    if (ok) {
      h["Authorization"] = `Bearer ${getAccess()}`;
      const res2 = await fetch(`${API_BASE}${path}`, {
        method,
        headers: h,
        body: body ? JSON.stringify(body) : null,
      });
      return res2;
    }
  }

  return res;
}

async function refreshToken() {
  const refresh = getRefresh();
  if (!refresh) return false;

  const res = await fetch(`${API_BASE}/auth/refresh/`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ refresh }),
  });

  if (!res.ok) {
    logout();
    return false;
  }

  const data = await res.json();
  if (data.access) localStorage.setItem("access", data.access);
  return true;
}

async function fetchMe() {
  const res = await apiFetch("/auth/me/");
  if (!res.ok) return null;
  const me = await res.json();
  setProfile(me);
  return me;
}

async function login(username, password) {
  const res = await fetch(`${API_BASE}/auth/login/`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ username, password }),
  });

  const data = await res.json();
  if (!res.ok) {
    throw new Error(data.detail || "Login failed");
  }

  setTokens(data);
  await fetchMe();
  return data;
}

function requireAuth() {
  if (!getAccess()) window.location.href = "/login.html";
}

// ADMIN: list users
async function adminListUsers() {
  const res = await apiFetch("/auth/users/");
  if (!res.ok) return [];
  return await res.json();
}

// ADMIN: impersonate user (switch tokens)
async function adminImpersonate(userId) {
  // сохраним админскую сессию 1 раз
  if (!hasAdminStash()) stashAdminSession();

  const res = await apiFetch("/auth/impersonate/", {
    method: "POST",
    body: { user_id: userId },
  });

  const data = await res.json().catch(()=> ({}));
  if (!res.ok) throw new Error(data.detail || "Impersonate failed");

  setTokens(data);
  await fetchMe();
}


const CABINET_NAV_ITEMS = [
  { key: "cabinet", href: "/cabinet.html", label: "Кабинет" },
  { key: "stores", href: "/stores.html", label: "Магазины" },
  { key: "prices", href: "/prices.html", label: "Цены" },
  { key: "orders", href: "/orders.html", label: "Заказы" },
  { key: "actions", href: "/actions.html", label: "Акции" },
  { key: "fbo_planning", href: "/fbo_planning.html", label: "Планирование FBO" },
  { key: "dashboard", href: "/dashboard.html", label: "Дашборд" },
  { key: "settings", href: "/settings.html", label: "Настройки" }
];

function mpUnitEscapeHtml(value) {
  return String(value == null ? "" : value).replace(/[&<>"']/g, function (ch) {
    return {
      "&": "&amp;",
      "<": "&lt;",
      ">": "&gt;",
      '"': "&quot;",
      "'": "&#039;"
    }[ch];
  });
}

function mpUnitGetCachedMe() {
  var keys = ["me", "profile", "user"];
  for (var i = 0; i < keys.length; i++) {
    try {
      var raw = localStorage.getItem(keys[i]);
      if (!raw) continue;
      var parsed = JSON.parse(raw);
      if (parsed && typeof parsed === "object") return parsed;
    } catch (e) {}
  }
  return null;
}

function mpUnitEnsureCabinetNavStyles() {
  if (document.getElementById("cabinet-nav-css")) return;

  var link = document.createElement("link");
  link.id = "cabinet-nav-css";
  link.rel = "stylesheet";
  link.href = "/cabinet.css";
  document.head.appendChild(link);
}

function cabinetLogout() {
  localStorage.removeItem("access");
  localStorage.removeItem("refresh");
  localStorage.removeItem("me");
  localStorage.removeItem("profile");
  localStorage.removeItem("user");
  localStorage.removeItem("as_user");
  window.location.href = "/login.html";
}

async function renderCabinetNav(active) {
  var mount = document.getElementById("topnav");
  if (!mount) return;

  mpUnitEnsureCabinetNavStyles();

  var me = mpUnitGetCachedMe();
  if ((!me || typeof me !== "object") && typeof fetchMe === "function") {
    try {
      me = await fetchMe();
    } catch (e) {}
    if (!me) me = mpUnitGetCachedMe();
  }
  if (!me || typeof me !== "object") me = {};

  var displayName = "";
  if (me.first_name && String(me.first_name).trim()) {
    displayName = String(me.first_name).trim();
  } else if (me.username && String(me.username).trim()) {
    displayName = String(me.username).trim();
  } else if (me.email && String(me.email).trim()) {
    displayName = String(me.email).trim();
  } else {
    displayName = "Пользователь";
  }

  var badges = "";
  if (me.is_staff) {
    badges += '<span class="cabinet-badge admin">admin</span>';
  }

  var navLinks = CABINET_NAV_ITEMS.map(function (item) {
    var activeClass = item.key === active ? "active" : "";
    return '<a class="cabinet-nav-link ' + activeClass + '" href="' + item.href + '">' +
      mpUnitEscapeHtml(item.label) + '</a>';
  }).join("");

  mount.innerHTML = '' +
    '<div class="cabinet-topnav-wrap">' +
      '<div class="cabinet-topnav">' +
        '<div class="cabinet-brand">' +
          '<a class="cabinet-brand-title" href="/cabinet.html">mp-unit</a>' +
          '<div class="cabinet-userbox">' +
            '<span>' + mpUnitEscapeHtml(displayName) + '</span>' +
            badges +
          '</div>' +
        '</div>' +
        '<div class="cabinet-nav-links">' +
          navLinks +
          '<button type="button" class="cabinet-logout-btn" id="cabinet-logout-btn">Выйти</button>' +
        '</div>' +
      '</div>' +
    '</div>';

  var logoutBtn = document.getElementById("cabinet-logout-btn");
  if (logoutBtn) {
    logoutBtn.addEventListener("click", cabinetLogout);
  }
}

async function initCabinetPage(active, initFn) {
  if (typeof requireAuth === "function") {
    requireAuth();
  }

  if (typeof fetchMe === "function") {
    try {
      await fetchMe();
    } catch (e) {}
  }

  await renderCabinetNav(active);

  if (typeof initFn === "function") {
    await initFn();
  }
}

window.renderCabinetNav = renderCabinetNav;
window.initCabinetPage = initCabinetPage;
window.cabinetLogout = cabinetLogout;
