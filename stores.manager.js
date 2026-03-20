(function () {
  'use strict';

  // Stores are persisted in the browser (localStorage). This avoids editing files on the server.
  // WARNING: api_key will be stored in the user's browser profile.
  const STORAGE_KEY = 'ozon-stores-simple-v1';

  const BUILTIN_STORES = [
    { name: 'ПМ-Трейд (демо)', client_id: 'demo-client', api_key: 'demo-key' },
  ];

  function normalize(raw) {
    if (Array.isArray(raw)) return raw;
    if (raw && Array.isArray(raw.stores)) return raw.stores;
    return [];
  }

  function safeJsonParse(text, fallback) {
    try { return JSON.parse(text); } catch (_) { return fallback; }
  }

  function loadLocalStores() {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (!raw) return [];
    const parsed = safeJsonParse(raw, []);
    const stores = normalize(parsed);
    return Array.isArray(stores) ? stores : [];
  }

  function saveLocalStores(stores) {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(stores || []));
  }

  function mask(value) {
    const s = String(value || '');
    if (s.length <= 8) return '••••••••';
    return `${s.slice(0, 4)}…${s.slice(-4)}`;
  }

  function getSimpleStores() {
    const local = loadLocalStores();
    if (local.length) return local;

    const secrets = normalize(window.OZON_STORES);
    const sample = normalize(window.OZON_STORES_SAMPLE);
    const fallback = sample.length ? sample : BUILTIN_STORES;

    const stores = secrets.length ? secrets : fallback;
    return stores.length ? stores : BUILTIN_STORES;
  }

  function injectStylesOnce() {
    if (document.getElementById('ozonStoresManagerStyles')) return;
    const style = document.createElement('style');
    style.id = 'ozonStoresManagerStyles';
    style.textContent = `
      .ozon-modal-overlay {
        position: fixed;
        inset: 0;
        background: rgba(0,0,0,0.55);
        display: flex;
        align-items: flex-start;
        justify-content: center;
        padding: 40px 16px;
        z-index: 9999;
      }
      .ozon-modal {
        width: min(920px, 100%);
        background: var(--panel);
        border: 1px solid var(--border);
        border-radius: 12px;
        box-shadow: var(--shadow);
        padding: 16px;
      }
      .ozon-modal h2 { margin: 0 0 12px; }
      .ozon-modal .row { align-items: flex-end; }
      .ozon-modal table { width: 100%; border-collapse: collapse; }
      .ozon-modal th, .ozon-modal td { padding: 8px; border-bottom: 1px solid var(--border); }
      .ozon-modal .muted { color: var(--muted); font-size: 13px; }
      .ozon-modal .footer { display:flex; gap:10px; justify-content:flex-end; margin-top: 12px; flex-wrap: wrap; }
      .ozon-modal .danger { background: var(--danger); }
      .ozon-modal .ghost { background: transparent; border: 1px solid var(--border); }
    `;
    document.head.appendChild(style);
  }

  function openManager(options) {
    injectStylesOnce();
    const opts = options || {};
    const shouldReload = opts.reloadOnSave !== false;

    let stores = getSimpleStores().map(s => ({
      name: String(s?.name || '').trim(),
      client_id: String(s?.client_id || '').trim(),
      api_key: String(s?.api_key || '').trim(),
    })).filter(s => s.name || s.client_id || s.api_key);

    const overlay = document.createElement('div');
    overlay.className = 'ozon-modal-overlay';

    const modal = document.createElement('div');
    modal.className = 'ozon-modal';
    modal.innerHTML = `
      <h2>Магазины</h2>
      <div class="muted" style="margin-bottom:12px;">
        Магазины сохраняются в браузере (localStorage). На новом устройстве/в новом профиле список нужно добавить заново.
      </div>

      <div class="panel" style="padding:12px; margin-bottom:12px;">
        <div class="row" style="gap:10px;">
          <label style="flex:1; min-width:220px;">Название
            <input id="ozonStoreName" type="text" placeholder="Например, ПМ-Трейд" />
          </label>
          <label style="flex:1; min-width:180px;">Client ID
            <input id="ozonStoreClientId" type="text" placeholder="Например, 123456" />
          </label>
          <label style="flex:1; min-width:240px;">API Key
            <input id="ozonStoreApiKey" type="text" placeholder="xxxx-xxxx-xxxx-xxxx" />
          </label>
          <button id="ozonStoreAddBtn" type="button">Добавить</button>
        </div>
      </div>

      <div class="panel" style="padding:0; overflow:auto;">
        <table aria-label="Список магазинов">
          <thead>
            <tr>
              <th>Название</th>
              <th>Client ID</th>
              <th>API Key</th>
              <th style="width:1%; white-space:nowrap; text-align:right;">Действия</th>
            </tr>
          </thead>
          <tbody id="ozonStoresTableBody"></tbody>
        </table>
      </div>

      <div class="footer">
        <button id="ozonStoreImportBtn" type="button" class="ghost">Импорт JSON</button>
        <button id="ozonStoreExportBtn" type="button" class="ghost">Экспорт JSON</button>
        <button id="ozonStoreResetBtn" type="button" class="danger">Сбросить</button>
        <button id="ozonStoreCloseBtn" type="button" class="ghost">Закрыть</button>
      </div>
    `;

    overlay.appendChild(modal);
    document.body.appendChild(overlay);

    function escapeHtml(str) {
      return String(str || '')
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#39;');
    }

    function render() {
      const tbody = modal.querySelector('#ozonStoresTableBody');
      tbody.innerHTML = stores.length
        ? stores.map((s, idx) => `
            <tr>
              <td>${escapeHtml(s.name)}</td>
              <td>${escapeHtml(s.client_id)}</td>
              <td title="Сохранено в браузере">${escapeHtml(mask(s.api_key))}</td>
              <td style="text-align:right; white-space:nowrap;">
                <button type="button" class="danger" data-del="${idx}">Удалить</button>
              </td>
            </tr>
          `).join('')
        : `<tr><td colspan="4" class="muted" style="padding:12px;">Список пуст. Добавьте магазин выше.</td></tr>`;
    }

    function close() {
      overlay.remove();
    }

    function persist() {
      saveLocalStores(stores);
      if (typeof opts.onSave === 'function') opts.onSave(stores);
      if (shouldReload) location.reload();
    }

    modal.addEventListener('click', (e) => {
      const del = e.target && e.target.getAttribute && e.target.getAttribute('data-del');
      if (del !== null) {
        const idx = Number(del);
        if (!Number.isNaN(idx)) {
          stores.splice(idx, 1);
          render();
          persist();
        }
      }
    });

    overlay.addEventListener('click', (e) => {
      if (e.target === overlay) close();
    });

    modal.querySelector('#ozonStoreCloseBtn').addEventListener('click', close);

    modal.querySelector('#ozonStoreAddBtn').addEventListener('click', () => {
      const name = modal.querySelector('#ozonStoreName').value.trim();
      const clientId = modal.querySelector('#ozonStoreClientId').value.trim();
      const apiKey = modal.querySelector('#ozonStoreApiKey').value.trim();

      if (!name || !clientId || !apiKey) {
        alert('Заполните название, client_id и api_key.');
        return;
      }
      stores.push({ name, client_id: clientId, api_key: apiKey });
      modal.querySelector('#ozonStoreName').value = '';
      modal.querySelector('#ozonStoreClientId').value = '';
      modal.querySelector('#ozonStoreApiKey').value = '';
      render();
      persist();
    });

    modal.querySelector('#ozonStoreResetBtn').addEventListener('click', () => {
      if (!confirm('Удалить все магазины, сохранённые в браузере?')) return;
      stores = [];
      render();
      persist();
    });

    modal.querySelector('#ozonStoreExportBtn').addEventListener('click', async () => {
      const json = JSON.stringify(stores, null, 2);
      try {
        await navigator.clipboard.writeText(json);
        alert('JSON скопирован в буфер обмена.');
      } catch (_) {
        prompt('Скопируйте JSON:', json);
      }
    });

    modal.querySelector('#ozonStoreImportBtn').addEventListener('click', () => {
      const text = prompt('Вставьте JSON (массив магазинов или объект {stores:[...]})');
      if (!text) return;
      const parsed = safeJsonParse(text, null);
      if (!parsed) {
        alert('Не удалось прочитать JSON.');
        return;
      }
      const imported = normalize(parsed)
        .map(s => ({
          name: String(s?.name || '').trim(),
          client_id: String(s?.client_id || '').trim(),
          api_key: String(s?.api_key || '').trim(),
        }))
        .filter(s => s.name && s.client_id && s.api_key);

      if (!imported.length) {
        alert('В JSON не найдено ни одного магазина с полями name/client_id/api_key.');
        return;
      }
      stores = imported;
      render();
      persist();
    });

    render();
  }

  window.OzonStoresManager = {
    getSimpleStores,
    openManager,
    loadLocalStores,
    saveLocalStores,
  };
})();
