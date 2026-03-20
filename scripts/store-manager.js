(function () {
  const api = window.OzonCore;
  if (!api) return;

  const form = document.getElementById('storeForm');
  const nameInput = document.getElementById('name');
  const clientIdInput = document.getElementById('clientId');
  const apiKeyInput = document.getElementById('apiKey');
  const storesList = document.getElementById('storesList');
  const statusBox = document.getElementById('statusBox');

  let editClientId = null;
  let stores = [];

  function setStatus(text, type) {
    statusBox.textContent = text || '';
    statusBox.className = type ? `status ${type}` : 'status';
  }

  function escapeHtml(s) {
    return String(s || '').replace(/[&<>"']/g, (c) => ({
      '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'
    }[c]));
  }

  function render() {
    if (!stores.length) {
      storesList.innerHTML = '<div class="hint">Магазинов пока нет. Добавьте первый магазин выше.</div>';
      return;
    }

    const rows = stores.map(s => `
      <div class="card" style="padding:12px; display:flex; justify-content:space-between; align-items:center; gap:12px;">
        <div style="min-width: 0;">
          <div style="font-weight:600; white-space:nowrap; overflow:hidden; text-overflow:ellipsis;">${escapeHtml(s.name)}</div>
          <div class="muted" style="font-size:12px;">client_id: ${escapeHtml(s.client_id)}</div>
        </div>
        <div style="display:flex; gap:8px; flex:0 0 auto;">
          <button class="ghost" data-act="edit" data-id="${escapeHtml(s.client_id)}">Изменить</button>
          <button class="danger" data-act="del" data-id="${escapeHtml(s.client_id)}">Удалить</button>
        </div>
      </div>
    `).join('');

    storesList.innerHTML = `<div style="display:grid; gap:10px;">${rows}</div>`;
  }

  async function refresh() {
    setStatus('');
    try {
      stores = await api.loadStoresAsync();
      render();
    } catch (e) {
      console.error(e);
      setStatus('Не удалось загрузить магазины с сервера. Проверьте, что сайт открыт через веб‑сервер с поддержкой PHP.', 'error');
    }
  }

  form.addEventListener('submit', async (ev) => {
    ev.preventDefault();
    const store = {
      name: nameInput.value.trim(),
      client_id: clientIdInput.value.trim(),
      api_key: apiKeyInput.value.trim(),
    };
    if (!store.client_id || !store.api_key) {
      setStatus('client_id и api_key обязательны.', 'error');
      return;
    }
    if (!store.name) store.name = store.client_id;

    try {
      setStatus('Сохраняю...', 'info');
      await api.upsertStoreAsync(store);
      editClientId = null;
      form.reset();
      setStatus('Сохранено на сервере.', 'ok');
      await refresh();
    } catch (e) {
      console.error(e);
      setStatus('Ошибка сохранения. Проверьте права записи на stores.secrets.js.', 'error');
    }
  });

  storesList.addEventListener('click', async (ev) => {
    const btn = ev.target.closest('button');
    if (!btn) return;
    const act = btn.getAttribute('data-act');
    const id = btn.getAttribute('data-id');
    const s = stores.find(x => String(x.client_id) === String(id));
    if (!s) return;

    if (act === 'edit') {
      editClientId = s.client_id;
      nameInput.value = s.name || '';
      clientIdInput.value = s.client_id || '';
      apiKeyInput.value = s.api_key || '';
      setStatus('Режим редактирования: внесите изменения и нажмите «Сохранить».', 'info');
      return;
    }

    if (act === 'del') {
      if (!confirm(`Удалить магазин "${s.name}"?`)) return;
      try {
        setStatus('Удаляю...', 'info');
        await api.deleteStoreAsync(s.client_id);
        setStatus('Удалено.', 'ok');
        await refresh();
      } catch (e) {
        console.error(e);
        setStatus('Ошибка удаления. Проверьте права записи.', 'error');
      }
    }
  });

  refresh();
})();