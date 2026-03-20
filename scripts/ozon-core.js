(function () {
  'use strict';

  function safeJsonParse(text, fallback) {
    try { return JSON.parse(text); } catch (_) { return fallback; }
  }

  function normalizeStores(x) {
    if (!x) return [];
    if (Array.isArray(x)) return x;
    if (x && Array.isArray(x.stores)) return x.stores;
    return [];
  }

  async function fetchJson(url, options) {
    const res = await fetch(url, options);
    const text = await res.text();
    const data = safeJsonParse(text, null);
    if (!res.ok) {
      const msg = (data && (data.error || data.message)) ? (data.error || data.message) : text;
      throw new Error(`${res.status} ${res.statusText}: ${msg}`);
    }
    return data;
  }

  // Stores in this setup are loaded from stores.secrets.js (window.OZON_STORES)
  async function loadStoresAsync() {
    // Stores are loaded from stores.secrets.js (window.OZON_STORES).
    const stores = normalizeStores(window.OZON_STORES);
    if (stores.length) return stores;
    return normalizeStores(window.OZON_STORES_SAMPLE);
  }

  // These endpoints exist in codebase, but may not be executable as PHP in current nginx proxy config.
  async function upsertStoreAsync(store) {
    const payload = { store };
    return await fetchJson('api/stores.php', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
      cache: 'no-store',
    });
  }

  async function deleteStoreAsync(clientId) {
    const url = `api/stores.php?client_id=${encodeURIComponent(String(clientId || ''))}`;
    return await fetchJson(url, { method: 'DELETE', cache: 'no-store' });
  }

  async function postJson(url, body, headers = {}) {
    const res = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', ...headers },
      body: JSON.stringify(body),
    });
    const text = await res.text();
    const data = safeJsonParse(text, null);
    if (!res.ok) {
      const msg = (data && (data.message || data.error)) ? (data.message || data.error) : text;
      const e = new Error(`HTTP ${res.status}: ${msg}`);
      e.response = data;
      throw e;
    }
    return data;
  }

  function splitDateRange(dateFrom, dateTo, maxDays) {
    const from = new Date(dateFrom);
    const to = new Date(dateTo);
    const dayMs = 24 * 60 * 60 * 1000;
    const chunks = [];
    let cursor = new Date(from);
    while (cursor <= to) {
      const end = new Date(cursor.getTime() + (maxDays - 1) * dayMs);
      const boundedEnd = end > to ? new Date(to) : end;
      chunks.push({
        from: cursor.toISOString().slice(0, 10),
        to: boundedEnd.toISOString().slice(0, 10),
      });
      cursor = new Date(boundedEnd.getTime() + dayMs);
    }
    return chunks;
  }

  async function fetchByDateChunks({ dateFrom, dateTo, maxDays, loader, onChunk }) {
    const chunks = splitDateRange(dateFrom, dateTo, maxDays);
    const out = [];
    for (const ch of chunks) {
      const part = await loader(ch.from, ch.to);
      if (onChunk) onChunk(ch, part);
      if (Array.isArray(part)) out.push(...part);
      else if (part) out.push(part);
    }
    return out;
  }

  window.OzonCore = {
    loadStoresAsync,
    upsertStoreAsync,
    deleteStoreAsync,
    postJson,
    splitDateRange,
    fetchByDateChunks,
  };
})();
