-- Unified FBS stock schema for Ozon / Wildberries / Yandex Market.
-- Safe to apply repeatedly where IF NOT EXISTS is supported (SQLite).

PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS unified_fbs_sku (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    shop_id INTEGER NOT NULL,
    internal_sku TEXT NOT NULL,
    title TEXT,
    physical_stock INTEGER NOT NULL DEFAULT 0 CHECK (physical_stock >= 0),
    safety_buffer INTEGER NOT NULL DEFAULT 0 CHECK (safety_buffer >= 0),
    enabled INTEGER NOT NULL DEFAULT 1 CHECK (enabled IN (0,1)),
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (shop_id, internal_sku)
);

CREATE TABLE IF NOT EXISTS unified_fbs_mapping (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    sku_id INTEGER NOT NULL REFERENCES unified_fbs_sku(id) ON DELETE CASCADE,
    marketplace TEXT NOT NULL CHECK (marketplace IN ('ozon','wb','yandex')),
    marketplace_offer_id TEXT NOT NULL,
    marketplace_product_id TEXT,
    warehouse_id TEXT NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 1 CHECK (enabled IN (0,1)),
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (marketplace, marketplace_offer_id, warehouse_id)
);

CREATE TABLE IF NOT EXISTS unified_fbs_order_reservation (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    sku_id INTEGER NOT NULL REFERENCES unified_fbs_sku(id) ON DELETE RESTRICT,
    marketplace TEXT NOT NULL CHECK (marketplace IN ('ozon','wb','yandex')),
    order_id TEXT NOT NULL,
    order_item_id TEXT NOT NULL DEFAULT '',
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    state TEXT NOT NULL CHECK (state IN ('reserved','released','shipped','returned')),
    first_seen_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (marketplace, order_id, order_item_id, sku_id)
);

CREATE TABLE IF NOT EXISTS unified_fbs_movement (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    sku_id INTEGER NOT NULL REFERENCES unified_fbs_sku(id) ON DELETE RESTRICT,
    movement_type TEXT NOT NULL CHECK (movement_type IN (
        'initial','receipt','manual_adjustment','writeoff','order_reserve',
        'order_release','shipment','return'
    )),
    quantity_delta INTEGER NOT NULL,
    physical_after INTEGER NOT NULL CHECK (physical_after >= 0),
    reserved_after INTEGER NOT NULL CHECK (reserved_after >= 0),
    available_after INTEGER NOT NULL CHECK (available_after >= 0),
    marketplace TEXT,
    order_id TEXT,
    external_event_key TEXT,
    note TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (external_event_key)
);

CREATE TABLE IF NOT EXISTS unified_fbs_publish_state (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    sku_id INTEGER NOT NULL REFERENCES unified_fbs_sku(id) ON DELETE CASCADE,
    marketplace TEXT NOT NULL CHECK (marketplace IN ('ozon','wb','yandex')),
    warehouse_id TEXT NOT NULL,
    expected_stock INTEGER NOT NULL CHECK (expected_stock >= 0),
    last_sent_stock INTEGER,
    last_confirmed_stock INTEGER,
    send_status TEXT NOT NULL DEFAULT 'pending' CHECK (send_status IN ('pending','ok','error','dry_run')),
    last_error TEXT,
    last_sent_at TEXT,
    last_confirmed_at TEXT,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (sku_id, marketplace, warehouse_id)
);

CREATE TABLE IF NOT EXISTS unified_fbs_sync_event (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    marketplace TEXT NOT NULL CHECK (marketplace IN ('ozon','wb','yandex')),
    direction TEXT NOT NULL CHECK (direction IN ('orders_in','stocks_out','stocks_verify')),
    external_key TEXT,
    status TEXT NOT NULL CHECK (status IN ('ok','error','dry_run','skipped')),
    payload_json TEXT,
    response_json TEXT,
    error_text TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_unified_fbs_reservation_sku_state
    ON unified_fbs_order_reservation (sku_id, state);
CREATE INDEX IF NOT EXISTS idx_unified_fbs_movement_sku_created
    ON unified_fbs_movement (sku_id, created_at);
CREATE INDEX IF NOT EXISTS idx_unified_fbs_sync_created
    ON unified_fbs_sync_event (created_at);

-- Available stock is intentionally derived, not stored as source of truth:
-- max(0, physical_stock - active reservations - safety_buffer).
