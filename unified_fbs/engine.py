from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import Iterable, Optional


@dataclass(frozen=True)
class StockSnapshot:
    sku_id: int
    physical: int
    reserved: int
    buffer: int
    available: int


class UnifiedFbsEngine:
    """Transactional stock core.

    This module deliberately has no marketplace HTTP code. It owns stock,
    reservations, idempotency and publish targets. Marketplace adapters can
    only feed order events into it and consume desired stock from it.
    """

    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys = ON")

    def snapshot(self, sku_id: int) -> StockSnapshot:
        row = self.conn.execute(
            """
            SELECT s.id,
                   s.physical_stock,
                   s.safety_buffer,
                   COALESCE(SUM(CASE WHEN r.state='reserved' THEN r.quantity ELSE 0 END),0) AS reserved
            FROM unified_fbs_sku s
            LEFT JOIN unified_fbs_order_reservation r ON r.sku_id=s.id
            WHERE s.id=?
            GROUP BY s.id
            """,
            (sku_id,),
        ).fetchone()
        if row is None:
            raise KeyError(f"unknown sku_id={sku_id}")
        reserved = int(row["reserved"])
        physical = int(row["physical_stock"])
        buffer = int(row["safety_buffer"])
        available = max(0, physical - reserved - buffer)
        return StockSnapshot(sku_id, physical, reserved, buffer, available)

    def _queue_publish_targets(self, sku_id: int, available: int) -> None:
        rows = self.conn.execute(
            "SELECT marketplace, warehouse_id FROM unified_fbs_mapping WHERE sku_id=? AND enabled=1",
            (sku_id,),
        ).fetchall()
        for row in rows:
            self.conn.execute(
                """
                INSERT INTO unified_fbs_publish_state
                    (sku_id, marketplace, warehouse_id, expected_stock, send_status, updated_at)
                VALUES (?, ?, ?, ?, 'pending', CURRENT_TIMESTAMP)
                ON CONFLICT(sku_id, marketplace, warehouse_id)
                DO UPDATE SET expected_stock=excluded.expected_stock,
                              send_status='pending',
                              last_error=NULL,
                              updated_at=CURRENT_TIMESTAMP
                """,
                (sku_id, row["marketplace"], row["warehouse_id"], available),
            )

    def _movement(self, sku_id: int, movement_type: str, quantity_delta: int,
                  snapshot: StockSnapshot, *, marketplace: Optional[str] = None,
                  order_id: Optional[str] = None, external_event_key: Optional[str] = None,
                  note: Optional[str] = None) -> None:
        self.conn.execute(
            """
            INSERT INTO unified_fbs_movement
                (sku_id, movement_type, quantity_delta, physical_after, reserved_after,
                 available_after, marketplace, order_id, external_event_key, note)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (sku_id, movement_type, quantity_delta, snapshot.physical, snapshot.reserved,
             snapshot.available, marketplace, order_id, external_event_key, note),
        )

    def reserve_order(self, *, sku_id: int, marketplace: str, order_id: str,
                      quantity: int, order_item_id: str = "") -> StockSnapshot:
        if quantity <= 0:
            raise ValueError("quantity must be positive")
        event_key = f"reserve:{marketplace}:{order_id}:{order_item_id}:{sku_id}"
        with self.conn:
            existing = self.conn.execute(
                "SELECT state FROM unified_fbs_order_reservation WHERE marketplace=? AND order_id=? AND order_item_id=? AND sku_id=?",
                (marketplace, order_id, order_item_id, sku_id),
            ).fetchone()
            if existing is not None:
                return self.snapshot(sku_id)

            before = self.snapshot(sku_id)
            if before.available < quantity:
                raise RuntimeError(
                    f"insufficient stock for sku_id={sku_id}: available={before.available}, requested={quantity}"
                )

            self.conn.execute(
                """
                INSERT INTO unified_fbs_order_reservation
                    (sku_id, marketplace, order_id, order_item_id, quantity, state)
                VALUES (?, ?, ?, ?, ?, 'reserved')
                """,
                (sku_id, marketplace, order_id, order_item_id, quantity),
            )
            after = self.snapshot(sku_id)
            self._movement(
                sku_id, "order_reserve", -quantity, after,
                marketplace=marketplace, order_id=order_id, external_event_key=event_key,
            )
            self._queue_publish_targets(sku_id, after.available)
            return after

    def release_order(self, *, sku_id: int, marketplace: str, order_id: str,
                      order_item_id: str = "") -> StockSnapshot:
        event_key = f"release:{marketplace}:{order_id}:{order_item_id}:{sku_id}"
        with self.conn:
            row = self.conn.execute(
                """
                SELECT id, quantity, state FROM unified_fbs_order_reservation
                WHERE marketplace=? AND order_id=? AND order_item_id=? AND sku_id=?
                """,
                (marketplace, order_id, order_item_id, sku_id),
            ).fetchone()
            if row is None or row["state"] != "reserved":
                return self.snapshot(sku_id)
            self.conn.execute(
                "UPDATE unified_fbs_order_reservation SET state='released', updated_at=CURRENT_TIMESTAMP WHERE id=?",
                (row["id"],),
            )
            after = self.snapshot(sku_id)
            self._movement(
                sku_id, "order_release", int(row["quantity"]), after,
                marketplace=marketplace, order_id=order_id, external_event_key=event_key,
            )
            self._queue_publish_targets(sku_id, after.available)
            return after

    def ship_order(self, *, sku_id: int, marketplace: str, order_id: str,
                   order_item_id: str = "") -> StockSnapshot:
        event_key = f"ship:{marketplace}:{order_id}:{order_item_id}:{sku_id}"
        with self.conn:
            row = self.conn.execute(
                """
                SELECT id, quantity, state FROM unified_fbs_order_reservation
                WHERE marketplace=? AND order_id=? AND order_item_id=? AND sku_id=?
                """,
                (marketplace, order_id, order_item_id, sku_id),
            ).fetchone()
            if row is None:
                raise KeyError("reservation not found")
            if row["state"] == "shipped":
                return self.snapshot(sku_id)
            if row["state"] != "reserved":
                raise RuntimeError(f"cannot ship reservation in state={row['state']}")
            qty = int(row["quantity"])
            current = self.snapshot(sku_id)
            if current.physical < qty:
                raise RuntimeError("physical stock would become negative")
            self.conn.execute(
                "UPDATE unified_fbs_sku SET physical_stock=physical_stock-?, updated_at=CURRENT_TIMESTAMP WHERE id=?",
                (qty, sku_id),
            )
            self.conn.execute(
                "UPDATE unified_fbs_order_reservation SET state='shipped', updated_at=CURRENT_TIMESTAMP WHERE id=?",
                (row["id"],),
            )
            after = self.snapshot(sku_id)
            self._movement(
                sku_id, "shipment", -qty, after,
                marketplace=marketplace, order_id=order_id, external_event_key=event_key,
            )
            self._queue_publish_targets(sku_id, after.available)
            return after

    def adjust_physical(self, *, sku_id: int, new_physical: int, note: str,
                        external_event_key: Optional[str] = None) -> StockSnapshot:
        if new_physical < 0:
            raise ValueError("new_physical cannot be negative")
        with self.conn:
            before = self.snapshot(sku_id)
            self.conn.execute(
                "UPDATE unified_fbs_sku SET physical_stock=?, updated_at=CURRENT_TIMESTAMP WHERE id=?",
                (new_physical, sku_id),
            )
            after = self.snapshot(sku_id)
            self._movement(
                sku_id, "manual_adjustment", new_physical - before.physical, after,
                external_event_key=external_event_key, note=note,
            )
            self._queue_publish_targets(sku_id, after.available)
            return after

    def pending_publications(self) -> Iterable[sqlite3.Row]:
        return self.conn.execute(
            """
            SELECT p.*, m.marketplace_offer_id, m.marketplace_product_id
            FROM unified_fbs_publish_state p
            JOIN unified_fbs_mapping m
              ON m.sku_id=p.sku_id AND m.marketplace=p.marketplace AND m.warehouse_id=p.warehouse_id
            WHERE p.send_status='pending' AND m.enabled=1
            ORDER BY p.updated_at, p.id
            """
        ).fetchall()
