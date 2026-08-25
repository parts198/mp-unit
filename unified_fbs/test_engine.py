import sqlite3
import unittest
from pathlib import Path

from engine import UnifiedFbsEngine


HERE = Path(__file__).resolve().parent


class UnifiedFbsEngineTest(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        self.conn.executescript((HERE / "schema.sql").read_text(encoding="utf-8"))
        cur = self.conn.execute(
            "INSERT INTO unified_fbs_sku(shop_id, internal_sku, physical_stock, safety_buffer) VALUES (7,'TEST-SKU',20,0)"
        )
        self.sku_id = cur.lastrowid
        for marketplace in ("ozon", "wb", "yandex"):
            self.conn.execute(
                """
                INSERT INTO unified_fbs_mapping
                    (sku_id, marketplace, marketplace_offer_id, warehouse_id)
                VALUES (?, ?, 'TEST-SKU', ?)
                """,
                (self.sku_id, marketplace, f"{marketplace}-warehouse"),
            )
        self.conn.commit()
        self.engine = UnifiedFbsEngine(self.conn)

    def test_order_is_reserved_once_and_fanned_out(self):
        after = self.engine.reserve_order(
            sku_id=self.sku_id,
            marketplace="wb",
            order_id="WB-100",
            order_item_id="line-1",
            quantity=2,
        )
        self.assertEqual(after.physical, 20)
        self.assertEqual(after.reserved, 2)
        self.assertEqual(after.available, 18)

        publish = list(self.engine.pending_publications())
        self.assertEqual(len(publish), 3)
        self.assertEqual({row["marketplace"] for row in publish}, {"ozon", "wb", "yandex"})
        self.assertEqual({row["expected_stock"] for row in publish}, {18})

        # Same marketplace event can be returned by API repeatedly: no double reserve.
        duplicate = self.engine.reserve_order(
            sku_id=self.sku_id,
            marketplace="wb",
            order_id="WB-100",
            order_item_id="line-1",
            quantity=2,
        )
        self.assertEqual(duplicate.reserved, 2)
        self.assertEqual(duplicate.available, 18)

        released = self.engine.release_order(
            sku_id=self.sku_id,
            marketplace="wb",
            order_id="WB-100",
            order_item_id="line-1",
        )
        self.assertEqual(released.physical, 20)
        self.assertEqual(released.reserved, 0)
        self.assertEqual(released.available, 20)

    def test_shipping_reduces_physical_without_double_counting_reserve(self):
        self.engine.reserve_order(
            sku_id=self.sku_id,
            marketplace="ozon",
            order_id="OZ-200",
            quantity=3,
        )
        shipped = self.engine.ship_order(
            sku_id=self.sku_id,
            marketplace="ozon",
            order_id="OZ-200",
        )
        self.assertEqual(shipped.physical, 17)
        self.assertEqual(shipped.reserved, 0)
        self.assertEqual(shipped.available, 17)

        duplicate = self.engine.ship_order(
            sku_id=self.sku_id,
            marketplace="ozon",
            order_id="OZ-200",
        )
        self.assertEqual(duplicate.physical, 17)

    def test_buffer_is_not_sellable(self):
        self.conn.execute(
            "UPDATE unified_fbs_sku SET safety_buffer=2 WHERE id=?",
            (self.sku_id,),
        )
        self.conn.commit()
        snap = self.engine.snapshot(self.sku_id)
        self.assertEqual(snap.available, 18)


if __name__ == "__main__":
    unittest.main()
