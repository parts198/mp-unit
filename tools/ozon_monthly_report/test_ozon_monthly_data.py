import tempfile
import unittest
from pathlib import Path

import ozon_monthly_data as m


class TestHelpers(unittest.TestCase):
    def test_month_range(self):
        first, last = m._month_range("2026-08")
        self.assertEqual(first.isoformat(), "2026-08-01")
        self.assertEqual(last.isoformat(), "2026-08-31")

    def test_parse_stores_js_json(self):
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / "stores.secrets.js"
            p.write_text(
                'window.OZON_STORES = [{"name":"Косметика","client_id":"123","api_key":"secret"}];\n',
                encoding="utf-8",
            )
            stores = m._parse_stores_js(p)
            self.assertEqual(len(stores), 1)
            self.assertEqual(stores[0].name, "Косметика")
            self.assertEqual(stores[0].client_id, "123")
            self.assertEqual(stores[0].api_key, "secret")

    def test_response_extractors(self):
        self.assertEqual(m._first_list({"postings": [{"id": 1}]}, (("postings",),)), [{"id": 1}])
        self.assertEqual(m._extract_cursor({"result": {"cursor": "abc"}}), "abc")
        self.assertTrue(m._extract_has_next({"has_next": True}))


if __name__ == "__main__":
    unittest.main()
