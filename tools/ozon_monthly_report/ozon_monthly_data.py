#!/usr/bin/env python3
"""Collect one month of Ozon Seller/Performance API data for management reporting.

The collector intentionally saves raw/normalized source data first.  Excel/P&L logic can
then be built on top of a reproducible data pack instead of manually downloaded reports.

No secret is written to output or logs.
"""
from __future__ import annotations

import argparse
import calendar
import datetime as dt
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Iterator, Sequence

SELLER_BASE = "https://api-seller.ozon.ru"
PERF_BASE = "https://api-performance.ozon.ru"
USER_AGENT = "mp-unit-ozon-monthly/1.0"


class ApiError(RuntimeError):
    def __init__(self, method: str, url: str, status: int | None, body: str):
        self.method = method
        self.url = url
        self.status = status
        self.body = body
        super().__init__(f"{method} {url}: HTTP {status}: {body[:500]}")


def _json_dumps(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))


def _request(
    method: str,
    url: str,
    *,
    headers: dict[str, str] | None = None,
    json_body: Any | None = None,
    timeout: int = 120,
    retries: int = 4,
) -> tuple[bytes, dict[str, str], int]:
    body = None if json_body is None else _json_dumps(json_body).encode("utf-8")
    req_headers = {
        "User-Agent": USER_AGENT,
        "Accept": "application/json",
    }
    if headers:
        req_headers.update(headers)
    if json_body is not None:
        req_headers.setdefault("Content-Type", "application/json")

    last_exc: Exception | None = None
    for attempt in range(retries + 1):
        req = urllib.request.Request(url, data=body, headers=req_headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                data = resp.read()
                return data, {k.lower(): v for k, v in resp.headers.items()}, resp.status
        except urllib.error.HTTPError as e:
            raw = e.read().decode("utf-8", errors="replace")
            if e.code in (408, 425, 429, 500, 502, 503, 504) and attempt < retries:
                retry_after = e.headers.get("Retry-After")
                try:
                    delay = float(retry_after) if retry_after else min(10.0, 0.8 * (2 ** attempt))
                except ValueError:
                    delay = min(10.0, 0.8 * (2 ** attempt))
                time.sleep(delay)
                continue
            raise ApiError(method, url, e.code, raw) from e
        except (urllib.error.URLError, TimeoutError) as e:
            last_exc = e
            if attempt < retries:
                time.sleep(min(10.0, 0.8 * (2 ** attempt)))
                continue
            raise RuntimeError(f"{method} {url}: network error: {e}") from e
    raise RuntimeError(str(last_exc) if last_exc else "request failed")


def _json_request(method: str, url: str, **kwargs: Any) -> Any:
    data, _headers, _status = _request(method, url, **kwargs)
    try:
        return json.loads(data.decode("utf-8"))
    except Exception as e:
        raise RuntimeError(f"Expected JSON from {url}, got: {data[:500]!r}") from e


def _write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def _write_jsonl(path: Path, rows: Iterable[Any]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with path.open("w", encoding="utf-8", newline="\n") as f:
        for row in rows:
            f.write(_json_dumps(row) + "\n")
            count += 1
    return count


def _month_range(month: str) -> tuple[dt.date, dt.date]:
    try:
        year_s, month_s = month.split("-", 1)
        year, mon = int(year_s), int(month_s)
        first = dt.date(year, mon, 1)
    except Exception as e:
        raise argparse.ArgumentTypeError("--month must be YYYY-MM") from e
    last = dt.date(year, mon, calendar.monthrange(year, mon)[1])
    return first, last


def _iso_start(day: dt.date) -> str:
    return f"{day.isoformat()}T00:00:00.000Z"


def _iso_end(day: dt.date) -> str:
    return f"{day.isoformat()}T23:59:59.999Z"


def _date_iter(first: dt.date, last: dt.date) -> Iterator[dt.date]:
    cur = first
    while cur <= last:
        yield cur
        cur += dt.timedelta(days=1)


def _first_list(obj: Any, paths: Sequence[Sequence[str]]) -> list[Any]:
    for path in paths:
        cur = obj
        ok = True
        for key in path:
            if not isinstance(cur, dict) or key not in cur:
                ok = False
                break
            cur = cur[key]
        if ok and isinstance(cur, list):
            return cur
    return []


def _extract_cursor(obj: Any) -> str:
    if not isinstance(obj, dict):
        return ""
    for key in ("cursor", "next_cursor"):
        v = obj.get(key)
        if v:
            return str(v)
    result = obj.get("result")
    if isinstance(result, dict):
        for key in ("cursor", "next_cursor"):
            v = result.get(key)
            if v:
                return str(v)
    return ""


def _extract_has_next(obj: Any) -> bool | None:
    if not isinstance(obj, dict):
        return None
    if "has_next" in obj:
        return bool(obj.get("has_next"))
    result = obj.get("result")
    if isinstance(result, dict) and "has_next" in result:
        return bool(result.get("has_next"))
    return None


@dataclass(frozen=True)
class Store:
    name: str
    client_id: str
    api_key: str


class SellerClient:
    def __init__(self, store: Store):
        self.store = store
        self.headers = {
            "Client-Id": store.client_id,
            "Api-Key": store.api_key,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    def post(self, path: str, payload: Any) -> Any:
        return _json_request("POST", SELLER_BASE + path, headers=self.headers, json_body=payload)

    def accrual_types(self) -> Any:
        return self.post("/v1/finance/accrual/types", {})

    def iter_accruals(self, first: dt.date, last: dt.date) -> Iterator[dict[str, Any]]:
        for day in _date_iter(first, last):
            last_id = ""
            seen_last_ids: set[str] = set()
            for _page in range(500):
                resp = self.post(
                    "/v1/finance/accrual/by-day",
                    {"date": day.isoformat(), "last_id": last_id},
                )
                rows = resp.get("accruals", []) if isinstance(resp, dict) else []
                if not isinstance(rows, list):
                    rows = []
                for row in rows:
                    if isinstance(row, dict):
                        row = dict(row)
                        row["_store_name"] = self.store.name
                        row["_store_client_id"] = self.store.client_id
                        yield row
                next_id = str(resp.get("last_id") or "") if isinstance(resp, dict) else ""
                if not rows or not next_id or next_id == last_id or next_id in seen_last_ids:
                    break
                seen_last_ids.add(next_id)
                last_id = next_id
            else:
                raise RuntimeError(f"accrual pagination guard hit for {day}")

    def iter_fbs(self, first: dt.date, last: dt.date) -> Iterator[dict[str, Any]]:
        cursor = ""
        for _page in range(1000):
            payload = {
                "sort_dir": "asc",
                "filter": {"since": _iso_start(first), "to": _iso_end(last)},
                "limit": 100,
                "cursor": cursor,
                "with": {
                    "analytics_data": True,
                    "barcodes": True,
                    "financial_data": True,
                    "legal_info": True,
                    "translit": False,
                },
            }
            resp = self.post("/v4/posting/fbs/list", payload)
            rows = _first_list(resp, (("postings",), ("result", "postings"), ("result",)))
            for row in rows:
                if isinstance(row, dict):
                    row = dict(row)
                    row["_schema"] = "FBS"
                    row["_store_name"] = self.store.name
                    row["_store_client_id"] = self.store.client_id
                    yield row
            has_next = _extract_has_next(resp)
            next_cursor = _extract_cursor(resp)
            if not rows or has_next is False or not next_cursor or next_cursor == cursor:
                break
            cursor = next_cursor
        else:
            raise RuntimeError("FBS pagination guard hit")

    def iter_fbo(self, first: dt.date, last: dt.date) -> Iterator[dict[str, Any]]:
        cursor = ""
        for _page in range(1000):
            payload = {
                "cursor": cursor,
                "filter": {"since": _iso_start(first), "to": _iso_end(last)},
                "limit": 100,
                "sort_dir": "asc",
                "translit": False,
                "with": {
                    "analytics_data": True,
                    "financial_data": True,
                    "legal_info": True,
                },
            }
            resp = self.post("/v3/posting/fbo/list", payload)
            rows = _first_list(resp, (("result", "postings"), ("postings",), ("result",)))
            for row in rows:
                if isinstance(row, dict):
                    row = dict(row)
                    row["_schema"] = "FBO"
                    row["_store_name"] = self.store.name
                    row["_store_client_id"] = self.store.client_id
                    yield row
            has_next = _extract_has_next(resp)
            next_cursor = _extract_cursor(resp)
            if not rows or has_next is False or not next_cursor or next_cursor == cursor:
                break
            cursor = next_cursor
        else:
            raise RuntimeError("FBO pagination guard hit")

    def iter_returns(self, first: dt.date, as_of: dt.date) -> Iterator[dict[str, Any]]:
        """Returns whose logistics-return date falls between month start and as_of."""
        last_id = 0
        seen: set[int] = set()
        for _page in range(1000):
            payload = {
                "filter": {
                    "logistic_return_date": {
                        "time_from": _iso_start(first),
                        "time_to": _iso_end(as_of),
                    }
                },
                "limit": 500,
                "last_id": last_id,
            }
            resp = self.post("/v1/returns/list", payload)
            rows = resp.get("returns", []) if isinstance(resp, dict) else []
            if not isinstance(rows, list):
                rows = []
            max_id = last_id
            for row in rows:
                if not isinstance(row, dict):
                    continue
                out = dict(row)
                out["_store_name"] = self.store.name
                out["_store_client_id"] = self.store.client_id
                yield out
                try:
                    max_id = max(max_id, int(row.get("id") or 0))
                except Exception:
                    pass
            has_next = bool(resp.get("has_next")) if isinstance(resp, dict) else False
            response_last_id = 0
            if isinstance(resp, dict):
                try:
                    response_last_id = int(resp.get("last_id") or 0)
                except Exception:
                    response_last_id = 0
            next_id = response_last_id or max_id
            if not rows or not has_next or next_id <= last_id or next_id in seen:
                break
            seen.add(next_id)
            last_id = next_id
        else:
            raise RuntimeError("returns pagination guard hit")

    def buyouts(self, first: dt.date, last: dt.date) -> list[dict[str, Any]]:
        resp = self.post(
            "/v1/finance/products/buyout",
            {"date_from": first.isoformat(), "date_to": last.isoformat()},
        )
        rows = resp.get("products", []) if isinstance(resp, dict) else []
        out: list[dict[str, Any]] = []
        if isinstance(rows, list):
            for row in rows:
                if isinstance(row, dict):
                    r = dict(row)
                    r["_store_name"] = self.store.name
                    r["_store_client_id"] = self.store.client_id
                    out.append(r)
        return out


class PerformanceClient:
    def __init__(self, client_id: str, client_secret: str):
        self.client_id = client_id
        self.client_secret = client_secret
        self._token: str | None = None
        self._expires_at = 0.0

    def token(self) -> str:
        if self._token and time.time() + 60 < self._expires_at:
            return self._token
        resp = _json_request(
            "POST",
            PERF_BASE + "/api/client/token",
            headers={"Content-Type": "application/json", "Accept": "application/json"},
            json_body={
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "grant_type": "client_credentials",
            },
        )
        token = str(resp.get("access_token") or "") if isinstance(resp, dict) else ""
        if not token:
            raise RuntimeError("Performance API token response has no access_token")
        expires = int(resp.get("expires_in") or 1800)
        self._token = token
        self._expires_at = time.time() + expires
        return token

    def _headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self.token()}", "Accept": "application/json"}

    def get_json(self, path: str, params: Sequence[tuple[str, str]] | dict[str, Any] | None = None) -> Any:
        if params:
            query = urllib.parse.urlencode(params, doseq=True)
            url = PERF_BASE + path + "?" + query
        else:
            url = PERF_BASE + path
        return _json_request("GET", url, headers=self._headers())

    def post_json(self, path: str, body: Any) -> Any:
        headers = self._headers()
        headers["Content-Type"] = "application/json"
        return _json_request("POST", PERF_BASE + path, headers=headers, json_body=body)

    def list_campaigns(self, adv_object_type: str = "SKU") -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for page in range(1, 1000):
            resp = self.get_json(
                "/api/client/campaign",
                {"advObjectType": adv_object_type, "page": page, "pageSize": 100},
            )
            items = _first_list(resp, (("list",), ("campaigns",), ("result", "list")))
            if not items:
                break
            rows.extend(x for x in items if isinstance(x, dict))
            if len(items) < 100:
                break
        return rows

    def campaign_product_summary(self, first: dt.date, last: dt.date) -> Any:
        return self.get_json(
            "/api/client/statistics/campaign/product/json",
            {"dateFrom": first.isoformat(), "dateTo": last.isoformat()},
        )

    def request_stats_report(self, campaign_ids: list[str], first: dt.date, last: dt.date) -> str:
        resp = self.post_json(
            "/api/client/statistics",
            {
                "campaigns": campaign_ids,
                "dateFrom": first.isoformat(),
                "dateTo": last.isoformat(),
                "from": _iso_start(first),
                "to": _iso_end(last),
                "groupBy": "NO_GROUP_BY",
            },
        )
        uuid = str(resp.get("UUID") or resp.get("uuid") or "") if isinstance(resp, dict) else ""
        if not uuid:
            raise RuntimeError(f"Performance statistics request returned no UUID: {resp!r}")
        return uuid

    def request_all_sku_cpa_orders(self, first: dt.date, last: dt.date) -> str:
        params = [
            ("timeBounds.from", _iso_start(first)),
            ("timeBounds.to", _iso_end(last)),
        ]
        try:
            resp = self.get_json("/api/client/statistics/all_sku_promo/orders/generate", params)
        except Exception:
            resp = self.post_json(
                "/api/client/statistic/orders/generate",
                {"from": _iso_start(first), "to": _iso_end(last)},
            )
        uuid = str(resp.get("UUID") or resp.get("uuid") or "") if isinstance(resp, dict) else ""
        if not uuid:
            raise RuntimeError(f"CPA orders report returned no UUID: {resp!r}")
        return uuid

    def wait_report(self, uuid: str, timeout_s: int = 300) -> Any:
        deadline = time.time() + timeout_s
        last: Any = None
        while time.time() < deadline:
            last = self.get_json(f"/api/client/statistics/{urllib.parse.quote(uuid)}")
            state = str(last.get("state") or "").upper() if isinstance(last, dict) else ""
            if state in {"OK", "SUCCESS", "DONE"}:
                return last
            if state in {"ERROR", "FAILED"}:
                raise RuntimeError(f"Performance report {uuid} failed: {last!r}")
            time.sleep(2.0)
        raise TimeoutError(f"Performance report {uuid} not ready after {timeout_s}s: {last!r}")

    def download_report(self, uuid: str) -> tuple[bytes, str]:
        url = PERF_BASE + "/api/client/statistics/report?" + urllib.parse.urlencode({"UUID": uuid})
        data, headers, _ = _request("GET", url, headers=self._headers(), timeout=180)
        return data, headers.get("content-type", "application/octet-stream")


def _parse_stores_js(path: Path) -> list[Store]:
    raw = path.read_text(encoding="utf-8")
    m = re.search(r"window\.OZON_STORES\s*=\s*(\[[\s\S]*?\])\s*;", raw)
    if not m:
        raise RuntimeError(f"Could not find window.OZON_STORES array in {path}")
    text = m.group(1)
    try:
        rows = json.loads(text)
    except json.JSONDecodeError:
        text = re.sub(r"([{,])\s*([A-Za-z_][A-Za-z0-9_]*)\s*:", r'\1"\2":', text)
        text = text.replace("'", '"')
        rows = json.loads(text)
    stores: list[Store] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        cid = str(row.get("client_id") or row.get("clientId") or "").strip()
        key = str(row.get("api_key") or row.get("apiKey") or "").strip()
        name = str(row.get("name") or cid).strip()
        if cid and key:
            stores.append(Store(name=name, client_id=cid, api_key=key))
    return stores


def _load_stores(args: argparse.Namespace) -> list[Store]:
    stores: list[Store] = []
    if args.stores_js:
        path = Path(args.stores_js)
        if path.exists():
            stores.extend(_parse_stores_js(path))
        elif args.require_stores_js:
            raise FileNotFoundError(path)

    env_cid = os.getenv("OZON_CLIENT_ID", "").strip()
    env_key = os.getenv("OZON_API_KEY", "").strip()
    if env_cid and env_key:
        stores.append(Store(os.getenv("OZON_STORE_NAME", env_cid), env_cid, env_key))

    unique: dict[str, Store] = {}
    for store in stores:
        unique.setdefault(store.client_id, store)
    stores = list(unique.values())

    if args.store:
        needle = args.store.casefold().strip()
        selected = [
            s for s in stores
            if s.client_id == args.store or needle in s.name.casefold()
        ]
        if not selected:
            available = ", ".join(f"{s.name} ({s.client_id})" for s in stores) or "none"
            raise RuntimeError(f"Store {args.store!r} not found. Available: {available}")
        stores = selected
    if not stores:
        raise RuntimeError(
            "No Ozon Seller credentials found. Use --stores-js stores.secrets.js or "
            "OZON_CLIENT_ID/OZON_API_KEY environment variables."
        )
    return stores


def _safe_name(text: str) -> str:
    text = re.sub(r"[^0-9A-Za-zА-Яа-я._-]+", "_", text.strip())
    return text.strip("_") or "store"


def _save_binary_report(base: Path, prefix: str, uuid: str, data: bytes, content_type: str) -> Path:
    if data[:2] == b"PK" or "zip" in content_type.lower():
        ext = ".zip"
    elif "json" in content_type.lower() or data.lstrip().startswith((b"{", b"[")):
        ext = ".json"
    else:
        ext = ".csv"
    path = base / f"{prefix}_{uuid}{ext}"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)
    return path


def _extract_zip_csvs(zip_path: Path, dest: Path) -> list[str]:
    if not zipfile.is_zipfile(zip_path):
        return []
    dest.mkdir(parents=True, exist_ok=True)
    names: list[str] = []
    with zipfile.ZipFile(zip_path) as zf:
        for info in zf.infolist():
            if info.is_dir() or not info.filename.lower().endswith(".csv"):
                continue
            name = Path(info.filename).name
            target = dest / name
            target.write_bytes(zf.read(info))
            names.append(str(target))
    return names


def _collect_performance(
    perf: PerformanceClient,
    out_dir: Path,
    first: dt.date,
    last: dt.date,
) -> dict[str, Any]:
    perf_dir = out_dir / "performance"
    perf_dir.mkdir(parents=True, exist_ok=True)
    manifest: dict[str, Any] = {"enabled": True, "errors": []}

    try:
        campaigns = perf.list_campaigns("SKU")
        _write_json(perf_dir / "campaigns_cpc.json", campaigns)
        manifest["cpc_campaigns"] = len(campaigns)
    except Exception as e:
        campaigns = []
        manifest["errors"].append({"stage": "campaigns", "error": str(e)})

    try:
        summary = perf.campaign_product_summary(first, last)
        _write_json(perf_dir / "cpc_campaign_summary.json", summary)
    except Exception as e:
        manifest["errors"].append({"stage": "cpc_campaign_summary", "error": str(e)})

    report_files: list[str] = []
    campaign_ids = [str(x.get("id")) for x in campaigns if isinstance(x, dict) and x.get("id")]
    for batch_no in range(0, len(campaign_ids), 10):
        batch = campaign_ids[batch_no:batch_no + 10]
        try:
            uuid = perf.request_stats_report(batch, first, last)
            status = perf.wait_report(uuid)
            _write_json(perf_dir / f"cpc_status_{uuid}.json", status)
            data, content_type = perf.download_report(uuid)
            path = _save_binary_report(perf_dir, "cpc_sku", uuid, data, content_type)
            report_files.append(str(path))
            if path.suffix == ".zip":
                _extract_zip_csvs(path, perf_dir / "cpc_csv")
        except Exception as e:
            manifest["errors"].append({"stage": "cpc_sku", "campaigns": batch, "error": str(e)})
    manifest["cpc_report_files"] = report_files

    try:
        uuid = perf.request_all_sku_cpa_orders(first, last)
        status = perf.wait_report(uuid)
        _write_json(perf_dir / f"cpa_orders_status_{uuid}.json", status)
        data, content_type = perf.download_report(uuid)
        path = _save_binary_report(perf_dir, "cpa_orders", uuid, data, content_type)
        manifest["cpa_orders_file"] = str(path)
        if path.suffix == ".zip":
            _extract_zip_csvs(path, perf_dir / "cpa_csv")
    except Exception as e:
        manifest["errors"].append({"stage": "cpa_orders", "error": str(e)})

    return manifest


def collect(args: argparse.Namespace) -> int:
    first, month_last = _month_range(args.month)
    as_of = dt.date.fromisoformat(args.as_of) if args.as_of else dt.date.today()
    last = month_last
    out_root = Path(args.output).resolve()
    out_dir = out_root / args.month
    out_dir.mkdir(parents=True, exist_ok=True)

    stores = _load_stores(args)
    manifest: dict[str, Any] = {
        "version": 1,
        "month": args.month,
        "date_from": first.isoformat(),
        "date_to": last.isoformat(),
        "as_of": as_of.isoformat(),
        "created_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "stores": [{"name": s.name, "client_id": s.client_id} for s in stores],
        "seller_api": {"base": SELLER_BASE, "stores": []},
        "performance_api": {"enabled": False},
        "notes": [
            "Secrets are intentionally not written to this data pack.",
            "Monthly finance source is /v1/finance/accrual/by-day; deprecated transaction/list is not used.",
            "Returns are fetched through as_of and must be matched to month-created postings in report logic.",
        ],
    }

    for store in stores:
        store_dir = out_dir / "seller" / _safe_name(store.name)
        store_dir.mkdir(parents=True, exist_ok=True)
        client = SellerClient(store)
        sm: dict[str, Any] = {"name": store.name, "client_id": store.client_id, "errors": []}

        try:
            types = client.accrual_types()
            _write_json(store_dir / "accrual_types.json", types)
        except Exception as e:
            sm["errors"].append({"stage": "accrual_types", "error": str(e)})

        try:
            n = _write_jsonl(store_dir / "accruals.jsonl", client.iter_accruals(first, last))
            sm["accrual_rows"] = n
        except Exception as e:
            sm["errors"].append({"stage": "accruals", "error": str(e)})

        try:
            n = _write_jsonl(store_dir / "fbs_postings.jsonl", client.iter_fbs(first, last))
            sm["fbs_postings"] = n
        except Exception as e:
            sm["errors"].append({"stage": "fbs", "error": str(e)})

        try:
            n = _write_jsonl(store_dir / "fbo_postings.jsonl", client.iter_fbo(first, last))
            sm["fbo_postings"] = n
        except Exception as e:
            sm["errors"].append({"stage": "fbo", "error": str(e)})

        try:
            n = _write_jsonl(store_dir / "returns.jsonl", client.iter_returns(first, as_of))
            sm["returns"] = n
        except Exception as e:
            sm["errors"].append({"stage": "returns", "error": str(e)})

        try:
            buyouts = client.buyouts(first, last)
            _write_json(store_dir / "buyouts.json", buyouts)
            sm["buyout_rows"] = len(buyouts)
        except Exception as e:
            sm["errors"].append({"stage": "buyouts", "error": str(e)})

        manifest["seller_api"]["stores"].append(sm)

    perf_cid = os.getenv("OZON_PERFORMANCE_CLIENT_ID", "").strip()
    perf_secret = os.getenv("OZON_PERFORMANCE_CLIENT_SECRET", "").strip()
    if args.with_performance or (perf_cid and perf_secret):
        if not (perf_cid and perf_secret):
            manifest["performance_api"] = {
                "enabled": False,
                "error": "OZON_PERFORMANCE_CLIENT_ID / OZON_PERFORMANCE_CLIENT_SECRET are not set",
            }
        else:
            try:
                perf = PerformanceClient(perf_cid, perf_secret)
                manifest["performance_api"] = _collect_performance(perf, out_dir, first, last)
            except Exception as e:
                manifest["performance_api"] = {"enabled": True, "errors": [{"stage": "init", "error": str(e)}]}

    _write_json(out_dir / "manifest.json", manifest)

    print(f"Ozon monthly data pack: {out_dir}")
    for sm in manifest["seller_api"]["stores"]:
        print(
            f"- {sm['name']}: accruals={sm.get('accrual_rows','ERR')}, "
            f"FBS={sm.get('fbs_postings','ERR')}, FBO={sm.get('fbo_postings','ERR')}, "
            f"returns={sm.get('returns','ERR')}, buyouts={sm.get('buyout_rows','ERR')}, "
            f"errors={len(sm.get('errors', []))}"
        )
    perf_manifest = manifest.get("performance_api") or {}
    print(f"- performance: enabled={perf_manifest.get('enabled', False)}, errors={len(perf_manifest.get('errors', []))}")
    if any(sm.get("errors") for sm in manifest["seller_api"]["stores"]):
        print("WARNING: one or more Seller API stages failed; inspect manifest.json", file=sys.stderr)
        return 2
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Collect Ozon data for one monthly management report")
    p.add_argument("--month", required=True, help="Month in YYYY-MM format, e.g. 2026-08")
    p.add_argument("--store", help="Store name substring or Seller API client_id. Default: all stores")
    p.add_argument(
        "--stores-js",
        default="stores.secrets.js",
        help="Path to mp-unit stores.secrets.js (default: ./stores.secrets.js)",
    )
    p.add_argument("--require-stores-js", action="store_true", help="Fail if --stores-js file is absent")
    p.add_argument(
        "--output",
        default="data/ozon_monthly",
        help="Output root (default: data/ozon_monthly; already gitignored)",
    )
    p.add_argument(
        "--as-of",
        help="Status/returns cut-off YYYY-MM-DD. Default: today; useful for late returns/transitions",
    )
    p.add_argument(
        "--with-performance",
        action="store_true",
        help="Also fetch advertising. Requires OZON_PERFORMANCE_CLIENT_ID and _CLIENT_SECRET",
    )
    return p


def main() -> int:
    return collect(build_parser().parse_args())


if __name__ == "__main__":
    raise SystemExit(main())
