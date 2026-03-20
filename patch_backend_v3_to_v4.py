from pathlib import Path
import re

p = Path("ozon_backend_v3.py")
text = p.read_text(encoding="utf-8")

# --- helper extract_items_from_ozon_response ---
if "def extract_items_from_ozon_response(" not in text:
    marker = "\n\n# -------------------- Ozon HTTP client with retry/throttle --------------------\n"
    helper = '''
def extract_items_from_ozon_response(resp):
    """
    Поддерживает варианты:
      {"result":{"postings":[...]}}
      {"result":{"items":[...]}}
      {"result":[...]}
      {"postings":[...]}
      {"items":[...]}
      [...]
    """
    if isinstance(resp, list):
        return [x for x in resp if isinstance(x, dict)]

    if not isinstance(resp, dict):
        return []

    result = resp.get("result")

    if isinstance(result, dict):
        items = result.get("postings") or result.get("items") or []
        if isinstance(items, list):
            return [x for x in items if isinstance(x, dict)]

    if isinstance(result, list):
        return [x for x in result if isinstance(x, dict)]

    items = resp.get("postings") or resp.get("items") or []
    if isinstance(items, list):
        return [x for x in items if isinstance(x, dict)]

    return []
'''
    if marker in text:
        text = text.replace(marker, "\n\n" + helper + marker)

# --- product_info_list_v3 ---
text = re.sub(
    r'def product_info_list_v3\(client: OzonClient, offer_ids: List\[str\]\) -> List\[Dict\[str, Any\]\]:\n(?:    .*\n)+?(?=\n\ndef |\n# --------------------|\Z)',
    '''def product_info_list_v3(client: OzonClient, offer_ids: List[str]) -> List[Dict[str, Any]]:
    resp = client.post("/v3/product/info/list", {"offer_id": offer_ids})
    return extract_items_from_ozon_response(resp)
''',
    text,
    count=1
)

# --- analytics_stocks ---
text = re.sub(
    r'def analytics_stocks\(client: OzonClient, skus: List\[int\]\) -> List\[Dict\[str, Any\]\]:\n(?:    .*\n)+?(?=\n\ndef |\n# --------------------|\Z)',
    '''def analytics_stocks(client: OzonClient, skus: List[int]) -> List[Dict[str, Any]]:
    resp = client.post("/v1/analytics/stocks", {"skus": [int(x) for x in skus]})
    return extract_items_from_ozon_response(resp)
''',
    text,
    count=1
)

# --- ozon_postings_list ---
text = re.sub(
    r'def ozon_postings_list\(client: OzonClient, schema: str, since_iso: str, to_iso: str, limit: int = 1000\) -> List\[Dict\[str, Any\]\]:\n(?:    .*\n)+?(?=\n\ndef |\n# --------------------|\Z)',
    '''def ozon_postings_list(client: OzonClient, schema: str, since_iso: str, to_iso: str, limit: int = 1000) -> List[Dict[str, Any]]:
    path = "/v3/posting/fbs/list" if schema == "FBS" else "/v2/posting/fbo/list"
    out = []
    offset = 0
    for _ in range(0, 400):
        body = {
            "dir": "ASC",
            "filter": {"since": since_iso, "to": to_iso},
            "limit": limit,
            "offset": offset,
            "with": {"analytics_data": True, "financial_data": True},
        }
        resp = client.post(path, body)
        items = extract_items_from_ozon_response(resp)
        if not items:
            break
        for it in items:
            it["_schema"] = schema
            it["_store"] = client.store.get("name")
            out.append(it)
        if len(items) < limit:
            break
        offset += limit
    return out
''',
    text,
    count=1
)

# --- fetch_postings_cached: не валить всё из-за одного магазина ---
text = re.sub(
    r'for st in selected:\n        c = OzonClient\(st, min_interval_s=min_interval\)\n        for sch in schemas:\n            postings.extend\(ozon_postings_list\(c, sch, since_iso, to_iso\)\)\n\n    obj = \{"meta": \{"date_from": date_from, "date_to": date_to, "schemas": schemas, "stores": idxs\}, "postings": postings\}',
    '''errors = []
    for st in selected:
        c = OzonClient(st, min_interval_s=min_interval)
        for sch in schemas:
            try:
                postings.extend(ozon_postings_list(c, sch, since_iso, to_iso))
            except HTTPException as e:
                errors.append({"store": st.get("name"), "schema": sch, "detail": e.detail})

    obj = {"meta": {"date_from": date_from, "date_to": date_to, "schemas": schemas, "stores": idxs, "errors": errors}, "postings": postings}''',
    text,
    count=1
)

# --- upload acts: дедуп по supply_id + sha256 ---
text = re.sub(
    r'dedup = load_json\(ACTS_DEDUP_PATH, \{"hashes": \{\}\}\)\n    hashes: Dict\[str, str\] = dedup.get\("hashes"\) or \{\}\n    saved, skipped = \[\], \[\]',
    '''dedup = load_json(ACTS_DEDUP_PATH, {"hashes": {}, "supply_ids": {}})
    hashes: Dict[str, str] = dedup.get("hashes") or {}
    supply_ids: Dict[str, str] = dedup.get("supply_ids") or {}
    saved, skipped = [], []''',
    text,
    count=1
)

text = re.sub(
    r'content = await f.read\(\)\n        h = sha256_bytes\(content\)\n        if h in hashes:\n            skipped.append\(\{"file": f.filename, "reason": "уже загружен \(sha256\)", "as": hashes\[h\]\}\)\n            continue\n\n        out = ACTS_DIR / Path\(f.filename\).name',
    '''content = await f.read()
        h = sha256_bytes(content)
        sid = find_supply_id_from_filename(f.filename)

        if h in hashes:
            skipped.append({"file": f.filename, "reason": "уже загружен (sha256)", "as": hashes[h]})
            continue

        if sid and sid in supply_ids:
            skipped.append({"file": f.filename, "reason": "акт для этого supply_id уже загружен", "as": supply_ids[sid], "supply_id": sid})
            continue

        out = ACTS_DIR / Path(f.filename).name''',
    text,
    count=1
)

text = re.sub(
    r'hashes\[h\] = out.name\n        saved.append\(out.name\)\n\n    save_json\(ACTS_DEDUP_PATH, \{"hashes": hashes\}\)',
    '''hashes[h] = out.name
        if sid:
            supply_ids[sid] = out.name
        saved.append(out.name)

    save_json(ACTS_DEDUP_PATH, {"hashes": hashes, "supply_ids": supply_ids})''',
    text,
    count=1
)

# --- state counters ---
text = text.replace(
    '"acts_dedup": len((dedup or {}).get("hashes") or {}),',
    '"acts_dedup_hashes": len((dedup or {}).get("hashes") or {}),\n        "acts_dedup_supply_ids": len((dedup or {}).get("supply_ids") or {}),'
)

backup = Path("ozon_backend_v3.py.bak")
if not backup.exists():
    backup.write_text(p.read_text(encoding="utf-8"), encoding="utf-8")

Path("ozon_backend_v4.py").write_text(text, encoding="utf-8")
print("Готово: ozon_backend_v4.py")
