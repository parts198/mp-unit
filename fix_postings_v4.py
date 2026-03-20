from pathlib import Path
import re

p = Path("ozon_backend_v4.py")
text = p.read_text(encoding="utf-8")

helper = r'''
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

if "def extract_items_from_ozon_response(" not in text:
    marker = "\n\nclass OzonClient:\n"
    if marker not in text:
        raise SystemExit("Не найден marker для вставки helper")
    text = text.replace(marker, "\n\n" + helper + "\n\nclass OzonClient:\n", 1)

text = re.sub(
    r'def ozon_postings_list\(client: OzonClient, schema: str, since_iso: str, to_iso: str, limit: int = 1000\) -> List\[Dict\[str, Any\]\]:.*?(?=\n\ndef fetch_postings_cached)',
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
    flags=re.S,
    count=1
)

text = re.sub(
    r'def fetch_postings_cached\(date_from: str, date_to: str, schemas: List\[str\], store_sel: Any, min_interval: float\) -> Dict\[str, Any\]:.*?(?=\n\n# ---------- acts ----------)',
    '''def fetch_postings_cached(date_from: str, date_to: str, schemas: List[str], store_sel: Any, min_interval: float) -> Dict[str, Any]:
    stores_all = load_stores()
    selected = normalize_store_selection(store_sel, stores_all)

    idxs = []
    for st in selected:
        for i, s in enumerate(stores_all):
            if s["client_id"] == st["client_id"]:
                idxs.append(i)
    idxs = sorted(set(idxs))

    schemas = [s for s in schemas if s in ("FBO", "FBS")]
    if not schemas:
        schemas = ["FBO", "FBS"]

    key = f"{date_from}_{date_to}_{','.join(sorted(schemas))}_stores-{','.join(map(str, idxs))}"
    key = re.sub(r"[^A-Za-z0-9_,\\-.]", "_", key)
    cache_file = CACHE_DIR / f"postings_{key}.json"
    if cache_file.exists():
        obj = load_json(cache_file, {})
        save_json(ORDERS_LAST_PATH, obj)
        return obj

    since_iso = date_from + "T00:00:00.000Z"
    to_iso = date_to + "T23:59:59.999Z"

    postings = []
    errors = []

    for st in selected:
        c = OzonClient(st, min_interval_s=min_interval)
        for sch in schemas:
            try:
                postings.extend(ozon_postings_list(c, sch, since_iso, to_iso))
            except HTTPException as e:
                errors.append({
                    "store": st.get("name"),
                    "schema": sch,
                    "detail": e.detail,
                })

    obj = {
        "meta": {
            "date_from": date_from,
            "date_to": date_to,
            "schemas": schemas,
            "stores": idxs,
            "errors": errors,
        },
        "postings": postings,
    }
    save_json(cache_file, obj)
    save_json(ORDERS_LAST_PATH, obj)
    return obj
''',
    text,
    flags=re.S,
    count=1
)

# На всякий случай патчим ещё эти две функции
text = re.sub(
    r'def product_info_list_v3\(client: OzonClient, offer_ids: List\[str\]\) -> List\[Dict\[str, Any\]\]:.*?(?=\n\ndef analytics_stocks)',
    '''def product_info_list_v3(client: OzonClient, offer_ids: List[str]) -> List[Dict[str, Any]]:
    resp = client.post("/v3/product/info/list", {"offer_id": offer_ids})
    return extract_items_from_ozon_response(resp)
''',
    text,
    flags=re.S,
    count=1
)

text = re.sub(
    r'def analytics_stocks\(client: OzonClient, skus: List\[int\]\) -> List\[Dict\[str, Any\]\]:.*?(?=\n\ndef fetch_ozon_stock_via_api)',
    '''def analytics_stocks(client: OzonClient, skus: List[int]) -> List[Dict[str, Any]]:
    resp = client.post("/v1/analytics/stocks", {"skus": [int(x) for x in skus]})
    return extract_items_from_ozon_response(resp)
''',
    text,
    flags=re.S,
    count=1
)

p.write_text(text, encoding="utf-8")
print("Готово: ozon_backend_v4.py обновлён")
