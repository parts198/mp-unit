#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pathlib import Path
import ast

BASE = Path(".")
candidates = ["ozon_backend_v6.py", "ozon_backend_v5.py", "ozon_backend_v4.py"]
SRC = None
for name in candidates:
    p = BASE / name
    if p.exists():
        SRC = p
        break
if SRC is None:
    raise SystemExit("Не найден backend-файл: ozon_backend_v6.py / ozon_backend_v5.py / ozon_backend_v4.py")

DST = BASE / "ozon_backend_v7.py"
source = SRC.read_text(encoding="utf-8")

def parse(src: str):
    return ast.parse(src)

def find_func(src: str, name: str):
    tree = parse(src)
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name == name:
            return node
    raise RuntimeError(f"Функция не найдена: {name}")

def replace_func(src: str, name: str, new_code: str) -> str:
    node = find_func(src, name)
    lines = src.splitlines(keepends=True)
    start = node.lineno - 1
    end = node.end_lineno
    return "".join(lines[:start] + [new_code.rstrip() + "\n\n"] + lines[end:])

def insert_before_func(src: str, before_name: str, new_code: str) -> str:
    node = find_func(src, before_name)
    lines = src.splitlines(keepends=True)
    start = node.lineno - 1
    return "".join(lines[:start] + [new_code.rstrip() + "\n\n"] + lines[start:])

def has_func(src: str, name: str) -> bool:
    try:
        find_func(src, name)
        return True
    except Exception:
        return False

def ensure_import(src: str, import_line: str) -> str:
    if import_line in src:
        return src
    lines = src.splitlines(keepends=True)
    insert_at = 0
    for i, line in enumerate(lines):
        if line.startswith("import ") or line.startswith("from "):
            insert_at = i + 1
    lines.insert(insert_at, import_line + "\n")
    return "".join(lines)

source = ensure_import(source, "import zipfile")
source = ensure_import(source, "from fastapi.responses import FileResponse")

# 1) build_recommendations: резерв на складе + последовательное распределение по оборачиваемости
new_build_recommendations = """
def build_recommendations(
    demand_stats: Dict[Tuple[str, str], Dict[str, Any]],
    availability_map: Dict[Tuple[str, str], Dict[str, Any]],
    date_to: str,
    cover_days: int,
    my_stock: Dict[str, int],
    ozon_stock: Dict[Tuple[str, str], int],
    my_stock_reserve: int = 0
) -> List[Dict[str, Any]]:
    candidates_by_offer = defaultdict(list)

    for (cl, offer), d in demand_stats.items():
        total_delivered = int(d.get("total_delivered", 0) or 0)
        if total_delivered <= 0:
            continue

        cur_ozon = int(ozon_stock.get((cl, offer), 0) or 0)
        avail = availability_map.get((cl, offer))

        if avail:
            start_s = avail["start_date"]
            end_s = avail["end_date"]
            days_active = int(avail["days_active"])
        else:
            start_s = d.get("first_delivery_date")
            if not start_s:
                continue
            if cur_ozon > 0:
                end_s = date_to
            else:
                end_s = d.get("last_delivery_date") or date_to
            start_d = dt.date.fromisoformat(start_s)
            end_d = dt.date.fromisoformat(end_s)
            days_active = max((end_d - start_d).days + 1, 1)

        avg = float(total_delivered) / float(days_active)
        target = avg * float(cover_days)
        deficit = max(0.0, target - cur_ozon)
        need_int = int(round(deficit))

        if need_int <= 0:
            continue

        candidates_by_offer[offer].append({
            "cluster": cl,
            "offer_id": offer,
            "avg_daily": round(avg, 4),
            "cover_days": int(cover_days),
            "target": round(target, 2),
            "current_ozon": int(cur_ozon),
            "current_est": int(cur_ozon),
            "recommended": int(need_int),
            "recommended_capped": 0,
            "cap_reason": "",
            "my_stock_total": int(my_stock.get(offer, 0) or 0),
            "my_stock_reserve": int(my_stock_reserve or 0),
            "first_inflow_date": start_s,
            "last_depletion_date": end_s,
            "window_start": start_s,
            "window_end": end_s,
            "total_days": int(days_active),
            "total_sold": int(total_delivered),
            "first_delivery_date": d.get("first_delivery_date"),
            "last_delivery_date": d.get("last_delivery_date"),
        })

    rows = []
    for offer, items in candidates_by_offer.items():
        total_stock = int(my_stock.get(offer, 0) or 0)
        reserve = max(0, int(my_stock_reserve or 0))
        stock_left = max(0, total_stock - reserve)

        items.sort(
            key=lambda x: (
                x.get("avg_daily", 0),
                x.get("total_sold", 0),
                x.get("recommended", 0),
            ),
            reverse=True
        )

        for item in items:
            if stock_left <= 0:
                item["recommended_capped"] = 0
                item["cap_reason"] = f"Доступно к распределению: 0 (остаток {total_stock}, резерв {reserve})"
            else:
                alloc = min(stock_left, int(item["recommended"]))
                item["recommended_capped"] = int(alloc)
                stock_left -= int(alloc)
                item["cap_reason"] = f"Остаток {total_stock}, резерв {reserve}, осталось к распределению {stock_left}"

            if item["recommended_capped"] > 0:
                rows.append(item)

    rows.sort(
        key=lambda x: (
            x.get("avg_daily", 0),
            x.get("total_sold", 0),
            x.get("recommended_capped", 0),
            x.get("recommended", 0),
        ),
        reverse=True
    )
    return rows
""".strip()

source = replace_func(source, "build_recommendations", new_build_recommendations)

# 2) helper for export files
if not has_func(source, "sanitize_cluster_filename"):
    helper_code = """
def sanitize_cluster_filename(name: str) -> str:
    s = as_text(name).strip() or "cluster"
    s = re.sub(r"[\\\\/:*?\\\"<>|]+", "_", s)
    s = re.sub(r"\\s+", " ", s).strip()
    return s[:120]

def build_cluster_export_zip(recommendations: List[Dict[str, Any]], export_dir: Path) -> Path:
    export_dir.mkdir(parents=True, exist_ok=True)

    by_cluster = defaultdict(list)
    for row in recommendations:
        qty = int(row.get("recommended_capped", 0) or 0)
        if qty <= 0:
            continue
        cluster = as_text(row.get("cluster")).strip() or "—"
        offer = as_text(row.get("offer_id")).strip()
        if not offer:
            continue
        by_cluster[cluster].append({"offer_id": offer, "qty": qty})

    ts = dt.datetime.now(tz=dt.timezone.utc).strftime("%Y%m%d_%H%M%S")
    run_dir = export_dir / f"cluster_files_{ts}"
    run_dir.mkdir(parents=True, exist_ok=True)

    created_files = []
    for cluster, items in sorted(by_cluster.items(), key=lambda kv: kv[0]):
        items.sort(key=lambda x: x["offer_id"])
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = "Sheet1"
        ws["A1"] = "артикул"
        ws["B1"] = "имя (необязательно)"
        ws["C1"] = "количество"

        row_idx = 2
        for item in items:
            ws.cell(row=row_idx, column=1, value=item["offer_id"])
            ws.cell(row=row_idx, column=2, value=None)
            ws.cell(row=row_idx, column=3, value=int(item["qty"]))
            row_idx += 1

        ws.column_dimensions["A"].width = 22
        ws.column_dimensions["B"].width = 28
        ws.column_dimensions["C"].width = 14

        fname = f"{sanitize_cluster_filename(cluster)}.xlsx"
        fpath = run_dir / fname
        wb.save(fpath)
        created_files.append(fpath)

    zip_path = export_dir / f"cluster_files_{ts}.zip"
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for fpath in created_files:
            zf.write(fpath, arcname=fpath.name)

    return zip_path
""".strip()
    source = insert_before_func(source, "api_state", helper_code)

# 3) api_analyze with reserve
new_api_analyze = """
def api_analyze(payload: Dict[str, Any]):
    if not SUPPLIES_PATH.exists():
        raise HTTPException(status_code=400, detail="Сначала сформируйте supplies.json через /api/fetch/supplies")
    supplies = load_json(SUPPLIES_PATH, {})
    acts_idx = load_json(ACTS_INDEX_PATH, {"by_supply": {}})
    postings_obj = load_json(ORDERS_LAST_PATH, {})
    if not postings_obj:
        raise HTTPException(status_code=400, detail="Сначала подтяните заказы через /api/fetch/postings")

    date_from = as_text(payload.get("date_from")).strip()
    date_to = as_text(payload.get("date_to")).strip()
    mode = as_text(payload.get("mode") or "to").strip()
    cover_days = int(payload.get("cover_days") or 14)
    my_stock_reserve = int(payload.get("my_stock_reserve") or 0)
    if mode not in ("from", "to"):
        raise HTTPException(status_code=400, detail="mode должен быть 'from' или 'to'")

    inflows = build_inflows_from_supplies_and_acts(supplies, acts_idx)
    outflows_from = build_outflows_from_postings(postings_obj, mode="from")
    vel = compute_velocity_windows(inflows, outflows_from, date_from, date_to)

    demand_stats_selected = build_demand_stats_from_postings(postings_obj, mode=mode)
    demand_top = sorted(
        [
            {
                "cluster": d["cluster"],
                "offer_id": d["offer_id"],
                "delivered_qty": d["total_delivered"],
            }
            for d in demand_stats_selected.values()
        ],
        key=lambda x: x["delivered_qty"],
        reverse=True
    )[:2000]

    demand_stats_to = build_demand_stats_from_postings(postings_obj, mode="to")

    my_stock = load_my_stock()
    oz_stock = ozon_cluster_stock_map()
    availability_map = build_availability_map_from_velocity(vel, date_to=date_to, ozon_stock=oz_stock)

    recs = build_recommendations(
        demand_stats=demand_stats_to,
        availability_map=availability_map,
        date_to=date_to,
        cover_days=cover_days,
        my_stock=my_stock,
        ozon_stock=oz_stock,
        my_stock_reserve=my_stock_reserve,
    )

    vel_rows = []
    for (_, _), v in vel.items():
        if not v.get("first_inflow_date"):
            continue
        vel_rows.append({
            "cluster": v["cluster"],
            "offer_id": v["offer_id"],
            "first_inflow_date": v.get("first_inflow_date"),
            "last_depletion_date": v.get("last_depletion_date"),
            "avg_daily": round(float(v.get("avg_daily_sold", 0.0)), 4),
            "total_days": v.get("total_days"),
            "total_sold": v.get("total_sold"),
            "current_ozon": oz_stock.get((v["cluster"], v["offer_id"])),
        })
    vel_rows.sort(key=lambda x: (x["avg_daily"], x["total_sold"]), reverse=True)
    vel_rows = vel_rows[:5000]

    return {
        "ok": True,
        "meta": {
            "date_from": date_from,
            "date_to": date_to,
            "mode": mode,
            "cover_days": cover_days,
            "my_stock_reserve": my_stock_reserve,
            "recommendations_logic": "cluster_to demand with availability window and sequential allocation from my_stock by turnover desc",
            "orders_errors": (postings_obj.get("meta") or {}).get("errors") or []
        },
        "postings_count": len(postings_obj.get("postings") or []),
        "velocity_count": len(vel),
        "velocity_rows": vel_rows,
        "recommendations_count": len(recs),
        "recommendations": recs[:2000],
        "demand_top": demand_top,
    }
""".strip()

source = replace_func(source, "api_analyze", new_api_analyze)

# 4) endpoint export cluster files
if not has_func(source, "api_export_cluster_files"):
    endpoint_code = """
@app.post("/api/export/cluster_files")
def api_export_cluster_files(payload: Dict[str, Any]):
    if not SUPPLIES_PATH.exists():
        raise HTTPException(status_code=400, detail="Сначала сформируйте supplies.json через /api/fetch/supplies")
    postings_obj = load_json(ORDERS_LAST_PATH, {})
    if not postings_obj:
        raise HTTPException(status_code=400, detail="Сначала подтяните заказы через /api/fetch/postings")

    supplies = load_json(SUPPLIES_PATH, {})
    acts_idx = load_json(ACTS_INDEX_PATH, {"by_supply": {}})

    date_from = as_text(payload.get("date_from")).strip()
    date_to = as_text(payload.get("date_to")).strip()
    cover_days = int(payload.get("cover_days") or 14)
    my_stock_reserve = int(payload.get("my_stock_reserve") or 0)

    inflows = build_inflows_from_supplies_and_acts(supplies, acts_idx)
    outflows_from = build_outflows_from_postings(postings_obj, mode="from")
    vel = compute_velocity_windows(inflows, outflows_from, date_from, date_to)

    demand_stats_to = build_demand_stats_from_postings(postings_obj, mode="to")
    my_stock = load_my_stock()
    oz_stock = ozon_cluster_stock_map()
    availability_map = build_availability_map_from_velocity(vel, date_to=date_to, ozon_stock=oz_stock)

    recs = build_recommendations(
        demand_stats=demand_stats_to,
        availability_map=availability_map,
        date_to=date_to,
        cover_days=cover_days,
        my_stock=my_stock,
        ozon_stock=oz_stock,
        my_stock_reserve=my_stock_reserve,
    )

    export_dir = DATA_DIR / "exports"
    zip_path = build_cluster_export_zip(recs, export_dir=export_dir)

    return FileResponse(
        str(zip_path),
        media_type="application/zip",
        filename=zip_path.name,
    )
""".strip()
    source = insert_before_func(source, "main", endpoint_code)

source = source.replace('uvicorn.run("ozon_backend_v4:app"', 'uvicorn.run("ozon_backend_v7:app"')
source = source.replace('uvicorn.run("ozon_backend_v5:app"', 'uvicorn.run("ozon_backend_v7:app"')
source = source.replace('uvicorn.run("ozon_backend_v6:app"', 'uvicorn.run("ozon_backend_v7:app"')

DST.write_text(source, encoding="utf-8")
print("Готово:", DST.name)
