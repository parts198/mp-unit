from pathlib import Path

src = Path("ozon_backend_v4.py")
text = src.read_text(encoding="utf-8")

def replace_block(text: str, start_marker: str, end_marker: str, new_block: str) -> str:
    s = text.index(start_marker)
    e = text.index(end_marker, s)
    return text[:s] + new_block.rstrip() + "\n\n" + text[e:]

if "def build_demand_stats_from_postings(" not in text:
    insert_marker = "def compute_velocity_windows("
    demand_func = '''
def build_demand_stats_from_postings(postings_obj: Dict[str, Any], mode: str = "to") -> Dict[Tuple[str, str], Dict[str, Any]]:
    """
    Спрос по доставленным заказам.
    mode="to" => cluster_to (куда покупают)
    mode="from" => cluster_from (откуда отгружают)
    """
    events = build_outflows_from_postings(postings_obj, mode=mode)

    by_key = defaultdict(lambda: {
        "cluster": "",
        "offer_id": "",
        "total_delivered": 0,
        "first_delivery_date": None,
        "last_delivery_date": None,
    })

    for ev in events:
        if ev.get("bucket") != "Доставлено":
            continue

        cluster = ev.get("cluster") or "—"
        offer_id = ev.get("offer_id")
        qty = int(ev.get("qty") or 0)
        d = ev.get("date")

        key = (cluster, offer_id)
        row = by_key[key]
        row["cluster"] = cluster
        row["offer_id"] = offer_id
        row["total_delivered"] += qty

        if d:
            if row["first_delivery_date"] is None or d < row["first_delivery_date"]:
                row["first_delivery_date"] = d
            if row["last_delivery_date"] is None or d > row["last_delivery_date"]:
                row["last_delivery_date"] = d

    out = {}
    for key, row in by_key.items():
        fd = row["first_delivery_date"]
        ld = row["last_delivery_date"]
        if fd and ld:
            days = (dt.date.fromisoformat(ld) - dt.date.fromisoformat(fd)).days + 1
        else:
            days = 0
        days = max(days, 1)
        row["days_active"] = days
        row["avg_daily_delivered"] = (row["total_delivered"] / days) if days else 0.0
        out[key] = row

    return out
'''.strip()

    pos = text.index(insert_marker)
    text = text[:pos] + demand_func + "\n\n" + text[pos:]

new_build_recommendations = '''
def build_recommendations(demand_stats: Dict[Tuple[str, str], Dict[str, Any]], cover_days: int, my_stock: Dict[str, int], ozon_stock: Dict[Tuple[str, str], int]) -> List[Dict[str, Any]]:
    """
    Рекомендации считаются от СПРОСА в cluster_to:
      target = avg_daily_delivered * cover_days
      current = ozon_stock(cluster, offer)
      rec = max(0, target - current)
    """
    by_offer = defaultdict(list)

    for (cl, offer), d in demand_stats.items():
        avg = float(d.get("avg_daily_delivered", 0.0) or 0.0)
        if avg <= 0:
            continue

        target = avg * float(cover_days)
        cur_ozon = int(ozon_stock.get((cl, offer), 0) or 0)
        rec = max(0.0, target - cur_ozon)

        if rec <= 0:
            continue

        by_offer[offer].append((cl, d, rec, cur_ozon))

    rows = []
    for offer, lst in by_offer.items():
        total_rec = sum(x[2] for x in lst)
        if total_rec <= 0:
            continue

        limit = int(my_stock.get(offer, 0) or 0)

        for cl, d, rec, cur_ozon in lst:
            capped = 0
            reason = ""
            if limit <= 0:
                reason = "Нет остатка на вашем складе (не загружен или 0)"
            else:
                share = (rec / total_rec) if total_rec else 0.0
                capped = int(round(limit * share))
                reason = f"Остаток на вашем складе: {limit}"

            rows.append({
                "cluster": cl,
                "offer_id": offer,
                "avg_daily": round(float(d.get("avg_daily_delivered", 0.0)), 4),
                "cover_days": int(cover_days),
                "target": round(float(d.get("avg_daily_delivered", 0.0)) * cover_days, 2),
                "current_ozon": int(cur_ozon),
                "current_est": int(cur_ozon),
                "recommended": int(round(rec)),
                "recommended_capped": int(capped),
                "cap_reason": reason,
                "first_inflow_date": d.get("first_delivery_date"),
                "last_depletion_date": d.get("last_delivery_date"),
                "first_delivery_date": d.get("first_delivery_date"),
                "last_delivery_date": d.get("last_delivery_date"),
                "total_days": d.get("days_active"),
                "total_sold": d.get("total_delivered"),
            })

    rows.sort(
        key=lambda x: (
            x.get("recommended_capped", 0),
            x.get("recommended", 0),
            x.get("avg_daily", 0),
            x.get("total_sold", 0),
        ),
        reverse=True
    )
    return rows
'''.strip()

text = replace_block(
    text,
    "def build_recommendations(",
    "\n\n# ---------- FastAPI ----------",
    new_build_recommendations
)

new_api_analyze = '''
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
    recs = build_recommendations(
        demand_stats_to,
        cover_days=cover_days,
        my_stock=my_stock,
        ozon_stock=oz_stock,
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
            "recommendations_logic": "cluster_to demand - ozon stock, capped by my_stock",
            "orders_errors": (postings_obj.get("meta") or {}).get("errors") or []
        },
        "postings_count": len(postings_obj.get("postings") or []),
        "velocity_count": len(vel),
        "velocity_rows": vel_rows,
        "recommendations_count": len(recs),
        "recommendations": recs[:2000],
        "demand_top": demand_top,
    }
'''.strip()

text = replace_block(
    text,
    "def api_analyze(",
    "\n\ndef main():",
    new_api_analyze
)

text = text.replace('uvicorn.run("ozon_backend_v4:app"', 'uvicorn.run("ozon_backend_v5:app"')

Path("ozon_backend_v5.py").write_text(text, encoding="utf-8")
print("Готово: ozon_backend_v5.py")
