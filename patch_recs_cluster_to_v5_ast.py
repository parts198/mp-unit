from pathlib import Path
import ast

SRC = Path('ozon_backend_v4.py')
DST = Path('ozon_backend_v5.py')

source = SRC.read_text(encoding='utf-8')

def parse(src: str):
    return ast.parse(src)

def find_func(src: str, name: str):
    tree = parse(src)
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name == name:
            return node
    raise RuntimeError(f'Function not found: {name}')

def replace_func(src: str, name: str, new_code: str) -> str:
    node = find_func(src, name)
    lines = src.splitlines(keepends=True)
    start = node.lineno - 1
    end = node.end_lineno
    return ''.join(lines[:start] + [new_code.rstrip() + '\n\n'] + lines[end:])

def insert_before_func(src: str, before_name: str, new_code: str) -> str:
    node = find_func(src, before_name)
    lines = src.splitlines(keepends=True)
    start = node.lineno - 1
    return ''.join(lines[:start] + [new_code.rstrip() + '\n\n'] + lines[start:])

def has_func(src: str, name: str) -> bool:
    try:
        find_func(src, name)
        return True
    except Exception:
        return False

if not has_func(source, 'build_demand_stats_from_postings'):
    demand_func = """
def build_demand_stats_from_postings(postings_obj: Dict[str, Any], mode: str = 'to') -> Dict[Tuple[str, str], Dict[str, Any]]:
    events = build_outflows_from_postings(postings_obj, mode=mode)

    by_key = defaultdict(lambda: {
        'cluster': '',
        'offer_id': '',
        'total_delivered': 0,
        'first_delivery_date': None,
        'last_delivery_date': None,
    })

    for ev in events:
        if ev.get('bucket') != 'Доставлено':
            continue

        cluster = ev.get('cluster') or '—'
        offer_id = ev.get('offer_id')
        qty = int(ev.get('qty') or 0)
        d = ev.get('date')

        key = (cluster, offer_id)
        row = by_key[key]
        row['cluster'] = cluster
        row['offer_id'] = offer_id
        row['total_delivered'] += qty

        if d:
            if row['first_delivery_date'] is None or d < row['first_delivery_date']:
                row['first_delivery_date'] = d
            if row['last_delivery_date'] is None or d > row['last_delivery_date']:
                row['last_delivery_date'] = d

    out = {}
    for key, row in by_key.items():
        fd = row['first_delivery_date']
        ld = row['last_delivery_date']
        if fd and ld:
            days = (dt.date.fromisoformat(ld) - dt.date.fromisoformat(fd)).days + 1
        else:
            days = 0
        days = max(days, 1)
        row['sales_days'] = days
        row['avg_daily_delivered_sales_window'] = (row['total_delivered'] / days) if days else 0.0
        out[key] = row

    return out
""".strip()
    source = insert_before_func(source, 'compute_velocity_windows', demand_func)

if not has_func(source, 'build_availability_map_from_velocity'):
    availability_func = """
def build_availability_map_from_velocity(
    vel: Dict[Tuple[str, str], Dict[str, Any]],
    date_to: str,
    ozon_stock: Dict[Tuple[str, str], int]
) -> Dict[Tuple[str, str], Dict[str, Any]]:
    out = {}
    for key, v in vel.items():
        start_s = v.get('first_inflow_date')
        if not start_s:
            continue

        start_d = dt.date.fromisoformat(start_s)
        cur_ozon = int(ozon_stock.get(key, 0) or 0)

        if cur_ozon > 0:
            end_s = date_to
        else:
            end_s = v.get('last_depletion_date') or date_to

        end_d = dt.date.fromisoformat(end_s)
        days = (end_d - start_d).days + 1
        days = max(days, 1)

        out[key] = {
            'start_date': start_s,
            'end_date': end_s,
            'days_active': days,
            'current_ozon': cur_ozon,
        }

    return out
""".strip()
    source = insert_before_func(source, 'compute_velocity_windows', availability_func)

new_build_recommendations = """
def build_recommendations(
    demand_stats: Dict[Tuple[str, str], Dict[str, Any]],
    availability_map: Dict[Tuple[str, str], Dict[str, Any]],
    date_to: str,
    cover_days: int,
    my_stock: Dict[str, int],
    ozon_stock: Dict[Tuple[str, str], int]
) -> List[Dict[str, Any]]:
    by_offer = defaultdict(list)

    for (cl, offer), d in demand_stats.items():
        total_delivered = int(d.get('total_delivered', 0) or 0)
        if total_delivered <= 0:
            continue

        cur_ozon = int(ozon_stock.get((cl, offer), 0) or 0)
        avail = availability_map.get((cl, offer))

        if avail:
            start_s = avail['start_date']
            end_s = avail['end_date']
            days_active = int(avail['days_active'])
        else:
            start_s = d.get('first_delivery_date')
            if not start_s:
                continue
            if cur_ozon > 0:
                end_s = date_to
            else:
                end_s = d.get('last_delivery_date') or date_to
            start_d = dt.date.fromisoformat(start_s)
            end_d = dt.date.fromisoformat(end_s)
            days_active = max((end_d - start_d).days + 1, 1)

        avg = float(total_delivered) / float(days_active)
        target = avg * float(cover_days)
        rec = max(0.0, target - cur_ozon)

        if rec <= 0:
            continue

        by_offer[offer].append((cl, d, rec, cur_ozon, avg, start_s, end_s, days_active, total_delivered, target))

    rows = []
    for offer, lst in by_offer.items():
        total_rec = sum(x[2] for x in lst)
        if total_rec <= 0:
            continue

        limit = int(my_stock.get(offer, 0) or 0)

        for cl, d, rec, cur_ozon, avg, start_s, end_s, days_active, total_delivered, target in lst:
            capped = 0
            reason = ''
            if limit <= 0:
                reason = 'Нет остатка на вашем складе (не загружен или 0)'
            else:
                share = (rec / total_rec) if total_rec else 0.0
                capped = int(round(limit * share))
                reason = f'Остаток на вашем складе: {limit}'

            rows.append({
                'cluster': cl,
                'offer_id': offer,
                'avg_daily': round(avg, 4),
                'cover_days': int(cover_days),
                'target': round(target, 2),
                'current_ozon': int(cur_ozon),
                'current_est': int(cur_ozon),
                'recommended': int(round(rec)),
                'recommended_capped': int(capped),
                'cap_reason': reason,
                'first_inflow_date': start_s,
                'last_depletion_date': end_s,
                'window_start': start_s,
                'window_end': end_s,
                'total_days': int(days_active),
                'total_sold': int(total_delivered),
                'first_delivery_date': d.get('first_delivery_date'),
                'last_delivery_date': d.get('last_delivery_date'),
            })

    rows.sort(
        key=lambda x: (
            x.get('recommended_capped', 0),
            x.get('recommended', 0),
            x.get('avg_daily', 0),
            x.get('total_sold', 0),
        ),
        reverse=True
    )
    return rows
""".strip()
source = replace_func(source, 'build_recommendations', new_build_recommendations)

new_api_analyze = """
def api_analyze(payload: Dict[str, Any]):
    if not SUPPLIES_PATH.exists():
        raise HTTPException(status_code=400, detail='Сначала сформируйте supplies.json через /api/fetch/supplies')
    supplies = load_json(SUPPLIES_PATH, {})
    acts_idx = load_json(ACTS_INDEX_PATH, {'by_supply': {}})
    postings_obj = load_json(ORDERS_LAST_PATH, {})
    if not postings_obj:
        raise HTTPException(status_code=400, detail='Сначала подтяните заказы через /api/fetch/postings')

    date_from = as_text(payload.get('date_from')).strip()
    date_to = as_text(payload.get('date_to')).strip()
    mode = as_text(payload.get('mode') or 'to').strip()
    cover_days = int(payload.get('cover_days') or 14)
    if mode not in ('from', 'to'):
        raise HTTPException(status_code=400, detail="mode должен быть 'from' или 'to'")

    inflows = build_inflows_from_supplies_and_acts(supplies, acts_idx)
    outflows_from = build_outflows_from_postings(postings_obj, mode='from')
    vel = compute_velocity_windows(inflows, outflows_from, date_from, date_to)

    demand_stats_selected = build_demand_stats_from_postings(postings_obj, mode=mode)
    demand_top = sorted(
        [
            {
                'cluster': d['cluster'],
                'offer_id': d['offer_id'],
                'delivered_qty': d['total_delivered'],
            }
            for d in demand_stats_selected.values()
        ],
        key=lambda x: x['delivered_qty'],
        reverse=True
    )[:2000]

    demand_stats_to = build_demand_stats_from_postings(postings_obj, mode='to')

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
    )

    vel_rows = []
    for (_, _), v in vel.items():
        if not v.get('first_inflow_date'):
            continue
        vel_rows.append({
            'cluster': v['cluster'],
            'offer_id': v['offer_id'],
            'first_inflow_date': v.get('first_inflow_date'),
            'last_depletion_date': v.get('last_depletion_date'),
            'avg_daily': round(float(v.get('avg_daily_sold', 0.0)), 4),
            'total_days': v.get('total_days'),
            'total_sold': v.get('total_sold'),
            'current_ozon': oz_stock.get((v['cluster'], v['offer_id'])),
        })
    vel_rows.sort(key=lambda x: (x['avg_daily'], x['total_sold']), reverse=True)
    vel_rows = vel_rows[:5000]

    return {
        'ok': True,
        'meta': {
            'date_from': date_from,
            'date_to': date_to,
            'mode': mode,
            'cover_days': cover_days,
            'recommendations_logic': 'cluster_to demand with availability window (arrival -> depletion/today), minus ozon stock, capped by my_stock',
            'orders_errors': (postings_obj.get('meta') or {}).get('errors') or []
        },
        'postings_count': len(postings_obj.get('postings') or []),
        'velocity_count': len(vel),
        'velocity_rows': vel_rows,
        'recommendations_count': len(recs),
        'recommendations': recs[:2000],
        'demand_top': demand_top,
    }
""".strip()
source = replace_func(source, 'api_analyze', new_api_analyze)

source = source.replace('uvicorn.run("ozon_backend_v4:app"', 'uvicorn.run("ozon_backend_v5:app"')

DST.write_text(source, encoding='utf-8')
print('Готово:', DST.name)
