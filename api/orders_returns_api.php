<?php
ini_set("display_errors","0");
ini_set("log_errors","1");
ini_set("error_log", __DIR__ . "/../data/orders_returns/php_error.log");
error_reporting(E_ALL);
declare(strict_types=1);
header('Content-Type: application/json; charset=utf-8');

function respond($data, int $code = 200): void {
  http_response_code($code);
  echo json_encode($data, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
  exit;
}

function read_json(): array {
  $raw = file_get_contents('php://input');
  if (!$raw) return [];
  $j = json_decode($raw, true);
  return is_array($j) ? $j : [];
}

function db_open(): SQLite3 {
  $dbPath = __DIR__ . '/../data/orders_returns/orders_returns.sqlite';
  $dir = dirname($dbPath);
  if (!is_dir($dir)) @mkdir($dir, 0775, true);

  $db = new SQLite3($dbPath);
  $db->busyTimeout(5000);
  $db->exec("PRAGMA journal_mode=WAL;");
  $db->exec("PRAGMA synchronous=NORMAL;");
  $db->exec("PRAGMA temp_store=MEMORY;");
  db_init();
  sync_dim_sku_offer(, 365);
return $db;
}

function db_init(SQLite3 $db): void {
  $db->exec("
    CREATE TABLE IF NOT EXISTS dim_store (
      store_id TEXT PRIMARY KEY,
      name TEXT,
      updated_at TEXT
    );
  ");

  $db->exec("
    CREATE TABLE IF NOT EXISTS snapshot_run (
      snapshot_date TEXT PRIMARY KEY,
      created_at TEXT NOT NULL,
      date_from TEXT,
      date_to TEXT,
      stores_json TEXT,
      orders_rows INTEGER DEFAULT 0,
      returns_rows INTEGER DEFAULT 0,
      status TEXT DEFAULT 'ok',
      message TEXT
    );
  ");

  $db->exec("
    CREATE TABLE IF NOT EXISTS fact_orders_line (
      snapshot_date TEXT NOT NULL,
      created_day TEXT NOT NULL,
      store_id TEXT NOT NULL,
      schema TEXT NOT NULL,
      order_id TEXT NOT NULL,
      posting_number TEXT,
      sku TEXT NOT NULL,
      offer_id TEXT,
      qty INTEGER NOT NULL,
      amount REAL NOT NULL,
      status TEXT,
      updated_at TEXT,
      raw_json TEXT,
      PRIMARY KEY (snapshot_date, store_id, schema, order_id, sku)
    );
  ");
  $db->exec("CREATE INDEX IF NOT EXISTS idx_orders_day ON fact_orders_line(snapshot_date, created_day);");
  $db->exec("CREATE INDEX IF NOT EXISTS idx_orders_store ON fact_orders_line(snapshot_date, store_id);");
  $db->exec("CREATE INDEX IF NOT EXISTS idx_orders_sku ON fact_orders_line(snapshot_date, sku);");
  $db->exec("CREATE INDEX IF NOT EXISTS idx_orders_order ON fact_orders_line(snapshot_date, order_id);");

  $db->exec("
    CREATE TABLE IF NOT EXISTS fact_returns_line (
      snapshot_date TEXT NOT NULL,
      created_day TEXT NOT NULL,
      store_id TEXT NOT NULL,
      return_id TEXT NOT NULL,
      order_id TEXT,
      sku TEXT NOT NULL,
      offer_id TEXT,
      qty INTEGER NOT NULL,
      amount REAL NOT NULL,
      status TEXT,
      reason TEXT,
      updated_at TEXT,
      raw_json TEXT,
      PRIMARY KEY (snapshot_date, store_id, return_id, sku)
    );
  ");
  $db->exec("CREATE INDEX IF NOT EXISTS idx_returns_day ON fact_returns_line(snapshot_date, created_day);");
  $db->exec("CREATE INDEX IF NOT EXISTS idx_returns_store ON fact_returns_line(snapshot_date, store_id);");
  $db->exec("CREATE INDEX IF NOT EXISTS idx_returns_sku ON fact_returns_line(snapshot_date, sku);");
  $db->exec("CREATE INDEX IF NOT EXISTS idx_returns_return ON fact_returns_line(snapshot_date, return_id);");
  $db->exec("CREATE INDEX IF NOT EXISTS idx_returns_order ON fact_returns_line(snapshot_date, order_id);");

  $db->exec("
    CREATE TABLE IF NOT EXISTS daily_kpi (
      snapshot_date TEXT NOT NULL,
      day TEXT NOT NULL,
      store_id TEXT NOT NULL,
      orders_qty INTEGER NOT NULL DEFAULT 0,
      orders_sum REAL NOT NULL DEFAULT 0,
      returns_qty INTEGER NOT NULL DEFAULT 0,
      returns_sum REAL NOT NULL DEFAULT 0,
      PRIMARY KEY (snapshot_date, day, store_id)
    );
  ");
  $db->exec("CREATE INDEX IF NOT EXISTS idx_daily_day ON daily_kpi(snapshot_date, day);");
  $db->exec("CREATE INDEX IF NOT EXISTS idx_daily_store ON daily_kpi(snapshot_date, store_id);");
}

function ozon_call(array $store, string $path, array $payload): array {
  $clientId = (string)($store['client_id'] ?? $store['clientId'] ?? '');
  $apiKey   = (string)($store['api_key'] ?? $store['apiKey'] ?? '');
  if ($clientId === '' || $apiKey === '') return ['__err' => 'store creds missing'];

  $url = 'https://api-seller.ozon.ru' . $path;
  $ch = curl_init($url);
  curl_setopt_array($ch, [
    CURLOPT_RETURNTRANSFER => true,
    CURLOPT_POST => true,
    CURLOPT_HTTPHEADER => [
      'Client-Id: ' . $clientId,
      'Api-Key: ' . $apiKey,
      'Content-Type: application/json',
      'Accept: application/json'
    ],
    CURLOPT_POSTFIELDS => json_encode($payload, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES),
    CURLOPT_CONNECTTIMEOUT => 20,
    CURLOPT_TIMEOUT => 120,
  ]);
  $resp = curl_exec($ch);
  $code = (int)curl_getinfo($ch, CURLINFO_HTTP_CODE);
  $err  = curl_error($ch);
  curl_close($ch);

  if ($resp === false) return ['__err' => 'curl: ' . $err];
  $j = json_decode($resp, true);
  if (!is_array($j)) return ['__err' => 'bad json', '__http' => $code, '__raw' => mb_substr($resp, 0, 3000)];
  if ($code >= 400) return ['__err' => 'http ' . $code, '__body' => $j];
  return $j;
}

function iso_range(string $dateFrom, string $dateTo): array {
  $since = $dateFrom . 'T00:00:00.000Z';
  $to    = $dateTo   . 'T23:59:59.000Z';
  return [$since, $to];
}

function extract_postings(array $j): array {
  // пробуем типовые варианты структур
  if (isset($j['result']['postings']) && is_array($j['result']['postings'])) return $j['result']['postings'];
  if (isset($j['result']) && is_array($j['result'])) return $j['result']; // иногда уже массив
  if (isset($j['postings']) && is_array($j['postings'])) return $j['postings'];
  return [];
}

function extract_returns(array $j): array {
  if (isset($j['result']['returns']) && is_array($j['result']['returns'])) return $j['result']['returns'];
  if (isset($j['returns']) && is_array($j['returns'])) return $j['returns'];
  if (isset($j['result']['items']) && is_array($j['result']['items'])) return $j['result']['items'];
  if (isset($j['result']) && is_array($j['result'])) return $j['result'];
  return [];
}

function safe_day(string $iso): string {
  if ($iso === '') return '';
  return substr($iso, 0, 10);
}

function to_float($v): float {
  if ($v === null) return 0.0;
  if (is_string($v)) $v = str_replace(',', '.', $v);
  return (float)$v;
}

function to_int($v): int {
  return (int)floor((float)$v);
}

function refresh_snapshot(SQLite3 $db, string $snapshotDate, string $dateFrom, string $dateTo, array $stores, array $schemas): array {
  set_time_limit(0);

  $now = gmdate('c');
  $storesJson = json_encode($stores, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);

  // store dim upsert
  $stUp = $db->prepare("INSERT INTO dim_store(store_id, name, updated_at)
    VALUES (:id,:name,:u)
    ON CONFLICT(store_id) DO UPDATE SET name=excluded.name, updated_at=excluded.updated_at");
  foreach ($stores as $st) {
    $id = (string)($st['client_id'] ?? $st['clientId'] ?? '');
    $nm = (string)($st['name'] ?? $id);
    if ($id === '') continue;
    $stUp->bindValue(':id', $id, SQLITE3_TEXT);
    $stUp->bindValue(':name', $nm, SQLITE3_TEXT);
    $stUp->bindValue(':u', $now, SQLITE3_TEXT);
    $stUp->execute();
  }

  // чистим старые данные для этой даты снапшота в выбранном диапазоне
  $del1 = $db->prepare("DELETE FROM fact_orders_line WHERE snapshot_date=:s AND created_day BETWEEN :f AND :t");
  $del1->bindValue(':s', $snapshotDate, SQLITE3_TEXT);
  $del1->bindValue(':f', $dateFrom, SQLITE3_TEXT);
  $del1->bindValue(':t', $dateTo, SQLITE3_TEXT);
  $del1->execute();

  $del2 = $db->prepare("DELETE FROM fact_returns_line WHERE snapshot_date=:s AND created_day BETWEEN :f AND :t");
  $del2->bindValue(':s', $snapshotDate, SQLITE3_TEXT);
  $del2->bindValue(':f', $dateFrom, SQLITE3_TEXT);
  $del2->bindValue(':t', $dateTo, SQLITE3_TEXT);
  $del2->execute();

  $db->exec("DELETE FROM daily_kpi WHERE snapshot_date=" . $db->escapeString($snapshotDate) . " AND day BETWEEN " . $db->escapeString($dateFrom) . " AND " . $db->escapeString($dateTo));

  [$since, $to] = iso_range($dateFrom, $dateTo);

  $insO = $db->prepare("INSERT OR REPLACE INTO fact_orders_line
    (snapshot_date, created_day, store_id, schema, order_id, posting_number, sku, offer_id, qty, amount, status, updated_at, raw_json)
    VALUES (:s,:d,:store,:sch,:oid,:pn,:sku,:offer,:q,:a,:st,:u,:raw)");

  $insR = $db->prepare("INSERT OR REPLACE INTO fact_returns_line
    (snapshot_date, created_day, store_id, return_id, order_id, sku, offer_id, qty, amount, status, reason, updated_at, raw_json)
    VALUES (:s,:d,:store,:rid,:oid,:sku,:offer,:q,:a,:st,:reason,:u,:raw)");

  $ordersRows = 0;
  $returnsRows = 0;

  foreach ($stores as $store) {
    $storeId = (string)($store['client_id'] ?? $store['clientId'] ?? '');
    if ($storeId === '') continue;

    foreach ($schemas as $schema) {
      $schemaU = strtoupper((string)$schema);
      if ($schemaU !== 'FBS' && $schemaU !== 'FBO') continue;

      // ===== ORDERS / POSTINGS =====
      $offset = 0;
      $limit = 1000;
      $guard = 0;

      while ($guard < 200) {
        $guard++;

        $path = ($schemaU === 'FBS') ? '/v3/posting/fbs/list' : '/v2/posting/fbo/list';
        $body = [
          'filter' => [
            'since' => $since,
            'to' => $to
          ],
          'limit' => $limit,
          'offset' => $offset
        ];

        $j = ozon_call($store, $path, $body);
        if (isset($j['__err'])) {
          // продолжаем по другим магазинам, но фиксируем ошибку в ответ
          break;
        }

        $postings = extract_postings($j);
        if (!$postings) break;

        foreach ($postings as $p) {
          $orderId = (string)($p['order_id'] ?? $p['orderId'] ?? $p['posting_number'] ?? $p['postingNumber'] ?? '');
          $postingNumber = (string)($p['posting_number'] ?? $p['postingNumber'] ?? $orderId);
          if ($orderId === '') $orderId = $postingNumber;

          $createdAt = (string)($p['created_at'] ?? $p['createdAt'] ?? $p['in_process_at'] ?? $p['inProcessAt'] ?? $p['shipment_date'] ?? $p['shipmentDate'] ?? '');
          $day = safe_day($createdAt);
          if ($day === '') continue;

          $status = (string)($p['status'] ?? $p['status_name'] ?? $p['statusName'] ?? '');
          $updatedAt = (string)($p['updated_at'] ?? $p['updatedAt'] ?? $p['last_changed_at'] ?? $p['lastChangedAt'] ?? '');

          $products = [];
          if (isset($p['products']) && is_array($p['products'])) $products = $p['products'];

          foreach ($products as $prod) {
            $sku = (string)($prod['sku'] ?? '');
            $offer = (string)($prod['offer_id'] ?? $prod['offerId'] ?? '');
            $qty = to_int($prod['quantity'] ?? $prod['qty'] ?? 0);

            // price может быть строкой; иногда есть total_price/price
            $price = to_float($prod['price'] ?? $prod['total_price'] ?? $prod['totalPrice'] ?? 0);
            $amount = $price * max(1, $qty);

            if ($sku === '') continue;

            $raw = json_encode(['posting'=>$p,'product'=>$prod], JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);

            $insO->bindValue(':s', $snapshotDate, SQLITE3_TEXT);
            $insO->bindValue(':d', $day, SQLITE3_TEXT);
            $insO->bindValue(':store', $storeId, SQLITE3_TEXT);
            $insO->bindValue(':sch', $schemaU, SQLITE3_TEXT);
            $insO->bindValue(':oid', $orderId, SQLITE3_TEXT);
            $insO->bindValue(':pn', $postingNumber, SQLITE3_TEXT);
            $insO->bindValue(':sku', $sku, SQLITE3_TEXT);
            $insO->bindValue(':offer', $offer, SQLITE3_TEXT);
            $insO->bindValue(':q', $qty, SQLITE3_INTEGER);
            $insO->bindValue(':a', $amount, SQLITE3_FLOAT);
            $insO->bindValue(':st', $status, SQLITE3_TEXT);
            $insO->bindValue(':u', $updatedAt, SQLITE3_TEXT);
            $insO->bindValue(':raw', $raw, SQLITE3_TEXT);
            $insO->execute();
            $ordersRows++;
          }
        }

        if (count($postings) < $limit) break;
        $offset += $limit;
      }
    }

    // ===== RETURNS =====
    // ВАЖНО: /v1/returns/list работает через last_id + has_next,
    // и фильтруется не since/to, а через visual_status_change_moment (момент 'появления' возврата).
    $lastId = 0;
    $limit = 500;
    $guard = 0;

    while ($guard < 500) {
      $guard++;

      $path = '/v1/returns/list';

      // Некоторые окружения OZON могут по-разному называть фильтр.
      // Сначала пробуем visual_status_change_moment, при ошибке попробуем visual_status_change_moment.
      $filterA = [
        'visual_status_change_moment' => [
          'time_from' => $since,
          'time_to'   => $to
        ]
      ];
      $filterB = [
        'visual_status_change_moment' => [
          'time_from' => $since,
          'time_to'   => $to
        ]
      ];

      $body = [
        'filter'  => $filterA,
        'limit'   => $limit,
        'last_id' => $lastId
      ];

      $j = ozon_call($store, $path, $body);

      if (isset($j['__err'])) {
        // fallback на альтернативное имя фильтра
        $body['filter'] = $filterB;
        $j = ozon_call($store, $path, $body);
        if (isset($j['__err'])) break;
      }

      $items = [];
      if (isset($j['returns']) && is_array($j['returns'])) $items = $j['returns'];
      elseif (isset($j['result']['returns']) && is_array($j['result']['returns'])) $items = $j['result']['returns'];

      if (!$items) break;

      foreach ($items as $it) {
        $returnId = (string)($it['id'] ?? $it['return_id'] ?? $it['returnId'] ?? '');
        if ($returnId === '') continue;

        $orderId = (string)($it['order_id'] ?? $it['orderId'] ?? $it['posting_number'] ?? $it['postingNumber'] ?? '');

        // "появление" возврата — берём момент изменения визуального статуса
        $createdAt = (string)(
          $it['visual_status_change_moment'] ??
          ($it['visual']['change_moment'] ?? '') ??
          ($it['storage']['arrived_moment'] ?? '') ??
          ($it['logistic']['return_date'] ?? '')
        );
        $day = safe_day($createdAt);
        if ($day === '') continue;

        $status = (string)(
          $it['visual']['status']['display_name'] ??
          $it['visual']['status']['sys_name'] ??
          $it['status'] ?? ''
        );
        $reason = (string)($it['return_reason_name'] ?? $it['reason'] ?? $it['reason_name'] ?? '');

        // product может быть объектом или products[]
        $prod = [];
        if (isset($it['product']) && is_array($it['product'])) $prod = $it['product'];
        elseif (isset($it['products']) && is_array($it['products']) && isset($it['products'][0]) && is_array($it['products'][0])) $prod = $it['products'][0];

        $sku   = (string)($prod['sku'] ?? ($it['sku'] ?? ''));
        $offer = (string)($prod['offer_id'] ?? $prod['offerId'] ?? ($it['offer_id'] ?? $it['offerId'] ?? ''));

        $price = 0;
        if (isset($prod['price']['price'])) $price = $prod['price']['price'];
        elseif (isset($prod['price'])) $price = $prod['price'];
        elseif (isset($it['price'])) $price = $it['price'];

        $amount = to_float($price);

        if ($sku === '' && $offer === '') continue;

        $raw = json_encode($it, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);

        $insR->bindValue(':s', $snapshotDate, SQLITE3_TEXT);
        $insR->bindValue(':d', $day, SQLITE3_TEXT);
        $insR->bindValue(':store', $storeId, SQLITE3_TEXT);
        $insR->bindValue(':rid', $returnId, SQLITE3_TEXT);
        $insR->bindValue(':oid', $orderId, SQLITE3_TEXT);
        $insR->bindValue(':sku', $sku, SQLITE3_TEXT);
        $insR->bindValue(':offer', $offer, SQLITE3_TEXT);

        // ВАЖНО для твоей задачи: считаем "сколько возвратов появилось" => 1 строка = 1 возврат
        $insR->bindValue(':q', 1, SQLITE3_INTEGER);
        $insR->bindValue(':a', $amount, SQLITE3_FLOAT);

        $insR->bindValue(':st', $status, SQLITE3_TEXT);
        $insR->bindValue(':reason', $reason, SQLITE3_TEXT);
        $insR->bindValue(':u', $createdAt, SQLITE3_TEXT);
        $insR->bindValue(':raw', $raw, SQLITE3_TEXT);
        $insR->execute();
        $returnsRows++;
      }

      $hasNext = !empty($j['has_next']);
      $last = $items[count($items)-1] ?? null;
      $lastId = (int)($last['id'] ?? $lastId);

      if (!$hasNext) break;
      if ($lastId <= 0) break;
    }


  // пересчёт daily_kpi
  $db->exec("DELETE FROM daily_kpi WHERE snapshot_date=" . "'" . $db->escapeString($snapshotDate) . "'" . " AND day BETWEEN '" . $db->escapeString($dateFrom) . "' AND '" . $db->escapeString($dateTo) . "'");

  // orders
  $db->exec("
    INSERT INTO daily_kpi(snapshot_date, day, store_id, orders_qty, orders_sum, returns_qty, returns_sum)
    SELECT
      '" . $db->escapeString($snapshotDate) . "' as snapshot_date,
      created_day as day,
      store_id,
      SUM(qty) as orders_qty,
      SUM(amount) as orders_sum,
      0 as returns_qty,
      0 as returns_sum
    FROM fact_orders_line
    WHERE snapshot_date='" . $db->escapeString($snapshotDate) . "'
      AND created_day BETWEEN '" . $db->escapeString($dateFrom) . "' AND '" . $db->escapeString($dateTo) . "'
    GROUP BY created_day, store_id
    ON CONFLICT(snapshot_date, day, store_id)
    DO UPDATE SET
      orders_qty=excluded.orders_qty,
      orders_sum=excluded.orders_sum
  ");

  // returns
  $db->exec("
    INSERT INTO daily_kpi(snapshot_date, day, store_id, orders_qty, orders_sum, returns_qty, returns_sum)
    SELECT
      '" . $db->escapeString($snapshotDate) . "' as snapshot_date,
      created_day as day,
      store_id,
      0 as orders_qty,
      0 as orders_sum,
      SUM(qty) as returns_qty,
      SUM(amount) as returns_sum
    FROM fact_returns_line
    WHERE snapshot_date='" . $db->escapeString($snapshotDate) . "'
      AND created_day BETWEEN '" . $db->escapeString($dateFrom) . "' AND '" . $db->escapeString($dateTo) . "'
    GROUP BY created_day, store_id
    ON CONFLICT(snapshot_date, day, store_id)
    DO UPDATE SET
      returns_qty=excluded.returns_qty,
      returns_sum=excluded.returns_sum
  ");

  // snapshot run upsert
  $stmt = $db->prepare("
    INSERT INTO snapshot_run(snapshot_date, created_at, date_from, date_to, stores_json, orders_rows, returns_rows, status, message)
    VALUES (:s,:c,:f,:t,:sj,:o,:r,'ok','')
    ON CONFLICT(snapshot_date) DO UPDATE SET
      created_at=excluded.created_at,
      date_from=excluded.date_from,
      date_to=excluded.date_to,
      stores_json=excluded.stores_json,
      orders_rows=excluded.orders_rows,
      returns_rows=excluded.returns_rows,
      status='ok',
      message=''
  ");
  $stmt->bindValue(':s', $snapshotDate, SQLITE3_TEXT);
  $stmt->bindValue(':c', $now, SQLITE3_TEXT);
  $stmt->bindValue(':f', $dateFrom, SQLITE3_TEXT);
  $stmt->bindValue(':t', $dateTo, SQLITE3_TEXT);
  $stmt->bindValue(':sj', $storesJson, SQLITE3_TEXT);
  $stmt->bindValue(':o', $ordersRows, SQLITE3_INTEGER);
  $stmt->bindValue(':r', $returnsRows, SQLITE3_INTEGER);
  $stmt->execute();

  return [
    'ok' => true,
    'snapshot_date' => $snapshotDate,
    'date_from' => $dateFrom,
    'date_to' => $dateTo,
    'orders_rows' => $ordersRows,
    'returns_rows' => $returnsRows,
    'created_at' => $now
  ];
}

$db = db_open();

$action = (string)($_GET['action'] ?? '');

if ($action === 'snapshots') {
  $rows = [];
  $q = $db->query("SELECT snapshot_date, created_at, status, orders_rows, returns_rows FROM snapshot_run ORDER BY snapshot_date DESC LIMIT 60");
  while ($r = $q->fetchArray(SQLITE3_ASSOC)) $rows[] = $r;
  respond(['ok'=>true,'snapshots'=>$rows]);
}

if ($action === 'refresh') {
  $body = read_json();
  $dateFrom = (string)($body['date_from'] ?? '');
  $dateTo   = (string)($body['date_to'] ?? '');
  $stores   = (array)($body['stores'] ?? []);
  $schemas  = (array)($body['schemas'] ?? ['FBS','FBO']);
  $snapshotDate = (string)($body['snapshot_date'] ?? date('Y-m-d'));

  if (!preg_match('~^\d{4}-\d{2}-\d{2}$~', $dateFrom) || !preg_match('~^\d{4}-\d{2}-\d{2}$~', $dateTo)) {
    respond(['ok'=>false,'error'=>'bad date_from/date_to'], 400);
  }
  if (!$stores) respond(['ok'=>false,'error'=>'stores required'], 400);

  $res = refresh_snapshot($db, $snapshotDate, $dateFrom, $dateTo, $stores, $schemas);
  respond($res);
}

function snapshot_pick(SQLite3 $db, ?string $snapshotDate): string {
  if ($snapshotDate && preg_match('~^\d{4}-\d{2}-\d{2}$~', $snapshotDate)) return $snapshotDate;
  $r = $db->querySingle("SELECT snapshot_date FROM snapshot_run ORDER BY snapshot_date DESC LIMIT 1");
  return $r ? (string)$r : date('Y-m-d');
}

if ($action === 'kpi') {
  $dateFrom = (string)($_GET['date_from'] ?? '');
  $dateTo   = (string)($_GET['date_to'] ?? '');
  $snap     = snapshot_pick($db, $_GET['snapshot_date'] ?? null);
  $storeIds = isset($_GET['store_ids']) ? explode(',', (string)$_GET['store_ids']) : [];

  if (!preg_match('~^\d{4}-\d{2}-\d{2}$~', $dateFrom) || !preg_match('~^\d{4}-\d{2}-\d{2}$~', $dateTo)) {
    respond(['ok'=>false,'error'=>'bad date_from/date_to'], 400);
  }

  $where = "snapshot_date=:s AND day BETWEEN :f AND :t";
  if ($storeIds && $storeIds[0] !== '') {
    $in = [];
    foreach ($storeIds as $i=>$id) $in[] = ":id$i";
    $where .= " AND store_id IN (" . implode(',', $in) . ")";
  }

  $sql = "SELECT
      SUM(orders_qty) as orders_qty,
      SUM(orders_sum) as orders_sum,
      SUM(returns_qty) as returns_qty,
      SUM(returns_sum) as returns_sum
    FROM daily_kpi
    WHERE $where";
  $st = $db->prepare($sql);
  $st->bindValue(':s', $snap, SQLITE3_TEXT);
  $st->bindValue(':f', $dateFrom, SQLITE3_TEXT);
  $st->bindValue(':t', $dateTo, SQLITE3_TEXT);
  if ($storeIds && $storeIds[0] !== '') {
    foreach ($storeIds as $i=>$id) $st->bindValue(":id$i", $id, SQLITE3_TEXT);
  }
  $r = $st->execute()->fetchArray(SQLITE3_ASSOC) ?: [];
  $ordersSum = (float)($r['orders_sum'] ?? 0);
  $returnsSum = (float)($r['returns_sum'] ?? 0);
  $ordersQty = (int)($r['orders_qty'] ?? 0);
  $returnsQty = (int)($r['returns_qty'] ?? 0);

  // top reasons
  $rs = $db->prepare("
    SELECT reason, SUM(qty) q, SUM(amount) a
    FROM fact_returns_line
    WHERE snapshot_date=:s AND created_day BETWEEN :f AND :t
    GROUP BY reason
    ORDER BY a DESC
    LIMIT 5
  ");
  $rs->bindValue(':s', $snap, SQLITE3_TEXT);
  $rs->bindValue(':f', $dateFrom, SQLITE3_TEXT);
  $rs->bindValue(':t', $dateTo, SQLITE3_TEXT);
  $top = [];
  $q = $rs->execute();
  while ($x = $q->fetchArray(SQLITE3_ASSOC)) {
    $top[] = [
      'reason' => (string)($x['reason'] ?? ''),
      'qty' => (int)($x['q'] ?? 0),
      'sum' => (float)($x['a'] ?? 0),
    ];
  }

  respond([
    'ok'=>true,
    'snapshot_date'=>$snap,
    'date_from'=>$dateFrom,
    'date_to'=>$dateTo,
    'orders_qty'=>$ordersQty,
    'orders_sum'=>$ordersSum,
    'returns_qty'=>$returnsQty,
    'returns_sum'=>$returnsSum,
    'return_rate_sum'=> ($ordersSum > 0 ? ($returnsSum / $ordersSum) : 0),
    'return_rate_qty'=> ($ordersQty > 0 ? ($returnsQty / $ordersQty) : 0),
    'top_reasons'=>$top
  ]);
}

if ($action === 'ts') {
  $dateFrom = (string)($_GET['date_from'] ?? '');
  $dateTo   = (string)($_GET['date_to'] ?? '');
  $snap     = snapshot_pick($db, $_GET['snapshot_date'] ?? null);
  if (!preg_match('~^\d{4}-\d{2}-\d{2}$~', $dateFrom) || !preg_match('~^\d{4}-\d{2}-\d{2}$~', $dateTo)) {
    respond(['ok'=>false,'error'=>'bad date_from/date_to'], 400);
  }

  $st = $db->prepare("
    SELECT day,
      SUM(orders_qty) oq, SUM(orders_sum) os,
      SUM(returns_qty) rq, SUM(returns_sum) rs
    FROM daily_kpi
    WHERE snapshot_date=:s AND day BETWEEN :f AND :t
    GROUP BY day
    ORDER BY day ASC
  ");
  $st->bindValue(':s', $snap, SQLITE3_TEXT);
  $st->bindValue(':f', $dateFrom, SQLITE3_TEXT);
  $st->bindValue(':t', $dateTo, SQLITE3_TEXT);
  $rows = [];
  $q = $st->execute();
  while ($r = $q->fetchArray(SQLITE3_ASSOC)) {
    $os = (float)$r['os']; $rs = (float)$r['rs'];
    $oq = (int)$r['oq']; $rq = (int)$r['rq'];
    $rows[] = [
      'day' => $r['day'],
      'orders_qty' => $oq,
      'orders_sum' => $os,
      'returns_qty' => $rq,
      'returns_sum' => $rs,
      'return_rate_sum' => ($os > 0 ? $rs/$os : 0),
      'return_rate_qty' => ($oq > 0 ? $rq/$oq : 0),
    ];
  }
  respond(['ok'=>true,'snapshot_date'=>$snap,'rows'=>$rows]);
}

if ($action === 'stores') {
  $dateFrom = (string)($_GET['date_from'] ?? '');
  $dateTo   = (string)($_GET['date_to'] ?? '');
  $snap     = snapshot_pick($db, $_GET['snapshot_date'] ?? null);
  if (!preg_match('~^\d{4}-\d{2}-\d{2}$~', $dateFrom) || !preg_match('~^\d{4}-\d{2}-\d{2}$~', $dateTo)) {
    respond(['ok'=>false,'error'=>'bad date_from/date_to'], 400);
  }

  $st = $db->prepare("
    SELECT k.store_id, COALESCE(d.name, k.store_id) as name,
      SUM(k.orders_qty) oq, SUM(k.orders_sum) os,
      SUM(k.returns_qty) rq, SUM(k.returns_sum) rs
    FROM daily_kpi k
    LEFT JOIN dim_store d ON d.store_id = k.store_id
    WHERE k.snapshot_date=:s AND k.day BETWEEN :f AND :t
    GROUP BY k.store_id
    ORDER BY rs DESC
  ");
  $st->bindValue(':s', $snap, SQLITE3_TEXT);
  $st->bindValue(':f', $dateFrom, SQLITE3_TEXT);
  $st->bindValue(':t', $dateTo, SQLITE3_TEXT);

  $rows = [];
  $q = $st->execute();
  while ($r = $q->fetchArray(SQLITE3_ASSOC)) {
    $os = (float)$r['os']; $rs = (float)$r['rs'];
    $oq = (int)$r['oq']; $rq = (int)$r['rq'];
    $rate = ($os > 0 ? $rs/$os : 0);

    // простые аномалии (порог можно потом вынести в конфиг)
    $anoms = [];
    if ($rate > 0.20 && $os > 5000) $anoms[] = 'Возвраты >20% (сумма)';
    if ($oq === 0 && $rq > 0) $anoms[] = 'Есть возвраты без заказов';
    if ($os === 0 && $rs > 0) $anoms[] = 'Есть возвраты при нулевой сумме заказов';

    $rows[] = [
      'store_id' => $r['store_id'],
      'name' => $r['name'],
      'orders_qty' => $oq,
      'orders_sum' => $os,
      'returns_qty' => $rq,
      'returns_sum' => $rs,
      'return_rate_sum' => $rate,
      'anomalies' => $anoms
    ];
  }
  respond(['ok'=>true,'snapshot_date'=>$snap,'rows'=>$rows]);
}

if ($action === 'sku') {
  $dateFrom = (string)($_GET['date_from'] ?? '');
  $dateTo   = (string)($_GET['date_to'] ?? '');
  $snap     = snapshot_pick($db, $_GET['snapshot_date'] ?? null);
  $qText    = (string)($_GET['q'] ?? '');

  if (!preg_match('~^\d{4}-\d{2}-\d{2}$~', $dateFrom) || !preg_match('~^\d{4}-\d{2}-\d{2}$~', $dateTo)) {
    respond(['ok'=>false,'error'=>'bad date_from/date_to'], 400);
  }

  $like = '%' . $qText . '%';

  $sql = "
    WITH o AS (
      SELECT sku, SUM(qty) oq, SUM(amount) os
      FROM fact_orders_line
      WHERE snapshot_date=:s AND created_day BETWEEN :f AND :t
      " . ($qText !== '' ? " AND sku LIKE :like " : "") . "
      GROUP BY sku
    ),
    r AS (
      SELECT sku, SUM(qty) rq, SUM(amount) rs
      FROM fact_returns_line
      WHERE snapshot_date=:s AND created_day BETWEEN :f AND :t
      " . ($qText !== '' ? " AND sku LIKE :like " : "") . "
      GROUP BY sku
    )
    SELECT
      COALESCE(o.sku, r.sku) as sku,
      COALESCE(o.oq,0) as orders_qty,
      COALESCE(o.os,0) as orders_sum,
      COALESCE(r.rq,0) as returns_qty,
      COALESCE(r.rs,0) as returns_sum
    FROM o
    LEFT JOIN r ON r.sku=o.sku
    UNION ALL
    SELECT
      r.sku,
      0,0,
      r.rq, r.rs
    FROM r
    LEFT JOIN o ON o.sku=r.sku
    WHERE o.sku IS NULL
    ORDER BY returns_sum DESC
    LIMIT 300
  ";

  $st = $db->prepare($sql);
  $st->bindValue(':s', $snap, SQLITE3_TEXT);
  $st->bindValue(':f', $dateFrom, SQLITE3_TEXT);
  $st->bindValue(':t', $dateTo, SQLITE3_TEXT);
  if ($qText !== '') $st->bindValue(':like', $like, SQLITE3_TEXT);

  $rows = [];
  $qq = $st->execute();
  while ($r = $qq->fetchArray(SQLITE3_ASSOC)) {
    $os = (float)$r['orders_sum']; $rs = (float)$r['returns_sum'];
    $oq = (int)$r['orders_qty']; $rq = (int)$r['returns_qty'];
    $rows[] = [
      'sku' => $r['sku'],
      'orders_qty' => $oq,
      'orders_sum' => $os,
      'returns_qty' => $rq,
      'returns_sum' => $rs,
      'return_rate_sum' => ($os > 0 ? $rs/$os : 0),
      'return_rate_qty' => ($oq > 0 ? $rq/$oq : 0),
    ];
  }
  respond(['ok'=>true,'snapshot_date'=>$snap,'rows'=>$rows]);
}

if ($action === 'orders') {
  $snap = snapshot_pick($db, $_GET['snapshot_date'] ?? null);
  $qText = trim((string)($_GET['q'] ?? ''));
  $dateFrom = (string)($_GET['date_from'] ?? '0000-00-00');
  $dateTo   = (string)($_GET['date_to'] ?? '9999-12-31');

  $sql = "
    SELECT store_id, schema, order_id, posting_number, created_day,
           SUM(qty) oq, SUM(amount) os
    FROM fact_orders_line
    WHERE snapshot_date=:s
      AND created_day BETWEEN :f AND :t
      " . ($qText !== '' ? " AND order_id LIKE :q " : "") . "
    GROUP BY store_id, schema, order_id, posting_number, created_day
    ORDER BY created_day DESC, os DESC
    LIMIT 200
  ";
  $st = $db->prepare($sql);
  $st->bindValue(':s', $snap, SQLITE3_TEXT);
  $st->bindValue(':f', $dateFrom, SQLITE3_TEXT);
  $st->bindValue(':t', $dateTo, SQLITE3_TEXT);
  if ($qText !== '') $st->bindValue(':q', '%' . $qText . '%', SQLITE3_TEXT);

  $rows = [];
  $qq = $st->execute();
  while ($r = $qq->fetchArray(SQLITE3_ASSOC)) $rows[] = $r;
  respond(['ok'=>true,'snapshot_date'=>$snap,'rows'=>$rows]);
}

if ($action === 'returns') {
  $snap = snapshot_pick($db, $_GET['snapshot_date'] ?? null);
  $qText = trim((string)($_GET['q'] ?? ''));
  $dateFrom = (string)($_GET['date_from'] ?? '0000-00-00');
  $dateTo   = (string)($_GET['date_to'] ?? '9999-12-31');

  $sql = "
    SELECT store_id, return_id, order_id, created_day, status, reason,
           SUM(qty) rq, SUM(amount) rs
    FROM fact_returns_line
    WHERE snapshot_date=:s
      AND created_day BETWEEN :f AND :t
      " . ($qText !== '' ? " AND (return_id LIKE :q OR order_id LIKE :q) " : "") . "
    GROUP BY store_id, return_id, order_id, created_day, status, reason
    ORDER BY created_day DESC, rs DESC
    LIMIT 200
  ";
  $st = $db->prepare($sql);
  $st->bindValue(':s', $snap, SQLITE3_TEXT);
  $st->bindValue(':f', $dateFrom, SQLITE3_TEXT);
  $st->bindValue(':t', $dateTo, SQLITE3_TEXT);
  if ($qText !== '') $st->bindValue(':q', '%' . $qText . '%', SQLITE3_TEXT);

  $rows = [];
  $qq = $st->execute();
  while ($r = $qq->fetchArray(SQLITE3_ASSOC)) $rows[] = $r;
  respond(['ok'=>true,'snapshot_date'=>$snap,'rows'=>$rows]);
}

respond(['ok'=>false,'error'=>'unknown action'], 404);

/**
 * Обновляем соответствие SKU -> offer_id по заказам (SKU — стабильный ключ).
 * Ограничиваемся последними 365 днями, чтобы не сканировать всё бесконечно.
 */
function sync_dim_sku_offer(SQLite3 $db, int $daysBack = 365): void {
  $db->exec("CREATE TABLE IF NOT EXISTS dim_sku_offer (
    sku TEXT PRIMARY KEY,
    offer_id TEXT NOT NULL,
    updated_at TEXT NOT NULL
  )");
  $db->exec("CREATE INDEX IF NOT EXISTS idx_dim_sku_offer_offer_id ON dim_sku_offer(offer_id)");

  $cut = (new DateTimeImmutable('now'))->modify('-'.$daysBack.' days')->format('Y-m-d');

  $sql = "INSERT INTO dim_sku_offer(sku, offer_id, updated_at)
          SELECT sku, MAX(offer_id) AS offer_id, datetime('now')
          FROM fact_orders_line
          WHERE sku IS NOT NULL AND sku <> ''
            AND offer_id IS NOT NULL AND offer_id <> ''
            AND created_day >= :cut
          GROUP BY sku
          ON CONFLICT(sku) DO UPDATE SET
            offer_id = excluded.offer_id,
            updated_at = excluded.updated_at";

  $st = $db->prepare($sql);
  $st->bindValue(':cut', $cut, SQLITE3_TEXT);
  $st->execute();
}
