<?php
declare(strict_types=1);

header('Content-Type: application/json; charset=utf-8');

$ROOT = __DIR__;
$DATA = $ROOT . '/_server_data';
$CACHE_DIR = $DATA . '/cache';
$RAW_DIR   = $DATA . '/raw';
$LOG_DIR   = $DATA . '/logs';

@mkdir($CACHE_DIR, 0700, true);
@mkdir($RAW_DIR,   0700, true);
@mkdir($LOG_DIR,   0700, true);

function jexit(int $code, array $payload): void {
  http_response_code($code);
  echo json_encode($payload, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
  exit;
}

function read_json_body(): array {
  $raw = file_get_contents('php://input');
  if ($raw === false || trim($raw) === '') return [];
  $j = json_decode($raw, true);
  return is_array($j) ? $j : [];
}

function safe_write(string $path, string $content): void {
  $dir = dirname($path);
  @mkdir($dir, 0700, true);
  file_put_contents($path, $content, LOCK_EX);
}

function now_iso(): string {
  return gmdate('c');
}

// --- ПАРСИНГ stores.secrets.js ---
// Ожидаем, что в файле есть массив магазинов с полями: name, client_id, api_key (и опционально vat).
function load_stores_from_secrets_js(string $root): array {
  $candidates = [
    $root . '/stores.secrets.js',
    $root . '/stores_secrets.js',
    $root . '/stores.secret.js',
  ];

  $txt = null;
  foreach ($candidates as $f) {
    if (is_file($f)) { $txt = file_get_contents($f); break; }
  }
  if ($txt === null || $txt === false) return [];

  // 1) пытаемся найти первый массив [...] (обычно там список магазинов)
  if (!preg_match('~\[(?:.|\s)*?\]~u', $txt, $m)) return [];
  $arr = $m[0];

  // 2) приводим JS-литерал к JSON (упрощённо, но для типичных stores подходит)
  //   - ключи без кавычек -> "key":
  $arr = preg_replace('~([{\s,])([a-zA-Z_][a-zA-Z0-9_]*)\s*:~u', '$1"$2":', $arr);
  //   - одинарные кавычки -> двойные
  $arr = preg_replace("~'~u", '"', $arr);
  //   - убираем висячие запятые
  $arr = preg_replace('~,\s*([}\]])~u', '$1', $arr);

  $j = json_decode($arr, true);
  return is_array($j) ? $j : [];
}

function find_store(array $stores, string $clientId): ?array {
  foreach ($stores as $s) {
    if (!is_array($s)) continue;
    $cid = trim((string)($s['client_id'] ?? $s['clientId'] ?? $s['Client-Id'] ?? ''));
    if ($cid !== '' && $cid === $clientId) {
      $api = trim((string)($s['api_key'] ?? $s['apiKey'] ?? $s['Api-Key'] ?? ''));
      return [
        'client_id' => $cid,
        'api_key'   => $api,
        'name'      => (string)($s['name'] ?? ''),
        'vat'       => (string)($s['vat'] ?? $s['nds'] ?? ''),
      ];
    }
  }
  return null;
}

function is_write_endpoint(string $path): bool {
  // Импорт/обновления — не кешируем (но логируем)
  return (bool)preg_match('~/(import|update|set|create|delete|push)/~i', $path);
}

function endpoint_max_days(string $path): int {
  // Базово режем по 30 дней (безопасно для многих методов).
  // Для конкретных эндпоинтов можно задать отдельные лимиты.
  $map = [
    '/v3/finance/transaction/list' => 30, // в документации: максимум 1 месяц
  ];
  foreach ($map as $k => $v) {
    if (stripos($path, $k) !== false) return $v;
  }
  return 30;
}

function get_date_range(array $body): ?array {
  // Поддержка типовых вариантов:
  // body.filter.date.from/to  (как transaction/list)
  // body.filter.since/to      (часто в posting/list)
  $filter = $body['filter'] ?? null;
  if (!is_array($filter)) return null;

  if (isset($filter['date']) && is_array($filter['date'])) {
    $from = (string)($filter['date']['from'] ?? '');
    $to   = (string)($filter['date']['to']   ?? '');
    if ($from !== '' && $to !== '') return ['kind' => 'date', 'from' => $from, 'to' => $to];
  }
  $from = (string)($filter['since'] ?? '');
  $to   = (string)($filter['to']    ?? '');
  if ($from !== '' && $to !== '') return ['kind' => 'since', 'from' => $from, 'to' => $to];

  return null;
}

function set_date_range(array &$body, string $kind, string $from, string $to): void {
  if (!isset($body['filter']) || !is_array($body['filter'])) $body['filter'] = [];
  if ($kind === 'date') {
    if (!isset($body['filter']['date']) || !is_array($body['filter']['date'])) $body['filter']['date'] = [];
    $body['filter']['date']['from'] = $from;
    $body['filter']['date']['to']   = $to;
  } else {
    $body['filter']['since'] = $from;
    $body['filter']['to']    = $to;
  }
}

function split_range_iso(string $fromIso, string $toIso, int $maxDays): array {
  $a = strtotime($fromIso);
  $b = strtotime($toIso);
  if ($a === false || $b === false) return [['from' => $fromIso, 'to' => $toIso]];
  if ($b <= $a) return [['from' => $fromIso, 'to' => $toIso]];

  $out = [];
  $cur = $a;
  $step = $maxDays * 86400;
  while ($cur < $b) {
    $nxt = min($b, $cur + $step);
    $out[] = ['from' => gmdate('c', $cur), 'to' => gmdate('c', $nxt)];
    $cur = $nxt;
  }
  return $out;
}

function http_post_json(string $url, array $headers, array $body): array {
  $ch = curl_init($url);
  if ($ch === false) throw new Exception('curl_init failed');

  $h = [];
  foreach ($headers as $k => $v) $h[] = $k . ': ' . $v;

  curl_setopt_array($ch, [
    CURLOPT_POST => true,
    CURLOPT_RETURNTRANSFER => true,
    CURLOPT_HTTPHEADER => $h,
    CURLOPT_POSTFIELDS => json_encode($body, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES),
    CURLOPT_TIMEOUT => 120,
  ]);

  $resp = curl_exec($ch);
  $code = (int)curl_getinfo($ch, CURLINFO_RESPONSE_CODE);
  $err  = curl_error($ch);
  curl_close($ch);

  if ($resp === false) throw new Exception($err ?: 'curl_exec failed');
  $j = json_decode($resp, true);
  if (!is_array($j)) $j = ['_raw' => $resp];

  if ($code < 200 || $code >= 300) {
    $msg = (string)($j['message'] ?? $j['error'] ?? $j['error_message'] ?? ('HTTP ' . $code));
    throw new Exception($msg);
  }

  return $j;
}

function merge_result(array $acc, array $part): array {
  // 1) transaction/list: result.operations
  if (isset($part['result']['operations']) && is_array($part['result']['operations'])) {
    $acc['result'] = $acc['result'] ?? [];
    $acc['result']['operations'] = $acc['result']['operations'] ?? [];
    $acc['result']['operations'] = array_merge($acc['result']['operations'], $part['result']['operations']);
    $acc['result']['page_count'] = 0;
    $acc['result']['row_count']  = count($acc['result']['operations']);
    return $acc;
  }

  // 2) posting/list часто: result.postings / result.has_next
  if (isset($part['result']['postings']) && is_array($part['result']['postings'])) {
    $acc['result'] = $acc['result'] ?? [];
    $acc['result']['postings'] = $acc['result']['postings'] ?? [];
    $acc['result']['postings'] = array_merge($acc['result']['postings'], $part['result']['postings']);
    $acc['result']['has_next'] = false;
    return $acc;
  }

  // 3) универсально: result.items
  if (isset($part['result']['items']) && is_array($part['result']['items'])) {
    $acc['result'] = $acc['result'] ?? [];
    $acc['result']['items'] = $acc['result']['items'] ?? [];
    $acc['result']['items'] = array_merge($acc['result']['items'], $part['result']['items']);
    return $acc;
  }

  // Fallback: если не поняли формат — возвращаем последний
  return $part;
}

function paginate_if_needed(string $apiBase, string $path, array $store, array $body, array $first): array {
  // Поддержка page/page_size + result.page_count (как transaction/list)
  if (!isset($body['page'])) return $first;
  $pc = (int)($first['result']['page_count'] ?? 0);
  if ($pc <= 1) return $first;

  $acc = $first;
  $page = (int)$body['page'];
  for ($p = $page + 1; $p <= $pc; $p++) {
    $b = $body;
    $b['page'] = $p;
    $part = http_post_json($apiBase . $path, [
      'Client-Id' => $store['client_id'],
      'Api-Key'   => $store['api_key'],
      'Content-Type' => 'application/json',
    ], $b);
    $acc = merge_result($acc, $part);
  }
  return $acc;
}

// ---- MAIN ----

$req = read_json_body();
$clientId = trim((string)($req['client_id'] ?? ''));
$urlOrPath = trim((string)($req['path'] ?? $req['url'] ?? ''));
$method = strtoupper(trim((string)($req['method'] ?? 'POST')));
$body = $req['body'] ?? [];
$force = !empty($req['force']);

if ($clientId === '') jexit(400, ['error' => 'client_id_required']);
if ($urlOrPath === '') jexit(400, ['error' => 'path_required']);
if ($method !== 'POST') jexit(400, ['error' => 'only_POST_supported']);

$path = $urlOrPath;
if (stripos($path, 'http') === 0) {
  $u = parse_url($path);
  $path = ($u['path'] ?? '') . (isset($u['query']) ? ('?' . $u['query']) : '');
}
if ($path === '' || $path[0] !== '/') jexit(400, ['error' => 'bad_path']);

$stores = load_stores_from_secrets_js($ROOT);
$store = find_store($stores, $clientId);
if (!$store || $store['api_key'] === '') jexit(400, ['error' => 'store_not_found_or_no_api_key', 'client_id' => $clientId]);

$apiBase = 'https://api-seller.ozon.ru';

$isWrite = is_write_endpoint($path);

$cacheKey = sha1($clientId . '|' . $path . '|' . json_encode($body, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES));
$cacheFile = $CACHE_DIR . '/' . $clientId . '/' . $cacheKey . '.json';

if (!$force && !$isWrite && is_file($cacheFile)) {
  // отдаём сохранённое
  echo file_get_contents($cacheFile);
  exit;
}

// логируем запрос
safe_write($LOG_DIR . '/' . gmdate('Ymd') . '.log', now_iso() . " client_id=$clientId path=$path cacheKey=$cacheKey\n" . file_get_contents($LOG_DIR . '/' . gmdate('Ymd') . '.log'));

try {
  $range = is_array($body) ? get_date_range($body) : null;

  // Если есть период — режем на куски и склеиваем
  if ($range) {
    $maxDays = endpoint_max_days($path);
    $segs = split_range_iso($range['from'], $range['to'], $maxDays);

    $acc = [];
    foreach ($segs as $seg) {
      $b = $body;
      set_date_range($b, $range['kind'], $seg['from'], $seg['to']);

      // Всегда начинаем с 1-й страницы, если есть page
      if (isset($b['page'])) $b['page'] = 1;

      $first = http_post_json($apiBase . $path, [
        'Client-Id' => $store['client_id'],
        'Api-Key'   => $store['api_key'],
        'Content-Type' => 'application/json',
      ], $b);

      $full = paginate_if_needed($apiBase, $path, $store, $b, $first);

      // сохраняем raw по сегменту
      $rawName = $RAW_DIR . '/' . $clientId . '/' . preg_replace('~[^a-zA-Z0-9_\-]+~', '_', trim($path, '/')) . '/' . sha1(json_encode($b)) . '.json';
      safe_write($rawName, json_encode($full, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES));

      $acc = ($acc === []) ? $full : merge_result($acc, $full);
    }

    $out = $acc;
  } else {
    // Без периода — обычный запрос, но с пагинацией если она есть
    $first = http_post_json($apiBase . $path, [
      'Client-Id' => $store['client_id'],
      'Api-Key'   => $store['api_key'],
      'Content-Type' => 'application/json',
    ], is_array($body) ? $body : []);

    $out = paginate_if_needed($apiBase, $path, $store, is_array($body) ? $body : [], $first);

    $rawName = $RAW_DIR . '/' . $clientId . '/' . preg_replace('~[^a-zA-Z0-9_\-]+~', '_', trim($path, '/')) . '/' . $cacheKey . '.json';
    safe_write($rawName, json_encode($out, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES));
  }

  // Кешируем только чтение
  if (!$isWrite) {
    safe_write($cacheFile, json_encode($out, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES));
  }

  echo json_encode($out, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
  exit;

} catch (Throwable $e) {
  jexit(500, ['error' => 'proxy_failed', 'message' => $e->getMessage()]);
}
