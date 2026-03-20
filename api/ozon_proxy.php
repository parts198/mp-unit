<?php
declare(strict_types=1);

header('Content-Type: application/json; charset=utf-8');
header('Access-Control-Allow-Origin: *');
header('Access-Control-Allow-Headers: Content-Type, X-Requested-With');
header('Access-Control-Allow-Methods: POST, OPTIONS');

if ($_SERVER['REQUEST_METHOD'] === 'OPTIONS') { http_response_code(204); exit; }
if ($_SERVER['REQUEST_METHOD'] !== 'POST') {
  http_response_code(405);
  echo json_encode(['error' => 'method_not_allowed'], JSON_UNESCAPED_UNICODE);
  exit;
}

$raw = file_get_contents('php://input');
$req = json_decode($raw, true);
if (!is_array($req)) {
  http_response_code(400);
  echo json_encode(['error' => 'bad_json'], JSON_UNESCAPED_UNICODE);
  exit;
}

$url   = (string)($req['url'] ?? '');
// === NO_CACHE_VOLATILE_ENDPOINTS_V3 ===
$__NO_CACHE = false;
// manual bypass: /api/ozon_proxy.php?nocache=1
if (!empty($_GET['nocache']) || !empty($_SERVER['HTTP_X_OZON_NOCACHE'])) $__NO_CACHE = true;
// disable cache for volatile endpoints
if (isset($url) && preg_match('~/(v4/product/info/stocks|v1/analytics/stocks|v2/products/stocks|v1/product/import/prices)~', $url)) $__NO_CACHE = true;


$store = $req['store'] ?? null;
$body  = $req['body'] ?? new stdClass();
$cache = $req['cache'] ?? [];

if ($url === '') {
  http_response_code(400);
  echo json_encode(['error' => 'missing_url'], JSON_UNESCAPED_UNICODE);
  exit;
}

if (!is_array($store)) {
  http_response_code(400);
  echo json_encode(['error' => 'missing_store'], JSON_UNESCAPED_UNICODE);
  exit;
}

$clientId = trim((string)($store['client_id'] ?? $store['clientId'] ?? $store['Client-Id'] ?? ''));
$apiKey   = trim((string)($store['api_key'] ?? $store['apiKey'] ?? $store['Api-Key'] ?? ''));

if ($clientId === '' || $apiKey === '') {
  http_response_code(400);
  echo json_encode(['error' => 'missing_client_or_key'], JSON_UNESCAPED_UNICODE);
  exit;
}

$apiBase = 'https://api-seller.ozon.ru';
$apiUrl = (preg_match('~^https?://~', $url)) ? $url : ($apiBase . (str_starts_with($url, '/') ? $url : ('/' . $url)));

$cacheDir = __DIR__ . '/../_server_cache/ozon/' . preg_replace('~[^0-9a-zA-Z_-]~', '_', $clientId);
if (!is_dir($cacheDir)) @mkdir($cacheDir, 0775, true);

$bodyJson = json_encode($body, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
if ($bodyJson === false) $bodyJson = '{}';

$cacheMode = (string)($cache['mode'] ?? 'use');   // use | bypass
$force     = (bool)($cache['force'] ?? false);
$ttlSec    = (int)($cache['ttl_sec'] ?? 0);       // 0 = не протухает

$key = sha1($apiUrl . '|' . $bodyJson);
$cacheFile = $cacheDir . '/' . $key . '.json';
$metaFile  = $cacheDir . '/' . $key . '.meta.json';

if ($cacheMode === 'use' && !$force && is_file($cacheFile)) {
  if ($ttlSec <= 0) {
    readfile($cacheFile);
    exit;
  }
  $mtime = filemtime($cacheFile);
  if ($mtime !== false && (time() - $mtime) <= $ttlSec) {
    readfile($cacheFile);
    exit;
  }
}

$ch = curl_init($apiUrl);
curl_setopt_array($ch, [
  CURLOPT_RETURNTRANSFER => true,
  CURLOPT_POST           => true,
  CURLOPT_HTTPHEADER     => [
    'Client-Id: ' . $clientId,
    'Api-Key: '   . $apiKey,
    'Content-Type: application/json',
  ],
  CURLOPT_POSTFIELDS     => $bodyJson,
  CURLOPT_TIMEOUT        => 90,
]);

$res = curl_exec($ch);
$err = curl_error($ch);
$code = (int)curl_getinfo($ch, CURLINFO_HTTP_CODE);
curl_close($ch);

if ($res === false) {
  http_response_code(502);
  echo json_encode(['error' => 'curl_failed', 'message' => $err], JSON_UNESCAPED_UNICODE);
  exit;
}

http_response_code($code > 0 ? $code : 200);

// сохраняем на диск ВСЁ, что получили
if(!$__NO_CACHE){
@file_put_contents($cacheFile, $res);
}
@file_put_contents($metaFile, json_encode([
  'saved_at' => gmdate('c'),
  'api_url'  => $apiUrl,
  'client_id'=> $clientId,
], JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES));

echo $res;
