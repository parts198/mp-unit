<?php
declare(strict_types=1);

header('Content-Type: application/json; charset=utf-8');

if ($_SERVER['REQUEST_METHOD'] !== 'POST') {
  http_response_code(405);
  echo json_encode(['ok'=>false,'error'=>'POST only'], JSON_UNESCAPED_UNICODE);
  exit;
}

$raw = file_get_contents('php://input');
if ($raw === false || $raw === '') {
  http_response_code(400);
  echo json_encode(['ok'=>false,'error'=>'Empty body'], JSON_UNESCAPED_UNICODE);
  exit;
}

$j = json_decode($raw, true);
if (!is_array($j) || !isset($j['stores']) || !is_array($j['stores'])) {
  http_response_code(400);
  echo json_encode(['ok'=>false,'error'=>'Expected JSON: {stores:[...]}'], JSON_UNESCAPED_UNICODE);
  exit;
}

function norm_store($s) {
  if (!is_array($s)) return null;
  $name = trim((string)($s['name'] ?? $s['title'] ?? ''));
  $client_id = trim((string)($s['client_id'] ?? $s['clientId'] ?? $s['Client-Id'] ?? ''));
  $api_key = trim((string)($s['api_key'] ?? $s['apiKey'] ?? $s['Api-Key'] ?? ''));
  if ($name === '' || $client_id === '' || $api_key === '') return null;

  // лёгкая санитаризация
  if (mb_strlen($name) > 120) $name = mb_substr($name, 0, 120);
  if (mb_strlen($client_id) > 40) $client_id = mb_substr($client_id, 0, 40);
  if (mb_strlen($api_key) > 160) $api_key = mb_substr($api_key, 0, 160);

  return ['name'=>$name, 'client_id'=>$client_id, 'api_key'=>$api_key];
}

$out = [];
$seen = [];
foreach ($j['stores'] as $s) {
  $st = norm_store($s);
  if (!$st) continue;
  $k = $st['name'].'|'.$st['client_id'];
  if (isset($seen[$k])) continue;
  $seen[$k] = true;
  $out[] = $st;
}

$js = "window.OZON_STORES = " . json_encode($out, JSON_UNESCAPED_UNICODE|JSON_PRETTY_PRINT) . ";\n";

$path = __DIR__ . "/stores.secrets.js";
$tmp  = $path . ".tmp." . getmypid();

$ok = file_put_contents($tmp, $js, LOCK_EX);
if ($ok === false) {
  http_response_code(500);
  echo json_encode(['ok'=>false,'error'=>'Cannot write temp file'], JSON_UNESCAPED_UNICODE);
  exit;
}
@chmod($tmp, 0640);

if (!@rename($tmp, $path)) {
  @unlink($tmp);
  http_response_code(500);
  echo json_encode(['ok'=>false,'error'=>'Cannot replace stores.secrets.js'], JSON_UNESCAPED_UNICODE);
  exit;
}

echo json_encode(['ok'=>true,'count'=>count($out)], JSON_UNESCAPED_UNICODE);
