<?php
/**
 * Stores registry persisted on server and exported to stores.secrets.js
 *
 * GET    /api/stores.php                         -> {stores:[...]}
 * POST   /api/stores.php  {"store":{...}}        -> upsert by client_id
 * DELETE /api/stores.php?client_id=123           -> delete by client_id
 *
 * Data is stored in ../stores.secrets.js as:
 *   window.OZON_STORES = [ {name, client_id, api_key}, ... ];
 *
 * Notes:
 * - Intended for private/internal usage. Add auth if exposed.
 * - stores.secrets.js contains API keys. Protect it accordingly.
 */

header('Content-Type: application/json; charset=utf-8');
header('Cache-Control: no-store');

$secretsJs = __DIR__ . '/../stores.secrets.js';

function readStores($path) {
  if (!file_exists($path)) return [];
  $raw = file_get_contents($path);
  if ($raw === false) return [];
  // Extract the first [...] array literal after "window.OZON_STORES"
  $m = [];
  if (!preg_match('/window\.OZON_STORES\s*=\s*(\[[\s\S]*?\])\s*;?/m', $raw, $m)) return [];
  $arrLiteral = $m[1];

  // Convert JS object literal to JSON (best-effort):
  // - quote keys
  // - convert single quotes to double quotes
  $jsonish = preg_replace('/(\{|,)\s*([a-zA-Z0-9_]+)\s*:/', '$1"$2":', $arrLiteral);
  $jsonish = str_replace("'", '"', $jsonish);

  $stores = json_decode($jsonish, true);
  if (!is_array($stores)) return [];

  // sanitize shape
  $out = [];
  foreach ($stores as $s) {
    if (!is_array($s)) continue;
    $out[] = [
      'name' => isset($s['name']) ? (string)$s['name'] : '',
      'client_id' => isset($s['client_id']) ? (string)$s['client_id'] : '',
      'api_key' => isset($s['api_key']) ? (string)$s['api_key'] : '',
    ];
  }
  // drop invalid (no client_id / api_key)
  $out = array_values(array_filter($out, function($s){
    return $s['client_id'] !== '' && $s['api_key'] !== '';
  }));
  return $out;
}

function writeStores($path, $stores) {
  $payload = "window.OZON_STORES = " . json_encode($stores, JSON_UNESCAPED_UNICODE|JSON_UNESCAPED_SLASHES) . ";\n";
  return file_put_contents($path, $payload, LOCK_EX) !== false;
}

$method = $_SERVER['REQUEST_METHOD'];
if ($method === 'GET') {
  $stores = readStores($secretsJs);
  echo json_encode(['stores' => $stores], JSON_UNESCAPED_UNICODE);
  exit;
}

if ($method === 'POST') {
  $raw = file_get_contents('php://input');
  $body = json_decode($raw, true);
  if (!is_array($body) || !isset($body['store']) || !is_array($body['store'])) {
    http_response_code(400);
    echo json_encode(['error' => 'Expected JSON: {"store":{name,client_id,api_key}}'], JSON_UNESCAPED_UNICODE);
    exit;
  }
  $s = $body['store'];
  $name = isset($s['name']) ? trim((string)$s['name']) : '';
  $clientId = isset($s['client_id']) ? trim((string)$s['client_id']) : '';
  $apiKey = isset($s['api_key']) ? trim((string)$s['api_key']) : '';

  if ($clientId === '' || $apiKey === '') {
    http_response_code(400);
    echo json_encode(['error' => 'client_id and api_key are required'], JSON_UNESCAPED_UNICODE);
    exit;
  }
  if ($name === '') $name = $clientId;

  $stores = readStores($secretsJs);
  $found = false;
  foreach ($stores as &$st) {
    if ((string)$st['client_id'] === $clientId) {
      $st['name'] = $name;
      $st['api_key'] = $apiKey;
      $found = true;
      break;
    }
  }
  unset($st);
  if (!$found) $stores[] = ['name'=>$name,'client_id'=>$clientId,'api_key'=>$apiKey];

  // stable sort by name
  usort($stores, function($a,$b){ return strcmp($a['name'],$b['name']); });

  if (!writeStores($secretsJs, $stores)) {
    http_response_code(500);
    echo json_encode(['error' => 'Failed to write stores.secrets.js'], JSON_UNESCAPED_UNICODE);
    exit;
  }
  echo json_encode(['ok'=>true,'stores'=>$stores], JSON_UNESCAPED_UNICODE);
  exit;
}

if ($method === 'DELETE') {
  $clientId = isset($_GET['client_id']) ? trim((string)$_GET['client_id']) : '';
  if ($clientId === '') {
    http_response_code(400);
    echo json_encode(['error' => 'client_id required'], JSON_UNESCAPED_UNICODE);
    exit;
  }
  $stores = readStores($secretsJs);
  $stores = array_values(array_filter($stores, function($s) use ($clientId){
    return (string)$s['client_id'] !== $clientId;
  }));
  if (!writeStores($secretsJs, $stores)) {
    http_response_code(500);
    echo json_encode(['error' => 'Failed to write stores.secrets.js'], JSON_UNESCAPED_UNICODE);
    exit;
  }
  echo json_encode(['ok'=>true,'stores'=>$stores], JSON_UNESCAPED_UNICODE);
  exit;
}

http_response_code(405);
echo json_encode(['error'=>'Method not allowed'], JSON_UNESCAPED_UNICODE);
