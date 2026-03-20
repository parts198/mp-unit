<?php
header('Content-Type: text/plain; charset=utf-8');
echo "PHP_VERSION=".PHP_VERSION."\n";
echo "SAPI=".php_sapi_name()."\n";
echo "SQLite3_class=".(class_exists('SQLite3')?'YES':'NO')."\n";
echo "PDO_sqlite=".(in_array('pdo_sqlite', get_loaded_extensions())?'YES':'NO')."\n";
echo "Loaded extensions:\n";
print_r(get_loaded_extensions());
