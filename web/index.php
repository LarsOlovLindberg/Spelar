<?php
// Fallback entrypoint if the server prefers index.php over index.html.
// Serves the static site without requiring .htaccess support.

$indexHtml = __DIR__ . DIRECTORY_SEPARATOR . 'index.html';

if (is_file($indexHtml)) {
  header('Content-Type: text/html; charset=utf-8');
  readfile($indexHtml);
  exit;
}

http_response_code(404);
header('Content-Type: text/plain; charset=utf-8');
echo "Missing index.html";
