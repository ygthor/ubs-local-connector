<?php
$exePath = 'C:\\xampp\\htdocs\\ubs-local-connector\\php_sync_server\\dbf_scripts\\reindex.exe';

// Optional: Set working directory where the DBF is located
chdir('C:\\UBSSTK2015\\Sample');

// Execute and capture output
$output = [];
$returnCode = 0;
exec($exePath, $output, $returnCode);

// Debug output
echo "Return code: $returnCode\n";
echo implode("\n", $output);
?>
