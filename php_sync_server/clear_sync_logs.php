<?php
require_once 'bootstrap/app.php';

$db = new mysql();
$db->query('DELETE FROM sync_logs');
echo "Sync logs cleared! Next sync will be a full sync.\n";
?>
