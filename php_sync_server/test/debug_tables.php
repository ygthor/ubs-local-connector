<?php
require_once 'bootstrap/app.php';

$db = new mysql();

// Check local UBS table
$local_count = $db->first('SELECT COUNT(*) as total FROM ubs_ubsstk2015_ictran')['total'];
echo "Local UBS table ubs_ubsstk2015_ictran has: $local_count records\n";

// Check remote table
$db->connect_remote();
$remote_count = $db->first('SELECT COUNT(*) as total FROM artrans_items')['total'];
echo "Remote table artrans_items has: $remote_count records\n";

// Reconnect to local database to check local table
$db = new mysql(); // This connects to local database

// Check if there are any UPDATED_ON values in local table
$updated_count = $db->first('SELECT COUNT(*) as total FROM ubs_ubsstk2015_ictran WHERE UPDATED_ON IS NOT NULL')['total'];
echo "Local records with UPDATED_ON: $updated_count\n";

// Check actual UPDATED_ON values
$date_info = $db->first('SELECT MIN(UPDATED_ON) as min_date, MAX(UPDATED_ON) as max_date FROM ubs_ubsstk2015_ictran WHERE UPDATED_ON IS NOT NULL');
echo "Min UPDATED_ON: " . $date_info['min_date'] . "\n";
echo "Max UPDATED_ON: " . $date_info['max_date'] . "\n";

// Check last sync time
$last_sync = lastSyncAt();
echo "Last sync time: $last_sync\n";
?>
