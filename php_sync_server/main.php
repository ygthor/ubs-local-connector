<?php
include(__DIR__ . '/bootstrap/app.php');

$db = new mysql();


// insertSyncLog();
$last_synced_at = lastSyncAt();

$last_synced_at = "2025-06-01 00:00:00"; // For testing purpose, set to a fixed date


$sql = "
    SELECT * FROM `ubs_ubsacc2015_arcust` 
    WHERE UPDATED_ON > '$last_synced_at'
";
$ubs_data = $db->get($sql);


$remote_data = fetchServerData('ubs_ubsacc2015_arcust', $last_synced_at);

$d = syncEntity('customer',$ubs_data, $remote_data);
dd($d);





dd($data);
