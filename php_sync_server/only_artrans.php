<?php
include(__DIR__ . '/bootstrap/app.php');

$referenceNo = $argv[1] ?? null;

$TABLE_ARR = [
    'ubs_ubsstk2015_artran' => 'orders',
    // 'ubs_ubsstk2015_ictran' => 'order_items', // cannot work for 
];

$db_local = new mysql();
$db_remote = new mysql;
$db_remote->connect_remote();

$last_synced_at = lastSyncAt(); // Commented out for full sync


foreach($TABLE_ARR  as $UBS_TABLE => $REMOTE_TABLE)
$sql = "
    SELECT * FROM `$UBS_TABLE` 
    WHERE 1
    AND " . sqlWhereUpdatedOn($last_synced_at) . "
    ORDER BY UPDATED_ON ASC
";
$ubsData = $db_local->get($sql);


$sql = "
    SELECT * FROM $REMOTE_TABLE
    WHERE updated_at >= '$last_synced_at'
";
$remoteData = $db_remote->get($sql);
$ubsData    = keyBy($ubsData, 'REFNO');
$remoteData = keyBy($remoteData, 'reference_no');

$mergeData = [];

// union of all refs
$allRefs = array_unique(
    array_merge(array_keys($ubsData), array_keys($remoteData))
);

foreach ($allRefs as $ref) {
    $ubs    = $ubsData[$ref]    ?? null;
    $remote = $remoteData[$ref] ?? null;

    // only UBS exists
    if ($ubs && !$remote) {
        $mergeData[$ref] = $ubs;
        continue;
    }

    // only remote exists
    if ($remote && !$ubs) {
        $mergeData[$ref] = $remote;
        continue;
    }

    // both exist → compare UPDATED time
    if (toTimestamp($ubs['UPDATED_ON']) >= toTimestamp($remote['updated_at'])) {
        $mergeData[$ref] = $ubs;
    } else {
        $mergeData[$ref] = $remote;
    }
}

foreach($mergeData as $d){
    $converted = convert($REMOTE_TABLE, $d, 'to_ubs');
    upsertUbs($UBS_TABLE, $converted);
}

    
echo 'DONE';