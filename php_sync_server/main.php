<?php
include(__DIR__ . '/bootstrap/app.php');

$db = new mysql();


// insertSyncLog();
$last_synced_at = lastSyncAt();
$last_synced_at = "20025-07-06 00:00:00"; // For testing purpose, set to a fixed date


$ubsTables = Converter::ubsTable();

foreach($ubsTables as $ubs_table){
    $sql = "
        SELECT * FROM `$ubs_table` 
        WHERE UPDATED_ON > '$last_synced_at'
    ";
    $ubs_data = $db->get($sql);
    $remote_data = fetchServerData($ubs_table, $last_synced_at);

    $comparedData = syncEntity($ubs_table,$ubs_data, $remote_data);

    $to_insert_ubs = $comparedData['to_insert_ubs'];
    $to_insert_remote = $comparedData['to_insert_remote'];
    $to_update_ubs = $comparedData['to_update_ubs'];
    $to_update_remote = $comparedData['to_update_remote'];

    foreach($to_insert_remote as $arr){
        upsertRemote($ubs_table,$arr,);
    }
    foreach($to_update_remote as $arr){
        upsertRemote($ubs_table,$arr,);
    }
    foreach($to_insert_ubs as $arr){
        upsertUbs($ubs_table,$arr);
    }
    foreach($to_update_ubs as $arr){
        upsertUbs($ubs_table,$arr);
    }

}



sync_all_dbf_to_local();

dd('SUCCESS');