<?php
include(__DIR__ . '/bootstrap/app.php');

$db = new mysql();


// insertSyncLog();
$last_synced_at = lastSyncAt();
$last_synced_at = "2025-07-19 00:00:00"; // For testing purpose, set to a fixed date


$ubsTables = Converter::ubsTable();

foreach($ubsTables as $ubs_table){
    $sql = "
        SELECT * FROM `$ubs_table` 
        WHERE UPDATED_ON > '$last_synced_at'
    ";
    $ubs_data = $db->get($sql);
    $remote_data = fetchServerData($ubs_table, $last_synced_at);

    $comparedData = syncEntity($ubs_table,$ubs_data, $remote_data);

    $remote_data = $comparedData['remote_data'];
    $ubs_data = $comparedData['ubs_data'];
    
    dump($comparedData);
    // $to_insert_ubs = [];
    // $to_update_ubs = [];


    foreach($remote_data as $arr){
        upsertRemote($ubs_table,$arr,);
    }
    foreach($ubs_data as $arr){
        upsertUbs($ubs_table,$arr);
    }


    $table_trigger_reset = ['customer','orders'];
    $remote_table_name = Converter::table_map($ubs_table);
    if(in_array($remote_table_name,$table_trigger_reset)){
        $Core = Core::getInstance();
        $Core->initRemoteData();
    }
}



// sync_all_dbf_to_local();

dd('SUCCESS');