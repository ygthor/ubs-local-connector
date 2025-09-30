<?php

use XBase\DataConverter\Field\DBase7\TimestampConverter;

include(__DIR__ . '/bootstrap/app.php');
include(__DIR__ . '/bootstrap/cache.php');


// Initialize sync environment and progress display
initializeSyncEnvironment();
ProgressDisplay::start("🚀 Starting UBS Local Connector Sync Process");


$db = new mysql();
$last_synced_at = "1970-01-01 00:00:0"; // Set to null for FULL SYNC (process all records)

$ubsTables = Converter::ubsTable();
$tableMapping = Converter::table_map();

$totalTables = count($ubsTables);

ProgressDisplay::info("Found $totalTables tables to sync: " . implode(', ', $ubsTables));
ProgressDisplay::info("🕐 Syncing records updated after: $last_synced_at");

$processedTables = 0;
$syncResults = []; // Track sync results for each table


// Log successful sync
$db->query('TRUNCATE sync_logs');

foreach ($ubsTables as $ubs_table) {
    $remoteTable = $tableMapping[$ubs_table] ?? null;

    if ($remoteTable == null) {
        dd('TABLE NOT FOUND');
    }


    $db_remote = new mysql;
    $db_remote->connect_remote();
    $db_remote->query("TRUNCATE $remoteTable");


    $processedTables++;
    ProgressDisplay::info("📁 Processing table $processedTables/$totalTables: $ubs_table");

    $sql = "
        SELECT * FROM `$ubs_table` 
        WHERE UPDATED_ON IS NOT NULL
        ORDER BY UPDATED_ON ASC
    ";
    $ubs_data = $db->get($sql);

    // Validate and fix UPDATED_ON fields in UBS data
    $ubs_data = validateAndFixUpdatedOn($ubs_data);
    $comparedData = syncEntity($ubs_table, $ubs_data, []);

    $remote_data_to_upsert = $comparedData['remote_data'];

    // Use batch processing for better performance
    if (!empty($remote_data_to_upsert)) {
        ProgressDisplay::info("Upserting " . count($remote_data_to_upsert) . " remote records");
        // batchUpsertRemote($ubs_table, $remote_data_to_upsert);
    }

    // Add small delay to prevent file lock conflicts
    usleep(500000); // 0.5 second delay

    echo "🔍 Finished processing $ubs_table, moving to next table...\n";
}

// Log successful sync
$db->insert('sync_logs', [
    'synced_at' => date('Y-m-d H:i:s')
]);
