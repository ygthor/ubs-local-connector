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
        dd("TABLE NOT FOUND: $ubs_table");
    }

    $db_remote = new mysql;
    $db_remote->connect_remote();
    $db_remote->query("TRUNCATE $remoteTable");

    $processedTables++;
    ProgressDisplay::info("📁 Processing table $processedTables/$totalTables: $ubs_table");

    // Count total rows to process
    $countSql = "SELECT COUNT(*) as total FROM `$ubs_table` WHERE UPDATED_ON IS NOT NULL";
    $totalRows = $db->first($countSql)['total'] ?? 0;

    ProgressDisplay::info("Total rows to process: $totalRows");

    $chunkSize = 1000;
    $offset = 0;

    while ($offset < $totalRows) {
        ProgressDisplay::info("📦 Fetching chunk " . (($offset / $chunkSize) + 1) . " (offset: $offset)");

        // Fetch a chunk of data
        $sql = "
            SELECT * FROM `$ubs_table`
            WHERE UPDATED_ON IS NOT NULL
            ORDER BY UPDATED_ON ASC
            LIMIT $chunkSize OFFSET $offset
        ";
        $ubs_data = $db->get($sql);

        if (empty($ubs_data)) {
            break; // No more data
        }

        // ✅ Validate timestamps
        $ubs_data = validateAndFixUpdatedOn($ubs_data);

        // ✅ Compare and prepare for sync
        $comparedData = syncEntity($ubs_table, $ubs_data, []);
        $remote_data_to_upsert = $comparedData['remote_data'];

        // ✅ Batch upsert to remote
        if (!empty($remote_data_to_upsert)) {
            ProgressDisplay::info("⬆️ Upserting " . count($remote_data_to_upsert) . " records...");
            batchUpsertRemote($ubs_table, $remote_data_to_upsert);
        }

        // ✅ Free memory and move to next chunk
        unset($ubs_data, $comparedData, $remote_data_to_upsert);
        gc_collect_cycles();

        $offset += $chunkSize;

        // Small delay to avoid locking issues
        usleep(300000); // 0.3s
    }

    echo "✅ Finished processing $ubs_table\n";
}


// Log successful sync
$db->insert('sync_logs', [
    'synced_at' => date('Y-m-d H:i:s')
]);
