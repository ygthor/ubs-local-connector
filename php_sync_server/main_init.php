<?php

use XBase\DataConverter\Field\DBase7\TimestampConverter;

include(__DIR__ . '/bootstrap/app.php');
include(__DIR__ . '/bootstrap/cache.php');


// Initialize sync environment and progress display
initializeSyncEnvironment();
ProgressDisplay::start("🚀 Starting UBS Local Connector Initial Sync Process");

// Check if Python sync is running
if (isSyncRunning('python')) {
    ProgressDisplay::error("❌ Python sync is currently running. Please wait for it to complete.");
    exit(1);
}

// Acquire PHP sync lock
if (!acquireSyncLock('php')) {
    ProgressDisplay::error("❌ PHP sync is already running or lock file exists. Please check and remove lock file if needed.");
    exit(1);
}

// Register shutdown function to release lock
register_shutdown_function(function() {
    releaseSyncLock('php');
});

try {
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
        
        // Truncate remote table (including icgroup)
        $db_remote->query("TRUNCATE $remoteTable");

        $processedTables++;
        ProgressDisplay::info("📁 Processing table $processedTables/$totalTables: $ubs_table");

        // For icgroup, sync ALL records including NULL CREATED_ON/UPDATED_ON
        $isIcgroup = ($ubs_table === 'ubs_ubsstk2015_icgroup');
        
        // Count total rows to process
        if ($isIcgroup) {
            $countSql = "SELECT COUNT(*) as total FROM `$ubs_table`";
            ProgressDisplay::info("🔄 FORCE SYNC: Syncing ALL icgroup records (including NULL CREATED_ON/UPDATED_ON)");
        } else {
            $countSql = "SELECT COUNT(*) as total FROM `$ubs_table` WHERE UPDATED_ON IS NOT NULL";
        }
        $totalRows = $db->first($countSql)['total'] ?? 0;

        ProgressDisplay::info("Total rows to process: $totalRows");

        $chunkSize = 1000;
        $offset = 0;

        while ($offset < $totalRows) {
            ProgressDisplay::info("📦 Fetching chunk " . (($offset / $chunkSize) + 1) . " (offset: $offset)");

            // Fetch a chunk of data
            if ($isIcgroup) {
                // For icgroup, fetch ALL records including NULL values
                $sql = "
                    SELECT * FROM `$ubs_table`
                    ORDER BY COALESCE(UPDATED_ON, '1970-01-01') ASC
                    LIMIT $chunkSize OFFSET $offset
                ";
            } else {
                $sql = "
                    SELECT * FROM `$ubs_table`
                    WHERE UPDATED_ON IS NOT NULL
                    ORDER BY UPDATED_ON ASC
                    LIMIT $chunkSize OFFSET $offset
                ";
            }
            $ubs_data = $db->get($sql);

            if (empty($ubs_data)) {
                break; // No more data
            }

            // ✅ Validate timestamps
            // For icgroup, preserve NULL values
            $ubs_data = validateAndFixUpdatedOn($ubs_data, $ubs_table);

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
        
        // Link customers to users after customers sync
        if ($remoteTable === 'customers') {
            try {
                linkCustomersToUsers($db_remote);
            } catch (Exception $e) {
                ProgressDisplay::warning("⚠️  Could not link customers to users: " . $e->getMessage());
            }
        }
        
        // ✅ NOTE: icgroup is now synced directly from icgroup.dbf (enabled in converter.class.php)
        // The following generation logic is kept as fallback but commented out to avoid conflicts
        // Uncomment if you need to generate icgroup from icitem GROUP values as a fallback
        /*
        if ($ubs_table === 'ubs_ubsstk2015_icitem') {
            ProgressDisplay::info("🔄 Syncing icgroup from icitem GROUP values...");
            // Ensure icgroup is truncated (already done above, but ensure it's done)
            $db_remote->query("TRUNCATE icgroup");
            syncIcgroupFromIcitem($db, $db_remote);
        }
        */
        
        // Close remote connection for this table
        $db_remote->close();
    }

    // ============================
    // Clean up duplicate orders and order_items after sync completes
    // ============================
    try {
        ProgressDisplay::info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        validateAndCleanDuplicateOrders();
    } catch (Exception $e) {
        ProgressDisplay::warning("⚠️  Could not validate and clean duplicate orders: " . $e->getMessage());
        // Don't fail the entire sync if duplicate cleanup fails
    }

    // Log successful sync
    $db->insert('sync_logs', [
        'synced_at' => date('Y-m-d H:i:s')
    ]);

} catch (Exception $e) {
    ProgressDisplay::error("❌ Initial sync failed: " . $e->getMessage());
    releaseSyncLock('php');
    exit(1);
}

// Release lock
releaseSyncLock('php');
