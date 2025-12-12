<?php

use XBase\DataConverter\Field\DBase7\TimestampConverter;

include(__DIR__ . '/bootstrap/app.php');
include(__DIR__ . '/bootstrap/cache.php');


// Initialize sync environment and progress display
initializeSyncEnvironment();
ProgressDisplay::start("🚀 Starting UBS Local Connector Initial Sync Process");

// Remove any stale PHP locks before starting (from remove_lock.php functionality)
$lockDir = __DIR__ . '/locks';
if (is_dir($lockDir)) {
    $lockFile = $lockDir . '/php_sync.lock';
    $pidFile = $lockDir . '/php_sync.pid';
    
    // Check if lock exists and if process is still running
    if (file_exists($lockFile)) {
        $pid = file_exists($pidFile) ? file_get_contents($pidFile) : null;
        
        // If no PID or process is not running, remove stale lock
        if (!$pid || !isProcessRunning($pid)) {
            @unlink($lockFile);
            @unlink($pidFile);
            ProgressDisplay::info("🔓 Removed stale PHP sync lock");
        }
    }
}

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

        $chunkSize = 5000; // Increased from 1000 to 5000 for better performance
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

    // ============================
    // Clean up old DO orders (keep only latest per agent)
    // ============================
    try {
        ProgressDisplay::info("🧹 Cleaning up old DO orders (keeping latest per agent)...");
        
        $db_remote = new mysql();
        $db_remote->connect_remote();
        
        // Use current date for cleanup (escape for SQL safety)
        $cleanupDate = $db_remote->escape(date('Y-m-d'));
        
        // Start transaction
        $db_remote->query("START TRANSACTION");
        
        // Create temporary table with latest DO order per agent
        $createTempTableSql = "
            CREATE TEMPORARY TABLE latest_do AS
            SELECT 
                agent_no,
                reference_no
            FROM (
                SELECT 
                    agent_no,
                    reference_no,
                    ROW_NUMBER() OVER (PARTITION BY agent_no ORDER BY order_date DESC, reference_no DESC) AS rn
                FROM orders
                WHERE type = 'DO'
                  AND order_date <= '$cleanupDate'
            ) x
            WHERE rn = 1
        ";
        $db_remote->query($createTempTableSql);
        
        if ($db_remote->getError()) {
            throw new Exception("Failed to create temporary table: " . $db_remote->getError());
        }
        
        // Delete old DO orders (except latest per agent)
        $deleteOrdersSql = "
            DELETE o
            FROM orders o
            LEFT JOIN latest_do l 
                   ON o.reference_no = l.reference_no
                  AND o.agent_no = l.agent_no
            WHERE o.type = 'DO'
              AND o.order_date <= '$cleanupDate'
              AND l.reference_no IS NULL
        ";
        $db_remote->query($deleteOrdersSql);
        
        if ($db_remote->getError()) {
            throw new Exception("Failed to delete old DO orders: " . $db_remote->getError());
        }
        
        $deletedOrders = $db_remote->getAffectedRows();
        
        // Delete orphan order_items
        $deleteItemsSql = "
            DELETE oi
            FROM order_items oi
            LEFT JOIN orders o
                   ON oi.reference_no = o.reference_no
            WHERE o.reference_no IS NULL
              AND oi.created_at <= '$cleanupDate'
        ";
        $db_remote->query($deleteItemsSql);
        
        if ($db_remote->getError()) {
            throw new Exception("Failed to delete orphan order_items: " . $db_remote->getError());
        }
        
        $deletedItems = $db_remote->getAffectedRows();
        
        // Commit transaction
        $db_remote->query("COMMIT");
        $db_remote->close();
        
        if ($deletedOrders > 0 || $deletedItems > 0) {
            ProgressDisplay::info("✅ Cleaned up old DO orders: Deleted $deletedOrders orders and $deletedItems orphan items");
        } else {
            ProgressDisplay::info("✅ No old DO orders to clean up");
        }
    } catch (Exception $e) {
        // Rollback on error
        if (isset($db_remote) && $db_remote) {
            try {
                $db_remote->query("ROLLBACK");
                $db_remote->close();
            } catch (Exception $rollbackError) {
                // Ignore rollback errors
            }
        }
        ProgressDisplay::warning("⚠️  Could not clean up old DO orders: " . $e->getMessage());
        // Don't fail the entire sync if cleanup fails
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
