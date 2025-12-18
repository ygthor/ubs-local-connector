<?php

use XBase\DataConverter\Field\DBase7\TimestampConverter;

include(__DIR__ . '/bootstrap/app.php');
include(__DIR__ . '/bootstrap/cache.php');

// Initialize sync environment and progress display
initializeSyncEnvironment();
ProgressDisplay::start("🚀 Starting UBS Local Connector Sync Process");

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
    
    // Check for resync date parameter
    $resync_date = null;
    $resync_mode = false;
    
    // Parse command line arguments
    $args = $argv ?? [];
    for ($i = 0; $i < count($args); $i++) {
        if ($args[$i] === '--resync-date' && isset($args[$i + 1])) {
            $resync_date = $args[$i + 1];
            $resync_mode = true;
            break;
        }
    }
    
    if ($resync_mode && $resync_date) {
        // Validate date format
        $date_obj = DateTime::createFromFormat('Y-m-d', $resync_date);
        if (!$date_obj || $date_obj->format('Y-m-d') !== $resync_date) {
            ProgressDisplay::error("❌ Invalid date format. Please use YYYY-MM-DD format (e.g., 2025-01-15)");
            exit(1);
        }
        
        ProgressDisplay::info("🔄 RESYNC MODE: Syncing all records where created_at or updated_at = $resync_date");
        $last_synced_at = null; // Will use date filter instead
    } else {
        // Normal sync mode
        // Get last sync time
        $last_synced_at = lastSyncAt(); // Commented out for full sync
        // $last_synced_at = null; // Set to null for FULL SYNC (process all records)
        
        // Clear sync cache for full sync
        clearSyncCache();
        ProgressDisplay::info("🧹 Cleared sync cache for full sync");
        
        // If no last sync time, use a date far in the past to get all records
        if (empty($last_synced_at)) {
            $last_synced_at = '2025-08-01 00:00:00';
        }
        // $last_synced_at = '2025-08-01 00:00:00';
    }
    
    // ProgressDisplay::info("Last sync time: $last_synced_at");
    // ProgressDisplay::info("Memory limit set to: " . ini_get('memory_limit'));
    
    // ✅ SAFER APPROACH: Skip orders without items from syncing to UBS instead of deleting them
    // This prevents race conditions where orders are being created while sync is running
    // Orders without items will simply not be synced to UBS, and will be synced automatically
    // once items are added in the next sync run
    // 
    // The filtering is done in:
    // 1. fetchRemoteDataByKeys() - filters orders without items when fetching by keys
    // 2. Remote data queries in main sync loop - filters orders without items
    // 
    // OLD APPROACH (commented out for safety):
    // deleteOrdersWithNoItems($last_synced_at, $resync_date, $resync_mode);
    ProgressDisplay::info("ℹ️  Using safer approach: Orders without items will be skipped from sync (not deleted)");
    
    // Initialize sync tracking variables
    $processedTables = 0;
    $syncResults = []; // Track sync results for each table
    $tableStats = []; // Track insert/update statistics per table
    
    // ✅ SYNC ICITEM AND ICGROUP FIRST (Force sync - all records)
    // These are master data tables that need to be synced completely every time
    try {
        ProgressDisplay::info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        ProgressDisplay::info("📦 Step 1: Syncing icitem and icgroup (master data - force sync all records)");
        ProgressDisplay::info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        
        $icitemResult = syncIcitemAndIcgroup();
        
        ProgressDisplay::info("✅ icitem & icgroup sync completed:");
        ProgressDisplay::info("   - icitem records: " . $icitemResult['icitem_count']);
        ProgressDisplay::info("   - icgroup records: " . $icitemResult['icgroup_count']);
        
        // Add to sync results
        $syncResults[] = [
            'table' => 'ubs_ubsstk2015_icitem',
            'ubs_count' => $icitemResult['icitem_count'],
            'remote_count' => $icitemResult['icitem_count'],
            'processed_records' => $icitemResult['icitem_count']
        ];
        $syncResults[] = [
            'table' => 'ubs_ubsstk2015_icgroup',
            'ubs_count' => $icitemResult['icgroup_count'],
            'remote_count' => $icitemResult['icgroup_count'],
            'processed_records' => $icitemResult['icgroup_count']
        ];
        
    } catch (Exception $e) {
        ProgressDisplay::error("❌ Error syncing icitem and icgroup: " . $e->getMessage());
        // Continue with other tables even if icitem/icgroup sync fails
    }
    
    ProgressDisplay::info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    ProgressDisplay::info("📦 Step 2: Syncing other tables");
    ProgressDisplay::info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    
    $ubsTables = Converter::ubsTable();
    $totalTables = count($ubsTables);
    
    ProgressDisplay::info("Found $totalTables tables to sync: " . implode(', ', $ubsTables));
    if ($resync_mode) {
        ProgressDisplay::info("🕐 Resyncing records for date: $resync_date");
    } else {
        ProgressDisplay::info("🕐 Syncing records updated after: $last_synced_at");
    }
    
    foreach($ubsTables as $ubs_table) {
        // ✅ Skip icitem and icgroup - already synced above with force sync
        if ($ubs_table === 'ubs_ubsstk2015_icitem' || $ubs_table === 'ubs_ubsstk2015_icgroup') {
            ProgressDisplay::info("⏭️  Skipping $ubs_table (already synced with force sync above)");
            continue;
        }
        
        $remote_table_name = Converter::table_convert_remote($ubs_table);
        
        $processedTables++;
        ProgressDisplay::info("📁 Processing table $processedTables/$totalTables: $ubs_table");
        
        // ✅ SAFE: Verify indexes exist (read-only check)
        try {
            $indexStatus = verifySyncIndexes($ubs_table, 'local');
            if (!$indexStatus['has_updated_on_index']) {
                ProgressDisplay::warning("⚠️  No index on UPDATED_ON for $ubs_table - sync may be slower");
            }
            if (!$indexStatus['has_primary_key']) {
                ProgressDisplay::warning("⚠️  No primary key on $ubs_table - this may cause issues");
            }
        } catch (Exception $e) {
            // Safe: Continue even if index check fails
            ProgressDisplay::warning("Could not verify indexes for $ubs_table: " . $e->getMessage());
        }
        
        try {
            // ProgressDisplay::info("🔍 Inside try block for $ubs_table");
            // Check if we can resume from previous run (DISABLED for full sync)
            // $resumeData = canResumeSync();
            // if ($resumeData && $resumeData['table'] === $ubs_table) {
            //     ProgressDisplay::info("Resuming sync for $ubs_table from previous run");
            //     ProgressDisplay::info("Previous progress: {$resumeData['processed_records']}/{$resumeData['total_records']} records");
            // }
            
            // Get data counts first for better progress tracking
            
            // ✅ FORCE SYNC: icitem and icgroup always sync all records regardless of timestamp
            $forceSyncTables = ['ubs_ubsstk2015_icitem', 'ubs_ubsstk2015_icgroup'];
            $isForceSync = in_array($ubs_table, $forceSyncTables);
            
            try {
                // Check if table exists first
                $tableCheckSql = "SHOW TABLES LIKE '$ubs_table'";
                $tableExists = $db->first($tableCheckSql);
                
                if (empty($tableExists)) {
                    ProgressDisplay::warning("⚠️  Table '$ubs_table' does not exist in local database. Skipping...");
                    continue;
                }
                
                if ($isForceSync) {
                    // Force sync: Get ALL records regardless of timestamp or NULL values
                    $countSql = "SELECT COUNT(*) as total FROM `$ubs_table`";
                } elseif ($resync_mode && $resync_date) {
                    // Resync mode: Get records where DATE(created_at) = date OR DATE(updated_at) = date
                    $countSql = "SELECT COUNT(*) as total FROM `$ubs_table` 
                                 WHERE (DATE(CREATED_ON) = '$resync_date' OR DATE(UPDATED_ON) = '$resync_date')";
                } else {
                    // Normal sync: Only records updated after last sync
                    $countSql = "SELECT COUNT(*) as total FROM `$ubs_table` WHERE UPDATED_ON > '$last_synced_at'";
                }
                
                $ubsCount = $db->first($countSql)['total'];
                
                // Debug: Check if table exists and has any records at all
                $totalCountSql = "SELECT COUNT(*) as total FROM `$ubs_table`";
                $totalCount = $db->first($totalCountSql)['total'];
                
                // Only show if there are records to process
                if ($ubsCount > 0) {
                    ProgressDisplay::info("📊 $ubs_table: $ubsCount records (total: $totalCount)");
                }
            } catch (Exception $e) {
                ProgressDisplay::error("❌ Error checking table $ubs_table: " . $e->getMessage());
                continue;
            }
            
            // ✅ OPTIMIZATION: Don't load all remote data upfront - fetch per chunk instead
            // This saves memory and is much faster for large tables
            $remoteCount = 0;
            $remote_data = []; // Will be fetched per chunk if needed
            
            // Check if this is artran (orders) or ictran (order_items) - needs special handling
            $isArtran = ($ubs_table === 'ubs_ubsstk2015_artran');
            $isIctran = ($ubs_table === 'ubs_ubsstk2015_ictran');
            $needsSpecialHandling = ($isArtran || $isIctran);
            
            // Check if this is customers table - needs special handling when UBS is empty
            $isCustomers = ($ubs_table === 'ubs_ubsacc2015_arcust');
            $needsEmptyUbsCheck = ($needsSpecialHandling || $isCustomers);
            
            // Only check remote count if we have UBS data to compare, OR if it's artran/ictran/customers (always check when UBS is empty)
            if ($ubsCount > 0 || $needsEmptyUbsCheck) {
                try {
                    $db_remote_check = new mysql();
                    $db_remote_check->connect_remote();
                    $remote_table_name = Converter::table_convert_remote($ubs_table);
                    $column_updated_at = Converter::mapUpdatedAtField($remote_table_name);
                    
                    if ($isForceSync) {
                        // ✅ SAFER: Only count orders that have at least one order_item
                        if ($isArtran) {
                            $countSql = "SELECT COUNT(DISTINCT o.reference_no) as total FROM $remote_table_name o
                                        INNER JOIN order_items oi ON o.reference_no = oi.reference_no";
                        } else {
                            $countSql = "SELECT COUNT(*) as total FROM $remote_table_name";
                        }
                    } elseif ($resync_mode && $resync_date) {
                        // Resync mode: Check DATE(created_at) = date OR DATE(updated_at) = date
                        // ✅ SAFER: Only count orders that have at least one order_item
                        if ($isArtran) {
                            $countSql = "SELECT COUNT(DISTINCT o.reference_no) as total FROM $remote_table_name o
                                        INNER JOIN order_items oi ON o.reference_no = oi.reference_no
                                        WHERE (DATE(o.created_at) = '$resync_date' OR DATE(o.updated_at) = '$resync_date' OR DATE(o.order_date) = '$resync_date')";
                        } elseif ($isIctran) {
                            $countSql = "SELECT COUNT(*) as total FROM $remote_table_name oi
                                        INNER JOIN orders o ON oi.reference_no = o.reference_no
                                        WHERE (DATE(oi.created_at) = '$resync_date' OR DATE(oi.updated_at) = '$resync_date' OR DATE(o.order_date) = '$resync_date')";
                        } else {
                            $countSql = "SELECT COUNT(*) as total FROM $remote_table_name 
                                        WHERE (DATE(created_at) = '$resync_date' OR DATE(updated_at) = '$resync_date')";
                        }
                    } elseif ($isArtran) {
                        // For artran (orders): Check both updated_at AND order_date to catch recent orders
                        // ✅ SAFER: Only count orders that have at least one order_item
                        $countSql = "SELECT COUNT(DISTINCT o.reference_no) as total FROM $remote_table_name o
                                    INNER JOIN order_items oi ON o.reference_no = oi.reference_no
                                    WHERE (o.$column_updated_at > '$last_synced_at' OR o.order_date > '$last_synced_at')";
                    } elseif ($isIctran) {
                        // For ictran (order_items): Check both updated_at AND parent order's order_date
                        $countSql = "SELECT COUNT(*) as total FROM $remote_table_name oi
                                    INNER JOIN orders o ON oi.reference_no = o.reference_no
                                    WHERE (oi.$column_updated_at > '$last_synced_at' OR o.order_date > '$last_synced_at')";
                    } else {
                        $countSql = "SELECT COUNT(*) as total FROM $remote_table_name WHERE $column_updated_at > '$last_synced_at'";
                    }
                    $remoteCount = $db_remote_check->first($countSql)['total'] ?? 0;
                    
                    // For artran/ictran/customers, also check total count if no recent updates (when UBS is empty)
                    // ✅ SAFER: Only count orders that have at least one order_item
                    if ($needsEmptyUbsCheck && $ubsCount == 0 && $remoteCount == 0) {
                        if ($isArtran) {
                            $totalRemoteSql = "SELECT COUNT(DISTINCT o.reference_no) as total FROM $remote_table_name o
                                             INNER JOIN order_items oi ON o.reference_no = oi.reference_no";
                        } else {
                            $totalRemoteSql = "SELECT COUNT(*) as total FROM $remote_table_name";
                        }
                        $totalRemoteCount = $db_remote_check->first($totalRemoteSql)['total'] ?? 0;
                        if ($totalRemoteCount > 0) {
                            if ($isArtran) {
                                $tableLabel = 'orders';
                            } elseif ($isIctran) {
                                $tableLabel = 'order_items';
                            } elseif ($isCustomers) {
                                $tableLabel = 'customers';
                            } else {
                                $tableLabel = $remote_table_name;
                            }
                            ProgressDisplay::info("📊 " . ucfirst($tableLabel) . ": $totalRemoteCount total records in remote (none updated recently)");
                            // Set remoteCount to total so it doesn't get skipped
                            $remoteCount = $totalRemoteCount;
                        }
                    }
                    
                    $db_remote_check->close();
                } catch (Exception $e) {
                    // Ignore - will fetch per chunk anyway
                }
            }
            
            // ✅ OPTIMIZED: If no data on either side, skip with concise message
            // BUT: For artran (orders), ictran (order_items), and customers, always check remote even if local is empty
            if ($ubsCount == 0 && $remoteCount == 0 && !$needsEmptyUbsCheck) {
                ProgressDisplay::info("⏭️  SKIP $ubs_table (no data)");
                continue;
            }
            
            // Special handling for artran/ictran/customers: Always check remote for missing records
            if ($needsEmptyUbsCheck && $ubsCount == 0) {
                if ($isArtran) {
                    $tableLabel = 'orders';
                } elseif ($isIctran) {
                    $tableLabel = 'order_items';
                } elseif ($isCustomers) {
                    $tableLabel = 'customers';
                } else {
                    $tableLabel = $remote_table_name;
                }
                ProgressDisplay::info("🔍 " . ucfirst($tableLabel) . ": No local updates, checking remote for missing records...");
            }
            
            // Only show detailed info if there's actual data to process
            if ($ubsCount > 0 || $remoteCount > 0) {
                ProgressDisplay::info("📊 $ubs_table: UBS=$ubsCount, Remote=$remoteCount");
            }
            
            // Start cache tracking with total records to process
            $totalRecordsToProcess = max($ubsCount, $remoteCount);
            // startSyncCache($ubs_table, $totalRecordsToProcess);
            
            // Process data in chunks to avoid memory issues
            $chunkSize = 5000; // Increased from 500 to 5000 for better performance
            $offset = 0;
            $processedRecords = 0;
            $maxIterations = 100; // Safety limit to prevent infinite loops
            $iterationCount = 0;
            
            
            // ✅ OPTIMIZATION: Skip remote-only processing - will be handled per chunk
            // This avoids loading all remote data and all UBS keys upfront
            if ($ubsCount == 0) {
                // ✅ If no remote data found with timestamp filter, but local has no data,
                // fetch ALL remote records to check if there are any missing in local
                try {
                    $db_remote_all = new mysql();
                    $db_remote_all->connect_remote();
                    $remote_table_name = Converter::table_convert_remote($ubs_table);
                    $column_updated_at = Converter::mapUpdatedAtField($remote_table_name);
                    
                    // ✅ FIX: When UBS is empty, fetch ALL remote records (not just recent ones)
                    // This ensures customers with old updated_at values are still synced to DBF
                    // For artran/ictran: Also check order_date to catch recent orders
                    // ✅ SAFER: Only sync orders that have at least one order_item to prevent race conditions
                    if ($isArtran) {
                        $allRemoteSql = "SELECT o.* FROM $remote_table_name o
                                        INNER JOIN order_items oi ON o.reference_no = oi.reference_no
                                        WHERE (o.$column_updated_at > '$last_synced_at' OR o.order_date > '$last_synced_at')
                                        GROUP BY o.reference_no";
                    } elseif ($isIctran) {
                        $allRemoteSql = "SELECT oi.* FROM $remote_table_name oi
                                       INNER JOIN orders o ON oi.reference_no = o.reference_no
                                       WHERE (oi.$column_updated_at > '$last_synced_at' OR o.order_date > '$last_synced_at')";
                    } else {
                        // ✅ FIX: When UBS table is empty, fetch ALL remote records regardless of timestamp
                        // This ensures all customers are synced to DBF even if they have old updated_at values
                        $allRemoteSql = "SELECT * FROM $remote_table_name";
                    }
                    $allRemoteData = $db_remote_all->get($allRemoteSql);
                    $db_remote_all->close();
                    
                    if (!empty($allRemoteData)) {
                        // Get all local UBS keys
                        $allUbsKeys = [];
                        $ubs_key = Converter::primaryKey($ubs_table);
                        $is_composite_key = is_array($ubs_key);
                        
                        if ($is_composite_key) {
                            $keyColumns = array_map(function($k) { return "`$k`"; }, $ubs_key);
                            $keySql = "SELECT " . implode(', ', $keyColumns) . " FROM `$ubs_table`";
                        } else {
                            $keySql = "SELECT `$ubs_key` FROM `$ubs_table`";
                        }
                        
                        $allUbsKeysData = $db->get($keySql);
                        foreach ($allUbsKeysData as $row) {
                            if ($is_composite_key) {
                                $composite_keys = [];
                                foreach ($ubs_key as $k) {
                                    $composite_keys[] = $row[$k] ?? '';
                                }
                                $key = implode('|', $composite_keys);
                            } else {
                                $key = $row[$ubs_key] ?? '';
                            }
                            $allUbsKeys[$key] = true;
                        }
                        unset($allUbsKeysData);
                        
                        // Find missing records
                        $remote_key = Converter::primaryKey($remote_table_name);
                        $missing_records = [];
                        foreach ($allRemoteData as $remote_row) {
                            $remoteKey = $remote_row[$remote_key] ?? '';
                            if (!isset($allUbsKeys[$remoteKey])) {
                                $missing_records[] = $remote_row;
                            }
                        }
                        
                        if (!empty($missing_records)) {
                            $comparedData = syncEntity($ubs_table, [], $missing_records);
                            $ubs_data_to_upsert = $comparedData['ubs_data'];
                            
                            if (!empty($ubs_data_to_upsert)) {
                                // ✅ DEBUG: Log first record to see what data is being synced
                                $firstRecord = $ubs_data_to_upsert[0];
                                $primaryKey = Converter::primaryKey($ubs_table);
                                $primaryKeyValue = is_array($primaryKey) ? 
                                    implode('|', array_map(function($k) use ($firstRecord) { return $firstRecord[strtoupper($k)] ?? $firstRecord[$k] ?? ''; }, $primaryKey)) :
                                    ($firstRecord[strtoupper($primaryKey)] ?? $firstRecord[$primaryKey] ?? '');
                                ProgressDisplay::info("🔍 DEBUG: First record to sync - Primary key ($primaryKey): '$primaryKeyValue', Available fields: " . implode(', ', array_keys($firstRecord)));
                                
                                $tempUbsStats = ['inserts' => [], 'updates' => []];
                                executeSyncWithTransaction(function() use ($ubs_table, $ubs_data_to_upsert, &$tempUbsStats) {
                                    ProgressDisplay::info("⬇️ $ubs_table: Syncing " . count($ubs_data_to_upsert) . " missing remote→UBS");
                                    $tempUbsStats = batchUpsertUbs($ubs_table, $ubs_data_to_upsert);
                                }, true);
                                
                                // Store statistics
                                if (!isset($tableStats[$ubs_table])) {
                                    $tableStats[$ubs_table] = [
                                        'remote' => ['inserts' => [], 'updates' => []],
                                        'ubs' => ['inserts' => [], 'updates' => []]
                                    ];
                                }
                                $tableStats[$ubs_table]['ubs']['inserts'] = array_merge($tableStats[$ubs_table]['ubs']['inserts'], $tempUbsStats['inserts']);
                                $tableStats[$ubs_table]['ubs']['updates'] = array_merge($tableStats[$ubs_table]['ubs']['updates'], $tempUbsStats['updates']);
                            }
                        }
                        unset($allUbsKeys, $missing_records, $allRemoteData);
                    }
                } catch (Exception $e) {
                    ProgressDisplay::warning("⚠️  $ubs_table: " . $e->getMessage());
                }
            }
            
            // Process UBS data in chunks if it exists
            // ✅ FORCE SYNC: Use different WHERE clause for force sync tables
            while ($offset < $ubsCount && $iterationCount < $maxIterations) {
              
                $iterationCount++;
                
                if ($isForceSync) {
                    // Force sync: Get ALL records regardless of timestamp or NULL values
                    $sql = "
                        SELECT * FROM `$ubs_table` 
                        ORDER BY COALESCE(UPDATED_ON, '1970-01-01') ASC
                        LIMIT $chunkSize OFFSET $offset
                    ";
                } else {
                    // Normal sync: Only records updated after last sync
                    $sql = "
                        SELECT * FROM `$ubs_table` 
                        WHERE UPDATED_ON > '$last_synced_at'
                        ORDER BY UPDATED_ON ASC
                        LIMIT $chunkSize OFFSET $offset
                    ";
                }
                
                $ubs_data = $db->get($sql);
                
                if (empty($ubs_data)) break;
                
                // Validate and fix UPDATED_ON fields in UBS data
                // For icgroup, preserve NULL values
                $ubs_data = validateAndFixUpdatedOn($ubs_data, $ubs_table);
                
                // ✅ OPTIMIZATION: Fetch only remote records that match current UBS chunk keys
                // This is MUCH faster than loading all remote data upfront
                $ubs_key = Converter::primaryKey($ubs_table);
                $is_composite_key = is_array($ubs_key);
                $chunk_keys = [];
                
                foreach ($ubs_data as $row) {
                    if ($is_composite_key) {
                        $composite_keys = [];
                        foreach ($ubs_key as $k) {
                            $composite_keys[] = $row[$k] ?? '';
                        }
                        $key = implode('|', $composite_keys);
                    } else {
                        $key = $row[$ubs_key] ?? '';
                    }
                    if (!empty($key)) {
                        $chunk_keys[] = $key;
                    }
                }
                
                // Fetch only matching remote records for this chunk
                $chunk_remote_data = [];
                if (!empty($chunk_keys)) {
                    $updatedAfter = $isForceSync ? null : $last_synced_at;
                    // For artran/ictran, also check order_date (handled in fetchRemoteDataByKeys)
                    // Pass resync_date if in resync mode
                    $chunk_remote_data = fetchRemoteDataByKeys($ubs_table, $chunk_keys, 
                        ($resync_mode && $resync_date) ? null : $updatedAfter, 
                        ($resync_mode && $resync_date) ? $resync_date : null);
                }
                
                // ✅ FAST PATH: If no remote data, skip syncEntity (like main_init.php)
                // This avoids expensive comparison when there's nothing to compare
                if (empty($chunk_remote_data)) {
                    // All UBS records need to sync to remote
                    $remote_data_to_upsert = [];
                    foreach ($ubs_data as $row) {
                        $remote_data_to_upsert[] = convert(Converter::table_convert_remote($ubs_table), $row, 'to_remote');
                    }
                    $ubs_data_to_upsert = [];
                } else {
                    // Compare with remote data
                    $comparedData = syncEntity($ubs_table, $ubs_data, $chunk_remote_data);
                    $remote_data_to_upsert = $comparedData['remote_data'];
                    $ubs_data_to_upsert = $comparedData['ubs_data'];
                }
                
                // ✅ SAFE: Use transaction wrapper for data integrity
                // Use batch processing for better performance
                if (!empty($remote_data_to_upsert) || !empty($ubs_data_to_upsert)) {
                    $remoteStats = ['inserts' => [], 'updates' => []];
                    $ubsStats = ['inserts' => [], 'updates' => []];
                    
                    executeSyncWithTransaction(function() use ($ubs_table, $remote_data_to_upsert, $ubs_data_to_upsert, &$remoteStats, &$ubsStats) {
                        if (!empty($remote_data_to_upsert)) {
                            ProgressDisplay::info("⬆️ $ubs_table: " . count($remote_data_to_upsert) . " UBS→Remote");
                            $remoteStats = batchUpsertRemote($ubs_table, $remote_data_to_upsert);
                        }

                        if (!empty($ubs_data_to_upsert)) {
                            ProgressDisplay::info("⬇️ $ubs_table: " . count($ubs_data_to_upsert) . " Remote→UBS");
                            $ubsStats = batchUpsertUbs($ubs_table, $ubs_data_to_upsert);
                        }
                    }, true); // Use transactions
                    
                    // Store statistics for this table
                    if (!isset($tableStats[$ubs_table])) {
                        $tableStats[$ubs_table] = [
                            'remote' => ['inserts' => [], 'updates' => []],
                            'ubs' => ['inserts' => [], 'updates' => []]
                        ];
                    }
                    $tableStats[$ubs_table]['remote']['inserts'] = array_merge($tableStats[$ubs_table]['remote']['inserts'], $remoteStats['inserts']);
                    $tableStats[$ubs_table]['remote']['updates'] = array_merge($tableStats[$ubs_table]['remote']['updates'], $remoteStats['updates']);
                    $tableStats[$ubs_table]['ubs']['inserts'] = array_merge($tableStats[$ubs_table]['ubs']['inserts'], $ubsStats['inserts']);
                    $tableStats[$ubs_table]['ubs']['updates'] = array_merge($tableStats[$ubs_table]['ubs']['updates'], $ubsStats['updates']);
                }
                
                $processedRecords += count($ubs_data);
                $offset += $chunkSize;
                
                // Update cache with progress
                // updateSyncCache($processedRecords, $offset);
                
                // Memory cleanup between chunks
                gc_collect_cycles();
                
                // Small delay between chunks to prevent file locks
                usleep(100000); // 0.1 second delay
                
                // ProgressDisplay::display("Processed $ubs_table", $processedRecords, $totalRecordsToProcess);
            }
            
            // Check if we hit the safety limit
            if ($iterationCount >= $maxIterations) {
                ProgressDisplay::error("⚠️  Safety limit reached for $ubs_table - stopping to prevent infinite loop");
                break; // This breaks out of the while loop, not the foreach loop
            }
            
            // Additional safety check: if we've processed more records than exist, break
            if ($processedRecords >= $ubsCount) {
                // All records processed, break silently
            }
            
            // ✅ For artran/ictran: Also check for missing remote records (Remote → UBS)
            // This ensures orders created in remote but not yet in UBS are synced
            if ($needsSpecialHandling && $ubsCount > 0) {
                try {
                    $db_remote_check = new mysql();
                    $db_remote_check->connect_remote();
                    $remote_table_name = Converter::table_convert_remote($ubs_table);
                    $column_updated_at = Converter::mapUpdatedAtField($remote_table_name);
                    
                    // Fetch remote records that should be synced (check both updated_at and order_date)
                    // ✅ SAFER: Only sync orders that have at least one order_item to prevent race conditions
                    if ($isArtran) {
                        $missingRemoteSql = "SELECT o.* FROM $remote_table_name o
                                           INNER JOIN order_items oi ON o.reference_no = oi.reference_no
                                           WHERE (o.$column_updated_at > '$last_synced_at' OR o.order_date > '$last_synced_at')
                                           GROUP BY o.reference_no";
                    } elseif ($isIctran) {
                        $missingRemoteSql = "SELECT oi.* FROM $remote_table_name oi
                                           INNER JOIN orders o ON oi.reference_no = o.reference_no
                                           WHERE (oi.$column_updated_at > '$last_synced_at' OR o.order_date > '$last_synced_at')";
                    } else {
                        $missingRemoteSql = "SELECT * FROM $remote_table_name WHERE $column_updated_at > '$last_synced_at'";
                    }
                    
                    $allRemoteData = $db_remote_check->get($missingRemoteSql);
                    $db_remote_check->close();
                    
                    if (!empty($allRemoteData)) {
                        // Get all local UBS keys
                        $allUbsKeys = [];
                        $ubs_key = Converter::primaryKey($ubs_table);
                        $is_composite_key = is_array($ubs_key);
                        
                        if ($is_composite_key) {
                            $keyColumns = array_map(function($k) { return "`$k`"; }, $ubs_key);
                            $keySql = "SELECT " . implode(', ', $keyColumns) . " FROM `$ubs_table`";
                        } else {
                            $keySql = "SELECT `$ubs_key` FROM `$ubs_table`";
                        }
                        
                        $allUbsKeysData = $db->get($keySql);
                        foreach ($allUbsKeysData as $row) {
                            if ($is_composite_key) {
                                $composite_keys = [];
                                foreach ($ubs_key as $k) {
                                    $composite_keys[] = $row[$k] ?? '';
                                }
                                $key = implode('|', $composite_keys);
                            } else {
                                $key = $row[$ubs_key] ?? '';
                            }
                            if (!empty($key)) {
                                $allUbsKeys[$key] = true;
                            }
                        }
                        unset($allUbsKeysData);
                        
                        // Find missing records (in remote but not in UBS)
                        $remote_key = Converter::primaryKey($remote_table_name);
                        $missing_records = [];
                        foreach ($allRemoteData as $remote_row) {
                            $remoteKey = $remote_row[$remote_key] ?? '';
                            if (!empty($remoteKey) && !isset($allUbsKeys[$remoteKey])) {
                                $missing_records[] = $remote_row;
                            }
                        }
                        
                        if (!empty($missing_records)) {
                            $comparedData = syncEntity($ubs_table, [], $missing_records);
                            $ubs_data_to_upsert = $comparedData['ubs_data'];
                            
                            if (!empty($ubs_data_to_upsert)) {
                                $tempUbsStats2 = ['inserts' => [], 'updates' => []];
                                executeSyncWithTransaction(function() use ($ubs_table, $ubs_data_to_upsert, &$tempUbsStats2) {
                                    $tableLabel = $isArtran ? 'orders' : 'order_items';
                                    ProgressDisplay::info("⬇️ " . ucfirst($tableLabel) . ": Syncing " . count($ubs_data_to_upsert) . " missing remote→UBS record(s)");
                                    $tempUbsStats2 = batchUpsertUbs($ubs_table, $ubs_data_to_upsert);
                                }, true);
                                
                                // Store statistics
                                if (!isset($tableStats[$ubs_table])) {
                                    $tableStats[$ubs_table] = [
                                        'remote' => ['inserts' => [], 'updates' => []],
                                        'ubs' => ['inserts' => [], 'updates' => []]
                                    ];
                                }
                                $tableStats[$ubs_table]['ubs']['inserts'] = array_merge($tableStats[$ubs_table]['ubs']['inserts'], $tempUbsStats2['inserts']);
                                $tableStats[$ubs_table]['ubs']['updates'] = array_merge($tableStats[$ubs_table]['ubs']['updates'], $tempUbsStats2['updates']);
                            }
                        }
                        
                        unset($allUbsKeys, $missing_records, $allRemoteData);
                    }
                } catch (Exception $e) {
                    ProgressDisplay::warning("⚠️  $ubs_table: Error checking for missing remote records: " . $e->getMessage());
                }
            }
            
            // ✅ OPTIMIZATION: Skip remote-only sync when UBS is empty
            // This is handled per chunk above, no need to load all remote data here
            
            // Handle table-specific triggers
            $table_trigger_reset = ['customer','orders'];
            $remote_table_name = Converter::table_convert_remote($ubs_table);
            if(in_array($remote_table_name, $table_trigger_reset)) {
                ProgressDisplay::info("Resetting triggers for $remote_table_name");
                $Core = Core::getInstance();
                $Core->initRemoteData();
            }
            
            // Link customers to users after customers sync
            if ($remote_table_name === 'customers') {
                try {
                    $db_remote = new mysql();
                    $db_remote->connect_remote();
                    linkCustomersToUsers($db_remote);
                    $db_remote->close();
                } catch (Exception $e) {
                    ProgressDisplay::warning("⚠️  Could not link customers to users: " . $e->getMessage());
                }
            }
            
            // Track sync results for this table
            $syncResults[] = [
                'table' => $ubs_table,
                'ubs_count' => $ubsCount,
                'remote_count' => $remoteCount,
                'processed_records' => $processedRecords
            ];
            
            ProgressDisplay::info("✅ Completed sync for $ubs_table (UBS: $ubsCount, Remote: $remoteCount, Processed: $processedRecords)");
            
            // ✅ NOTE: icgroup is now synced directly from icgroup.dbf (enabled in converter.class.php)
            // Both icitem and icgroup are force synced (all records) every time, regardless of timestamp
            
            // Complete cache for this table
            completeSyncCache();
            ProgressDisplay::info("🔄 Cache completed for $ubs_table, moving to next table...");
            
        } catch (Exception $e) {
            echo "🔍 Exception caught for $ubs_table: " . $e->getMessage() . "\n";
            ProgressDisplay::error("Failed to sync $ubs_table: " . $e->getMessage());
            ProgressDisplay::error("Exception details: " . $e->getTraceAsString());
            // Clear cache on error
            clearSyncCache();
            // Continue with next table instead of stopping
            continue;
        }
        
        // Memory cleanup between tables
        gc_collect_cycles();
        
        // Add small delay to prevent file lock conflicts
        usleep(500000); // 0.5 second delay
        
        echo "🔍 Finished processing $ubs_table, moving to next table...\n";
    }
    
    echo "🔍 Foreach loop completed, processed $processedTables tables\n";
    
    // Log successful sync
    $db->insert('sync_logs', [
        'synced_at' => date('Y-m-d H:i:s')
    ]);
    
    // Display detailed sync results
    ProgressDisplay::info("📋 SYNC RESULTS SUMMARY:");
    ProgressDisplay::info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    
    $totalUbsRecords = 0;
    $totalRemoteRecords = 0;
    $totalProcessed = 0;
    
    foreach ($syncResults as $result) {
        $tableName = $result['table'];
        $ubsCount = $result['ubs_count'];
        $remoteCount = $result['remote_count'];
        $processed = $result['processed_records'];
        
        ProgressDisplay::info("📁 $tableName: UBS: $ubsCount, Remote: $remoteCount, Processed: $processed");
        
        $totalUbsRecords += $ubsCount;
        $totalRemoteRecords += $remoteCount;
        $totalProcessed += $processed;
    }
    
    ProgressDisplay::info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    ProgressDisplay::info("📊 TOTALS: Tables: " . count($syncResults) . ", UBS: $totalUbsRecords, Remote: $totalRemoteRecords, Processed: $totalProcessed");
    
    // ✅ Clean up duplicate orders and order_items after sync completes
    try {
        ProgressDisplay::info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        validateAndCleanDuplicateOrders();
    } catch (Exception $e) {
        ProgressDisplay::warning("⚠️  Could not validate and clean duplicate orders: " . $e->getMessage());
        // Don't fail the entire sync if duplicate cleanup fails
    }
    
    // 📋 Display detailed summary with inserts/updates per table
    ProgressDisplay::info("");
    ProgressDisplay::info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    ProgressDisplay::info("📋 DETAILED SYNC SUMMARY (Inserts/Updates per Table)");
    ProgressDisplay::info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    
    $logContent = [];
    $logContent[] = "=" . str_repeat("=", 80);
    $logContent[] = "SYNC RUN SUMMARY - " . date('Y-m-d H:i:s');
    $logContent[] = "=" . str_repeat("=", 80);
    $logContent[] = "";
    
    foreach ($tableStats as $table => $stats) {
        $remote_table_name = Converter::table_convert_remote($table);
        $tableLabel = $remote_table_name ?: $table;
        
        // Remote (UBS → Remote) statistics
        $remoteInserts = count($stats['remote']['inserts']);
        $remoteUpdates = count($stats['remote']['updates']);
        $remoteInsertKeys = $stats['remote']['inserts'];
        $remoteUpdateKeys = $stats['remote']['updates'];
        
        // UBS (Remote → UBS) statistics
        $ubsInserts = count($stats['ubs']['inserts']);
        $ubsUpdates = count($stats['ubs']['updates']);
        $ubsInsertKeys = $stats['ubs']['inserts'];
        $ubsUpdateKeys = $stats['ubs']['updates'];
        
        // Display summary
        ProgressDisplay::info("");
        ProgressDisplay::info("📁 Table: $tableLabel");
        
        // Remote summary
        if ($remoteInserts > 0 || $remoteUpdates > 0) {
            ProgressDisplay::info("  ⬆️  UBS → Remote:");
            ProgressDisplay::info("     Insert: $remoteInserts");
            if ($remoteInserts > 0) {
                $keysDisplay = implode(', ', array_slice($remoteInsertKeys, 0, 10));
                if (count($remoteInsertKeys) > 10) {
                    $keysDisplay .= '... (+' . (count($remoteInsertKeys) - 10) . ' more)';
                }
                ProgressDisplay::info("     - " . $keysDisplay);
            }
            ProgressDisplay::info("     Update: $remoteUpdates");
            if ($remoteUpdates > 0) {
                $keysDisplay = implode(', ', array_slice($remoteUpdateKeys, 0, 10));
                if (count($remoteUpdateKeys) > 10) {
                    $keysDisplay .= '... (+' . (count($remoteUpdateKeys) - 10) . ' more)';
                }
                ProgressDisplay::info("     - " . $keysDisplay);
            }
        }
        
        // UBS summary
        if ($ubsInserts > 0 || $ubsUpdates > 0) {
            ProgressDisplay::info("  ⬇️  Remote → UBS:");
            ProgressDisplay::info("     Insert: $ubsInserts");
            if ($ubsInserts > 0) {
                $keysDisplay = implode(', ', array_slice($ubsInsertKeys, 0, 10));
                if (count($ubsInsertKeys) > 10) {
                    $keysDisplay .= '... (+' . (count($ubsInsertKeys) - 10) . ' more)';
                }
                ProgressDisplay::info("     - " . $keysDisplay);
            }
            ProgressDisplay::info("     Update: $ubsUpdates");
            if ($ubsUpdates > 0) {
                $keysDisplay = implode(', ', array_slice($ubsUpdateKeys, 0, 10));
                if (count($ubsUpdateKeys) > 10) {
                    $keysDisplay .= '... (+' . (count($ubsUpdateKeys) - 10) . ' more)';
                }
                ProgressDisplay::info("     - " . $keysDisplay);
            }
        }
        
        // Log to file
        $logContent[] = "Table: $tableLabel";
        if ($remoteInserts > 0 || $remoteUpdates > 0) {
            $logContent[] = "  UBS → Remote:";
            $logContent[] = "    Insert: $remoteInserts";
            if ($remoteInserts > 0) {
                $logContent[] = "    - " . implode(', ', $remoteInsertKeys);
            }
            $logContent[] = "    Update: $remoteUpdates";
            if ($remoteUpdates > 0) {
                $logContent[] = "    - " . implode(', ', $remoteUpdateKeys);
            }
        }
        if ($ubsInserts > 0 || $ubsUpdates > 0) {
            $logContent[] = "  Remote → UBS:";
            $logContent[] = "    Insert: $ubsInserts";
            if ($ubsInserts > 0) {
                $logContent[] = "    - " . implode(', ', $ubsInsertKeys);
            }
            $logContent[] = "    Update: $ubsUpdates";
            if ($ubsUpdates > 0) {
                $logContent[] = "    - " . implode(', ', $ubsUpdateKeys);
            }
        }
        $logContent[] = "";
    }
    
    $logContent[] = "=" . str_repeat("=", 80);
    $logContent[] = "";
    
    // Write log to file
    $logDir = __DIR__ . '/logs';
    if (!is_dir($logDir)) {
        mkdir($logDir, 0755, true);
    }
    $logFile = $logDir . '/sync_summary_' . date('Y-m-d_His') . '.log';
    file_put_contents($logFile, implode("\n", $logContent));
    ProgressDisplay::info("");
    ProgressDisplay::info("📝 Log saved to: $logFile");
    
    ProgressDisplay::info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    ProgressDisplay::complete("🎉 Sync process completed successfully! All " . count($syncResults) . " tables processed.");
    
} catch (Exception $e) {
    ProgressDisplay::error("Sync process failed: " . $e->getMessage());
    releaseSyncLock('php');
    exit(1);
} finally {
    // Ensure lock is released
    releaseSyncLock('php');
}