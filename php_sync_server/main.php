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
    
    // ProgressDisplay::info("Last sync time: $last_synced_at");
    // ProgressDisplay::info("Memory limit set to: " . ini_get('memory_limit'));
    
    $ubsTables = Converter::ubsTable();
    $totalTables = count($ubsTables);
    
    ProgressDisplay::info("Found $totalTables tables to sync: " . implode(', ', $ubsTables));
    ProgressDisplay::info("🕐 Syncing records updated after: $last_synced_at");
    
    $processedTables = 0;
    $syncResults = []; // Track sync results for each table
    
    foreach($ubsTables as $ubs_table) {
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
                        $countSql = "SELECT COUNT(*) as total FROM $remote_table_name";
                    } elseif ($isArtran) {
                        // For artran (orders): Check both updated_at AND order_date to catch recent orders
                        $countSql = "SELECT COUNT(*) as total FROM $remote_table_name 
                                    WHERE ($column_updated_at > '$last_synced_at' OR order_date > '$last_synced_at')";
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
                    if ($needsEmptyUbsCheck && $ubsCount == 0 && $remoteCount == 0) {
                        $totalRemoteSql = "SELECT COUNT(*) as total FROM $remote_table_name";
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
                    if ($isArtran) {
                        $allRemoteSql = "SELECT * FROM $remote_table_name 
                                        WHERE ($column_updated_at > '$last_synced_at' OR order_date > '$last_synced_at')";
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
                                executeSyncWithTransaction(function() use ($ubs_table, $ubs_data_to_upsert) {
                                    ProgressDisplay::info("⬇️ $ubs_table: Syncing " . count($ubs_data_to_upsert) . " missing remote→UBS");
                                    batchUpsertUbs($ubs_table, $ubs_data_to_upsert);
                                }, true);
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
                    $chunk_remote_data = fetchRemoteDataByKeys($ubs_table, $chunk_keys, $updatedAfter);
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
                    executeSyncWithTransaction(function() use ($ubs_table, $remote_data_to_upsert, $ubs_data_to_upsert) {
                        if (!empty($remote_data_to_upsert)) {
                            ProgressDisplay::info("⬆️ $ubs_table: " . count($remote_data_to_upsert) . " UBS→Remote");
                            batchUpsertRemote($ubs_table, $remote_data_to_upsert);
                        }

                        if (!empty($ubs_data_to_upsert)) {
                            ProgressDisplay::info("⬇️ $ubs_table: " . count($ubs_data_to_upsert) . " Remote→UBS");
                            batchUpsertUbs($ubs_table, $ubs_data_to_upsert);
                        }
                    }, true); // Use transactions
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
                    if ($isArtran) {
                        $missingRemoteSql = "SELECT * FROM $remote_table_name 
                                           WHERE ($column_updated_at > '$last_synced_at' OR order_date > '$last_synced_at')";
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
                                executeSyncWithTransaction(function() use ($ubs_table, $ubs_data_to_upsert) {
                                    $tableLabel = $isArtran ? 'orders' : 'order_items';
                                    ProgressDisplay::info("⬇️ " . ucfirst($tableLabel) . ": Syncing " . count($ubs_data_to_upsert) . " missing remote→UBS record(s)");
                                    batchUpsertUbs($ubs_table, $ubs_data_to_upsert);
                                }, true);
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
    
    ProgressDisplay::complete("🎉 Sync process completed successfully! All " . count($syncResults) . " tables processed.");
    
} catch (Exception $e) {
    ProgressDisplay::error("Sync process failed: " . $e->getMessage());
    releaseSyncLock('php');
    exit(1);
} finally {
    // Ensure lock is released
    releaseSyncLock('php');
}