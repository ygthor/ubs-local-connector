<?php

use XBase\DataConverter\Field\DBase7\TimestampConverter;

include(__DIR__ . '/bootstrap/app.php');
include(__DIR__ . '/bootstrap/cache.php');



$db = new mysql();

$last_synced_at = lastSyncAt(); // Commented out for full sync
$ubsTables = [
    'ubs_ubsstk2015_artran', // invoice
    'ubs_ubsstk2015_ictran', // invoice item
];

$processedTables = 0;
$syncResults = []; // Track sync results for each table
$tableStats = []; // Track insert/update statistics per table
$doItemsSynced = []; // Track DO (Delivery Order) items synced to remote

foreach ($ubsTables as $ubs_table) {
    $remote_table_name = Converter::table_convert_remote($ubs_table);

    $processedTables++;


    $forceSyncTables = ['ubs_ubsstk2015_icitem', 'ubs_ubsstk2015_icgroup'];
    $isForceSync = in_array($ubs_table, $forceSyncTables);

    // ✅ CUSTOMER FULL SYNC: Check if customers should use full sync for recent records
    $isCustomers = ($ubs_table === 'ubs_ubsacc2015_arcust');
    $customerFullSyncCutoff = null;
    if ($isCustomers && $CUSTOMER_FULL_SYNC_DAYS > 0) {
        // Calculate cutoff date: N days ago from now
        $cutoffDate = new DateTime();
        $cutoffDate->modify("-{$CUSTOMER_FULL_SYNC_DAYS} days");
        $customerFullSyncCutoff = $cutoffDate->format('Y-m-d H:i:s');
        ProgressDisplay::info("🔄 Customer Full Sync Mode: Syncing customers updated after $customerFullSyncCutoff (last $CUSTOMER_FULL_SYNC_DAYS days)");
    }


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
    } elseif ($isCustomers && $customerFullSyncCutoff !== null) {
        // Customer full sync: Get records updated after cutoff date (ignore last_synced_at)
        $countSql = "SELECT COUNT(*) as total FROM `$ubs_table` WHERE UPDATED_ON > '$customerFullSyncCutoff'";
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


    // ✅ OPTIMIZATION: Don't load all remote data upfront - fetch per chunk instead
    // This saves memory and is much faster for large tables
    $remoteCount = 0;
    $remote_data = []; // Will be fetched per chunk if needed

    // Check if this is artran (orders), ictran (order_items), or customers - needs special handling
    $isArtran = ($ubs_table === 'ubs_ubsstk2015_artran');
    $isIctran = ($ubs_table === 'ubs_ubsstk2015_ictran');
    $needsSpecialHandling = ($isArtran || $isIctran || $isCustomers);

    // Alias for readability: tables that need special handling also need check when UBS is empty
    $needsEmptyUbsCheck = $needsSpecialHandling;

    // Only check remote count if we have UBS data to compare, OR if it's artran/ictran/customers (always check when UBS is empty)
    if ($ubsCount > 0 || $needsEmptyUbsCheck) {

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
        } elseif ($isCustomers && $customerFullSyncCutoff !== null) {
            // Customer full sync: Use cutoff date instead of last_synced_at for remote count
            $countSql = "SELECT COUNT(*) as total FROM $remote_table_name WHERE $column_updated_at > '$customerFullSyncCutoff'";
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
            // Note: Customer full sync cutoff is not used here because we want to populate UBS completely
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
                $keyColumns = array_map(function ($k) {
                    return "`$k`";
                }, $ubs_key);
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
                        implode('|', array_map(function ($k) use ($firstRecord) {
                            return $firstRecord[strtoupper($k)] ?? $firstRecord[$k] ?? '';
                        }, $primaryKey)) : ($firstRecord[strtoupper($primaryKey)] ?? $firstRecord[$primaryKey] ?? '');
                    ProgressDisplay::info("🔍 DEBUG: First record to sync - Primary key ($primaryKey): '$primaryKeyValue', Available fields: " . implode(', ', array_keys($firstRecord)));

                    $tempUbsStats = ['inserts' => [], 'updates' => []];
                    executeSyncWithTransaction(function () use ($ubs_table, $ubs_data_to_upsert, &$tempUbsStats) {
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
        } elseif ($isCustomers && $customerFullSyncCutoff !== null) {
            // Customer full sync: Get records updated after cutoff date (ignore last_synced_at)
            $sql = "
                        SELECT * FROM `$ubs_table` 
                        WHERE UPDATED_ON > '$customerFullSyncCutoff'
                        ORDER BY UPDATED_ON ASC
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

        // ✅ FORCE SYNC: For icitem and icgroup, always sync all records (no timestamp comparison)
        // ✅ CUSTOMER FULL SYNC: For customers within full sync period, sync UBS→Remote without timestamp comparison
        // BUT: Still fetch remote data to allow Remote→UBS sync (critical for bidirectional sync)
        if ($isForceSync) {
            // Force sync: Convert all UBS data to remote format and upsert (ignore remote data comparison)
            ProgressDisplay::info("🔄 Using Force Sync mode - skipping timestamp comparison for this chunk");
            $remote_data_to_upsert = [];
            foreach ($ubs_data as $row) {
                $remote_data_to_upsert[] = convert(Converter::table_convert_remote($ubs_table), $row, 'to_remote');
            }
            $ubs_data_to_upsert = [];
        } elseif ($isCustomers && $customerFullSyncCutoff !== null) {
            // Customer full sync: Sync UBS→Remote without timestamp comparison, but still fetch remote data for Remote→UBS
            ProgressDisplay::info("🔄 Using Customer Full Sync mode - UBS→Remote without timestamp comparison, but allowing Remote→UBS");
            // Fetch remote data using cutoff date for comparison (allows Remote→UBS sync)
            $chunk_remote_data = [];
            if (!empty($chunk_keys)) {
                $chunk_remote_data = fetchRemoteDataByKeys($ubs_table, $chunk_keys, $customerFullSyncCutoff, null);
            }

            // UBS→Remote: Sync all UBS data without timestamp comparison (full sync)
            $remote_data_to_upsert = [];
            foreach ($ubs_data as $row) {
                $remote_data_to_upsert[] = convert(Converter::table_convert_remote($ubs_table), $row, 'to_remote');
            }

            // Remote→UBS: Find remote records not in UBS chunk and sync them
            $ubs_data_to_upsert = [];
            if (!empty($chunk_remote_data)) {
                $ubs_key = Converter::primaryKey($ubs_table);
                $ubs_key_set = [];
                foreach ($ubs_data as $row) {
                    $key = $row[$ubs_key] ?? '';
                    if (!empty($key)) {
                        $ubs_key_set[$key] = true;
                    }
                }
                // Find remote records that don't exist in current UBS chunk
                $remote_key = Converter::primaryKey(Converter::table_convert_remote($ubs_table));
                foreach ($chunk_remote_data as $remote_row) {
                    $remoteKey = $remote_row[$remote_key] ?? '';
                    if (!empty($remoteKey) && !isset($ubs_key_set[$remoteKey])) {
                        // Remote record exists but not in current UBS chunk - sync to UBS
                        $ubs_data_to_upsert[] = convert(Converter::table_convert_remote($ubs_table), $remote_row, 'to_ubs');
                    }
                }
            }
        } else {
            // Normal sync: Fetch remote data and compare
            $chunk_remote_data = [];
            if (!empty($chunk_keys)) {
                $updatedAfter = $last_synced_at;
                // For artran/ictran, also check order_date (handled in fetchRemoteDataByKeys)
                // Pass resync_date if in resync mode
                $chunk_remote_data = fetchRemoteDataByKeys(
                    $ubs_table,
                    $chunk_keys,
                    ($resync_mode && $resync_date) ? null : $updatedAfter,
                    ($resync_mode && $resync_date) ? $resync_date : null
                );
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
        }

        // ✅ SAFE: Use transaction wrapper for data integrity
        // Use batch processing for better performance
        if (!empty($remote_data_to_upsert) || !empty($ubs_data_to_upsert)) {
            $remoteStats = ['inserts' => [], 'updates' => []];
            $ubsStats = ['inserts' => [], 'updates' => []];

            executeSyncWithTransaction(function () use ($ubs_table, $remote_data_to_upsert, $ubs_data_to_upsert, &$remoteStats, &$ubsStats) {
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

    // ✅ For artran/ictran/customers: Also check for missing remote records (Remote → UBS)
    // This ensures records created in remote but not yet in UBS are synced
    if ($needsSpecialHandling && $ubsCount > 0) {

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
        } elseif ($isCustomers && $customerFullSyncCutoff !== null) {
            // Customer full sync: Use cutoff date instead of last_synced_at for missing remote records
            $missingRemoteSql = "SELECT * FROM $remote_table_name WHERE $column_updated_at > '$customerFullSyncCutoff'";
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
                $keyColumns = array_map(function ($k) {
                    return "`$k`";
                }, $ubs_key);
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
                    executeSyncWithTransaction(function () use ($ubs_table, $ubs_data_to_upsert, &$tempUbsStats2, $isArtran) {
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
    }

    // ✅ OPTIMIZATION: Skip remote-only sync when UBS is empty
    // This is handled per chunk above, no need to load all remote data here

    // Handle table-specific triggers
    $table_trigger_reset = ['customer', 'orders'];
    $remote_table_name = Converter::table_convert_remote($ubs_table);
    if (in_array($remote_table_name, $table_trigger_reset)) {
        ProgressDisplay::info("Resetting triggers for $remote_table_name");
        $Core = Core::getInstance();
        $Core->initRemoteData();
    }

    // Link customers to users after customers sync
    if ($remote_table_name === 'customers') {

        $db_remote = new mysql();
        $db_remote->connect_remote();
        linkCustomersToUsers($db_remote);
        $db_remote->close();
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


    // Memory cleanup between tables
    gc_collect_cycles();

    // Add small delay to prevent file lock conflicts
    usleep(500000); // 0.5 second delay

    echo "🔍 Finished processing $ubs_table, moving to next table...\n";
}

echo "DONE !!!";

