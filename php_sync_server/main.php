<?php

use XBase\DataConverter\Field\DBase7\TimestampConverter;

include(__DIR__ . '/bootstrap/app.php');
include(__DIR__ . '/bootstrap/cache.php');

// Initialize sync environment and progress display
initializeSyncEnvironment();
ProgressDisplay::start("🚀 Starting UBS Local Connector Sync Process");

try {
    $db = new mysql();
    
    // Get last sync time
    // $last_synced_at = lastSyncAt(); // Commented out for full sync
    $last_synced_at = null; // Set to null for FULL SYNC (process all records)
    
    // Clear sync cache for full sync
    clearSyncCache();
    ProgressDisplay::info("🧹 Cleared sync cache for full sync");
    
    // If no last sync time, use a date far in the past to get all records
    if (empty($last_synced_at)) {
        $last_synced_at = '2025-08-01 00:00:00';
    }
    
    // ProgressDisplay::info("Last sync time: $last_synced_at");
    // ProgressDisplay::info("Memory limit set to: " . ini_get('memory_limit'));
    
    $ubsTables = Converter::ubsTable();
    $totalTables = count($ubsTables);
    
    ProgressDisplay::info("Found $totalTables tables to sync: " . implode(', ', $ubsTables));
    ProgressDisplay::info("🕐 Syncing records updated after: $last_synced_at");
    
    $processedTables = 0;
    $syncResults = []; // Track sync results for each table
    
    foreach($ubsTables as $ubs_table) {
        // Temporarily exclude artrans table
        $remote_table_name = Converter::table_convert_remote($ubs_table);
        if ($remote_table_name === 'artrans') {
            ProgressDisplay::info("⏭️  Skipping $ubs_table (artrans temporarily excluded)");
            continue;
        }
        if ($remote_table_name === 'ictran') {
            ProgressDisplay::info("⏭️  Skipping $ubs_table (artrans temporarily excluded)");
            continue;
        }
        
        $processedTables++;
        ProgressDisplay::info("📁 Processing table $processedTables/$totalTables: $ubs_table");
        
        try {
            // ProgressDisplay::info("🔍 Inside try block for $ubs_table");
            // Check if we can resume from previous run (DISABLED for full sync)
            // $resumeData = canResumeSync();
            // if ($resumeData && $resumeData['table'] === $ubs_table) {
            //     ProgressDisplay::info("Resuming sync for $ubs_table from previous run");
            //     ProgressDisplay::info("Previous progress: {$resumeData['processed_records']}/{$resumeData['total_records']} records");
            // }
            
            // Get data counts first for better progress tracking
            
            try {
                $countSql = "SELECT COUNT(*) as total FROM `$ubs_table` WHERE UPDATED_ON > '$last_synced_at'";
                $ubsCount = $db->first($countSql)['total'];
                
                // Debug: Check if table exists and has any records at all
                $totalCountSql = "SELECT COUNT(*) as total FROM `$ubs_table`";
                $totalCount = $db->first($totalCountSql)['total'];
                
                ProgressDisplay::info("📊 Found $ubsCount UBS records to process for $ubs_table (Total in table: $totalCount)");
            } catch (Exception $e) {
                ProgressDisplay::error("❌ Error checking table $ubs_table: " . $e->getMessage());
                continue;
            }
            
            // Always fetch remote data to check for server-side updates
            // ProgressDisplay::info("🔍 About to fetch remote data for $ubs_table");
            try {
                $remote_data = fetchServerData($ubs_table, $last_synced_at);
                $remoteCount = count($remote_data);
                ProgressDisplay::info("📊 Found $remoteCount remote records to compare for $ubs_table");
            } catch (Exception $e) {
                ProgressDisplay::error("❌ Error fetching remote data for $ubs_table: " . $e->getMessage());
                $remote_data = [];
                $remoteCount = 0;
            }
            
            // ProgressDisplay::info("Fetched $remoteCount remote records for $ubs_table");
            // If no data on either side, skip this table
            if ($ubsCount == 0 && $remoteCount == 0) {
                ProgressDisplay::info("No data to sync for $ubs_table (no UBS or remote updates), skipping...");
                continue;
            }
            
            // Start cache tracking with total records to process
            $totalRecordsToProcess = max($ubsCount, $remoteCount);
            // startSyncCache($ubs_table, $totalRecordsToProcess);
            
            // Process data in chunks to avoid memory issues
            $chunkSize = 500; // Reduced chunk size to prevent file lock conflicts
            $offset = 0;
            $processedRecords = 0;
            $maxIterations = 100; // Safety limit to prevent infinite loops
            $iterationCount = 0;
            
            
            // Process UBS data in chunks if it exists
            // dd("$offset < $ubsCount && $iterationCount < $maxIterations");
            while ($offset < $ubsCount && $iterationCount < $maxIterations) {
              
                $iterationCount++;
                $sql = "
                    SELECT * FROM `$ubs_table` 
                    WHERE UPDATED_ON > '$last_synced_at'
                    
                    ORDER BY UPDATED_ON ASC
                    LIMIT $chunkSize OFFSET $offset
                ";
                
                $ubs_data = $db->get($sql);
                
                if (empty($ubs_data)) break;
                
                // Validate and fix UPDATED_ON fields in UBS data
                $ubs_data = validateAndFixUpdatedOn($ubs_data);
                
                ProgressDisplay::info("Syncing " . count($ubs_data) . " UBS records with " . count($remote_data) . " remote records");
                
                // Debug output removed - loop issue fixed with ORDER BY clause
                // echo "🔍 About to call syncEntity for $ubs_table\n";
                $comparedData = syncEntity($ubs_table, $ubs_data, $remote_data);

                // dd($comparedData);
                
                ProgressDisplay::info("🔍 syncEntity completed for $ubs_table");
                // echo "🔍 After syncEntity, about to process results\n";
                
                $remote_data_to_upsert = $comparedData['remote_data'];
                $ubs_data_to_upsert = $comparedData['ubs_data'];
                
                // Use batch processing for better performance
                if (!empty($remote_data_to_upsert)) {
                    ProgressDisplay::info("Upserting " . count($remote_data_to_upsert) . " remote records");
                    batchUpsertRemote($ubs_table, $remote_data_to_upsert);
                }

                if (!empty($ubs_data_to_upsert)) {
                    ProgressDisplay::info("Upserting " . count($ubs_data_to_upsert) . " UBS records");
                    batchUpsertUbs($ubs_table, $ubs_data_to_upsert);
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
                ProgressDisplay::info("✅ All UBS records processed for $ubs_table");
            }
            
            // If no UBS data but remote data exists, sync remote-only changes
            if ($ubsCount == 0 && $remoteCount > 0) {
                ProgressDisplay::info("No UBS data, but found $remoteCount remote records to sync to UBS");
                
                $comparedData = syncEntity($ubs_table, [], $remote_data);
                
                $ubs_data_to_upsert = $comparedData['ubs_data'];
                
                if (!empty($ubs_data_to_upsert)) {
                    ProgressDisplay::info("Upserting " . count($ubs_data_to_upsert) . " remote records to UBS");
                    batchUpsertUbs($ubs_table, $ubs_data_to_upsert);
                }
                
                $processedRecords = $remoteCount;
                updateSyncCache($processedRecords, $processedRecords);
            }
            
            // Handle table-specific triggers
            $table_trigger_reset = ['customer','orders'];
            $remote_table_name = Converter::table_convert_remote($ubs_table);
            if(in_array($remote_table_name, $table_trigger_reset)) {
                ProgressDisplay::info("Resetting triggers for $remote_table_name");
                $Core = Core::getInstance();
                $Core->initRemoteData();
            }
            
            // Track sync results for this table
            $syncResults[] = [
                'table' => $ubs_table,
                'ubs_count' => $ubsCount,
                'remote_count' => $remoteCount,
                'processed_records' => $processedRecords
            ];
            
            ProgressDisplay::info("✅ Completed sync for $ubs_table (UBS: $ubsCount, Remote: $remoteCount, Processed: $processedRecords)");
            
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
    
    ProgressDisplay::complete("🎉 Sync process completed successfully! All " . count($syncResults) . " tables processed.");
    
} catch (Exception $e) {
    ProgressDisplay::error("Sync process failed: " . $e->getMessage());
    exit(1);
}