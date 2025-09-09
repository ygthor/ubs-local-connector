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
    $last_synced_at = lastSyncAt();
    $last_synced_at = "2000-08-01 00:20:00"; // For testing purpose, set to a fixed date
    
    ProgressDisplay::info("Last sync time: $last_synced_at");
    ProgressDisplay::info("Memory limit set to: " . ini_get('memory_limit'));
    
    $ubsTables = Converter::ubsTable();
    $totalTables = count($ubsTables);
    
    ProgressDisplay::info("Found $totalTables tables to sync: " . implode(', ', $ubsTables));
    
    $processedTables = 0;
    
    foreach($ubsTables as $ubs_table) {
        $processedTables++;
        ProgressDisplay::display("Processing table $ubs_table", $processedTables, $totalTables);
        
        try {
            // Check if we can resume from previous run
            $resumeData = canResumeSync();
            if ($resumeData && $resumeData['table'] === $ubs_table) {
                ProgressDisplay::info("Resuming sync for $ubs_table from previous run");
                ProgressDisplay::info("Previous progress: {$resumeData['processed_records']}/{$resumeData['total_records']} records");
            }
            
            // Get data counts first for better progress tracking
            $countSql = "SELECT COUNT(*) as total FROM `$ubs_table` WHERE UPDATED_ON > '$last_synced_at'";
            $ubsCount = $db->first($countSql)['total'];
            
            ProgressDisplay::info("Found $ubsCount UBS records to process for $ubs_table");
            
            if ($ubsCount == 0) {
                ProgressDisplay::info("No UBS data to sync for $ubs_table, skipping...");
                continue;
            }
            
            // Start cache tracking
            startSyncCache($ubs_table, $ubsCount);
            
            // Fetch remote data once for the entire table
            $remote_data = fetchServerData($ubs_table, $last_synced_at);
            ProgressDisplay::info("Fetched " . count($remote_data) . " remote records for $ubs_table");
            
            // Process data in chunks to avoid memory issues
            $chunkSize = 2000; // Process 2000 records at a time for better memory management
            $offset = 0;
            $processedRecords = 0;
            
            while ($offset < $ubsCount) {
                $sql = "
                    SELECT * FROM `$ubs_table` 
                    WHERE UPDATED_ON > '$last_synced_at'
                    LIMIT $chunkSize OFFSET $offset
                ";
                
                $ubs_data = $db->get($sql);
                
                if (empty($ubs_data)) {
                    break;
                }
                
                ProgressDisplay::info("Syncing " . count($ubs_data) . " UBS records with " . count($remote_data) . " remote records");
                
                $comparedData = syncEntity($ubs_table, $ubs_data, $remote_data);
                
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
                updateSyncCache($processedRecords, $offset);
                
                // Memory cleanup between chunks
                gc_collect_cycles();
                
                ProgressDisplay::display("Processed $ubs_table", $processedRecords, $ubsCount);
            }
            
            // Handle table-specific triggers
            $table_trigger_reset = ['customer','orders'];
            $remote_table_name = Converter::table_convert_remote($ubs_table);
            if(in_array($remote_table_name, $table_trigger_reset)) {
                ProgressDisplay::info("Resetting triggers for $remote_table_name");
                $Core = Core::getInstance();
                $Core->initRemoteData();
            }
            
            ProgressDisplay::info("✅ Completed sync for $ubs_table");
            
            // Complete cache for this table
            completeSyncCache();
            
        } catch (Exception $e) {
            ProgressDisplay::error("Failed to sync $ubs_table: " . $e->getMessage());
            // Clear cache on error
            clearSyncCache();
            // Continue with next table instead of stopping
            continue;
        }
        
        // Memory cleanup between tables
        gc_collect_cycles();
    }
    
    // Log successful sync
    $db->insert('sync_logs', [
        'synced_at' => date('Y-m-d H:i:s')
    ]);
    
    ProgressDisplay::complete("🎉 Sync process completed successfully! All $totalTables tables processed.");
    
} catch (Exception $e) {
    ProgressDisplay::error("Sync process failed: " . $e->getMessage());
    exit(1);
}