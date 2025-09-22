<?php

use XBase\DataConverter\Field\DBase7\TimestampConverter;

include(__DIR__ . '/bootstrap/app.php');

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
        // ProgressDisplay::display("Processing table $ubs_table", $processedTables, $totalTables);
        
        try {
            // Get data counts first for better progress tracking
            $countSql = "SELECT COUNT(*) as total FROM `$ubs_table` WHERE UPDATED_ON > '$last_synced_at'";
            $ubsCount = $db->first($countSql)['total'];
            
            ProgressDisplay::info("Found $ubsCount UBS records to process for $ubs_table");
            
            if ($ubsCount == 0) {
                ProgressDisplay::info("No UBS data to sync for $ubs_table, skipping...");
                continue;
            }
            
            // Process data in chunks to avoid memory issues
            $chunkSize = 5000; // Process 5000 records at a time
            $offset = 0;
            $processedRecords = 0;
            
            while ($offset < $ubsCount) {
                $sql = "
                    SELECT * FROM `$ubs_table` 
                    WHERE UPDATED_ON > '$last_synced_at'
                    LIMIT $chunkSize OFFSET $offset
                ";
                
                $ubs_data = $db->get($sql);
                $remote_data = fetchServerData($ubs_table, $last_synced_at);
                
                if (empty($ubs_data) && empty($remote_data)) {
                    break;
                }
                
                ProgressDisplay::info("Syncing " . count($ubs_data) . " UBS records and " . count($remote_data) . " remote records");
                
                $comparedData = syncEntity($ubs_table, $ubs_data, $remote_data);
                
                $remote_data = $comparedData['remote_data'];
                $ubs_data = $comparedData['ubs_data'];
                
                // Use batch processing for better performance
                if (!empty($remote_data)) {
                    ProgressDisplay::info("Upserting " . count($remote_data) . " remote records");
                    batchUpsertRemote($ubs_table, $remote_data);
                }
                
                if (!empty($ubs_data)) {
                    ProgressDisplay::info("Upserting " . count($ubs_data) . " UBS records");
                    batchUpsertUbs($ubs_table, $ubs_data);
                }
                
                $processedRecords += count($ubs_data);
                $offset += $chunkSize;
                
                // Memory cleanup between chunks
                gc_collect_cycles();
                
                // ProgressDisplay::display("Processed $ubs_table", $processedRecords, $ubsCount);
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
            
        } catch (Exception $e) {
            ProgressDisplay::error("Failed to sync $ubs_table: " . $e->getMessage());
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