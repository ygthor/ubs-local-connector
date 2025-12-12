<?php

/**
 * Delete Duplicates on Server (Remote Database)
 * 
 * This script identifies and removes duplicate records from all tables in the remote database.
 * For each duplicate group, it keeps the most recent record (based on updated_at) and deletes the rest.
 * 
 * Usage: php delete_duplicates.php [--table=table_name] [--dry-run]
 * 
 * Options:
 *   --table=table_name  : Only check specific table (e.g., orders, customers)
 *   --dry-run          : Show duplicates without deleting them
 */

include(__DIR__ . '/bootstrap/app.php');
include(__DIR__ . '/bootstrap/cache.php');

// Initialize sync environment and progress display
initializeSyncEnvironment();
ProgressDisplay::start("🔍 Starting Duplicate Detection and Cleanup on Server");

// Parse command line arguments
$options = getopt("", ["table:", "dry-run"]);
$specific_table = $options['table'] ?? null;
$dry_run = isset($options['dry-run']);

if ($dry_run) {
    ProgressDisplay::info("🔍 DRY RUN MODE - No records will be deleted");
}

try {
    $db = new mysql();
    $db->connect_remote();
    
    $start_time = microtime(true);
    $memory_start = getMemoryUsage();
    
    // Define all tables and their primary keys for duplicate checking
    $tables_config = [
        'orders' => [
            'primary_key' => 'reference_no',
            'primary_key_field' => 'reference_no',
            'id_field' => 'id',
            'timestamp_field' => 'updated_at',
            'related_tables' => ['order_items' => 'order_id'] // Delete related records too
        ],
        'order_items' => [
            'primary_key' => 'unique_key',
            'primary_key_field' => 'unique_key',
            'id_field' => 'id',
            'timestamp_field' => 'updated_at',
            'related_tables' => []
        ],
        'customers' => [
            'primary_key' => 'customer_code',
            'primary_key_field' => 'customer_code',
            'id_field' => 'id',
            'timestamp_field' => 'updated_at',
            'related_tables' => []
        ],
        'gldata' => [
            'primary_key' => 'ACCNO',
            'primary_key_field' => 'ACCNO',
            'id_field' => 'id',
            'timestamp_field' => 'UPDATED_ON',
            'related_tables' => []
        ],
        'icitem' => [
            'primary_key' => 'ITEMNO',
            'primary_key_field' => 'ITEMNO',
            'id_field' => 'id',
            'timestamp_field' => 'UPDATED_ON',
            'related_tables' => []
        ],
        'icgroup' => [
            'primary_key' => 'name',
            'primary_key_field' => 'name',
            'id_field' => 'id',
            'timestamp_field' => 'UPDATED_ON',
            'related_tables' => []
        ]
    ];
    
    // Filter to specific table if requested
    if ($specific_table) {
        if (!isset($tables_config[$specific_table])) {
            ProgressDisplay::error("❌ Unknown table: $specific_table");
            ProgressDisplay::info("Available tables: " . implode(', ', array_keys($tables_config)));
            exit(1);
        }
        $tables_config = [$specific_table => $tables_config[$specific_table]];
    }
    
    $total_stats = [
        'tables_checked' => 0,
        'tables_with_duplicates' => 0,
        'total_duplicate_groups' => 0,
        'total_duplicates_found' => 0,
        'total_deleted' => 0,
        'total_related_deleted' => 0,
        'execution_time' => 0,
        'total_queries' => 0
    ];
    
    // Process each table
    foreach ($tables_config as $table_name => $config) {
        $table_start_time = microtime(true);
        $total_stats['tables_checked']++;
        
        ProgressDisplay::info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        ProgressDisplay::info("📦 Checking table: $table_name (primary key: {$config['primary_key']})");
        
        $primary_key = $config['primary_key_field'];
        $id_field = $config['id_field'];
        $timestamp_field = $config['timestamp_field'];
        $related_tables = $config['related_tables'];
        
        // Check if table exists
        $table_exists_sql = "SHOW TABLES LIKE '$table_name'";
        $table_exists = $db->first($table_exists_sql);
        if (!$table_exists) {
            ProgressDisplay::warning("⚠️  Table '$table_name' does not exist. Skipping...");
            continue;
        }
        
        // Find duplicates
        $duplicates_sql = "
            SELECT 
                `$primary_key` as duplicate_key,
                COUNT(*) as count,
                GROUP_CONCAT(`$id_field` ORDER BY `$timestamp_field` DESC, `$id_field` DESC) as ids
            FROM `$table_name`
            WHERE `$primary_key` IS NOT NULL AND `$primary_key` != ''
            GROUP BY `$primary_key`
            HAVING COUNT(*) > 1
            ORDER BY count DESC
        ";
        
        $duplicate_groups = $db->get($duplicates_sql);
        $total_stats['total_queries']++;
        
        if (empty($duplicate_groups)) {
            ProgressDisplay::info("✅ No duplicates found in $table_name");
            $table_end_time = microtime(true);
            continue;
        }
        
        $total_stats['tables_with_duplicates']++;
        $duplicate_groups_count = count($duplicate_groups);
        $total_stats['total_duplicate_groups'] += $duplicate_groups_count;
        
        ProgressDisplay::warning("⚠️  Found $duplicate_groups_count duplicate group(s) in $table_name");
        
        $table_duplicates = 0;
        $table_deleted = 0;
        $table_related_deleted = 0;
        
        foreach ($duplicate_groups as $group) {
            $duplicate_key = $group['duplicate_key'];
            $count = (int)$group['count'];
            $ids = array_map('intval', explode(',', $group['ids']));
            
            $table_duplicates += ($count - 1); // Minus 1 because we keep one
            $total_stats['total_duplicates_found'] += ($count - 1);
            
            ProgressDisplay::info("  📋 Key '$duplicate_key': $count duplicate(s) found (IDs: " . implode(', ', $ids) . ")");
            
            // Get all records with this key, sorted by most recent first
            $records_sql = "
                SELECT `$id_field`, `$timestamp_field`, created_at
                FROM `$table_name`
                WHERE `$primary_key` = '" . $db->escape($duplicate_key) . "'
                ORDER BY `$timestamp_field` DESC, `$id_field` DESC
            ";
            
            $records = $db->get($records_sql);
            $total_stats['total_queries']++;
            
            if (count($records) > 1) {
                // Keep the first (most recent) record
                $keep_record = $records[0];
                $keep_id = $keep_record[$id_field];
                
                // Delete all other duplicates
                $delete_ids = [];
                foreach (array_slice($records, 1) as $record) {
                    $delete_ids[] = (int)$record[$id_field];
                }
                
                if (!empty($delete_ids)) {
                    $delete_ids_str = implode(',', $delete_ids);
                    
                    if ($dry_run) {
                        ProgressDisplay::info("    🔍 [DRY RUN] Would keep ID: $keep_id, would delete: " . implode(', ', $delete_ids));
                        
                        // Also show related records that would be deleted
                        foreach ($related_tables as $related_table => $foreign_key) {
                            $related_check_sql = "
                                SELECT COUNT(*) as count 
                                FROM `$related_table` 
                                WHERE `$foreign_key` IN ($delete_ids_str)
                            ";
                            $related_count = $db->first($related_check_sql);
                            if ($related_count && $related_count['count'] > 0) {
                                ProgressDisplay::info("    🔍 [DRY RUN] Would also delete $related_count[count] related record(s) from $related_table");
                            }
                        }
                    } else {
                        // Delete duplicates
                        $delete_sql = "DELETE FROM `$table_name` WHERE `$id_field` IN ($delete_ids_str)";
                        $db->query($delete_sql);
                        $total_stats['total_queries']++;
                        
                        $deleted_count = count($delete_ids);
                        $table_deleted += $deleted_count;
                        $total_stats['total_deleted'] += $deleted_count;
                        
                        ProgressDisplay::info("    ✅ Kept ID: $keep_id (most recent), deleted $deleted_count duplicate(s): " . implode(', ', $delete_ids));
                        
                        // Delete related records from related tables
                        foreach ($related_tables as $related_table => $foreign_key) {
                            $related_delete_sql = "DELETE FROM `$related_table` WHERE `$foreign_key` IN ($delete_ids_str)";
                            $db->query($related_delete_sql);
                            $total_stats['total_queries']++;
                            
                            $affected_related = mysqli_affected_rows($db->con);
                            if ($affected_related > 0) {
                                $table_related_deleted += $affected_related;
                                $total_stats['total_related_deleted'] += $affected_related;
                                ProgressDisplay::info("    🗑️  Also deleted $affected_related associated record(s) from $related_table");
                            }
                        }
                    }
                }
            }
        }
        
        $table_end_time = microtime(true);
        $table_execution_time = round($table_end_time - $table_start_time, 2);
        
        if ($table_duplicates > 0) {
            if ($dry_run) {
                ProgressDisplay::info("📊 $table_name: Found $table_duplicates duplicate(s) in $duplicate_groups_count group(s) (Time: {$table_execution_time}s)");
            } else {
                ProgressDisplay::info("📊 $table_name: Deleted $table_deleted duplicate(s) from $duplicate_groups_count group(s) (Time: {$table_execution_time}s)");
                if ($table_related_deleted > 0) {
                    ProgressDisplay::info("   Also deleted $table_related_deleted related record(s)");
                }
            }
        }
    }
    
    // Final summary
    $end_time = microtime(true);
    $total_stats['execution_time'] = round($end_time - $start_time, 2);
    $memory_end = getMemoryUsage();
    $memory_used = round($memory_end - $memory_start, 2);
    
    ProgressDisplay::info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    ProgressDisplay::info("📊 Duplicate Cleanup Summary:");
    ProgressDisplay::info("    • Tables checked: " . $total_stats['tables_checked']);
    ProgressDisplay::info("    • Tables with duplicates: " . $total_stats['tables_with_duplicates']);
    ProgressDisplay::info("    • Total duplicate groups: " . $total_stats['total_duplicate_groups']);
    ProgressDisplay::info("    • Total duplicates found: " . $total_stats['total_duplicates_found']);
    
    if ($dry_run) {
        ProgressDisplay::info("    • Mode: DRY RUN (no records deleted)");
    } else {
        ProgressDisplay::info("    • Total deleted: " . $total_stats['total_deleted']);
        if ($total_stats['total_related_deleted'] > 0) {
            ProgressDisplay::info("    • Related records deleted: " . $total_stats['total_related_deleted']);
        }
    }
    
    ProgressDisplay::info("    • Execution time: " . $total_stats['execution_time'] . "s");
    ProgressDisplay::info("    • Memory used: " . $memory_used . " MB");
    ProgressDisplay::info("    • Total queries: " . $total_stats['total_queries']);
    
    if ($total_stats['total_duplicates_found'] > 0) {
        if ($dry_run) {
            ProgressDisplay::complete("✅ Duplicate detection completed. Run without --dry-run to delete duplicates.");
        } else {
            ProgressDisplay::complete("✅ Duplicate cleanup completed successfully!");
        }
    } else {
        ProgressDisplay::complete("✅ No duplicates found. Database is clean!");
    }
    
} catch (Exception $e) {
    ProgressDisplay::error("❌ Error during duplicate cleanup: " . $e->getMessage());
    ProgressDisplay::error("Stack trace: " . $e->getTraceAsString());
    exit(1);
}
