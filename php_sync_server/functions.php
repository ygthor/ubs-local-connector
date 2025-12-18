<?php

// Configuration and setup functions
function initializeSyncEnvironment()
{
    // Set memory limit for large data processing
    ini_set('memory_limit', '4G');

    // Enable garbage collection
    ini_set('zend.enable_gc', 1);

    // Set execution time limit (0 = unlimited)
    set_time_limit(0);

    // Disable output buffering for real-time progress display
    if (ob_get_level()) {
        ob_end_flush();
    }

    // Set error reporting - suppress warnings for cleaner CLI output
    error_reporting(E_ERROR | E_PARSE);
    ini_set('display_errors', 0);

    // Log configuration - suppressed for cleaner output
    // if (function_exists('dump')) {
    //     dump("🚀 Sync environment initialized:");
    //     dump("- Memory limit: " . ini_get('memory_limit'));
    //     dump("- Execution time limit: " . ini_get('max_execution_time'));
    //     dump("- Garbage collection: " . (ini_get('zend.enable_gc') ? 'Enabled' : 'Disabled'));
    // }
    
    // Clean up old backup files (older than 7 days) at the start of sync
    if (function_exists('cleanupAllOldBackups')) {
        cleanupAllOldBackups();
    }
}

// Memory management functions
function increaseMemoryLimit($limit = '512M')
{
    ini_set('memory_limit', $limit);
    return ini_get('memory_limit');
}

function getMemoryUsage()
{
    return [
        'memory_limit' => ini_get('memory_limit'),
        'memory_usage' => memory_get_usage(true),
        'memory_peak' => memory_get_peak_usage(true),
        'memory_usage_mb' => round(memory_get_usage(true) / 1024 / 1024, 2),
        'memory_peak_mb' => round(memory_get_peak_usage(true) / 1024 / 1024, 2)
    ];
}

/**
 * Validates and sets timestamp field in record if missing or invalid
 * Preserves existing valid timestamps, only sets current time as fallback
 * 
 * @param array &$record The record array to modify
 * @param string $field_name The timestamp field name (e.g., 'updated_at', 'UPDATED_ON')
 */
function ensureValidTimestamp(&$record, $field_name)
{
    if (!isset($record[$field_name]) || 
        empty($record[$field_name]) || 
        $record[$field_name] === '0000-00-00' || 
        $record[$field_name] === '0000-00-00 00:00:00' ||
        strtotime($record[$field_name]) === false) {
        $record[$field_name] = date('Y-m-d H:i:s');
    }
}

// Enhanced progress display system
class ProgressDisplay
{
    private static $startTime;
    private static $lastUpdate = 0;
    private static $updateInterval = 1; // Update every 1 second

    public static function start($message = "Starting process...")
    {
        self::$startTime = microtime(true);
        self::display($message, 0, 0, true);
    }

    // DONT USE, it stuck..
    public static function display($message, $current = 0, $total = 0, $force = false)
    {
        $now = microtime(true);

        // Only update if forced or enough time has passed
        if (!$force && ($now - self::$lastUpdate) < self::$updateInterval) {
            return;
        }

        self::$lastUpdate = $now;

        $memory = getMemoryUsage();
        $elapsed = $now - self::$startTime;

        // Clear line and move cursor to beginning
        echo "\r\033[K";

        if ($total > 0) {
            $percentage = round(($current / $total) * 100, 1);
            $barLength = 30;
            $filledLength = round(($percentage / 100) * $barLength);
            $bar = str_repeat('█', $filledLength) . str_repeat('░', $barLength - $filledLength);

            $eta = self::calculateETA($current, $total, $elapsed);

            echo sprintf(
                "[%s] %s (%d/%d) %s%% | Memory: %sMB | Time: %s | ETA: %s",
                date('H:i:s'),
                $bar,
                $current,
                $total,
                $percentage,
                $memory['memory_usage_mb'],
                self::formatTime($elapsed),
                $eta
            );
        } else {
            echo sprintf(
                "[%s] %s | Memory: %sMB | Time: %s",
                date('H:i:s'),
                $message,
                $memory['memory_usage_mb'],
                self::formatTime($elapsed)
            );
        }

        // Flush output to ensure immediate display
        flush();
    }

    public static function complete($message = "Process completed!")
    {
        $elapsed = microtime(true) - self::$startTime;
        $memory = getMemoryUsage();

        echo "\n";
        echo "✅ " . $message . "\n";
        echo "📊 Total Time: " . self::formatTime($elapsed) . "\n";
        echo "💾 Peak Memory: " . $memory['memory_peak_mb'] . "MB\n";
        echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n";
    }

    public static function error($message)
    {
        echo "\n❌ ERROR: " . $message . "\n";
    }

    public static function warning($message)
    {
        // Suppress warnings for cleaner output - only show critical warnings
        // echo "\n⚠️  WARNING: " . $message . "\n";
    }

    public static function info($message)
    {
        echo "ℹ️  INFO: " . $message . "\n";
    }

    private static function calculateETA($current, $total, $elapsed)
    {
        if ($current <= 0 || $elapsed <= 0) {
            return "Calculating...";
        }

        $rate = $current / $elapsed;
        $remaining = $total - $current;
        $eta = $remaining / $rate;

        return self::formatTime($eta);
    }

    private static function formatTime($seconds)
    {
        if ($seconds < 60) {
            return round($seconds, 1) . "s";
        } elseif ($seconds < 3600) {
            $minutes = floor($seconds / 60);
            $seconds = $seconds % 60;
            return $minutes . "m " . round($seconds, 1) . "s";
        } else {
            $hours = floor($seconds / 3600);
            $minutes = floor(($seconds % 3600) / 60);
            return $hours . "h " . $minutes . "m";
        }
    }
}

function optimizeMemoryUsage()
{
    // Force garbage collection
    gc_collect_cycles();

    // Clear any cached data
    if (function_exists('opcache_reset')) {
        opcache_reset();
    }

    return getMemoryUsage();
}

function batchProcessData($data, $callback, $batchSize = 1000)
{
    $total = count($data);
    $processed = 0;
    $results = [];

    for ($i = 0; $i < $total; $i += $batchSize) {
        $batch = array_slice($data, $i, $batchSize);
        $batchResults = $callback($batch);
        $results = array_merge($results, $batchResults);

        $processed += count($batch);

        // Memory optimization between batches
        if ($i + $batchSize < $total) {
            gc_collect_cycles();
        }

        // Log progress
        $percentage = round(($processed / $total) * 100, 2);
        // dump("Processed: $processed/$total ($percentage%) - Memory: " . getMemoryUsage()['memory_usage_mb'] . "MB");
    }

    return $results;
}

// Note: batchProcessData has a deprecated parameter order warning in PHP 8.0+
// The function is currently not used in the codebase, but kept for potential future use

// High-performance batch processing functions
function batchUpsertRemote($table, $records, $batchSize = 1000)
{
    if (empty($records)) {
        return ['inserts' => [], 'updates' => []];
    }

    $stats = ['inserts' => [], 'updates' => []];

    // ✅ SAFE: Use retry logic for reliability
    retryOperation(function () use ($table, $records, $batchSize, &$stats) {
        $db = new mysql();
        $db->connect_remote();

        $remote_table_name = Converter::table_convert_remote($table);
        $primary_key = Converter::primaryKey($remote_table_name);
        $Core = Core::getInstance();

        $totalRecords = count($records);
        $processed = 0;

        ProgressDisplay::info("Starting high-performance batch upsert for $remote_table_name ($totalRecords records)");

        // Pre-process all records for better performance
        $processedRecords = [];
        foreach ($records as $record) {
            // Apply table-specific conversions
            $table_convert = ['orders'];
            if (in_array($remote_table_name, $table_convert)) {
                $customer_lists = $Core->remote_customer_lists;
                $customer_id = $customer_lists[$record['customer_code']] ?? null;
                $record['customer_id'] = $customer_id;
            }

            if ($remote_table_name == 'order_items') {
                $order_lists = $Core->remote_order_lists;
                $record[$primary_key] = $record['reference_no'] . '|' . $record['item_count'];
                $record['order_id'] = $order_lists[$record['reference_no']] ?? null;
            }

            // Remove CREATED_BY and UPDATED_BY fields that may not exist in remote tables
            // Check both uppercase and lowercase variations to be safe
            unset($record['CREATED_BY']);
            unset($record['UPDATED_BY']);
            unset($record['created_by']);
            unset($record['updated_by']);

            // ✅ FIX: Remove 'id' field - remote tables use auto-increment ID that shouldn't be set manually
            // This prevents "Column 'id' cannot be null" errors
            unset($record['id']);
            unset($record['ID']);

            // ✅ FIX: Use Converter::mapUpdatedAtField() to get correct timestamp field name
            // Some tables use 'updated_at' (lowercase), others use 'UPDATED_ON' (uppercase)
            $updated_at_field = Converter::mapUpdatedAtField($remote_table_name);
            ensureValidTimestamp($record, $updated_at_field);
            
            // Handle created_at field (optional - only if mapping exists)
            $created_at_field = Converter::mapCreatedAtField($remote_table_name);
            if ($created_at_field !== null) {
                ensureValidTimestamp($record, $created_at_field);
            }
            

            if (count($record) > 0) {
                $processedRecords[] = $record;
            }
        }

        // Ensure primary key is correctly detected before proceeding
        if (empty($primary_key)) {
            ProgressDisplay::error("❌ No primary key defined for table: $remote_table_name");
            return false;
        }

        // Use bulk upsert for better performance
        // bulkUpsert() handles ALL deduplication safely - it deletes old duplicates and keeps newest
        for ($i = 0; $i < count($processedRecords); $i += $batchSize) {
            $batch = array_slice($processedRecords, $i, $batchSize);

            // Bulk upsert using MySQL's ON DUPLICATE KEY UPDATE
            $batchStats = null;
            $db->bulkUpsert($remote_table_name, $batch, $primary_key, $batchStats);
            
            // Merge batch stats into overall stats
            if ($batchStats !== null) {
                $stats['inserts'] = array_merge($stats['inserts'], $batchStats['inserts']);
                $stats['updates'] = array_merge($stats['updates'], $batchStats['updates']);
            }

            $processed += count($batch);
            // ProgressDisplay::display("Processing $remote_table_name", $processed, $totalRecords);

            // Memory cleanup between batches
            if ($i + $batchSize < count($processedRecords)) {
                gc_collect_cycles();
            }
        }

        ProgressDisplay::info("Completed high-performance batch upsert for $remote_table_name");
        return true; // Success
    }, 3); // Max 3 retries
    
    return $stats;
}

function batchUpsertUbs($table, $records, $batchSize = 500)
{
    if (empty($records)) {
        return ['inserts' => [], 'updates' => []];
    }
    
    $stats = ['inserts' => [], 'updates' => []];

    $arr = parseUbsTable($table);
    $table_name = $arr['table'];
    $directory = strtoupper($arr['database']);
    $path = "C:/$directory/" . ENV::DBF_SUBPATH . "/{$table_name}.dbf";

    $keyField = Converter::primaryKey($table);
    $totalRecords = count($records);
    $processed = 0;

        // Track REFNOs for artran recalculation (only for ictran table)
        // Skip recalculation for TYPE='DO' (Delivery Order - inventory only, no need for GROSS_BIL/GRAND_BIL)
        $refNosToRecalculate = [];
        $refNosToSkip = []; // REFNOs with TYPE='DO' that should skip recalculation
        $artranTypeCache = []; // Cache for artran TYPE lookups to avoid repeated file reads

    // ✅ Initialize lock variable for cleanup
    $lockFp = null;
    $backupPath = null;

    ProgressDisplay::info("Starting batch upsert for UBS $table_name ($totalRecords records)");

    try {
        // ✅ SAFETY CHECK 1: Check if UBS software is running
        if (isUbsRunning()) {
            throw new Exception("❌ UBS software is currently running! Please close UBS before syncing to prevent DBF corruption.");
        }

        // ✅ Get primary key field name(s) for validation
        $primaryKeyField = is_array($keyField) ? $keyField : [$keyField];

        // Check if file exists and is accessible
        if (!file_exists($path)) {
            throw new Exception("DBF file not found: $path");
        }

        if (!is_readable($path)) {
            throw new Exception("DBF file is not readable: $path");
        }

        // ✅ SAFETY CHECK 2: Check if DBF file is locked/in use
        if (isDbfFileLocked($path)) {
            throw new Exception("❌ DBF file is currently locked or in use: $table_name. Another process may be accessing it. Please wait and try again.");
        }

        // ✅ SAFETY CHECK 3: Create backup before writing
        $backupPath = backupDbfFile($path);
        if ($backupPath === false) {
            ProgressDisplay::warning("⚠️  Could not create backup, but continuing anyway...");
        }

        // ✅ SAFETY CHECK 4: Acquire file lock
        $lockFp = acquireDbfLock($path);
        if ($lockFp === false) {
            throw new Exception("❌ Cannot acquire exclusive lock on DBF file: $table_name. File may be in use.");
        }

        // Try to detect and repair corruption before processing
        try {
            $testReader = new \XBase\TableReader($path);
            $testReader->close();
        } catch (\Throwable $e) {
            $errorMsg = strtolower($e->getMessage());
            if (
                strpos($errorMsg, 'clone') !== false ||
                strpos($errorMsg, 'corrupt') !== false ||
                strpos($errorMsg, 'length') !== false ||
                strpos($errorMsg, 'invalid') !== false ||
                strpos($errorMsg, 'bytes') !== false
            ) {
                ProgressDisplay::warning("Possible DBF corruption detected for $table_name: " . $e->getMessage());
                ProgressDisplay::info("Attempting automatic repair...");

                // Load repair function if not already loaded
                if (!function_exists('repairDbfFile')) {
                    $repairScript = __DIR__ . '/repair_dbf.php';
                    if (file_exists($repairScript)) {
                        require_once $repairScript;
                    }
                }

                if (function_exists('attemptDbfRepair') && attemptDbfRepair($path)) {
                    ProgressDisplay::info("DBF file repaired successfully, retrying...");
                    // Retry opening the file
                    try {
                        $testReader = new \XBase\TableReader($path);
                        $testReader->close();
                    } catch (\Throwable $retryError) {
                        ProgressDisplay::error("File still corrupted after repair attempt");
                        throw new Exception("DBF file appears corrupted and repair failed. Please run manually: php repair_dbf.php \"$path\"");
                    }
                } else {
                    ProgressDisplay::error("Failed to repair DBF file automatically");
                    ProgressDisplay::error("Please run manually: php repair_dbf.php \"$path\"");
                    throw new Exception("DBF file appears corrupted. Please repair it first: " . $e->getMessage());
                }
            } else {
                // Re-throw if it's not a corruption-related error
                throw $e;
            }
        }

    // Group records by operation type for better performance
    $updateRecords = [];
    $insertRecords = [];

    // ✅ SAFETY: Always use CLONE mode (never REALTIME) to prevent corruption
    $editMode = \XBase\TableEditor::EDIT_MODE_CLONE;
    $editor = null;
    $maxRetries = 3;
    $retryCount = 0;

    while ($retryCount < $maxRetries) {
        try {
            $editor = new \XBase\TableEditor($path, [
                'editMode' => $editMode,
            ]);
            break; // Success, exit retry loop
        } catch (\Throwable $e) {
            $retryCount++;
            if ($retryCount >= $maxRetries) {
                // ✅ REMOVED REALTIME FALLBACK - Always abort if CLONE fails
                // This prevents direct writes that could corrupt the file
                releaseDbfLock($lockFp);
                throw new Exception("❌ Failed to open DBF file in CLONE mode after $maxRetries retries. UBS may be running or file is corrupted. Error: " . $e->getMessage());
            }
            // Wait a bit before retry (exponential backoff)
            usleep(100000 * $retryCount); // 0.1s, 0.2s, 0.3s
        }
    }

    // Create index of existing records
    $existingRecords = [];
    try {
        while ($row = $editor->nextRecord()) {
            $key = getRecordKey($row, $keyField);
            $existingRecords[$key] = $row;
        }
        $editor->close();
    } catch (\Throwable $e) {
        if ($editor) {
            try {
                $editor->close();
            } catch (\Throwable $closeError) {
                // Ignore close errors
            }
        }
        throw new Exception("Failed to read existing records from $table_name: " . $e->getMessage());
    }

    // ✅ DEBUG: Log existing records count
    $existingCount = count($existingRecords);
    if ($existingCount > 0) {
        $existingKeys = array_keys($existingRecords);
        ProgressDisplay::info("🔍 DEBUG: Found $existingCount existing record(s) in $table_name DBF. Keys: " . implode(', ', array_slice($existingKeys, 0, 10)) . ($existingCount > 10 ? '...' : ''));
    } else {
        ProgressDisplay::info("🔍 DEBUG: No existing records found in $table_name DBF (file is empty or new)");
    }

    // Categorize records (store only keys, not row objects since they can't be reused)
    foreach ($records as $record) {
        $key = getRecordKey($record, $keyField);
        
        // ✅ DEBUG: Log categorization decision
        if (empty($key)) {
            ProgressDisplay::warning("⚠️  WARNING: Record has empty key, skipping. Available fields: " . implode(', ', array_keys($record)));
            continue;
        }

        // Track REFNO for artran recalculation (only for ictran table)
        // Skip recalculation for TYPE='DO' (Delivery Order - inventory only)
        if ($table === 'ubs_ubsstk2015_ictran' && isset($record['REFNO'])) {
            $refNo = trim($record['REFNO']);
            if ($refNo) {
                // Check cached TYPE first, then lookup if needed
                if (!isset($artranTypeCache[$refNo])) {
                    $artranTypeCache[$refNo] = getArtranTypeByRefNo($refNo, $directory);
                }
                $artranType = $artranTypeCache[$refNo];
                
                if ($artranType === 'DO') {
                    if (!in_array($refNo, $refNosToSkip)) {
                        $refNosToSkip[] = $refNo;
                    }
                } else {
                    // Only recalculate for non-DO types
                    if (!in_array($refNo, $refNosToRecalculate)) {
                        $refNosToRecalculate[] = $refNo;
                    }
                }
            }
        }

        if (isset($existingRecords[$key])) {
            ProgressDisplay::info("🔍 DEBUG: Record with key '$key' exists in DBF - will UPDATE");
            $updateRecords[] = ['key' => $key, 'record' => $record];
            $stats['updates'][] = $key;
        } else {
            ProgressDisplay::info("🔍 DEBUG: Record with key '$key' NOT found in DBF - will INSERT");
            $insertRecords[] = $record;
            $stats['inserts'][] = $key;
        }
    }

    // ✅ Extract existing keys BEFORE clearing existingRecords to avoid re-scanning the table
    $existingKeys = [];
    foreach (array_keys($existingRecords) as $key) {
        $existingKeys[$key] = true;
    }

    // Clear existingRecords to free memory (we only need the keys now)
    unset($existingRecords);

    // Process updates in batches
    if (!empty($updateRecords)) {
        $totalUpdates = count($updateRecords);
        ProgressDisplay::info("Processing $totalUpdates updates for $table_name");

        for ($i = 0; $i < count($updateRecords); $i += $batchSize) {
            $batch = array_slice($updateRecords, $i, $batchSize);
            $batchEditor = null;
            $retryCount = 0;
            $batchNum = (int)($i / $batchSize) + 1;
            $totalBatches = (int)ceil($totalUpdates / $batchSize);

            while ($retryCount < $maxRetries) {
                try {
                    $batchEditor = new \XBase\TableEditor($path, [
                        'editMode' => $editMode,
                    ]);

                    // Build index of record keys to positions for faster lookup
                    $recordPositions = [];
                    $batchEditor->moveTo(0);
                    $position = 0;
                    while ($row = $batchEditor->nextRecord()) {
                        $rowKey = getRecordKey($row, $keyField);
                        if (!isset($recordPositions[$rowKey])) {
                            $recordPositions[$rowKey] = $position;
                        }
                        $position++;
                    }

                    // Need to find the records again since we're opening a new editor
                    $batchIndex = 0;
                    foreach ($batch as $item) {
                        $batchIndex++;
                        $key = $item['key'];
                        $record = $item['record'];

                        // Show progress every 5 records or at the end
                        if ($batchIndex % 5 == 0 || $batchIndex == count($batch)) {
                            ProgressDisplay::info("  ⏳ Updating $batchIndex/" . count($batch) . " in batch $batchNum/$totalBatches...");
                        }

                        // Use position index for faster lookup if available
                        if (isset($recordPositions[$key])) {
                            // Try to move directly to position (may not work with all XBase versions)
                            try {
                                $batchEditor->moveTo($recordPositions[$key]);
                                $row = $batchEditor->nextRecord();
                                if ($row) {
                                    $rowKey = getRecordKey($row, $keyField);
                                    if ($rowKey === $key) {
                                        updateUbsRecord($batchEditor, $row, $record, $table_name);
                                        continue; // Success, move to next
                                    }
                                }
                            } catch (\Throwable $e) {
                                // Position lookup failed, fall back to scan
                            }
                        }

                        // Fallback: Scan from beginning (slower but reliable)
                        $batchEditor->moveTo(0);
                        $found = false;
                        while ($row = $batchEditor->nextRecord()) {
                            $rowKey = getRecordKey($row, $keyField);
                            if ($rowKey === $key) {
                                ProgressDisplay::info("🔍 DEBUG: Found record with key '$key' in DBF, updating...");
                                updateUbsRecord($batchEditor, $row, $record, $table_name);
                                $found = true;
                                ProgressDisplay::info("✅ DEBUG: Successfully updated record with key '$key' in DBF");
                                break;
                            }
                        }
                        if (!$found) {
                            ProgressDisplay::warning("⚠️  Record with key '$key' not found for update in $table_name - this should not happen if it was detected as existing");
                        }
                    }
                    
                    ProgressDisplay::info("  ✅ Completed batch $batchNum/$totalBatches ($totalUpdates total updates)");

                    if ($editMode === \XBase\TableEditor::EDIT_MODE_CLONE) {
                        $batchEditor->save();
                        
                        // ✅ Validate file after save
                        if (!validateDbfFile($path)) {
                            throw new Exception("DBF file validation failed after save. File may be corrupted.");
                        }
                    }
                    $batchEditor->close();
                    break; // Success
                } catch (\Throwable $e) {
                    $retryCount++;
                    if ($batchEditor) {
                        try {
                            $batchEditor->close();
                        } catch (\Throwable $closeError) {
                            // Ignore close errors
                        }
                    }

                    if ($retryCount >= $maxRetries) {
                        // ✅ REMOVED REALTIME FALLBACK - Always abort if CLONE fails
                        releaseDbfLock($lockFp);
                        throw new Exception("❌ Failed to update batch in $table_name after $maxRetries retries: " . $e->getMessage());
                    }
                    usleep(100000 * $retryCount); // Exponential backoff
                }
            }

            $processed += count($batch);
            gc_collect_cycles();
        }
    }

    // Process inserts in batches
    if (!empty($insertRecords)) {
        // ✅ Validate primary key for all tables to prevent duplicates
        // Get primary key field(s) - can be single field or composite key
        $primaryKeyFields = is_array($keyField) ? $keyField : [$keyField];
        $keyFieldNames = implode('+', $primaryKeyFields);

        ProgressDisplay::info("🔍 Validating $keyFieldNames for $table_name inserts to prevent duplicates...");

        // ✅ Reuse existingKeys from first scan instead of re-reading the entire table
        // $existingKeys already populated above from $existingRecords

        // Filter out records with duplicate primary keys
        $validInsertRecords = [];
        $duplicateCount = 0;
        $emptyKeyCount = 0;

        foreach ($insertRecords as $record) {
            // Build key from record using getRecordKey (same function used in first scan)
            $key = getRecordKey($record, $keyField);

            // Check if key is empty (for composite keys, getRecordKey handles this)
            if (empty($key) || (is_array($keyField) && strpos($key, '|') === false)) {
                ProgressDisplay::warning("⚠️  Skipping record with empty/invalid $keyFieldNames");
                $emptyKeyCount++;
                continue;
            }

            if (isset($existingKeys[$key])) {
                $keyDisplay = is_array($keyField) ? $keyFieldNames . " = " . str_replace('|', '+', $key) : "$keyFieldNames = $key";
                ProgressDisplay::warning("⚠️  Skipping duplicate $keyFieldNames in UBS: $keyDisplay");
                $duplicateCount++;
                continue;
            }

            $validInsertRecords[] = $record;
            $existingKeys[$key] = true; // Track newly inserted keys in this batch
        }

        if ($emptyKeyCount > 0) {
            ProgressDisplay::info("⚠️  Filtered out $emptyKeyCount record(s) with empty/invalid $keyFieldNames");
        }
        if ($duplicateCount > 0) {
            ProgressDisplay::info("⚠️  Filtered out $duplicateCount duplicate $keyFieldNames from UBS insert");
        }

        $insertRecords = $validInsertRecords;
        if (empty($insertRecords)) {
            ProgressDisplay::info("⚠️  No valid records to insert into UBS after $keyFieldNames validation");
        } else {
            ProgressDisplay::info("✅ Validated " . count($insertRecords) . " records for UBS insert");
        }

        if (!empty($insertRecords)) {
            ProgressDisplay::info("Processing " . count($insertRecords) . " inserts for $table_name");

            for ($i = 0; $i < count($insertRecords); $i += $batchSize) {
                $batch = array_slice($insertRecords, $i, $batchSize);
                $batchEditor = null;
                $retryCount = 0;

                while ($retryCount < $maxRetries) {
                    try {
                        $batchEditor = new \XBase\TableEditor($path, [
                            'editMode' => $editMode,
                        ]);

                        $insertedCount = 0;
                        foreach ($batch as $record) {
                            try {
                                insertUbsRecord($batchEditor, $record, $table_name, $table, $keyField);
                                $insertedCount++;
                            } catch (\Throwable $insertError) {
                                ProgressDisplay::error("❌ Failed to insert record into $table_name: " . $insertError->getMessage());
                                ProgressDisplay::error("Record data: " . json_encode($record, JSON_PARTIAL_OUTPUT_ON_ERROR));
                                throw $insertError; // Re-throw to trigger retry logic
                            }
                        }

                        if ($editMode === \XBase\TableEditor::EDIT_MODE_CLONE) {
                            $batchEditor->save();
                            
                            // ✅ Validate file after save
                            if (!validateDbfFile($path)) {
                                throw new Exception("DBF file validation failed after save. File may be corrupted.");
                            }
                        }
                        $batchEditor->close();
                        
                        ProgressDisplay::info("✅ Successfully inserted $insertedCount record(s) into $table_name DBF file");
                        break; // Success
                    } catch (\Throwable $e) {
                        $retryCount++;
                        if ($batchEditor) {
                            try {
                                $batchEditor->close();
                            } catch (\Throwable $closeError) {
                                // Ignore close errors
                            }
                        }

                        if ($retryCount >= $maxRetries) {
                            // ✅ REMOVED REALTIME FALLBACK - Always abort if CLONE fails
                            releaseDbfLock($lockFp);
                            throw new Exception("❌ Failed to insert batch in $table_name after $maxRetries retries: " . $e->getMessage());
                        }
                        usleep(100000 * $retryCount); // Exponential backoff
                    }
                }

                $processed += count($batch);
                gc_collect_cycles();
            }
        }
    }

    // ✅ Also update local MySQL to keep it in sync
    // This ensures that when PHP sync updates UBS, local MySQL is also updated
    // This prevents issues when re-syncing
    try {
        ProgressDisplay::info("Updating local MySQL for $table ($totalRecords records)");
        $db_local = new mysql(); // Connects to local database by default

        // Convert records back to local MySQL format if needed
        // The records are already in UBS format, but we need to exclude remote-only columns
        // Remote-only columns are auto-increment IDs used by the remote PHP application
        // but don't exist in local UBS MySQL tables
        // Note: Case-sensitive matching - UBS uses uppercase, MySQL uses lowercase
        $remoteOnlyColumns = [
            'id',
            'order_items_id',       // Remote primary key for order_items (if exists)
        ];

        // Get actual columns that exist in local MySQL table to filter out non-existent columns
        $db_local_check = new mysql();
        $db_local_check->connect();
        $tableColumns = [];
        try {
            $columnsResult = $db_local_check->query("SHOW COLUMNS FROM `$table`");
            if ($columnsResult) {
                while ($col = mysqli_fetch_assoc($columnsResult)) {
                    $tableColumns[strtolower($col['Field'])] = true;
                }
            }
        } catch (Exception $e) {
            ProgressDisplay::warning("⚠️  Could not fetch table columns for $table: " . $e->getMessage());
        }
        $db_local_check->close();

        // ✅ FIX: Use convert() function to properly convert records to UBS format
        // This ensures updated_at → UPDATED_ON conversion and removes remote-only fields
        $remote_table_name = Converter::table_convert_remote($table);
        if (!$remote_table_name) {
            // If no remote table mapping, assume it's already in UBS format
            $remote_table_name = $table;
        }

        $localRecords = [];
        foreach ($records as $record) {
            // Use convert() to properly convert remote format to UBS format
            // This handles updated_at → UPDATED_ON conversion automatically
            $convertedRecord = convert($remote_table_name, $record, 'to_ubs');

            // Filter out columns that don't exist in local table (safety check)
            $filteredRecord = [];
            foreach ($convertedRecord as $key => $value) {
                $keyLower = strtolower($key);
                // Skip remote-only columns (id, etc.)
                if (in_array($keyLower, $remoteOnlyColumns)) {
                    continue;
                }
                // Skip columns that don't exist in local table
                if (!empty($tableColumns) && !isset($tableColumns[$keyLower])) {
                    continue;
                }
                $filteredRecord[$key] = $value;
            }

            // Ensure UPDATED_ON is set (convert() should handle this, but double-check)
            if (!isset($filteredRecord['UPDATED_ON']) || empty($filteredRecord['UPDATED_ON'])) {
                $filteredRecord['UPDATED_ON'] = date('Y-m-d H:i:s');
            }

            $localRecords[] = $filteredRecord;
        }

        if (!empty($localRecords)) {
            // ✅ Validate primary key for all tables to prevent duplicates in local MySQL
            // Get primary key field(s) - can be single field or composite key
            $primaryKeyFields = is_array($keyField) ? $keyField : [$keyField];
            $keyFieldNames = implode('+', $primaryKeyFields);

            ProgressDisplay::info("🔍 Validating $keyFieldNames for local MySQL $table inserts to prevent duplicates...");
            $existingKeys = [];

            try {
                // Build SQL to get existing keys
                $keyColumns = array_map(function ($k) {
                    return "`$k`";
                }, $primaryKeyFields);
                $keyCheckSql = "SELECT DISTINCT " . implode(', ', $keyColumns) . " FROM `$table`";
                $keyResults = $db_local->get($keyCheckSql);

                foreach ($keyResults as $row) {
                    $keyParts = [];
                    foreach ($primaryKeyFields as $pkField) {
                        $value = trim($row[$pkField] ?? '');
                        if (!empty($value)) {
                            $keyParts[] = $value;
                        }
                    }
                    if (count($keyParts) === count($primaryKeyFields)) {
                        $key = implode('|', $keyParts);
                        $existingKeys[$key] = true;
                    }
                }
            } catch (Exception $e) {
                ProgressDisplay::warning("⚠️  Could not validate $keyFieldNames in local MySQL: " . $e->getMessage());
            }

            // Filter out records with duplicate primary keys
            $validLocalRecords = [];
            $duplicateCount = 0;
            $emptyKeyCount = 0;

            foreach ($localRecords as $record) {
                // Build key from record
                $keyParts = [];
                foreach ($primaryKeyFields as $pkField) {
                    $value = trim($record[$pkField] ?? '');
                    if (empty($value)) {
                        break; // Skip if any part of key is empty
                    }
                    $keyParts[] = $value;
                }

                if (count($keyParts) !== count($primaryKeyFields)) {
                    ProgressDisplay::warning("⚠️  Skipping record with empty/invalid $keyFieldNames in local MySQL");
                    $emptyKeyCount++;
                    continue;
                }

                $key = implode('|', $keyParts);
                if (isset($existingKeys[$key])) {
                    $keyDisplay = is_array($keyField) ? $keyFieldNames . " = " . implode('+', $keyParts) : "$keyFieldNames = $key";
                    ProgressDisplay::warning("⚠️  Skipping duplicate $keyFieldNames in local MySQL: $keyDisplay");
                    $duplicateCount++;
                    continue;
                }

                $validLocalRecords[] = $record;
                $existingKeys[$key] = true; // Track newly inserted keys in this batch
            }

            if ($emptyKeyCount > 0) {
                ProgressDisplay::info("⚠️  Filtered out $emptyKeyCount record(s) with empty/invalid $keyFieldNames from local MySQL");
            }
            if ($duplicateCount > 0) {
                ProgressDisplay::info("⚠️  Filtered out $duplicateCount duplicate $keyFieldNames from local MySQL insert");
            }

            $localRecords = $validLocalRecords;
            if (empty($localRecords)) {
                ProgressDisplay::info("⚠️  No valid records to insert into local MySQL after $keyFieldNames validation");
            } else {
                ProgressDisplay::info("✅ Validated " . count($localRecords) . " records for local MySQL insert");
            }
        }

        if (!empty($localRecords)) {
            // Debug: Show first record structure
            ProgressDisplay::info("📋 First record keys: " . implode(', ', array_keys($localRecords[0])));
            ProgressDisplay::info("📋 Primary key field: " . (is_array($keyField) ? implode(',', $keyField) : $keyField));

            // Use bulk upsert for local MySQL
            $db_local->bulkUpsert($table, $localRecords, $keyField);

            // Verify the insert by checking record count
            $verifySql = "SELECT COUNT(*) as count FROM `$table`";
            if (is_array($keyField)) {
                // For composite keys, check if any of the records exist
                $keyConditions = [];
                foreach ($localRecords as $rec) {
                    $conditions = [];
                    foreach ($keyField as $k) {
                        $val = $rec[$k] ?? null;
                        if ($val !== null) {
                            $val = $db_local->escape($val);
                            $conditions[] = "`$k` = '$val'";
                        }
                    }
                    if (!empty($conditions)) {
                        $keyConditions[] = '(' . implode(' AND ', $conditions) . ')';
                    }
                }
                if (!empty($keyConditions)) {
                    $verifySql = "SELECT COUNT(*) as count FROM `$table` WHERE " . implode(' OR ', array_slice($keyConditions, 0, 1));
                }
            } else {
                $firstKey = $localRecords[0][$keyField] ?? null;
                if ($firstKey !== null) {
                    $firstKey = $db_local->escape($firstKey);
                    $verifySql = "SELECT COUNT(*) as count FROM `$table` WHERE `$keyField` = '$firstKey'";
                }
            }

            $verifyResult = $db_local->first($verifySql);
            $verifyCount = $verifyResult['count'] ?? 0;

            if ($verifyCount > 0) {
                ProgressDisplay::info("✅ Updated local MySQL for $table (verified: $verifyCount record(s) found)");
            } else {
                ProgressDisplay::warning("⚠️  Local MySQL update completed but verification failed - record not found in table");
                ProgressDisplay::warning("⚠️  Table: $table, Key: " . (is_array($keyField) ? implode(',', $keyField) : $keyField));
            }
        } else {
            ProgressDisplay::warning("⚠️  No local records to update (empty array)");
        }
    } catch (Exception $e) {
        // Log error but don't fail the entire sync
        ProgressDisplay::error("❌ Failed to update local MySQL for $table: " . $e->getMessage());
        ProgressDisplay::error("❌ Stack trace: " . $e->getTraceAsString());
        ProgressDisplay::warning("UBS update succeeded, but local MySQL was not updated. This may cause issues on next sync.");
    } catch (Throwable $e) {
        // Catch any other errors
        ProgressDisplay::error("❌ Fatal error updating local MySQL for $table: " . $e->getMessage());
        ProgressDisplay::error("❌ Stack trace: " . $e->getTraceAsString());
    }

        ProgressDisplay::info("Completed batch upsert for UBS $table_name");

        // ✅ Recalculate artran totals when ictran records are inserted/updated
        // Skip recalculation for TYPE='DO' (Delivery Order - inventory only, no need for GROSS_BIL/GRAND_BIL)
        if ($table === 'ubs_ubsstk2015_ictran' && !empty($refNosToRecalculate)) {
            recalculateArtranTotals($refNosToRecalculate);
        }
        
        if (!empty($refNosToSkip)) {
            ProgressDisplay::info("⏭️  Skipped GROSS_BIL/GRAND_BIL recalculation for " . count($refNosToSkip) . " DO type invoice(s)");
        }
        
        return $stats;

    } catch (\Throwable $e) {
        // ✅ If error occurred and we have a backup, restore it
        if ($backupPath !== null && file_exists($backupPath)) {
            ProgressDisplay::warning("⚠️  Error occurred during DBF write. Attempting to restore from backup...");
            try {
                copy($backupPath, $path);
                ProgressDisplay::info("✅ DBF file restored from backup");
            } catch (\Throwable $restoreError) {
                ProgressDisplay::error("❌ Failed to restore DBF file from backup: " . $restoreError->getMessage());
            }
        }
        // Re-throw the exception
        throw $e;
    } finally {
        // ✅ Always release lock, even if error occurred
        if ($lockFp !== null) {
            releaseDbfLock($lockFp);
        }
    }
}

/**
 * Get TYPE from artran record by REFNO
 * Returns TYPE value or null if not found
 */
function getArtranTypeByRefNo($refNo, $directory)
{
    try {
        $artranPath = "C:/$directory/" . ENV::DBF_SUBPATH . "/artran.dbf";
        if (!file_exists($artranPath)) {
            return null;
        }
        
        $reader = new \XBase\TableReader($artranPath);
        while ($row = $reader->nextRecord()) {
            if (isset($row['REFNO']) && trim($row['REFNO']) === $refNo) {
                $type = isset($row['TYPE']) ? trim($row['TYPE']) : null;
                $reader->close();
                return $type;
            }
        }
        $reader->close();
        return null;
    } catch (\Throwable $e) {
        // If we can't read artran, assume we should recalculate (safer)
        return null;
    }
}

/**
 * Recalculate artran totals based on ictran records
 * Updates GROSS_BIL, NET_BIL, GRAND_BIL, DEBIT_BIL, INVGROSS, NET, GRAND, DEBITAMT
 * 
 * ✅ OPTIMIZED: Batch processing - reads files once, updates all REFNOs in single pass
 * 
 * @param array $refNos Array of REFNOs (reference numbers) to recalculate
 */
function recalculateArtranTotals($refNos)
{
    if (empty($refNos)) {
        return;
    }

    try {
        $refNoCount = count($refNos);
        ProgressDisplay::info("🔄 Recalculating artran totals for $refNoCount reference(s)");

        // Parse artran table path
        $artranTable = 'ubs_ubsstk2015_artran';
        $arr = parseUbsTable($artranTable);
        $artranTableName = $arr['table'];
        $directory = strtoupper($arr['database']);
        $artranPath = "C:/$directory/" . ENV::DBF_SUBPATH . "/{$artranTableName}.dbf";

        // Parse ictran table path
        $ictranTable = 'ubs_ubsstk2015_ictran';
        $arr = parseUbsTable($ictranTable);
        $ictranTableName = $arr['table'];
        $ictranPath = "C:/$directory/" . ENV::DBF_SUBPATH . "/{$ictranTableName}.dbf";

        // ✅ OPTIMIZATION 1: Create normalized REFNO lookup (case-insensitive, trimmed)
        $refNosLookup = [];
        foreach ($refNos as $refNo) {
            $refNosLookup[strtoupper(trim($refNo))] = trim($refNo);
        }

        // ✅ SAFETY: Add safety checks before processing
        if (isUbsRunning()) {
            throw new Exception("❌ UBS software is currently running! Cannot update artran DBF.");
        }

        if (isDbfFileLocked($artranPath)) {
            throw new Exception("❌ Artran DBF file is currently locked or in use.");
        }

        // ✅ OPTIMIZATION 2: Create backup ONCE before processing all REFNOs
        $artranBackupPath = backupDbfFile($artranPath);
        if ($artranBackupPath === false) {
            ProgressDisplay::warning("⚠️  Could not create backup for artran, but continuing anyway...");
        }

        // ✅ OPTIMIZATION 3: Acquire file lock ONCE
        $artranLockFp = acquireDbfLock($artranPath);
        if ($artranLockFp === false) {
            throw new Exception("❌ Cannot acquire exclusive lock on artran DBF file.");
        }

        try {
            // ✅ OPTIMIZATION 4: Read ictran file ONCE and group totals by REFNO
            ProgressDisplay::info("📊 Reading ictran records...");
            $ictranTotals = []; // [REFNO => totals array]
            
            $ictranReader = new \XBase\TableReader($ictranPath);
            $ictranRecordCount = 0;
            
            while ($row = $ictranReader->nextRecord()) {
                if ($row->isDeleted()) continue;
                
                $rowRefNo = strtoupper(trim($row->get('REFNO') ?? ''));
                
                // Only process REFNOs we need to update
                if (!isset($refNosLookup[$rowRefNo])) {
                    continue;
                }
                
                $ictranRecordCount++;
                
                // Initialize totals for this REFNO if not exists
                if (!isset($ictranTotals[$rowRefNo])) {
                    $ictranTotals[$rowRefNo] = [
                        'grossBil' => 0,
                        'netBil' => 0,
                        'invgross' => 0,
                        'net' => 0,
                        'totalDiscount' => 0,
                        'totalTax' => 0,
                        'itemCount' => 0
                    ];
                }
                
                // Sum up amounts from ictran
                $amtBil = (float)($row->get('AMT_BIL') ?? 0);
                $amt = (float)($row->get('AMT') ?? 0);
                $disamtBil = (float)($row->get('DISAMT_BIL') ?? 0);
                $taxamt = (float)($row->get('TAXAMT') ?? 0);
                
                $ictranTotals[$rowRefNo]['grossBil'] += $amtBil;
                $ictranTotals[$rowRefNo]['netBil'] += ($amtBil - $disamtBil);
                $ictranTotals[$rowRefNo]['invgross'] += $amt;
                $ictranTotals[$rowRefNo]['net'] += ($amt - $disamtBil);
                $ictranTotals[$rowRefNo]['totalDiscount'] += $disamtBil;
                $ictranTotals[$rowRefNo]['totalTax'] += $taxamt;
                $ictranTotals[$rowRefNo]['itemCount']++;
            }
            
            $ictranReader->close();
            ProgressDisplay::info("📊 Processed $ictranRecordCount ictran records for " . count($ictranTotals) . " REFNO(s)");

            // ✅ OPTIMIZATION 5: Calculate grand totals for all REFNOs
            $calculatedTotals = [];
            foreach ($ictranTotals as $refNoUpper => $totals) {
                $grandBil = $totals['netBil'] + $totals['totalTax'];
                $grand = $totals['net'] + $totals['totalTax'];
                
                $calculatedTotals[$refNoUpper] = [
                    'grossBil' => $totals['grossBil'],
                    'netBil' => $totals['netBil'],
                    'grandBil' => $grandBil,
                    'debitBil' => $grandBil,
                    'invgross' => $totals['invgross'],
                    'net' => $totals['net'],
                    'grand' => $grand,
                    'debitamt' => $grand,
                    'itemCount' => $totals['itemCount']
                ];
            }

            // ✅ OPTIMIZATION 6: Open artranEditor ONCE and update all REFNOs in single pass
            ProgressDisplay::info("📝 Updating artran DBF records...");
            $artranEditor = new \XBase\TableEditor($artranPath, [
                'editMode' => \XBase\TableEditor::EDIT_MODE_CLONE,
            ]);

            $updatedCount = 0;
            $artranEditor->moveTo(0); // Reset to beginning

            while ($row = $artranEditor->nextRecord()) {
                if ($row->isDeleted()) continue;

                $rowRefNo = strtoupper(trim($row->get('REFNO') ?? ''));

                // Check if this REFNO needs updating
                if (!isset($calculatedTotals[$rowRefNo])) {
                    continue;
                }

                $totals = $calculatedTotals[$rowRefNo];
                $refNoOriginal = $refNosLookup[$rowRefNo];

                try {
                    // Get order TYPE to check if it's CN (Credit Note)
                    $orderType = strtoupper(trim($row->get('TYPE') ?? ''));

                    // Update totals
                    $row->set('GROSS_BIL', $totals['grossBil']);
                    $row->set('NET_BIL', $totals['netBil']);
                    $row->set('GRAND_BIL', $totals['grandBil']);
                    $row->set('DEBIT_BIL', $totals['debitBil']);
                    $row->set('INVGROSS', $totals['invgross']);
                    $row->set('NET', $totals['net']);
                    $row->set('GRAND', $totals['grand']);
                    
                    // ✅ Special handling for CN (Credit Note) type
                    if ($orderType === 'CN') {
                        // For CN: Move DEBITAMT value to CREDITAMT, set DEBITAMT to 0
                        $row->set('CREDITAMT', $totals['debitamt']);
                        $row->set('DEBITAMT', 0);
                    } else {
                        // For other types (INV, SO, etc.): Set DEBITAMT normally
                        $row->set('DEBITAMT', $totals['debitamt']);
                        // Clear CREDITAMT for non-CN types (set to 0)
                        $row->set('CREDITAMT', 0);
                    }

                    $artranEditor->writeRecord();
                    $updatedCount++;

                    $typeInfo = $orderType === 'CN' ? " (CN: CREDITAMT={$totals['debitamt']}, DEBITAMT=0)" : "";
                    ProgressDisplay::info("📝 Updated artran DBF record for REFNO: $refNoOriginal (GROSS_BIL: {$totals['grossBil']}, GRAND_BIL: {$totals['grandBil']}, Items: {$totals['itemCount']})$typeInfo");
                } catch (\Throwable $e) {
                    ProgressDisplay::error("❌ Error setting artran fields for REFNO $refNoOriginal: " . $e->getMessage());
                    // Continue with other REFNOs
                }
            }

            // ✅ OPTIMIZATION 7: Save ONCE after all updates
            if ($updatedCount > 0) {
                $artranEditor->save();
                
                // Validate file after save
                if (!validateDbfFile($artranPath)) {
                    throw new Exception("DBF file validation failed after artran update. File may be corrupted.");
                }
                
                ProgressDisplay::info("✅ Updated $updatedCount artran DBF record(s)");
            } else {
                ProgressDisplay::warning("⚠️  No artran records found to update");
            }

            $artranEditor->close();

            // ✅ OPTIMIZATION 8: Batch update MySQL instead of one-by-one
            if ($updatedCount > 0) {
                try {
                    $db_local = new mysql();
                    $db_local->connect();

                    $updateCount = 0;
                    foreach ($calculatedTotals as $refNoUpper => $totals) {
                        $refNoOriginal = $refNosLookup[$refNoUpper];
                        
                        // Get order TYPE to check if it's CN
                        $refNoEscaped = $db_local->escape($refNoOriginal);
                        $typeSql = "SELECT TYPE FROM `ubs_ubsstk2015_artran` WHERE `REFNO` = '$refNoEscaped'";
                        $typeResult = $db_local->first($typeSql);
                        $orderType = strtoupper(trim($typeResult['TYPE'] ?? ''));
                        
                        $updateData = [
                            'GROSS_BIL' => $totals['grossBil'],
                            'NET_BIL' => $totals['netBil'],
                            'GRAND_BIL' => $totals['grandBil'],
                            'DEBIT_BIL' => $totals['debitBil'],
                            'INVGROSS' => $totals['invgross'],
                            'NET' => $totals['net'],
                            'GRAND' => $totals['grand'],
                            'UPDATED_ON' => date('Y-m-d H:i:s')
                        ];
                        
                        // ✅ Special handling for CN (Credit Note) type
                        if ($orderType === 'CN') {
                            // For CN: Move DEBITAMT value to CREDITAMT, set DEBITAMT to 0
                            $updateData['CREDITAMT'] = $totals['debitamt'];
                            $updateData['DEBITAMT'] = 0;
                        } else {
                            // For other types: Set DEBITAMT normally, clear CREDITAMT
                            $updateData['DEBITAMT'] = $totals['debitamt'];
                            $updateData['CREDITAMT'] = 0;
                        }

                        // Update using REFNO as the key
                        $updateSql = "UPDATE `ubs_ubsstk2015_artran` SET ";
                        $updateFields = [];
                        foreach ($updateData as $field => $value) {
                            $fieldEscaped = $db_local->escape($field);
                            $valueEscaped = $db_local->escape($value);
                            $updateFields[] = "`$fieldEscaped` = '$valueEscaped'";
                        }
                        $updateSql .= implode(', ', $updateFields) . " WHERE `REFNO` = '$refNoEscaped'";

                        $db_local->query($updateSql);
                        $updateCount++;
                    }

                    ProgressDisplay::info("✅ Updated $updateCount local MySQL artran record(s)");
                    $db_local->close();
                } catch (\Throwable $e) {
                    ProgressDisplay::error("❌ Error updating local MySQL artran: " . $e->getMessage());
                    // Continue - DBF update succeeded
                }
            }

        } catch (\Throwable $e) {
            // ✅ Restore from backup if error occurred
            if ($artranBackupPath !== null && file_exists($artranBackupPath)) {
                ProgressDisplay::warning("⚠️  Error occurred during artran DBF write. Attempting to restore from backup...");
                try {
                    copy($artranBackupPath, $artranPath);
                    ProgressDisplay::info("✅ Artran DBF file restored from backup");
                } catch (\Throwable $restoreError) {
                    ProgressDisplay::error("❌ Failed to restore artran DBF file from backup: " . $restoreError->getMessage());
                }
            }
            throw $e;
        } finally {
            // ✅ Always release lock
            if (isset($artranLockFp) && $artranLockFp !== null) {
                releaseDbfLock($artranLockFp);
            }
        }

    } catch (\Throwable $e) {
        ProgressDisplay::error("❌ Error in recalculateArtranTotals: " . $e->getMessage());
    }
}

function getRecordKey($record, $keyField)
{
    if (is_array($keyField)) {
        $composite_keys = [];
        foreach ($keyField as $k) {
            // Handle both array records and XBase Record objects
            if (is_array($record)) {
                $composite_keys[] = trim($record[$k] ?? '');
            } else {
                // XBase Record object - use get() method
                $composite_keys[] = trim($record->get($k) ?? '');
            }
        }
        return implode('|', $composite_keys);
    } else {
        // Handle both array records and XBase Record objects
        if (is_array($record)) {
            return trim($record[$keyField] ?? '');
        } else {
            // XBase Record object - use get() method
            return trim($record->get($keyField) ?? '');
        }
    }
}

function updateUbsRecord($editor, $row, $record, $table_name)
{
    $columns = $editor->getColumns();
    $columnMap = [];
    // ✅ FIX: Create map with both lowercase and uppercase keys for case-insensitive lookup
    foreach ($columns as $column) {
        $colName = $column->getName();
        $colNameLower = strtolower($colName);
        $colNameUpper = strtoupper($colName);
        // Store with both cases for flexible lookup
        $columnMap[$colNameLower] = $column;
        $columnMap[$colNameUpper] = $column;
        $columnMap[$colName] = $column; // Also store original case
    }

    $updatedFields = [];
    $skippedFields = [];
    foreach ($record as $field => $value) {
        // ✅ FIX: Try multiple case variations to find the column
        $column = $columnMap[strtolower($field)] ?? 
                  $columnMap[strtoupper($field)] ?? 
                  $columnMap[$field] ?? null;
        
        if ($column == null) {
            // Field doesn't exist in DBF structure - log and skip
            $skippedFields[] = $field;
            continue;
        }
        
        // ✅ FIX: Use the actual column name from DBF (not the field name from record)
        // This ensures case matches what DBF expects
        $actualFieldName = $column->getName();

        $fieldType = $column->getType();
        $fieldLength = $column->getLength();
        $fieldDecimal = $column->getDecimalCount();

        // Special handling for UPDATED_ON field regardless of data type
        if (strtoupper($field) === 'UPDATED_ON') {
            $value = validateUpdatedOnField($value, $field);
        }

        // Handle boolean fields
        if ($fieldType === 'L') {
            $value = empty($value) ? false : filter_var($value, FILTER_VALIDATE_BOOLEAN);
        }

        // Handle date fields
        if ($fieldType === 'D') {
            $parsedDate = parseDateRobust($value);
            if ($parsedDate !== null) {
                $value = $parsedDate;
            } else {
                $value = null;
            }
        }

        // Truncate string values to match column length (important for DBF files)
        if (in_array($fieldType, ['C', 'M']) && is_string($value)) {
            // Convert to string and ensure it fits within field length
            $value = (string)$value;

            // Try to convert encoding, but handle errors gracefully
            $converted = @mb_convert_encoding($value, 'CP1252', 'UTF-8');
            if ($converted !== false) {
                $value = $converted;
            }

            // Truncate to exact byte length (DBF uses fixed-length fields)
            if (strlen($value) > $fieldLength) {
                $value = substr($value, 0, $fieldLength);
            }
        }

        // Handle numeric fields
        if (in_array($fieldType, ['N', 'F']) && $value !== null && $value !== '') {
            if (is_numeric($value)) {
                // Ensure number fits within field constraints
                $maxValue = pow(10, $fieldLength - ($fieldDecimal > 0 ? $fieldDecimal + 1 : 0)) - 1;
                if (abs($value) > $maxValue) {
                    $value = $maxValue * ($value < 0 ? -1 : 1);
                }
                if ($fieldDecimal > 0) {
                    $value = round($value, $fieldDecimal);
                } else {
                    $value = (int)$value;
                }
            }
        }

        try {
            // ✅ FIX: Use actual field name from DBF column (correct case)
            $row->set($actualFieldName, $value);
            $updatedFields[] = $actualFieldName;
        } catch (\Throwable $e) {
            // Log the problematic field but continue
            ProgressDisplay::warning("⚠️  Skipping field '$field' (DBF: '$actualFieldName') in $table_name: " . $e->getMessage());
        }
    }
    
    // ✅ DEBUG: Log what fields were updated and skipped
    if (!empty($updatedFields)) {
        ProgressDisplay::info("🔍 DEBUG: Updated " . count($updatedFields) . " field(s) in $table_name: " . implode(', ', array_slice($updatedFields, 0, 10)) . (count($updatedFields) > 10 ? '...' : ''));
    }
    if (!empty($skippedFields)) {
        ProgressDisplay::warning("⚠️  Skipped " . count($skippedFields) . " field(s) in $table_name (not in DBF structure): " . implode(', ', array_slice($skippedFields, 0, 10)) . (count($skippedFields) > 10 ? '...' : ''));
    }

    $editor->writeRecord();
}

function insertUbsRecord($editor, $record, $table_name, $full_table_name = null, $primaryKey = null)
{
    $columns = $editor->getColumns();
    $columnMap = [];
    $structure = [];
    foreach ($columns as $column) {
        $colName = strtoupper($column->getName());
        $columnMap[$colName] = $column;
        $structure[$colName] = [
            'type' => $column->getType(),
            'length' => $column->getLength(),
            'decimal' => $column->getDecimalCount()
        ];
    }

    $newRow = $editor->appendRecord();
    
    // ✅ FIX: Use provided primary key or get it from converter using full table name
    if ($primaryKey === null) {
        if ($full_table_name !== null) {
            $primaryKey = Converter::primaryKey($full_table_name);
        } else {
            $primaryKey = Converter::primaryKey($table_name);
        }
    }
    
    if (empty($primaryKey)) {
        throw new Exception("Cannot insert record into $table_name: Primary key not defined in Converter. Full table: " . ($full_table_name ?? $table_name) . ". Record keys: " . implode(', ', array_keys($record)));
    }
    
    // Try multiple ways to get the primary key value (handle case variations)
    $primaryKeyValue = null;
    if (is_array($primaryKey)) {
        $primaryKeyValue = [];
        foreach ($primaryKey as $key) {
            // Try uppercase, lowercase, and original case
            $value = $record[strtoupper($key)] ?? $record[strtolower($key)] ?? $record[$key] ?? null;
            $primaryKeyValue[] = $value;
        }
        $primaryKeyValue = implode('|', array_filter($primaryKeyValue)); // Filter out nulls
    } else {
        // Try uppercase, lowercase, and original case
        $primaryKeyValue = $record[strtoupper($primaryKey)] ?? $record[strtolower($primaryKey)] ?? $record[$primaryKey] ?? null;
        
        // Also try trimming whitespace
        if (is_string($primaryKeyValue)) {
            $primaryKeyValue = trim($primaryKeyValue);
        }
    }
    
    if (empty($primaryKeyValue)) {
        // Show all possible variations we tried
        $triedKeys = [];
        if (is_array($primaryKey)) {
            foreach ($primaryKey as $key) {
                $triedKeys[] = strtoupper($key);
                $triedKeys[] = strtolower($key);
                $triedKeys[] = $key;
            }
        } else {
            $triedKeys[] = strtoupper($primaryKey);
            $triedKeys[] = strtolower($primaryKey);
            $triedKeys[] = $primaryKey;
        }
        throw new Exception("Cannot insert record into $table_name: Primary key field '$primaryKey' is empty or missing. Tried keys: " . implode(', ', array_unique($triedKeys)) . ". Record keys: " . implode(', ', array_keys($record)) . ". Record CUSTNO value: " . ($record['CUSTNO'] ?? $record['custno'] ?? 'NOT FOUND'));
    }

    $skippedFields = [];
    foreach ($record as $field => $value) {
        // ✅ FIX: Try multiple case variations to find the column
        $fieldUpper = strtoupper($field);
        $fieldLower = strtolower($field);
        
        // Try uppercase first (matches insertUbsRecord pattern), then lowercase, then original
        $structureKey = $structure[$fieldUpper] ?? 
                       $structure[$fieldLower] ?? 
                       $structure[$field] ?? null;
        
        // ✅ FIX: Get the actual column object to get correct field name
        $column = $columnMap[$fieldUpper] ?? 
                  $columnMap[$fieldLower] ?? 
                  $columnMap[$field] ?? null;
        
        if ($structureKey === null || $column === null) {
            // Field doesn't exist in DBF structure - log and skip
            $skippedFields[] = $field;
            continue;
        }
        
        // ✅ FIX: Use the actual column name from DBF (not the field name from record)
        $actualFieldName = $column->getName();

        try {
            if ($value === null) $value = "";

            $fieldType = $structureKey['type'];
            $fieldLength = $structureKey['length'];

            // Special handling for UPDATED_ON field regardless of data type
            if ($fieldUpper === 'UPDATED_ON') {
                $value = validateUpdatedOnField($value, $field);
            }

            // Handle boolean fields
            if ($fieldType == 'L') {
                $value = empty($value) ? false : filter_var($value, FILTER_VALIDATE_BOOLEAN);
            }

            // Handle date fields
            if ($fieldType === 'D') {
                $parsedDate = parseDateRobust($value);
                if ($parsedDate !== null) {
                    $value = $parsedDate;
                } else {
                    $value = null;
                }
            }

            // Truncate string values to match column length (important for DBF files)
            if (in_array($fieldType, ['C', 'M']) && is_string($value)) {
                // Convert to string and ensure it fits within field length
                $value = (string)$value;

                // Try to convert encoding, but handle errors gracefully
                $converted = @mb_convert_encoding($value, 'CP1252', 'UTF-8');
                if ($converted !== false) {
                    $value = $converted;
                }

                // Truncate to exact byte length (DBF uses fixed-length fields)
                if (strlen($value) > $fieldLength) {
                    $value = substr($value, 0, $fieldLength);
                }
            }

            // Handle numeric fields
            if (in_array($fieldType, ['N', 'F']) && $value !== null && $value !== '') {
                $decimal = $structureKey['decimal'];
                if (is_numeric($value)) {
                    // Ensure number fits within field constraints
                    $maxValue = pow(10, $fieldLength - ($decimal > 0 ? $decimal + 1 : 0)) - 1;
                    if (abs($value) > $maxValue) {
                        $value = $maxValue * ($value < 0 ? -1 : 1);
                    }
                    if ($decimal > 0) {
                        $value = round($value, $decimal);
                    } else {
                        $value = (int)$value;
                    }
                }
            }

            // ✅ FIX: Use actual field name from DBF column (correct case)
            $newRow->set($actualFieldName, $value);
        } catch (\Throwable $e) {
            // Log the problematic field but continue
            ProgressDisplay::warning("⚠️  Skipping field '$field' (DBF: '$actualFieldName') in $table_name: " . $e->getMessage());
        }
    }
    
    // ✅ DEBUG: Log skipped fields
    if (!empty($skippedFields)) {
        ProgressDisplay::warning("⚠️  Skipped " . count($skippedFields) . " field(s) in $table_name insert (not in DBF structure): " . implode(', ', array_slice($skippedFields, 0, 10)) . (count($skippedFields) > 10 ? '...' : ''));
    }

    // ✅ FIX: Write the record explicitly - ensure the appended record is written
    try {
        $editor->writeRecord($newRow);
    } catch (\Throwable $e) {
        // If writeRecord with argument fails, try without (some XBase versions)
        try {
            $editor->writeRecord();
        } catch (\Throwable $e2) {
            throw new Exception("Failed to write record to $table_name DBF file: " . $e->getMessage() . " / " . $e2->getMessage());
        }
    }
}

function insertSyncLog()
{
    $db = new mysql();
    $db->insert('sync_logs', [
        'synced_at' => timestamp(),
    ]);
}
function lastSyncAt()
{
    $db = new mysql();
    $data = $db->first('SELECT * FROM sync_logs ORDER BY synced_at DESC LIMIT 1');
    return $data ? $data['synced_at'] : null;
}

function fetchServerData($table, $updatedAfter = null, $bearerToken = null)
{
    // Increase memory limit for large data fetching
    increaseMemoryLimit('4G');

    $db = new mysql;
    $db->connect_remote();

    $alias_table = Converter::table_convert_remote($table);
    $column_updated_at = Converter::mapUpdatedAtField($alias_table);

    // ✅ FORCE SYNC: If $updatedAfter is null, fetch ALL records
    // For icgroup, don't filter NULL values - sync all records including NULL CREATED_ON/UPDATED_ON
    $forceSyncTables = ['ubs_ubsstk2015_icitem', 'ubs_ubsstk2015_icgroup'];
    $isForceSyncTable = in_array($table, $forceSyncTables);

    if ($updatedAfter === null) {
        if ($isForceSyncTable && $alias_table === 'icgroup') {
            // For icgroup, fetch ALL records including NULL values
            $sql = "SELECT * FROM $alias_table";
        } else {
            $sql = "SELECT * FROM $alias_table WHERE $column_updated_at IS NOT NULL";
        }
    } else {
        $sql = "SELECT * FROM $alias_table WHERE $column_updated_at > '$updatedAfter'";
    }

    // Debug information for troubleshooting
    dump("fetchServerData Debug:");
    dump("  Table: $table");
    dump("  Remote table: $alias_table");
    dump("  Updated column: $column_updated_at");
    dump("  Updated after: " . ($updatedAfter === null ? 'NULL (FORCE SYNC - ALL RECORDS)' : $updatedAfter));
    dump("  SQL: $sql");

    // Also check what the actual max date is in the remote table
    $maxDateSql = "SELECT MAX($column_updated_at) as max_date, COUNT(*) as total FROM $alias_table";
    $maxDateResult = $db->first($maxDateSql);
    dump("  Remote table stats - Total records: " . ($maxDateResult['total'] ?? 0) . ", Max date: " . ($maxDateResult['max_date'] ?? 'NULL'));

    // Log memory usage before fetching - suppressed
    // $memoryBefore = getMemoryUsage();
    // dump("Memory before fetch: " . $memoryBefore['memory_usage_mb'] . "MB");

    $data = $db->get($sql);

    // Debug: Check actual UPDATED_ON values in remote table
    if ($table === 'ubs_ubsstk2015_ictran') {
        $debug_sql = "SELECT COUNT(*) as total, MIN($column_updated_at) as min_date, MAX($column_updated_at) as max_date FROM $alias_table";
        $debug_result = $db->first($debug_sql);
        // dump("Remote table $alias_table stats:");
        // dump("  Total records: " . $debug_result['total']);
        // dump("  Min UPDATED_ON: " . $debug_result['min_date']);
        // dump("  Max UPDATED_ON: " . $debug_result['max_date']);
    }

    // Log memory usage after fetching
    $memoryAfter = getMemoryUsage();
    dump("📊 Memory after fetch: " . $memoryAfter['memory_usage_mb'] . "MB");
    dump("📊 Data rows fetched: " . count($data));

    return $data;
}


function convert($remote_table_name, $dataRow, $direction = 'to_remote')
{
    $converted = [];

    $map = Converter::mapColumns($remote_table_name);

    // ✅ FIX: When map is empty (like icitem), include ALL fields from dataRow
    // This ensures price fields (PRICE, UCOST, PRICEU2, etc.) are synced
    if ($map == []) {
        // For empty maps, copy all fields from dataRow to converted
        // This ensures all fields including price fields are included in sync
        foreach ($dataRow as $field => $value) {
            $converted[$field] = $value;
        }
        // Continue processing to handle timestamps and field cleanup
    } else {

    // Apply explicit mappings
    foreach ($map as $ubs => $remote) {
        if ($direction === 'to_remote') {
            // Skip null mappings (fields to ignore, like SALEC)
            if ($remote !== null && $ubs !== null) {
                $converted[$remote] = $dataRow[$ubs] ?? null;
            }
        } else {
            if ($ubs && $remote) {
                $value = $dataRow[$remote] ?? null;
                // Ensure quantity-related fields are properly set (not null) for order_items
                if ($remote_table_name == 'order_items' && $remote == 'quantity' && ($value === null || $value === '')) {
                    $value = '0'; // Set to 0 instead of null to prevent BASE_RECORD default from being used
                }
                $converted[$ubs] = $value;
            }
        }
    }

        // Auto-map: Include fields with identical names that aren't explicitly mapped
        // This handles fields like CREATED_ON, UPDATED_ON that have same name in both tables
        if ($direction === 'to_remote') {
            $mappedUbsFields = array_keys($map);
            $mappedRemoteFields = array_values(array_filter($map, function ($v) {
                return $v !== null;
            }));

            foreach ($dataRow as $ubsField => $value) {
                // Auto-map if: not in map, not already converted, and field name matches remote column naming
                if (!in_array($ubsField, $mappedUbsFields) && !isset($converted[$ubsField])) {
                    // Common fields that should auto-map (same name in both tables)
                    // Note: CREATED_BY and UPDATED_BY are excluded as they may not exist in all remote tables
                    // Note: 'id' is excluded because remote tables use auto-increment ID that shouldn't be set manually
                    $autoMapFields = ['CREATED_ON', 'UPDATED_ON'];
                    if (in_array(strtoupper($ubsField), array_map('strtoupper', $autoMapFields))) {
                        $converted[$ubsField] = $value;
                    }
                }
            }
        }
    } // End of else block for when map is not empty

    // Validate and fix UPDATED_ON/updated_at field in converted data
    $updatedAtValue = null;

    // Check for UPDATED_ON (uppercase) first
    if (isset($converted['UPDATED_ON'])) {
        $updatedAtValue = $converted['UPDATED_ON'];
    } elseif (isset($converted['updated_at'])) {
        $updatedAtValue = $converted['updated_at'];
    }

    // Validate the value
    if ($updatedAtValue !== null) {
        if (
            empty($updatedAtValue) ||
            $updatedAtValue === '0000-00-00' ||
            $updatedAtValue === '0000-00-00 00:00:00' ||
            strtotime($updatedAtValue) === false
        ) {
            $updatedAtValue = date('Y-m-d H:i:s'); // Use current timestamp for invalid dates
        }
    } else {
        // No UPDATED_ON found - use current timestamp
        $updatedAtValue = date('Y-m-d H:i:s');
    }

    // Set the appropriate field name based on direction
    if ($direction === 'to_remote') {
        // ✅ FIX: Use Converter::mapUpdatedAtField() to get correct field name
        // Some remote tables use 'updated_at' (lowercase), others use 'UPDATED_ON' (uppercase)
        $updated_at_field = Converter::mapUpdatedAtField($remote_table_name);
        $converted[$updated_at_field] = $updatedAtValue;
        // Remove the alternative field if it exists
        if ($updated_at_field === 'updated_at') {
            unset($converted['UPDATED_ON']);
        } else {
            unset($converted['updated_at']);
        }
    } else {
        // UBS/local MySQL uses uppercase 'UPDATED_ON'
        $converted['UPDATED_ON'] = $updatedAtValue;
        // Remove lowercase version if it exists
        unset($converted['updated_at']);
    }

    // ✅ FIX: Remove remote-specific fields that don't exist in UBS/local MySQL
    // When converting to UBS, remove 'updated_at', 'created_at' if UPDATED_ON/CREATED_ON exist
    if ($direction === 'to_ubs') {
        // Remove lowercase versions if uppercase versions exist
        if (isset($converted['UPDATED_ON'])) {
            unset($converted['updated_at']);
        }
        if (isset($converted['CREATED_ON'])) {
            unset($converted['created_at']);
        }
        // Also remove any other remote-only fields that might slip through
        unset($converted['id']); // Remote auto-increment ID doesn't exist in UBS
    }

    if ($direction == 'to_remote') {
        // Special handling for order_items: Fix quantity sync issue
        // Multiple UBS fields (QTY_BIL, QTY, QTY1) map to 'quantity'
        // The mapping loop processes them in order, so QTY1 (last) overwrites previous values
        // QTY1 might be 1 (default from BASE_RECORD), so we use QTY as the source for quantity
        if ($remote_table_name == 'order_items') {
            // Use QTY as the source for quantity (prioritize QTY over QTY_BIL and QTY1)
            $qty = $dataRow['QTY'] ?? $dataRow['qty'] ?? null;
            if ($qty !== null && $qty !== '') {
                $converted['quantity'] = $qty;
            }
        }


        // Remove fields that should not be in remote tables
        // These fields may exist in UBS but not in remote tables
        $fieldsToRemove = ['CREATED_BY', 'UPDATED_BY', 'created_by', 'updated_by'];
        foreach ($fieldsToRemove as $field) {
            unset($converted[$field]);
        }

        foreach ($converted as $key => $val) {
            $check_table_link = strpos($key, '|');
            if ($check_table_link !== false || empty($key)) {
                unset($converted[$key]);
            }
        }
    }

    if ($direction == 'to_ubs') {
        $db = new mysql;
        $db->connect_remote();
        foreach ($converted as $key => $val) {
            $check_table_link = strpos($val, '|');
            if ($check_table_link !== false) {
                $explode = explode('|', $val);
                $table = $explode[0];
                $field = $explode[1];

                if ($remote_table_name == 'order_items') {
                    // Try to use order_id first, fallback to reference_no
                    if (isset($dataRow['order_id']) && !empty($dataRow['order_id'])) {
                        $id = $dataRow['order_id'];
                        $sql = "SELECT $field FROM $table WHERE id='$id'";
                    } elseif (isset($dataRow['reference_no']) && !empty($dataRow['reference_no'])) {
                        // Use reference_no as fallback
                        $referenceNo = $db->escape($dataRow['reference_no']);
                        $sql = "SELECT $field FROM $table WHERE reference_no='$referenceNo'";
                    } else {
                        // Skip if neither order_id nor reference_no is available
                        continue;
                    }

                    $col = $db->first($sql);
                    if ($col && isset($col[$field])) {
                        $converted[$key] = $col[$field];
                    }
                } else {
                    // For other tables, use the original logic if needed
                    if (isset($dataRow['id'])) {
                        $id = $dataRow['id'];
                        $sql = "SELECT $field FROM $table WHERE id='$id'";
                        $col = $db->first($sql);
                        if ($col && isset($col[$field])) {
                            $converted[$key] = $col[$field];
                        }
                    }
                }
            }
        }

        // ✅ Special handling for orders syncing to artran (CN type)
        if ($remote_table_name == 'orders') {
            // Get order type from dataRow
            $orderType = strtoupper(trim($converted['TYPE'] ?? $dataRow['type'] ?? ''));
            
            if ($orderType === 'CN') {
                // For CN: Move DEBITAMT value to CREDITAMT, set DEBITAMT to 0
                // Get the amount from grand_amount or GRAND_BIL
                $amount = $dataRow['grand_amount'] ?? $converted['GRAND_BIL'] ?? $converted['GRAND'] ?? 0;
                $converted['CREDITAMT'] = $amount;
                $converted['DEBITAMT'] = 0;
            } else {
                // For other types: Set DEBITAMT normally, clear CREDITAMT
                $amount = $dataRow['grand_amount'] ?? $converted['GRAND_BIL'] ?? $converted['GRAND'] ?? 0;
                $converted['DEBITAMT'] = $amount;
                $converted['CREDITAMT'] = 0;
            }
        }

        // Special handling for order_items syncing to ictran
        if ($remote_table_name == 'order_items') {
            // Format DATE field if it's already set (from mapping) - ensure it's YYYY-MM-DD format
            if (isset($converted['DATE'])) {
                $dateValue = $converted['DATE'];
                // Handle datetime/timestamp format - extract date part only
                if (is_numeric($dateValue)) {
                    // If it's a timestamp integer
                    $converted['DATE'] = date('Y-m-d', $dateValue);
                } elseif (is_string($dateValue)) {
                    // If it's a string, try to parse it
                    // Handle datetime format (YYYY-MM-DD HH:MM:SS) or date format (YYYY-MM-DD)
                    if (preg_match('/^(\d{4}-\d{2}-\d{2})(\s\d{2}:\d{2}:\d{2})?/', $dateValue, $matches)) {
                        // Extract just the date part (YYYY-MM-DD)
                        $converted['DATE'] = $matches[1];
                    } else {
                        // Try strtotime as fallback
                        $timestamp = strtotime($dateValue);
                        if ($timestamp !== false) {
                            $converted['DATE'] = date('Y-m-d', $timestamp);
                        }
                        // If strtotime fails, keep as-is (might already be YYYY-MM-DD)
                    }
                } else {
                    // If it's already a DateTime object or other format, try to convert
                    $timestamp = strtotime((string)$dateValue);
                    if ($timestamp !== false) {
                        $converted['DATE'] = date('Y-m-d', $timestamp);
                    } else {
                        $converted['DATE'] = date('Y-m-d'); // Fallback to today
                    }
                }
            }
            // Format TRANCODE as strpad 4 based on item_count (e.g., 1 => 0001)
            if (isset($converted['TRANCODE']) && isset($dataRow['item_count'])) {
                $itemCount = (int)$dataRow['item_count'];
                $converted['TRANCODE'] = str_pad($itemCount, 4, '0', STR_PAD_LEFT);
            }

            // Set FPERIOD to 8
            $converted['FPERIOD'] = '8';

            // Set JOB_VALUE and JOB2_VALUE to 0
            $converted['JOB_VALUE'] = '0';
            $converted['JOB2_VALUE'] = '0';

            // Set DISPC1, DISPC2, DISPC3 to 0
            $converted['DISPC1'] = '0';
            $converted['DISPC2'] = '0';
            $converted['DISPC3'] = '0';

            // Set TAXPEC1, TAXPEC2, TAXPEC3 to 0
            $converted['TAXPEC1'] = '0';
            $converted['TAXPEC2'] = '0';
            $converted['TAXPEC3'] = '0';

            // Set DISAMT to 0 (or null if not present)
            $converted['DISAMT'] = '0';

            // Set TAXAMT to 0
            $converted['TAXAMT'] = '0';

            // Set FACTOR1 and FACTOR2 to 1
            $converted['FACTOR1'] = '1';
            $converted['FACTOR2'] = '1';

            // Set GST_ITEM and TOTALUP to 'N'
            $converted['GST_ITEM'] = 'N';
            $converted['TOTALUP'] = 'N';

            // Set PUR_PRICE to 0
            $converted['PUR_PRICE'] = '0';

            // Ensure QTY1 is set from quantity (fix for quantity sync issue)
            // If QTY1 wasn't set in the mapping (e.g., quantity was null), set it explicitly
            if (!isset($converted['QTY1']) || $converted['QTY1'] === null) {
                if (isset($dataRow['quantity']) && $dataRow['quantity'] !== null) {
                    $converted['QTY1'] = $dataRow['quantity'];
                } else {
                    $converted['QTY1'] = '0';
                }
            }

            // Also ensure QTY_BIL and QTY are set from quantity if they weren't set
            if (!isset($converted['QTY_BIL']) || $converted['QTY_BIL'] === null) {
                if (isset($dataRow['quantity']) && $dataRow['quantity'] !== null) {
                    $converted['QTY_BIL'] = $dataRow['quantity'];
                } else {
                    $converted['QTY_BIL'] = '0';
                }
            }
            if (!isset($converted['QTY']) || $converted['QTY'] === null) {
                if (isset($dataRow['quantity']) && $dataRow['quantity'] !== null) {
                    $converted['QTY'] = $dataRow['quantity'];
                } else {
                    $converted['QTY'] = '0';
                }
            }

            // Set QTY2 - QTY6 to 0
            $converted['QTY2'] = '0';
            $converted['QTY3'] = '0';
            $converted['QTY4'] = '0';
            $converted['QTY5'] = '0';
            $converted['QTY6'] = '0';

            // Set QTY7 = QTY1 (if QTY1 exists)
            if (isset($converted['QTY1']) && $converted['QTY1'] !== null) {
                $converted['QTY7'] = $converted['QTY1'];
            } elseif (isset($dataRow['quantity']) && $dataRow['quantity'] !== null) {
                $converted['QTY7'] = $dataRow['quantity'];
            } else {
                $converted['QTY7'] = '0';
            }

            // Set TEMPFIG1, SERCOST to 0
            $converted['TEMPFIG1'] = '0';
            $converted['SERCOST'] = '0';

            // Set M_CHARGE1 - M_CHARGE5 to 0
            $converted['M_CHARGE1'] = '0';
            $converted['M_CHARGE2'] = '0';
            $converted['M_CHARGE3'] = '0';
            $converted['M_CHARGE4'] = '0';
            $converted['M_CHARGE5'] = '0';

            // Set ADTCOST1 - ADTCOST5 to 0
            $converted['ADTCOST1'] = '0';
            $converted['ADTCOST2'] = '0';
            $converted['ADTCOST3'] = '0';
            $converted['ADTCOST4'] = '0';
            $converted['ADTCOST5'] = '0';

            // Set IT_COST, AV_COST, POINT, INV_DISC, INV_TAX, EDI_COU1, WRITEOFF, TOSHIP, SHIPPED to 0
            $converted['IT_COST'] = '0';
            $converted['AV_COST'] = '0';
            $converted['POINT'] = '0';
            $converted['INV_DISC'] = '0';
            $converted['INV_TAX'] = '0';
            $converted['EDI_COU1'] = '0';
            $converted['WRITEOFF'] = '0';
            $converted['TOSHIP'] = '0';
            $converted['SHIPPED'] = '0';

            // Set MC1_BIL - MC5_BIL to 0
            $converted['MC1_BIL'] = '0';
            $converted['MC2_BIL'] = '0';
            $converted['MC3_BIL'] = '0';
            $converted['MC4_BIL'] = '0';
            $converted['MC5_BIL'] = '0';

            // Set DAMT, TEMP1, TEMP2 to 0
            $converted['DAMT'] = '0';
            $converted['TEMP1'] = '0';
            $converted['TEMP2'] = '0';

            // Set TOTAL_GROUP, MARK, TYPE_SEQ, TOURGROUP to 0
            $converted['TOTAL_GROUP'] = '0';
            $converted['MARK'] = '0';
            $converted['TYPE_SEQ'] = '0';
            $converted['TOURGROUP'] = '0';

            // Set FTP, DISPATCHED, APPLYSC to 0
            $converted['FTP'] = '0';
            $converted['DISPATCHED'] = '0';
            $converted['APPLYSC'] = '0';

            // Set SVCCHGAMT, SVCTAXAMT, WGST, SVCTAXPER, GRNLINK, PRICEGRN, TAXTRANS, IMPSVCT, PRGOODS, PRGDSAMT, AUTOMOBI to 0
            $converted['SVCCHGAMT'] = '0';
            $converted['SVCTAXAMT'] = '0';
            $converted['WGST'] = '0';
            $converted['SVCTAXPER'] = '0';
            $converted['GRNLINK'] = '0';
            $converted['PRICEGRN'] = '0';
            $converted['TAXTRANS'] = '0';
            $converted['IMPSVCT'] = '0';
            $converted['PRGOODS'] = '0';
            $converted['PRGDSAMT'] = '0';
            $converted['AUTOMOBI'] = '0';

            // Convert TRADATETIME to format "08/01/25 11:48 PM"
            $tradatetime = null;
            // Check for TRDATETIME (from mapping) or TRADATETIME (UBS field name) or created_at
            if (isset($converted['TRDATETIME'])) {
                $tradatetime = $converted['TRDATETIME'];
            } elseif (isset($converted['TRADATETIME'])) {
                $tradatetime = $converted['TRADATETIME'];
            } elseif (isset($dataRow['created_at'])) {
                $tradatetime = $dataRow['created_at'];
            } elseif (isset($dataRow['TRDATETIME'])) {
                $tradatetime = $dataRow['TRDATETIME'];
            } elseif (isset($dataRow['TRADATETIME'])) {
                $tradatetime = $dataRow['TRADATETIME'];
            }

            if ($tradatetime) {
                // Try to parse the datetime and convert to "MM/DD/YY HH:MM AM/PM" format
                $timestamp = strtotime($tradatetime);
                if ($timestamp !== false) {
                    // Format: MM/DD/YY HH:MM AM/PM (e.g., "08/01/25 11:48 PM")
                    $converted['TRADATETIME'] = date('m/d/y h:i A', $timestamp);
                } else {
                    // If parsing fails, use current time
                    $converted['TRADATETIME'] = date('m/d/y h:i A');
                }
            } else {
                // If no datetime found, use current time
                $converted['TRADATETIME'] = date('m/d/y h:i A');
            }

            // Set CURRATE to 1
            $converted['CURRRATE'] = '1';

            // Get LOCATION from customer's territory, ensure TYPE is set, set NAME = customer_name, CUSTNO, and DATE
            // Use reference_no to get order, then get customer territory and name
            $referenceNo = $dataRow['reference_no'] ?? null;
            if ($referenceNo) {
                // Get order data (customer_id, type, customer_name, customer_code, order_date, and discount)
                $orderSql = "SELECT customer_id, type, customer_name, customer_code, order_date, discount, grand_amount, net_amount FROM orders WHERE reference_no='" . $db->escape($referenceNo) . "'";
                $orderData = $db->first($orderSql);

                if ($orderData) {
                    // Ensure TYPE is set
                    if (!isset($converted['TYPE']) && isset($orderData['type'])) {
                        $converted['TYPE'] = $orderData['type'];
                    }
                    
                    // ✅ Special handling for CN (Credit Note) type when syncing from remote to UBS
                    $orderType = strtoupper(trim($converted['TYPE'] ?? $orderData['type'] ?? ''));
                    if ($orderType === 'CN') {
                        // For CN: Move DEBITAMT value to CREDITAMT, set DEBITAMT to 0
                        // Get the amount from grand_amount or net_amount
                        $amount = $orderData['grand_amount'] ?? $orderData['net_amount'] ?? $converted['GRAND_BIL'] ?? $converted['GRAND'] ?? 0;
                        $converted['CREDITAMT'] = $amount;
                        $converted['DEBITAMT'] = 0;
                    } else {
                        // For other types: Set DEBITAMT normally, clear CREDITAMT
                        $amount = $orderData['grand_amount'] ?? $orderData['net_amount'] ?? $converted['GRAND_BIL'] ?? $converted['GRAND'] ?? 0;
                        $converted['DEBITAMT'] = $amount;
                        $converted['CREDITAMT'] = 0;
                    }

                    // Set NAME = customer_name
                    if (isset($orderData['customer_name'])) {
                        $converted['NAME'] = $orderData['customer_name'];
                    }

                    // Set CUSTNO = customer_code from orders
                    if (isset($orderData['customer_code'])) {
                        $converted['CUSTNO'] = $orderData['customer_code'];
                    }

                    // Set DATE = order_date from orders (format: YYYY-MM-DD)
                    if (isset($orderData['order_date'])) {
                        $orderDate = $orderData['order_date'];
                        // Handle datetime/timestamp format - extract date part only
                        if (is_numeric($orderDate)) {
                            // If it's a timestamp integer
                            $converted['DATE'] = date('Y-m-d', $orderDate);
                        } elseif (is_string($orderDate)) {
                            // If it's a string, try to parse it
                            // Handle datetime format (YYYY-MM-DD HH:MM:SS) or date format (YYYY-MM-DD)
                            if (preg_match('/^(\d{4}-\d{2}-\d{2})(\s\d{2}:\d{2}:\d{2})?/', $orderDate, $matches)) {
                                // Extract just the date part (YYYY-MM-DD)
                                $converted['DATE'] = $matches[1];
                            } else {
                                // Try strtotime as fallback
                                $timestamp = strtotime($orderDate);
                                if ($timestamp !== false) {
                                    $converted['DATE'] = date('Y-m-d', $timestamp);
                                } else {
                                    // Last resort: use as-is (might already be YYYY-MM-DD)
                                    $converted['DATE'] = $orderDate;
                                }
                            }
                        } else {
                            // If it's already a DateTime object or other format, try to convert
                            $timestamp = strtotime((string)$orderDate);
                            if ($timestamp !== false) {
                                $converted['DATE'] = date('Y-m-d', $timestamp);
                            } else {
                                $converted['DATE'] = date('Y-m-d'); // Fallback to today
                            }
                        }
                    }

                    // Set DISAMT_BIL = order's discount
                    if (isset($orderData['discount'])) {
                        $converted['DISAMT_BIL'] = $orderData['discount'];
                    }

                    // Get LOCATION from customer's territory
                    if (isset($orderData['customer_id'])) {
                        $customerId = (int)$orderData['customer_id']; // Cast to int for safety
                        $customerSql = "SELECT territory FROM customers WHERE id=$customerId";
                        $customerData = $db->first($customerSql);

                        if ($customerData && isset($customerData['territory'])) {
                            $converted['LOCATION'] = $customerData['territory'];
                        }
                    }
                }
            }

            // Fallback: If CUSTNO or DATE are still not set, try to get them from the table link resolution
            // This handles cases where the mapping 'orders|customer_code' and 'orders|order_date' should have worked
            if (!isset($converted['CUSTNO']) && isset($dataRow['order_id'])) {
                $orderId = $dataRow['order_id'];
                $orderSql = "SELECT customer_code FROM orders WHERE id=$orderId";
                $orderData = $db->first($orderSql);
                if ($orderData && isset($orderData['customer_code'])) {
                    $converted['CUSTNO'] = $orderData['customer_code'];
                }
            }

            if (!isset($converted['DATE']) && isset($dataRow['order_id'])) {
                $orderId = $dataRow['order_id'];
                $orderSql = "SELECT order_date FROM orders WHERE id=$orderId";
                $orderData = $db->first($orderSql);
                if ($orderData && isset($orderData['order_date'])) {
                    $orderDate = $orderData['order_date'];
                    // Handle datetime/timestamp format - extract date part only
                    if (is_numeric($orderDate)) {
                        // If it's a timestamp integer
                        $converted['DATE'] = date('Y-m-d', $orderDate);
                    } elseif (is_string($orderDate)) {
                        // If it's a string, try to parse it
                        // Handle datetime format (YYYY-MM-DD HH:MM:SS) or date format (YYYY-MM-DD)
                        if (preg_match('/^(\d{4}-\d{2}-\d{2})(\s\d{2}:\d{2}:\d{2})?/', $orderDate, $matches)) {
                            // Extract just the date part (YYYY-MM-DD)
                            $converted['DATE'] = $matches[1];
                        } else {
                            // Try strtotime as fallback
                            $timestamp = strtotime($orderDate);
                            if ($timestamp !== false) {
                                $converted['DATE'] = date('Y-m-d', $timestamp);
                            } else {
                                // Last resort: use as-is (might already be YYYY-MM-DD)
                                $converted['DATE'] = $orderDate;
                            }
                        }
                    } else {
                        // If it's already a DateTime object or other format, try to convert
                        $timestamp = strtotime((string)$orderDate);
                        if ($timestamp !== false) {
                            $converted['DATE'] = date('Y-m-d', $timestamp);
                        } else {
                            $converted['DATE'] = date('Y-m-d'); // Fallback to today
                        }
                    }
                }
            }

            // Set SIGN to -1
            $converted['SIGN'] = '-1';

            // Set DISPEC1, DISPEC2, DISPEC3 to 0
            $converted['DISPEC1'] = '0';
            $converted['DISPEC2'] = '0';
            $converted['DISPEC3'] = '0';

            // Set QTY_RET to 0
            $converted['QTY_RET'] = '0';

            // DISAMT_BIL is set from order's discount above (if available), otherwise default to 0
            if (!isset($converted['DISAMT_BIL'])) {
                $converted['DISAMT_BIL'] = '0';
            }
        }
    }
    // dd($converted);
    return $converted;
}





function syncEntity($entity, $ubs_data, $remote_data)
{
    // Increase memory limit for large sync operations
    increaseMemoryLimit('4G');

    $remote_table_name = Converter::table_convert_remote($entity);
    $remote_key = Converter::primaryKey($remote_table_name);
    $ubs_key = Converter::primaryKey($entity);
    $column_updated_at = Converter::mapUpdatedAtField($remote_table_name);

    // Log initial memory usage - suppressed for cleaner output
    $memoryStart = getMemoryUsage();
    dump("🔄 SyncEntity start - Memory: " . $memoryStart['memory_usage_mb'] . "MB");
    dump("📊 UBS data count: " . count($ubs_data));
    dump("📊 Remote data count: " . count($remote_data));

    $is_composite_key = is_array($ubs_key);

    // Optimized sync using array operations instead of maps
    $sync = [
        'remote_data' => [],
        'ubs_data' => [],
    ];

    // ✅ OPTIMIZATION: Early exit if both are empty
    if (empty($ubs_data) && empty($remote_data)) {
        dump("✅ SyncEntity end - No data to process");
        return $sync;
    }

    // Create key-based arrays for faster lookup
    $ubs_keys = [];
    $remote_keys = [];

    // Process UBS data
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

        // Debug: Log if key is empty (might indicate wrong primary key field)
        if (empty($key) && $entity === 'ubs_ubsstk2015_icgroup') {
            dump("⚠️  WARNING: Empty key for icgroup record. UBS key field: '$ubs_key', Available fields: " . implode(', ', array_keys($row)));
        }

        $ubs_keys[$key] = $row;
    }

    // Debug: Log unique keys found
    if ($entity === 'ubs_ubsstk2015_icgroup') {
        dump("🔑 icgroup unique keys: " . count($ubs_keys) . " (keys: " . implode(', ', array_keys($ubs_keys)) . ")");
    }

    // ✅ OPTIMIZATION: If we have UBS data and large remote_data, filter remote_data to only matching keys
    $shouldFilterRemote = !empty($ubs_keys) && count($remote_data) > 1000;
    if ($shouldFilterRemote) {
        dump("⚡ Optimizing: Filtering remote_data from " . count($remote_data) . " records to only matching keys...");
        $ubs_key_set = array_flip(array_keys($ubs_keys)); // Fast lookup
        $filtered_remote_data = [];
        foreach ($remote_data as $row) {
            $key = $row[$remote_key] ?? '';
            if (!empty($key) && isset($ubs_key_set[$key])) {
                $filtered_remote_data[] = $row;
            }
        }
        $remote_data = $filtered_remote_data;
        dump("⚡ Filtered to " . count($remote_data) . " relevant remote records");
        unset($ubs_key_set, $filtered_remote_data);
    }

    // Process remote data
    // ✅ FIX: Handle duplicate keys properly - keep the most recent one
    foreach ($remote_data as $row) {
        $key = $row[$remote_key] ?? '';

        if (empty($key)) {
            continue; // Skip rows with empty keys
        }

        // If key already exists, compare timestamps and keep the most recent
        if (isset($remote_keys[$key])) {
            $existing_updated_at = $remote_keys[$key][$column_updated_at] ?? null;
            $new_updated_at = $row[$column_updated_at] ?? null;

            // Validate timestamps
            $existing_time = $existing_updated_at ? strtotime($existing_updated_at) : 0;
            $new_time = $new_updated_at ? strtotime($new_updated_at) : 0;

            if ($new_time > $existing_time) {
                // New record is more recent, replace it
                dump("⚠️  WARNING: Duplicate key '$key' found in remote data. Keeping more recent record (updated_at: $new_updated_at vs $existing_updated_at)");
                $remote_keys[$key] = $row;
            } else {
                // Existing record is more recent or equal, keep it
                dump("⚠️  WARNING: Duplicate key '$key' found in remote data. Keeping existing record (updated_at: $existing_updated_at vs $new_updated_at)");
            }
        } else {
            $remote_keys[$key] = $row;
        }
    }

    // ✅ OPTIMIZATION: If only UBS data exists (no remote), only process UBS keys
    // If only remote data exists (no UBS), only process remote keys
    if (empty($ubs_keys)) {
        // Only remote data - process all remote keys
        $all_keys = array_keys($remote_keys);
    } elseif (empty($remote_keys)) {
        // Only UBS data - process all UBS keys
        $all_keys = array_keys($ubs_keys);
    } else {
        // Both exist - merge keys
        $all_keys = array_unique(array_merge(array_keys($ubs_keys), array_keys($remote_keys)));
    }

    $totalKeys = count($all_keys);
    dump("🔑 Total unique keys to process: " . $totalKeys);

    // Process sync logic efficiently
    $processedKeys = 0;
    $progressInterval = max(100, intval($totalKeys / 20)); // Show progress every 5% or every 100 records, whichever is larger

    foreach ($all_keys as $key) {
        $ubs = $ubs_keys[$key] ?? null;
        $remote = $remote_keys[$key] ?? null;

        if ($ubs && !$remote) {
            // Only UBS data exists - sync to remote
            $sync['remote_data'][] = convert($remote_table_name, $ubs, 'to_remote');
        } elseif (!$ubs && $remote) {
            // Only remote data exists - sync to UBS
            $sync['ubs_data'][] = convert($remote_table_name, $remote, 'to_ubs');
        } elseif ($ubs && $remote) {
            // Both exist - compare timestamps with validation
            $ubs_updated_on = $ubs['UPDATED_ON'] ?? null;
            $remote_updated_on = $remote[$column_updated_at] ?? null;

            // Validate UPDATED_ON fields and convert invalid ones to a consistent fallback
            // Use 1970-01-01 for both to ensure consistent comparison when timestamps are invalid
            if (
                empty($ubs_updated_on) ||
                $ubs_updated_on === '0000-00-00' ||
                $ubs_updated_on === '0000-00-00 00:00:00' ||
                strtotime($ubs_updated_on) === false
            ) {
                $ubs_updated_on = '1970-01-01 00:00:00';
                // dump("Warning: Invalid UPDATED_ON in UBS data: '{$ubs['UPDATED_ON']}' - Using fallback: $ubs_updated_on");
            }

            if (
                empty($remote_updated_on) ||
                $remote_updated_on === '0000-00-00' ||
                $remote_updated_on === '0000-00-00 00:00:00' ||
                strtotime($remote_updated_on) === false
            ) {
                $remote_updated_on = '1970-01-01 00:00:00';
                // dump("Warning: Invalid UPDATED_ON in remote data: '{$remote[$column_updated_at]}' - Using fallback: $remote_updated_on");
            }

            $ubs_time = strtotime($ubs_updated_on);
            $remote_time = strtotime($remote_updated_on);

            if ($ubs_time > $remote_time) {
                $sync['remote_data'][] = convert($remote_table_name, $ubs, 'to_remote');
            } elseif ($remote_time > $ubs_time) {
                $sync['ubs_data'][] = convert($remote_table_name, $remote, 'to_ubs');
            } elseif ($ubs_time == $remote_time) {
                // ✅ SAFE: Log conflicts when timestamps are equal
                logSyncConflict($entity, $key, $ubs_updated_on, $remote_updated_on);
                // No sync needed - both are already in sync
            }
        }

        $processedKeys++;
        // Show progress every N records
        if ($processedKeys % $progressInterval === 0 || $processedKeys === $totalKeys) {
            $percentage = round(($processedKeys / $totalKeys) * 100, 1);
            $memory = getMemoryUsage();
            dump("⏳ Processing keys: $processedKeys/$totalKeys ($percentage%) - Memory: " . $memory['memory_usage_mb'] . "MB");
        }
    }

    // Memory cleanup
    unset($ubs_keys, $remote_keys, $all_keys);
    gc_collect_cycles();

    // Final memory cleanup - suppressed for cleaner output
    $memoryEnd = getMemoryUsage();
    dump("✅ SyncEntity end - Memory: " . $memoryEnd['memory_usage_mb'] . "MB");
    dump("📊 Sync results - Remote: " . count($sync['remote_data']) . ", UBS: " . count($sync['ubs_data']));

    return $sync;
}


function upsertUbs($table, $record)
{

    $keyField = Converter::primaryKey($table);

    $arr = parseUbsTable($table);
    $table = $arr['table'];
    $directory = strtoupper($arr['database']);

    // if(in_array($table,['artran'])) return;

    $path = "C:/$directory/" . ENV::DBF_SUBPATH . "/{$table}.dbf";


    $editor = new \XBase\TableEditor($path, [
        'editMode' => \XBase\TableEditor::EDIT_MODE_CLONE, // safer mode
    ]);

    // Create a column map for easy access
    $columns = $editor->getColumns();
    $columnMap = [];
    foreach ($columns as $column) {
        $columnMap[$column->getName()] = $column;
    }

    $found = false;

    $BASE_RECORD = null;
    // UPDATE IF EXISTS
    while ($row = $editor->nextRecord()) {

        if ($BASE_RECORD == null) {
            // if($row->get('TYPE') == 'SO' && $row->get('REFNO') == 'SO00003' ){
            $BASE_RECORD = $row->getData();
            $BASE_RECORD = array_change_key_case($BASE_RECORD, CASE_UPPER);
            // dd($BASE_RECORD);
            // }

        }

        if (is_array($keyField)) {
            $composite_keys = [];
            $record_composite_keys = [];
            foreach ($keyField as $k) {
                $composite_keys[] = trim($row->get($k));
                $record_composite_keys[] = trim($record[$k]);
            }
            $keyValue = implode('|', $composite_keys);
            $recordKeyValue = implode('|', $record_composite_keys);
        } else {
            $keyValue = trim($row->get($keyField));
            $recordKeyValue = trim($record[$keyField]);
        }

        if ($keyValue === $recordKeyValue) {
            // dump("update: $keyValue");
            // dump("$keyValue === $recordKeyValue");
            foreach ($record as $field => $value) {
                // Use the column map to get the column object directly
                // need lowerr case
                $column = $columnMap[strtolower($field)] ?? null;
                if ($column == null) {
                    continue;
                }
                $fieldType = $column->getType();

                // Special handling for UPDATED_ON field regardless of data type
                if (strtoupper($field) === 'UPDATED_ON') {
                    $value = validateUpdatedOnField($value, $field);
                }

                // Handle boolean fields
                if ($fieldType === 'L') {
                    $value = filter_var($value, FILTER_VALIDATE_BOOLEAN);
                }

                // Handle date fields
                if ($fieldType === 'D') {
                    $parsedDate = parseDateRobust($value);
                    if ($parsedDate !== null) {
                        $value = $parsedDate;
                    } else {
                        // dump("Warning: Invalid date format for field '$field'. Value: '$value'. Setting to null.");
                        $value = null;
                    }
                }



                // if (in_array($field, ['DATE', 'PLA_DODATE'])) {
                //     $value = date('Ymd', strtotime($value));
                // }




                // Check if the column is a boolean type
                // $isBooleanFields = [
                //     'URGENCY',
                //     'TAXINCL',
                //     'IMPSVC',
                //     'FTP',
                //     'DISPATCHED',
                //     'WGST',
                //     'APPLYSC',
                //     'EDGSTAMT',
                //     'RETEX',
                //     'SB',
                //     'IMPSVCT',
                //     'AUTOMOBI',
                //     'MODERNTRA',
                //     'EINVSENT'
                // ];


                // if (in_array($field, $isBooleanFields)) {
                //     $value = filter_var($value, FILTER_VALIDATE_BOOLEAN);
                // }

                // dump("$field => $value");
                try {
                    $row->set($field, $value);
                } catch (\Throwable $e) {
                    // dump($field);
                }
            }

            $editor->writeRecord();
            $editor->save();
            $editor->close();

            $found = true;
            break;
        }
    }

    // INSERT IF NOT FOUND
    $structure = [];
    foreach ($editor->getColumns() as $column) {
        $structure[strtoupper($column->getName())] = $column->getType();
    };

    if (!$found) {
        // dump('insert');
        $newRow = $editor->appendRecord();

        $new_record = $BASE_RECORD;


        foreach ($record as $field => $value) {
            $new_record[$field] = $value;
        }
        // dump($new_record);
        foreach ($new_record as $field => $value) {
            if (!isset($structure[$field])) continue;
            try {
                if ($value === null) $value = "";

                // Special handling for UPDATED_ON field regardless of data type
                if (strtoupper($field) === 'UPDATED_ON') {
                    $value = validateUpdatedOnField($value, $field);
                }

                if ($structure[$field] == 'L' && empty($value)) {
                    $value = false;
                }
                if ($structure[$field] === 'D') {
                    $parsedDate = parseDateRobust($value);
                    if ($parsedDate !== null) {
                        $value = $parsedDate;
                    } else {
                        // dump("Warning: Invalid date format for field '$field'. Value: '$value'. Setting to null.");
                        $value = null;
                    }
                }

                $newRow->set($field, $value);
            } catch (\Throwable $e) {
                var_dump($fieldType);
                var_dump($value);
                // dump("$field => $value caused problem");
                dd($e->getMessage());
            }
        }
        $editor->writeRecord(); // commit the new record
        $editor->save()->close();
    }
}


function upsertRemote($table, $record)
{
    $Core = Core::getInstance();
    $remote_table_name = Converter::table_convert_remote($table);
    // dump($remote_table_name);
    $primary_key = Converter::primaryKey($remote_table_name);

    $db = new mysql;
    $db->connect_remote();

    $table_convert = ['orders'];

    if (in_array($remote_table_name, $table_convert)) {
        $customer_lists = $Core->remote_customer_lists;
        $customer_id = $customer_lists[$record['customer_code']] ?? null;
        $record['customer_id'] = $customer_id;
    }

    if ($remote_table_name == 'order_items') {
        $order_lists = $Core->remote_order_lists;
        $record[$primary_key] = $record['reference_no'] . '|' . $record['item_count'];
        $record['order_id'] = $order_lists[$record['reference_no']] ?? null;
    }

    if (count($record) > 0) {

        // ✅ FIX: update_or_insert now handles duplicates automatically
        // No need for extra validation here - update_or_insert will check and clean duplicates
        $db->update_or_insert($remote_table_name, [$primary_key => $record[$primary_key]], $record);
    }
}




function serialize_record($record)
{
    foreach ($record as $key => $value) {
        if ($value instanceof DateTime) {
            $record[$key] = $value->format('Y-m-d');
        }
    }
    return $record;
}


function read_dbf($dbf_file_path)
{
    // Increase memory limit for large DBF files
    increaseMemoryLimit('4G');

    try {
        $table = new XBase\TableReader($dbf_file_path, [
            'encoding' => 'cp1252',
            // optionally specify columns: 'columns' => ['CUSTNO', 'NAME', ...]
        ]);

        $structure = [];
        foreach ($table->getColumns() as $column) {
            $structure[] = [
                'name' => $column->getName(),
                'type' => $column->getType(),
                'size' => $column->getLength(),
                'decs' => $column->getDecimalCount(),
            ];
        }

        // Log initial memory usage
        $memoryStart = getMemoryUsage();
        // dump("DBF read start - Memory: " . $memoryStart['memory_usage_mb'] . "MB");

        $rows = [];
        $rowCount = 0;
        $batchSize = 10000; // Process in batches of 10k rows

        while ($record = $table->nextRecord()) {
            if ($record->isDeleted()) continue;

            $rowData = [];
            foreach ($structure as $field) {
                $name = $field['name'];
                $value = $record->$name; // access as object property
                $rowData[$name] = ($value instanceof \DateTimeInterface)
                    ? $value->format('Y-m-d')
                    : trim((string)$value);
            }

            $rows[] = $rowData;
            $rowCount++;

            // Memory optimization every batchSize rows
            if ($rowCount % $batchSize === 0) {
                gc_collect_cycles();
                $memoryCurrent = getMemoryUsage();
                // dump("DBF read progress: $rowCount rows - Memory: " . $memoryCurrent['memory_usage_mb'] . "MB");
            }
        }

        // Final memory cleanup
        $memoryEnd = getMemoryUsage();
        // dump("DBF read end - Total rows: $rowCount - Memory: " . $memoryEnd['memory_usage_mb'] . "MB");

        return [
            'structure' => $structure,
            'rows' => $rows,
        ];
    } catch (Exception $e) {
        throw new Exception("Failed to read DBF file: " . $e->getMessage());
    }
}

function parseUbsTable($input)
{
    // Expecting format: ubs_[database]_[table]
    $parts = explode('_', $input, 3);

    if (count($parts) === 3 && $parts[0] === 'ubs') {
        return [
            'database' => $parts[1],
            'table' => $parts[2]
        ];
    }

    return null; // Invalid format
}

/**
 * Robust date parsing function that handles multiple date formats
 * @param string $dateString The date string to parse
 * @return string|null Returns formatted date string or null if invalid
 */
function parseDateRobust($dateString)
{
    if (empty($dateString) || $dateString === '0000-00-00') {
        return null; // Return null for invalid/empty dates
    }

    $dateFormats = [
        'Y-m-d H:i:s',
        'Y-m-d',
        'd/m/Y',
        'm/d/Y',
        'Y-m-d H:i:s.u',
        'Y-m-d\TH:i:s',
        'Y-m-d\TH:i:s.u',
        'd-m-Y',
        'm-d-Y'
    ];

    foreach ($dateFormats as $format) {
        $dateObj = DateTime::createFromFormat($format, $dateString);
        if ($dateObj !== false) {
            return $dateObj->format('Ymd'); // DBF format
        }
    }

    // Fallback to strtotime
    $timestamp = strtotime($dateString);
    if ($timestamp !== false) {
        return date('Ymd', $timestamp);
    }

    return null; // Return null for invalid dates
}

/**
 * Validates and fixes UPDATED_ON field values specifically
 * @param mixed $value The UPDATED_ON value to validate
 * @param string $fieldName The field name for logging
 * @return string Returns valid date string
 */
function validateUpdatedOnField($value, $fieldName = 'UPDATED_ON')
{
    $currentDate = date('Y-m-d H:i:s');

    // Check if UPDATED_ON is invalid
    if (
        empty($value) ||
        $value === '0000-00-00' ||
        $value === '0000-00-00 00:00:00' ||
        strtotime($value) === false ||
        $value === null
    ) {

        // dump("Warning: Invalid $fieldName detected: '$value' - Converting to current date: $currentDate");
        return $currentDate;
    }

    return $value;
}

/**
 * Attempt to repair a corrupted DBF file
 * @param string $filePath Path to the DBF file
 * @return bool True if repair was successful, false otherwise
 */
function attemptDbfRepair($filePath)
{
    if (!file_exists($filePath)) {
        return false;
    }

    try {
        // Create a temporary repair script path
        $repairScript = __DIR__ . '/repair_dbf.php';
        if (!file_exists($repairScript)) {
            ProgressDisplay::warning("Repair script not found at $repairScript");
            return false;
        }

        // Use the repair function directly
        require_once $repairScript;

        $tempPath = $filePath . '.temp.' . time();
        $success = repairDbfFile($filePath, $tempPath);

        if ($success && file_exists($tempPath)) {
            // Create backup of original
            $backupPath = $filePath . '.backup.' . date('YmdHis');
            copy($filePath, $backupPath);

            // Replace original with repaired version
            if (copy($tempPath, $filePath)) {
                unlink($tempPath);
                ProgressDisplay::info("DBF file repaired successfully (backup: $backupPath)");
                return true;
            } else {
                ProgressDisplay::warning("Repair successful but failed to replace original. Repaired file: $tempPath");
                return false;
            }
        }

        return false;
    } catch (\Throwable $e) {
        ProgressDisplay::error("Repair attempt failed: " . $e->getMessage());
        return false;
    }
}

/**
 * Sync icgroup table from icitem GROUP values
 * Truncates icgroup and re-inserts based on unique GROUP values from icitem
 * 
 * @param mysql $db_local Local database connection
 * @param mysql $db_remote Remote database connection
 */
function syncIcgroupFromIcitem($db_local, $db_remote)
{
    try {
        // Truncate icgroup table first
        ProgressDisplay::info("🗑️  Truncating icgroup table...");
        $db_remote->query("TRUNCATE icgroup");

        // Get unique GROUP values from icitem (remote table after sync)
        // Try different possible field names: GROUP, group, GROUP_NAME, etc.
        ProgressDisplay::info("📊 Extracting unique GROUP values from icitem...");

        // First, check what columns exist in icitem
        $columnsCheck = $db_remote->get("SHOW COLUMNS FROM icitem LIKE '%GROUP%'");
        $groupColumn = 'GROUP'; // default

        if (!empty($columnsCheck)) {
            // Use the first GROUP-related column found
            $groupColumn = $columnsCheck[0]['Field'];
        } else {
            // Try common variations
            $possibleColumns = ['GROUP', 'group', 'GROUP_NAME', 'group_name', 'GROUPCODE', 'groupcode'];
            foreach ($possibleColumns as $col) {
                $test = $db_remote->get("SHOW COLUMNS FROM icitem LIKE '$col'");
                if (!empty($test)) {
                    $groupColumn = $col;
                    break;
                }
            }
        }

        ProgressDisplay::info("Using column: $groupColumn for group extraction");

        $sql = "SELECT DISTINCT `$groupColumn` as group_name FROM icitem WHERE `$groupColumn` IS NOT NULL AND `$groupColumn` != '' ORDER BY `$groupColumn`";
        $groups = $db_remote->get($sql);

        if (empty($groups)) {
            ProgressDisplay::info("⚠️  No GROUP values found in icitem table");
            return;
        }

        ProgressDisplay::info("📦 Found " . count($groups) . " unique groups to insert");

        // Prepare icgroup records
        $icgroupRecords = [];
        $currentTime = date('Y-m-d H:i:s');

        foreach ($groups as $group) {
            $groupName = trim($group['group_name']);
            if (empty($groupName)) {
                continue;
            }

            $icgroupRecords[] = [
                'name' => $groupName,
                'description' => null,
                'CREATED_BY' => null,
                'UPDATED_BY' => null,
                'CREATED_ON' => $currentTime,
                'UPDATED_ON' => $currentTime,
                'created_at' => $currentTime,
                'updated_at' => $currentTime,
            ];
        }

        // Batch insert icgroup records
        if (!empty($icgroupRecords)) {
            ProgressDisplay::info("⬆️  Inserting " . count($icgroupRecords) . " groups into icgroup...");

            $batchSize = 100;
            for ($i = 0; $i < count($icgroupRecords); $i += $batchSize) {
                $batch = array_slice($icgroupRecords, $i, $batchSize);

                foreach ($batch as $record) {
                    $db_remote->insert('icgroup', $record);
                }

                ProgressDisplay::info("  ✓ Inserted " . min($i + $batchSize, count($icgroupRecords)) . "/" . count($icgroupRecords) . " groups");
            }

            ProgressDisplay::info("✅ Successfully synced " . count($icgroupRecords) . " groups to icgroup table");
        }
    } catch (Exception $e) {
        ProgressDisplay::error("❌ Error syncing icgroup from icitem: " . $e->getMessage());
        // Don't throw - allow sync to continue even if icgroup sync fails
    }
}

/**
 * Truncate and sync only icitem and icgroup tables
 * This function can be called independently to sync just these two tables
 * 
 * @param mysql|null $db_local Optional local database connection (creates new if not provided)
 * @param mysql|null $db_remote Optional remote database connection (creates new if not provided)
 * @return array Result with success status and counts
 */
function syncIcitemAndIcgroup($db_local = null, $db_remote = null)
{
    $result = [
        'success' => false,
        'icitem_count' => 0,
        'icgroup_count' => 0,
        'error' => null
    ];

    try {
        // Create connections if not provided
        if ($db_local === null) {
            $db_local = new mysql();
        }

        if ($db_remote === null) {
            $db_remote = new mysql();
            $db_remote->connect_remote();
        }

        $ubs_table = 'ubs_ubsstk2015_icitem';
        $remoteTable = 'icitem';

        ProgressDisplay::info("📁 Syncing icitem and icgroup tables only");

        // Step 1: Truncate icitem and icgroup
        ProgressDisplay::info("🗑️  Truncating icitem table...");
        $db_remote->query("TRUNCATE $remoteTable");

        ProgressDisplay::info("🗑️  Truncating icgroup table...");
        $db_remote->query("TRUNCATE icgroup");

        // Step 2: Sync icitem from local to remote
        ProgressDisplay::info("📊 Syncing icitem from local MySQL to remote MySQL...");

        // Count total rows to process (FORCE SYNC: Get ALL records including NULL UPDATED_ON)
        $countSql = "SELECT COUNT(*) as total FROM `$ubs_table`";
        $totalRows = $db_local->first($countSql)['total'] ?? 0;

        ProgressDisplay::info("Total icitem rows to process: $totalRows");

        $icitemCount = 0;

        if ($totalRows > 0) {
            $chunkSize = 1000;
            $offset = 0;

            while ($offset < $totalRows) {
                ProgressDisplay::info("📦 Fetching chunk " . (($offset / $chunkSize) + 1) . " (offset: $offset)");

                // Fetch a chunk of data (FORCE SYNC: Get ALL records including NULL UPDATED_ON)
                $sql = "
                    SELECT * FROM `$ubs_table`
                    ORDER BY COALESCE(UPDATED_ON, '1970-01-01') ASC
                    LIMIT $chunkSize OFFSET $offset
                ";
                $ubs_data = $db_local->get($sql);

                if (empty($ubs_data)) {
                    break; // No more data
                }

                // Validate timestamps
                // For icgroup, preserve NULL values
                $ubs_data = validateAndFixUpdatedOn($ubs_data, $ubs_table);

                // Compare and prepare for sync
                $comparedData = syncEntity($ubs_table, $ubs_data, []);
                $remote_data_to_upsert = $comparedData['remote_data'];

                // Batch upsert to remote
                if (!empty($remote_data_to_upsert)) {
                    ProgressDisplay::info("⬆️ Upserting " . count($remote_data_to_upsert) . " icitem records...");
                    batchUpsertRemote($ubs_table, $remote_data_to_upsert);
                    $icitemCount += count($remote_data_to_upsert);
                }

                // Free memory and move to next chunk
                unset($ubs_data, $comparedData, $remote_data_to_upsert);
                gc_collect_cycles();

                $offset += $chunkSize;

                // Small delay to avoid locking issues
                usleep(300000); // 0.3s
            }

            ProgressDisplay::info("✅ Finished syncing icitem ($icitemCount records)");
        } else {
            ProgressDisplay::info("⚠️  No icitem records to sync");
        }

        // Step 3: Sync icgroup from DBF (local MySQL to remote MySQL)
        $ubs_icgroup_table = 'ubs_ubsstk2015_icgroup';
        $remoteIcgroupTable = 'icgroup';

        ProgressDisplay::info("📊 Syncing icgroup from local MySQL to remote MySQL...");

        // Check if table exists first
        $tableCheckSql = "SHOW TABLES LIKE '$ubs_icgroup_table'";
        $tableExists = $db_local->first($tableCheckSql);

        if (empty($tableExists)) {
            ProgressDisplay::warning("⚠️  Table '$ubs_icgroup_table' does not exist in local database.");
            ProgressDisplay::warning("💡 Please run Python sync first to create the table from icgroup.dbf");
            ProgressDisplay::info("   Run: cd python_sync_local && python sync_icgroup.py");
            $icgroupTotalRows = 0;
        } else {
            // Force sync: Get ALL records regardless of timestamp or NULL values
            $icgroupCountSql = "SELECT COUNT(*) as total FROM `$ubs_icgroup_table`";
            $icgroupTotalRows = $db_local->first($icgroupCountSql)['total'] ?? 0;
            ProgressDisplay::info("🔄 FORCE SYNC: Syncing ALL icgroup records (including NULL UPDATED_ON)");
        }

        ProgressDisplay::info("Total icgroup rows to process: $icgroupTotalRows");

        $icgroupCount = 0;

        if ($icgroupTotalRows > 0) {
            $chunkSize = 1000;
            $offset = 0;

            while ($offset < $icgroupTotalRows) {
                ProgressDisplay::info("📦 Fetching icgroup chunk " . (($offset / $chunkSize) + 1) . " (offset: $offset)");

                // Fetch a chunk of data (FORCE SYNC: Get ALL records including NULL UPDATED_ON)
                $sql = "
                    SELECT * FROM `$ubs_icgroup_table`
                    ORDER BY COALESCE(UPDATED_ON, '1970-01-01') ASC
                    LIMIT $chunkSize OFFSET $offset
                ";
                $icgroup_ubs_data = $db_local->get($sql);

                if (empty($icgroup_ubs_data)) {
                    break; // No more data
                }

                // Validate timestamps
                // For icgroup, preserve NULL values
                $icgroup_ubs_data = validateAndFixUpdatedOn($icgroup_ubs_data, $ubs_icgroup_table);

                // Compare and prepare for sync
                $icgroup_comparedData = syncEntity($ubs_icgroup_table, $icgroup_ubs_data, []);
                $icgroup_remote_data_to_upsert = $icgroup_comparedData['remote_data'];

                // Batch upsert to remote
                if (!empty($icgroup_remote_data_to_upsert)) {
                    ProgressDisplay::info("⬆️ Upserting " . count($icgroup_remote_data_to_upsert) . " icgroup records...");
                    batchUpsertRemote($ubs_icgroup_table, $icgroup_remote_data_to_upsert);
                    $icgroupCount += count($icgroup_remote_data_to_upsert);
                }

                // Free memory and move to next chunk
                unset($icgroup_ubs_data, $icgroup_comparedData, $icgroup_remote_data_to_upsert);
                gc_collect_cycles();

                $offset += $chunkSize;

                // Small delay to avoid locking issues
                usleep(300000); // 0.3s
            }

            ProgressDisplay::info("✅ Finished syncing icgroup ($icgroupCount records)");
        } else {
            ProgressDisplay::info("⚠️  No icgroup records to sync");
        }

        $result['success'] = true;
        $result['icitem_count'] = $icitemCount;
        $result['icgroup_count'] = $icgroupCount;

        ProgressDisplay::info("✅ Successfully synced icitem ($icitemCount records) and icgroup ($icgroupCount records)");
    } catch (Exception $e) {
        $result['error'] = $e->getMessage();
        ProgressDisplay::error("❌ Error syncing icitem and icgroup: " . $e->getMessage());
        throw $e;
    }

    return $result;
}

/**
 * Link customers to users in user_customers table based on agent_no
 * Matches customer.agent_no to user.name or user.username
 * 
 * @param mysql|null $db_remote Optional remote database connection (creates new if not provided)
 */
function linkCustomersToUsers($db_remote = null)
{
    try {
        if ($db_remote === null) {
            $db_remote = new mysql();
            $db_remote->connect_remote();
        }

        ProgressDisplay::info("🔗 Linking customers to users based on agent_no...");

        // Get all customers with agent_no
        $customersSql = "SELECT id, customer_code, agent_no FROM customers WHERE agent_no IS NOT NULL AND agent_no != ''";
        $customers = $db_remote->get($customersSql);

        if (empty($customers)) {
            ProgressDisplay::info("⚠️  No customers with agent_no found");
            return;
        }

        ProgressDisplay::info("📊 Found " . count($customers) . " customers with agent_no");

        // Get all users with their name and username
        $usersSql = "SELECT id, name, username FROM users";
        $users = $db_remote->get($usersSql);

        if (empty($users)) {
            ProgressDisplay::warning("⚠️  No users found in database");
            return;
        }

        // Create a map of agent identifiers to user IDs
        // Match by name first, then username
        $agentToUserId = [];
        foreach ($users as $user) {
            $name = trim($user['name'] ?? '');
            $username = trim($user['username'] ?? '');

            if (!empty($name)) {
                $agentToUserId[strtoupper($name)] = $user['id'];
            }
            if (!empty($username)) {
                $agentToUserId[strtoupper($username)] = $user['id'];
            }
        }

        // Link customers to users
        $linkedCount = 0;
        $skippedCount = 0;
        $existingCount = 0;

        foreach ($customers as $customer) {
            $customerId = $customer['id'];
            $agentNo = trim($customer['agent_no'] ?? '');

            if (empty($agentNo)) {
                $skippedCount++;
                continue;
            }

            // Try to find matching user by agent_no (case-insensitive)
            $agentNoUpper = strtoupper($agentNo);
            $userId = $agentToUserId[$agentNoUpper] ?? null;

            if ($userId === null) {
                $skippedCount++;
                ProgressDisplay::warning("⚠️  No user found for agent_no: '$agentNo' (customer: {$customer['customer_code']})");
                continue;
            }

            // Check if link already exists (use escaped values for safety)
            $userIdEscaped = (int)$userId; // Cast to int for safety
            $customerIdEscaped = (int)$customerId; // Cast to int for safety
            $existingLinkSql = "SELECT id FROM user_customers WHERE user_id = $userIdEscaped AND customer_id = $customerIdEscaped";
            $existingLink = $db_remote->first($existingLinkSql);

            if ($existingLink) {
                $existingCount++;
                continue; // Link already exists
            }

            // Create new link using insert method
            $db_remote->insert('user_customers', [
                'user_id' => $userIdEscaped,
                'customer_id' => $customerIdEscaped,
                'created_at' => date('Y-m-d H:i:s'),
                'updated_at' => date('Y-m-d H:i:s')
            ]);
            $linkedCount++;
        }

        ProgressDisplay::info("✅ Customer-User linking completed:");
        ProgressDisplay::info("   - Linked: $linkedCount customers");
        ProgressDisplay::info("   - Already linked: $existingCount customers");
        ProgressDisplay::info("   - Skipped (no match): $skippedCount customers");
    } catch (Exception $e) {
        ProgressDisplay::error("❌ Error linking customers to users: " . $e->getMessage());
        throw $e;
    }
}

/**
 * Delete orders with 0 order_items from remote server
 * This should be called BEFORE sync starts to clean up orphaned orders
 * Only deletes orders within the sync date range
 * 
 * @param string|null $last_synced_at Last sync timestamp (for normal sync mode)
 * @param string|null $resync_date Resync date in YYYY-MM-DD format (for resync mode)
 * @param bool $resync_mode Whether we're in resync mode
 * @return int Number of orders deleted
 */
function deleteOrdersWithNoItems($last_synced_at = null, $resync_date = null, $resync_mode = false)
{
    try {
        ProgressDisplay::info("🧹 Cleaning up orders with 0 order_items (based on sync date)...");
        
        $db_remote = new mysql();
        $db_remote->connect_remote();
        
        // Start transaction
        $db_remote->query("START TRANSACTION");
        
        // Build date filter based on sync mode
        $dateFilter = "";
        if ($resync_mode && $resync_date) {
            // Resync mode: Filter by exact date
            $escaped_date = $db_remote->escape($resync_date);
            $dateFilter = "AND (DATE(o.order_date) = '$escaped_date' OR DATE(o.created_at) = '$escaped_date' OR DATE(o.updated_at) = '$escaped_date')";
        } elseif ($last_synced_at) {
            // Normal sync mode: Filter by last sync time
            $escaped_date = $db_remote->escape($last_synced_at);
            $dateFilter = "AND (o.order_date > '$escaped_date' OR o.created_at > '$escaped_date' OR o.updated_at > '$escaped_date')";
        }
        // If no date filter, don't delete anything (safety measure)
        if (empty($dateFilter)) {
            ProgressDisplay::info("⚠️  No sync date provided, skipping cleanup to prevent deleting all orders");
            $db_remote->query("ROLLBACK");
            $db_remote->close();
            return 0;
        }
        
        // Delete orders that have no order_items and match the date filter
        // Use LEFT JOIN to find orders without any matching order_items
        $deleteSql = "
            DELETE o FROM orders o
            LEFT JOIN order_items oi ON o.reference_no = oi.reference_no
            WHERE oi.reference_no IS NULL
            $dateFilter
        ";
        
        $db_remote->query($deleteSql);
        
        if ($db_remote->getError()) {
            throw new Exception("Failed to delete orders with no items: " . $db_remote->getError());
        }
        
        $deletedCount = $db_remote->getAffectedRows();
        
        // Commit transaction
        $db_remote->query("COMMIT");
        $db_remote->close();
        
        if ($deletedCount > 0) {
            $dateInfo = $resync_mode ? "for date $resync_date" : "after $last_synced_at";
            ProgressDisplay::info("✅ Deleted $deletedCount order(s) with 0 order_items $dateInfo");
        } else {
            ProgressDisplay::info("✅ No orders with 0 order_items to clean up");
        }
        
        return $deletedCount;
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
        ProgressDisplay::warning("⚠️  Could not delete orders with no items: " . $e->getMessage());
        // Don't fail the entire sync if cleanup fails
        return 0;
    }
}

/**
 * Validate and clean up duplicate orders and order_items
 * This function should be called after sync completes to ensure no duplicates exist
 * 
 * @return array Statistics about duplicates found and cleaned
 */
function validateAndCleanDuplicateOrders()
{
    $start_time = microtime(true);
    $memory_start = getMemoryUsage();

    ProgressDisplay::info("🔍 Starting duplicate orders and order_items validation...");

    $db = new mysql();
    $db->connect_remote();

    $total_stats = [
        'orders' => [
            'duplicate_groups' => 0,
            'total_duplicates' => 0,
            'deleted_records' => 0,
            'execution_time' => 0
        ],
        'order_items' => [
            'duplicate_groups' => 0,
            'total_duplicates' => 0,
            'deleted_records' => 0,
            'execution_time' => 0
        ],
        'total_execution_time' => 0,
        'total_queries' => 0
    ];

    // ============================================
    // 1. VALIDATE ORDERS (by reference_no)
    // ============================================
    $orders_start_time = microtime(true);
    ProgressDisplay::info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    ProgressDisplay::info("📦 Checking for duplicate ORDERS...");

    $duplicates_sql = "
        SELECT reference_no, COUNT(*) as count, GROUP_CONCAT(id ORDER BY updated_at DESC, id DESC) as ids
        FROM orders 
        WHERE reference_no IS NOT NULL AND reference_no != ''
        GROUP BY reference_no 
        HAVING COUNT(*) > 1
        ORDER BY count DESC
    ";

    $duplicate_groups = $db->get($duplicates_sql);
    $total_stats['total_queries']++;

    if (empty($duplicate_groups)) {
        ProgressDisplay::info("✅ No duplicate orders found. All reference_no values are unique.");
    } else {
        $total_duplicate_groups = count($duplicate_groups);
        $total_duplicates = 0;
        $total_deleted = 0;

        ProgressDisplay::warning("⚠️  Found $total_duplicate_groups duplicate reference_no group(s)");

        foreach ($duplicate_groups as $group) {
            $reference_no = $group['reference_no'];
            $count = (int)$group['count'];
            $ids = explode(',', $group['ids']);

            $total_duplicates += ($count - 1); // Minus 1 because we keep one

            ProgressDisplay::info("  📋 reference_no '$reference_no': $count duplicate(s) found (IDs: " . implode(', ', $ids) . ")");

            // Get all records with this reference_no, sorted by most recent first
            $records_sql = "
                SELECT id, updated_at, created_at 
                FROM orders 
                WHERE reference_no = '" . $db->escape($reference_no) . "'
                ORDER BY updated_at DESC, id DESC
            ";

            $records = $db->get($records_sql);
            $total_stats['total_queries']++;

            if (count($records) > 1) {
                // Keep the first (most recent) record
                $keep_record = $records[0];
                $keep_id = $keep_record['id'];

                // Delete all other duplicates
                $delete_ids = [];
                foreach (array_slice($records, 1) as $record) {
                    $delete_ids[] = (int)$record['id'];
                }

                if (!empty($delete_ids)) {
                    $delete_ids_str = implode(',', $delete_ids);
                    $delete_sql = "DELETE FROM orders WHERE id IN ($delete_ids_str)";
                    $db->query($delete_sql);
                    $total_stats['total_queries']++;

                    $deleted_count = count($delete_ids);
                    $total_deleted += $deleted_count;

                    ProgressDisplay::info("    ✅ Kept ID: $keep_id (most recent), deleted $deleted_count duplicate(s): " . implode(', ', $delete_ids));

                    // Also delete associated order_items for deleted orders
                    $order_items_delete_sql = "DELETE FROM order_items WHERE order_id IN ($delete_ids_str)";
                    $db->query($order_items_delete_sql);
                    $total_stats['total_queries']++;
                    $affected_items = $db->affected_rows();
                    if ($affected_items > 0) {
                        ProgressDisplay::info("    🗑️  Also deleted $affected_items associated order_item(s)");
                    }
                }
            }
        }

        $total_stats['orders'] = [
            'duplicate_groups' => $total_duplicate_groups,
            'total_duplicates' => $total_duplicates,
            'deleted_records' => $total_deleted
        ];
    }

    $orders_end_time = microtime(true);
    $total_stats['orders']['execution_time'] = round($orders_end_time - $orders_start_time, 3);
    ProgressDisplay::info("⏱️  Orders validation completed in " . $total_stats['orders']['execution_time'] . " seconds");

    // ============================================
    // 2. VALIDATE ORDER_ITEMS (by unique_key)
    // ============================================
    $order_items_start_time = microtime(true);
    ProgressDisplay::info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    ProgressDisplay::info("📦 Checking for duplicate ORDER_ITEMS...");

    $order_items_duplicates_sql = "
        SELECT unique_key, COUNT(*) as count, GROUP_CONCAT(id ORDER BY updated_at DESC, id DESC) as ids
        FROM order_items 
        WHERE unique_key IS NOT NULL AND unique_key != ''
        GROUP BY unique_key 
        HAVING COUNT(*) > 1
        ORDER BY count DESC
    ";

    $order_items_duplicate_groups = $db->get($order_items_duplicates_sql);
    $total_stats['total_queries']++;

    if (empty($order_items_duplicate_groups)) {
        ProgressDisplay::info("✅ No duplicate order_items found. All unique_key values are unique.");
    } else {
        $total_duplicate_groups = count($order_items_duplicate_groups);
        $total_duplicates = 0;
        $total_deleted = 0;

        ProgressDisplay::warning("⚠️  Found $total_duplicate_groups duplicate unique_key group(s) in order_items");

        foreach ($order_items_duplicate_groups as $group) {
            $unique_key = $group['unique_key'];
            $count = (int)$group['count'];
            $ids = explode(',', $group['ids']);

            $total_duplicates += ($count - 1); // Minus 1 because we keep one

            ProgressDisplay::info("  📋 unique_key '$unique_key': $count duplicate(s) found (IDs: " . implode(', ', $ids) . ")");

            // Get all records with this unique_key, sorted by most recent first
            $records_sql = "
                SELECT id, updated_at, created_at, order_id, reference_no
                FROM order_items 
                WHERE unique_key = '" . $db->escape($unique_key) . "'
                ORDER BY updated_at DESC, id DESC
            ";

            $records = $db->get($records_sql);
            $total_stats['total_queries']++;

            if (count($records) > 1) {
                // Keep the first (most recent) record
                $keep_record = $records[0];
                $keep_id = $keep_record['id'];

                // Delete all other duplicates
                $delete_ids = [];
                foreach (array_slice($records, 1) as $record) {
                    $delete_ids[] = (int)$record['id'];
                }

                if (!empty($delete_ids)) {
                    $delete_ids_str = implode(',', $delete_ids);
                    $delete_sql = "DELETE FROM order_items WHERE id IN ($delete_ids_str)";
                    $db->query($delete_sql);
                    $total_stats['total_queries']++;

                    $deleted_count = count($delete_ids);
                    $total_deleted += $deleted_count;

                    ProgressDisplay::info("    ✅ Kept ID: $keep_id (most recent), deleted $deleted_count duplicate(s): " . implode(', ', $delete_ids));
                }
            }
        }

        $total_stats['order_items'] = [
            'duplicate_groups' => $total_duplicate_groups,
            'total_duplicates' => $total_duplicates,
            'deleted_records' => $total_deleted
        ];
    }

    $order_items_end_time = microtime(true);
    $total_stats['order_items']['execution_time'] = round($order_items_end_time - $order_items_start_time, 3);
    ProgressDisplay::info("⏱️  Order items validation completed in " . $total_stats['order_items']['execution_time'] . " seconds");

    // ============================================
    // 3. SUMMARY
    // ============================================
    $end_time = microtime(true);
    $total_stats['total_execution_time'] = round($end_time - $start_time, 3);
    $memory_end = getMemoryUsage();
    $memory_used = round($memory_end['memory_usage_mb'] - $memory_start['memory_usage_mb'], 2);

    ProgressDisplay::info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    ProgressDisplay::info("📊 Duplicate Validation Summary:");
    ProgressDisplay::info("  ORDERS:");
    ProgressDisplay::info("    • Duplicate groups: " . $total_stats['orders']['duplicate_groups']);
    ProgressDisplay::info("    • Total duplicates: " . $total_stats['orders']['total_duplicates']);
    ProgressDisplay::info("    • Records deleted: " . $total_stats['orders']['deleted_records']);
    ProgressDisplay::info("    • Execution time: " . $total_stats['orders']['execution_time'] . "s");
    ProgressDisplay::info("  ORDER_ITEMS:");
    ProgressDisplay::info("    • Duplicate groups: " . $total_stats['order_items']['duplicate_groups']);
    ProgressDisplay::info("    • Total duplicates: " . $total_stats['order_items']['total_duplicates']);
    ProgressDisplay::info("    • Records deleted: " . $total_stats['order_items']['deleted_records']);
    ProgressDisplay::info("    • Execution time: " . $total_stats['order_items']['execution_time'] . "s");
    ProgressDisplay::info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    ProgressDisplay::info("⚡ Performance Metrics:");
    ProgressDisplay::info("    • Total execution time: " . $total_stats['total_execution_time'] . "s");
    ProgressDisplay::info("    • Total queries executed: " . $total_stats['total_queries']);
    ProgressDisplay::info("    • Memory used: " . $memory_used . "MB");
    ProgressDisplay::info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    $total_deleted_all = $total_stats['orders']['deleted_records'] + $total_stats['order_items']['deleted_records'];

    if ($total_deleted_all > 0) {
        ProgressDisplay::complete("✅ Duplicate validation completed. Cleaned up " . $total_stats['orders']['deleted_records'] . " order(s) and " . $total_stats['order_items']['deleted_records'] . " order_item(s) in " . $total_stats['total_execution_time'] . "s.");
    } else {
        ProgressDisplay::info("✅ Duplicate validation completed. No cleanup needed. (Time: " . $total_stats['total_execution_time'] . "s)");
    }

    // Close database connection
    $db->close();

    return [
        'orders' => $total_stats['orders'],
        'order_items' => $total_stats['order_items'],
        'total_deleted' => $total_deleted_all,
        'total_execution_time' => $total_stats['total_execution_time'],
        'total_queries' => $total_stats['total_queries'],
        'memory_used_mb' => $memory_used
    ];
}

/**
 * FORCE SYNC artran (orders) and ictran (order_items) with date filter
 * Always syncs ALL records with order_date >= minOrderDate (default: 2025-12-01), regardless of:
 * - updated_at timestamps
 * - sync logs
 * - existing records
 * - Skips all DO type orders (DO normally only insert from UBS to server, not synced back)
 * This is a true force sync that will upsert everything matching the date filter
 */
function syncArtranAndIctran($db_local = null, $db_remote = null, $minOrderDate = '2025-12-01')
{
    $result = [
        'success' => false,
        'artran_count' => 0,
        'ictran_count' => 0,
        'error' => null
    ];

    try {
        // Create connections if not provided
        if ($db_local === null) {
            $db_local = new mysql();
        }

        if ($db_remote === null) {
            $db_remote = new mysql();
            $db_remote->connect_remote();
        }

        ProgressDisplay::info("📁 Syncing artran (orders) and ictran (order_items) with order_date >= $minOrderDate");

        // ========== SYNC ARTRAN (ORDERS) ==========
        $ubs_artran_table = 'ubs_ubsstk2015_artran';
        $remoteArtranTable = 'orders';

        ProgressDisplay::info("📊 Syncing artran (orders) from local MySQL to remote MySQL...");

        // Check if table exists first
        $tableCheckSql = "SHOW TABLES LIKE '$ubs_artran_table'";
        $tableExists = $db_local->first($tableCheckSql);

        if (empty($tableExists)) {
            ProgressDisplay::warning("⚠️  Table '$ubs_artran_table' does not exist in local database.");
            $artranTotalRows = 0;
        } else {
            // Force sync with date filter: Get all records with DATE >= minOrderDate
            // Skip DO type orders (DO normally only insert from UBS to server, not synced back)
            $artranCountSql = "SELECT COUNT(*) as total FROM `$ubs_artran_table` WHERE DATE >= '$minOrderDate' AND TYPE != 'DO'";
            $artranTotalRows = $db_local->first($artranCountSql)['total'] ?? 0;
            ProgressDisplay::info("🔄 FORCE SYNC: Syncing artran records with DATE >= $minOrderDate (skipping DO type orders)");
        }

        ProgressDisplay::info("Total artran rows to process: $artranTotalRows");

        $artranCount = 0;

        if ($artranTotalRows > 0) {
            $chunkSize = 1000;
            $offset = 0;

            while ($offset < $artranTotalRows) {
                ProgressDisplay::info("📦 Fetching artran chunk " . (($offset / $chunkSize) + 1) . " (offset: $offset)");

                // Fetch a chunk of data with date filter
                // Skip DO type orders (DO normally only insert from UBS to server, not synced back)
                $sql = "
                    SELECT * FROM `$ubs_artran_table`
                    WHERE DATE >= '$minOrderDate' AND TYPE != 'DO'
                    ORDER BY DATE ASC, COALESCE(UPDATED_ON, '1970-01-01') ASC
                    LIMIT $chunkSize OFFSET $offset
                ";
                $artran_ubs_data = $db_local->get($sql);

                if (empty($artran_ubs_data)) {
                    break; // No more data
                }

                // Validate timestamps
                $artran_ubs_data = validateAndFixUpdatedOn($artran_ubs_data, $ubs_artran_table);

                // FORCE SYNC: Fetch ALL remote records with date filter (not just matching keys)
                // Skip DO type orders (DO normally only insert from UBS to server, not synced back)
                $artran_remote_data = [];
                $db_remote_check = new mysql();
                $db_remote_check->connect_remote();
                $remoteSql = "SELECT * FROM `$remoteArtranTable` WHERE order_date >= '$minOrderDate' AND order_type != 'DO'";
                $artran_remote_data = $db_remote_check->get($remoteSql);
                $db_remote_check->close();

                // FORCE SYNC: Convert all UBS data to remote format and upsert (ignore comparisons)
                $artran_remote_data_to_upsert = [];
                foreach ($artran_ubs_data as $row) {
                    $converted = convert($remoteArtranTable, $row, 'to_remote');
                    if (!empty($converted)) {
                        $artran_remote_data_to_upsert[] = $converted;
                    }
                }

                // Batch upsert to remote (FORCE - always sync all records)
                if (!empty($artran_remote_data_to_upsert)) {
                    ProgressDisplay::info("⬆️ FORCE Upserting " . count($artran_remote_data_to_upsert) . " artran records to remote...");
                    batchUpsertRemote($ubs_artran_table, $artran_remote_data_to_upsert);
                    $artranCount += count($artran_remote_data_to_upsert);
                }

                // Also sync remote → UBS for any missing records
                if (!empty($artran_remote_data)) {
                    $artran_ubs_data_to_upsert = [];
                    foreach ($artran_remote_data as $row) {
                        $converted = convert($remoteArtranTable, $row, 'to_ubs');
                        if (!empty($converted)) {
                            $artran_ubs_data_to_upsert[] = $converted;
                        }
                    }
                    
                    if (!empty($artran_ubs_data_to_upsert)) {
                        ProgressDisplay::info("⬇️ FORCE Upserting " . count($artran_ubs_data_to_upsert) . " artran records to UBS...");
                        executeSyncWithTransaction(function() use ($ubs_artran_table, $artran_ubs_data_to_upsert) {
                            batchUpsertUbs($ubs_artran_table, $artran_ubs_data_to_upsert);
                        }, true);
                    }
                }

                // Free memory and move to next chunk
                unset($artran_ubs_data, $artran_remote_data, $artran_comparedData, $artran_remote_data_to_upsert, $artran_ubs_data_to_upsert);
                gc_collect_cycles();

                $offset += $chunkSize;

                // Small delay to avoid locking issues
                usleep(300000); // 0.3s
            }

            ProgressDisplay::info("✅ Finished syncing artran ($artranCount records)");
        } else {
            ProgressDisplay::info("⚠️  No artran records to sync");
        }

        // ========== SYNC ICTRAN (ORDER_ITEMS) ==========
        $ubs_ictran_table = 'ubs_ubsstk2015_ictran';
        $remoteIctranTable = 'order_items';

        ProgressDisplay::info("📊 Syncing ictran (order_items) from local MySQL to remote MySQL...");

        // Check if table exists first
        $tableCheckSql = "SHOW TABLES LIKE '$ubs_ictran_table'";
        $tableExists = $db_local->first($tableCheckSql);

        if (empty($tableExists)) {
            ProgressDisplay::warning("⚠️  Table '$ubs_ictran_table' does not exist in local database.");
            $ictranTotalRows = 0;
        } else {
            // Force sync with date filter: Get all records with DATE >= minOrderDate
            // For ictran, we need to join with orders to filter by order_date
            // Skip ictran records that belong to DO type orders (DO normally only insert from UBS to server)
            $ictranCountSql = "
                SELECT COUNT(*) as total 
                FROM `$ubs_ictran_table` ictran
                INNER JOIN `$ubs_artran_table` artran ON ictran.REFNO = artran.REFNO
                WHERE artran.DATE >= '$minOrderDate' AND artran.TYPE != 'DO'
            ";
            $ictranTotalRows = $db_local->first($ictranCountSql)['total'] ?? 0;
            ProgressDisplay::info("🔄 FORCE SYNC: Syncing ictran records with order_date >= $minOrderDate (skipping DO type orders)");
        }

        ProgressDisplay::info("Total ictran rows to process: $ictranTotalRows");

        $ictranCount = 0;

        if ($ictranTotalRows > 0) {
            $chunkSize = 1000;
            $offset = 0;

            while ($offset < $ictranTotalRows) {
                ProgressDisplay::info("📦 Fetching ictran chunk " . (($offset / $chunkSize) + 1) . " (offset: $offset)");

                // Fetch a chunk of data with date filter (join with artran to filter by order_date)
                // Skip ictran records that belong to DO type orders (DO normally only insert from UBS to server)
                $sql = "
                    SELECT ictran.* 
                    FROM `$ubs_ictran_table` ictran
                    INNER JOIN `$ubs_artran_table` artran ON ictran.REFNO = artran.REFNO
                    WHERE artran.DATE >= '$minOrderDate' AND artran.TYPE != 'DO'
                    ORDER BY artran.DATE ASC, ictran.ITEMCOUNT ASC, COALESCE(ictran.UPDATED_ON, '1970-01-01') ASC
                    LIMIT $chunkSize OFFSET $offset
                ";
                $ictran_ubs_data = $db_local->get($sql);

                if (empty($ictran_ubs_data)) {
                    break; // No more data
                }

                // Validate timestamps
                $ictran_ubs_data = validateAndFixUpdatedOn($ictran_ubs_data, $ubs_ictran_table);

                // FORCE SYNC: Fetch ALL remote records with date filter (not just matching keys)
                // Skip ictran records that belong to DO type orders (DO normally only insert from UBS to server)
                $ictran_remote_data = [];
                $db_remote_check = new mysql();
                $db_remote_check->connect_remote();
                $remoteSql = "SELECT oi.* FROM `$remoteIctranTable` oi 
                             INNER JOIN `$remoteArtranTable` o ON oi.reference_no = o.reference_no 
                             WHERE o.order_date >= '$minOrderDate' AND o.order_type != 'DO'";
                $ictran_remote_data = $db_remote_check->get($remoteSql);
                $db_remote_check->close();

                // FORCE SYNC: Convert all UBS data to remote format and upsert (ignore comparisons)
                $ictran_remote_data_to_upsert = [];
                foreach ($ictran_ubs_data as $row) {
                    $converted = convert($remoteIctranTable, $row, 'to_remote');
                    if (!empty($converted)) {
                        $ictran_remote_data_to_upsert[] = $converted;
                    }
                }

                // Batch upsert to remote (FORCE - always sync all records)
                if (!empty($ictran_remote_data_to_upsert)) {
                    ProgressDisplay::info("⬆️ FORCE Upserting " . count($ictran_remote_data_to_upsert) . " ictran records to remote...");
                    batchUpsertRemote($ubs_ictran_table, $ictran_remote_data_to_upsert);
                    $ictranCount += count($ictran_remote_data_to_upsert);
                }

                // Also sync remote → UBS for any missing records
                if (!empty($ictran_remote_data)) {
                    $ictran_ubs_data_to_upsert = [];
                    foreach ($ictran_remote_data as $row) {
                        $converted = convert($remoteIctranTable, $row, 'to_ubs');
                        if (!empty($converted)) {
                            $ictran_ubs_data_to_upsert[] = $converted;
                        }
                    }
                    
                    if (!empty($ictran_ubs_data_to_upsert)) {
                        ProgressDisplay::info("⬇️ FORCE Upserting " . count($ictran_ubs_data_to_upsert) . " ictran records to UBS...");
                        executeSyncWithTransaction(function() use ($ubs_ictran_table, $ictran_ubs_data_to_upsert) {
                            batchUpsertUbs($ubs_ictran_table, $ictran_ubs_data_to_upsert);
                        }, true);
                    }
                }

                // Free memory and move to next chunk
                unset($ictran_ubs_data, $ictran_remote_data, $ictran_comparedData, $ictran_remote_data_to_upsert, $ictran_ubs_data_to_upsert);
                gc_collect_cycles();

                $offset += $chunkSize;

                // Small delay to avoid locking issues
                usleep(300000); // 0.3s
            }

            ProgressDisplay::info("✅ Finished syncing ictran ($ictranCount records)");
        } else {
            ProgressDisplay::info("⚠️  No ictran records to sync");
        }

        $result['success'] = true;
        $result['artran_count'] = $artranCount;
        $result['ictran_count'] = $ictranCount;

        ProgressDisplay::info("✅ Successfully synced artran ($artranCount records) and ictran ($ictranCount records)");
    } catch (Exception $e) {
        $result['error'] = $e->getMessage();
        ProgressDisplay::error("❌ Error syncing artran and ictran: " . $e->getMessage());
        throw $e;
    }

    return $result;
}
