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
        return;
    }

    // ✅ SAFE: Use retry logic for reliability
    retryOperation(function() use ($table, $records, $batchSize) {
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

        if ($remote_table_name == 'artrans_items') {
            $remote_artrans_lists = $Core->remote_artrans_lists;
            $record[$primary_key] = $record['REFNO'] . '|' . $record['ITEMCOUNT'];
            $record['artrans_id'] = $remote_artrans_lists[$record['REFNO']] ?? null;
        }

        if (count($record) > 0) {
            $processedRecords[] = $record;
        }
    }

    // Use bulk upsert for better performance
    for ($i = 0; $i < count($processedRecords); $i += $batchSize) {
        $batch = array_slice($processedRecords, $i, $batchSize);

        // Bulk upsert using MySQL's ON DUPLICATE KEY UPDATE
        $db->bulkUpsert($remote_table_name, $batch, $primary_key);

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
}

function batchUpsertUbs($table, $records, $batchSize = 500)
{
    if (empty($records)) {
        return;
    }

    $arr = parseUbsTable($table);
    $table_name = $arr['table'];
    $directory = strtoupper($arr['database']);
    $path = "C:/$directory/" . ENV::DBF_SUBPATH . "/{$table_name}.dbf";

    $keyField = Converter::primaryKey($table);
    $totalRecords = count($records);
    $processed = 0;

    ProgressDisplay::info("Starting batch upsert for UBS $table_name ($totalRecords records)");

    // Check if file exists and is accessible
    if (!file_exists($path)) {
        throw new Exception("DBF file not found: $path");
    }
    
    if (!is_readable($path)) {
        throw new Exception("DBF file is not readable: $path");
    }
    
    // Try to detect and repair corruption before processing
    try {
        $testReader = new \XBase\TableReader($path);
        $testReader->close();
    } catch (\Throwable $e) {
        $errorMsg = strtolower($e->getMessage());
        if (strpos($errorMsg, 'clone') !== false || 
            strpos($errorMsg, 'corrupt') !== false ||
            strpos($errorMsg, 'length') !== false ||
            strpos($errorMsg, 'invalid') !== false ||
            strpos($errorMsg, 'bytes') !== false) {
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

    // Try to open with clone mode first, fallback to realtime if clone fails
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
                // If clone mode fails, try realtime mode as fallback
                if ($editMode === \XBase\TableEditor::EDIT_MODE_CLONE) {
                    ProgressDisplay::warning("Clone mode failed for $table_name, trying realtime mode: " . $e->getMessage());
                    $editMode = \XBase\TableEditor::EDIT_MODE_REALTIME;
                    $retryCount = 0; // Reset retry count for realtime mode
                    continue;
                } else {
                    throw new Exception("Failed to open DBF file after $maxRetries retries: " . $e->getMessage());
                }
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

    // Categorize records (store only keys, not row objects since they can't be reused)
    foreach ($records as $record) {
        $key = getRecordKey($record, $keyField);
        if (isset($existingRecords[$key])) {
            $updateRecords[] = ['key' => $key, 'record' => $record];
        } else {
            $insertRecords[] = $record;
        }
    }
    
    // Clear existingRecords to free memory (we only need the keys now)
    unset($existingRecords);

    // Process updates in batches
    if (!empty($updateRecords)) {
        ProgressDisplay::info("Processing " . count($updateRecords) . " updates for $table_name");

        for ($i = 0; $i < count($updateRecords); $i += $batchSize) {
            $batch = array_slice($updateRecords, $i, $batchSize);
            $batchEditor = null;
            $retryCount = 0;
            
            while ($retryCount < $maxRetries) {
                try {
                    $batchEditor = new \XBase\TableEditor($path, [
                        'editMode' => $editMode,
                    ]);
                    
                    // Need to find the records again since we're opening a new editor
                    foreach ($batch as $item) {
                        $key = $item['key'];
                        $record = $item['record'];
                        
                        // Find the record in the editor
                        $batchEditor->moveTo(0);
                        $found = false;
                        while ($row = $batchEditor->nextRecord()) {
                            $rowKey = getRecordKey($row, $keyField);
                            if ($rowKey === $key) {
                                updateUbsRecord($batchEditor, $row, $record, $table_name);
                                $found = true;
                                break;
                            }
                        }
                        if (!$found) {
                            ProgressDisplay::warning("Record with key '$key' not found for update in $table_name");
                        }
                    }

                    if ($editMode === \XBase\TableEditor::EDIT_MODE_CLONE) {
                        $batchEditor->save();
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
                        if ($editMode === \XBase\TableEditor::EDIT_MODE_CLONE) {
                            ProgressDisplay::warning("Clone mode failed, trying realtime mode for batch update");
                            $editMode = \XBase\TableEditor::EDIT_MODE_REALTIME;
                            $retryCount = 0;
                            continue;
                        } else {
                            throw new Exception("Failed to update batch in $table_name: " . $e->getMessage());
                        }
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

                    foreach ($batch as $record) {
                        insertUbsRecord($batchEditor, $record, $table_name);
                    }

                    if ($editMode === \XBase\TableEditor::EDIT_MODE_CLONE) {
                        $batchEditor->save();
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
                        if ($editMode === \XBase\TableEditor::EDIT_MODE_CLONE) {
                            ProgressDisplay::warning("Clone mode failed, trying realtime mode for batch insert");
                            $editMode = \XBase\TableEditor::EDIT_MODE_REALTIME;
                            $retryCount = 0;
                            continue;
                        } else {
                            throw new Exception("Failed to insert batch in $table_name: " . $e->getMessage());
                        }
                    }
                    usleep(100000 * $retryCount); // Exponential backoff
                }
            }
            
            $processed += count($batch);
            gc_collect_cycles();
        }
    }

    // ✅ Also update local MySQL to keep it in sync
    // This ensures that when PHP sync updates UBS, local MySQL is also updated
    // This prevents issues when re-syncing
    try {
        ProgressDisplay::info("Updating local MySQL for $table ($totalRecords records)");
        $db_local = new mysql(); // Connects to local database by default
        
        // Convert records back to local MySQL format if needed
        // The records are already in UBS format, so we can use them directly
        $localRecords = [];
        foreach ($records as $record) {
            // Ensure UPDATED_ON is set to current time if updating
            if (!isset($record['UPDATED_ON']) || empty($record['UPDATED_ON'])) {
                $record['UPDATED_ON'] = date('Y-m-d H:i:s');
            }
            $localRecords[] = $record;
        }
        
        if (!empty($localRecords)) {
            // Use bulk upsert for local MySQL
            $db_local->bulkUpsert($table, $localRecords, $keyField);
            ProgressDisplay::info("✅ Updated local MySQL for $table");
        }
    } catch (Exception $e) {
        // Log error but don't fail the entire sync
        ProgressDisplay::warning("⚠️  Failed to update local MySQL for $table: " . $e->getMessage());
        ProgressDisplay::warning("UBS update succeeded, but local MySQL was not updated. This may cause issues on next sync.");
    }

    ProgressDisplay::info("Completed batch upsert for UBS $table_name");
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
    foreach ($columns as $column) {
        $colName = strtolower($column->getName());
        $columnMap[$colName] = $column;
    }

    foreach ($record as $field => $value) {
        if (in_array($field, ['artrans_id'])) {
            continue;
        }

        $column = $columnMap[strtolower($field)] ?? null;
        if ($column == null) {
            continue;
        }

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
            $row->set($field, $value);
        } catch (\Throwable $e) {
            // Log the problematic field but continue
            ProgressDisplay::warning("Skipping field '$field' in $table_name: " . $e->getMessage());
        }
    }

    $editor->writeRecord();
}

function insertUbsRecord($editor, $record, $table_name)
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

    foreach ($record as $field => $value) {
        $fieldUpper = strtoupper($field);
        if (!isset($structure[$fieldUpper])) continue;

        try {
            if ($value === null) $value = "";

            $fieldType = $structure[$fieldUpper]['type'];
            $fieldLength = $structure[$fieldUpper]['length'];

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
                $decimal = $structure[$fieldUpper]['decimal'];
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

            $newRow->set($field, $value);
        } catch (\Throwable $e) {
            // Log the problematic field but continue
            ProgressDisplay::warning("Skipping field '$field' in $table_name: " . $e->getMessage());
        }
    }

    $editor->writeRecord();
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

    $sql = "
        SELECT * FROM $alias_table WHERE $column_updated_at > '$updatedAfter'
    ";

    // Debug information - suppressed for cleaner output
    // dump("fetchServerData Debug:");
    // dump("  Table: $table");
    // dump("  Remote table: $alias_table");
    // dump("  Updated column: $column_updated_at");
    // dump("  Updated after: $updatedAfter");
    // dump("  SQL: $sql");

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

    if ($map == []) {
        return $dataRow; // no need convert
    }




    foreach ($map as $ubs => $remote) {
        if ($direction === 'to_remote') {
            $converted[$remote] = $ubs ? ($dataRow[$ubs] ?? null) : null;
        } else {
            if ($ubs) {
                $converted[$ubs] = $dataRow[$remote] ?? null;
            }
        }
    }

    // Validate and fix UPDATED_ON field in converted data
    if (isset($converted['UPDATED_ON'])) {
        $updatedOn = $converted['UPDATED_ON'];
        if (
            empty($updatedOn) ||
            $updatedOn === '0000-00-00' ||
            $updatedOn === '0000-00-00 00:00:00' ||
            strtotime($updatedOn) === false
        ) {
            $converted['UPDATED_ON'] = '1970-01-01 00:00:00';
            // dump("Warning: Invalid UPDATED_ON in converted data: '$updatedOn' - Using current date: {$converted['UPDATED_ON']}");
        }
    }

    if ($direction == 'to_remote') {
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
                    $id = $dataRow['order_id'];
                }

                $sql = "SELECT $field FROM $table WHERE id='$id'";
                $col = $db->first($sql);

                $converted[$key] = $col[$field];
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
        $ubs_keys[$key] = $row;
    }

    // Process remote data
    foreach ($remote_data as $row) {
        $key = $row[$remote_key] ?? '';
        $remote_keys[$key] = $row;
    }

    // Get all unique keys
    $all_keys = array_unique(array_merge(array_keys($ubs_keys), array_keys($remote_keys)));
    dump("🔑 Total unique keys to process: " . count($all_keys));

    // Process sync logic efficiently
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
                if (in_array($field, ['artrans_id'])) {
                    continue;
                }
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

    if ($remote_table_name == 'artrans_items') {
        $remote_artrans_lists = $Core->remote_artrans_lists;
        $record[$primary_key] = $record['REFNO'] . '|' . $record['ITEMCOUNT'];
        $record['artrans_id'] = $remote_artrans_lists[$record['REFNO']] ?? null;
    }

    if ($remote_table_name == 'artrans') {
        // dd($record);
        // dd($primary_key);
    }

    if (count($record) > 0) {
        if ($remote_table_name == 'artrans_items') {
            // dd($primary_key);
            // dd($record);
        }

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
        
        // Count total rows to process
        $countSql = "SELECT COUNT(*) as total FROM `$ubs_table` WHERE UPDATED_ON IS NOT NULL";
        $totalRows = $db_local->first($countSql)['total'] ?? 0;
        
        ProgressDisplay::info("Total icitem rows to process: $totalRows");
        
        $icitemCount = 0;
        
        if ($totalRows > 0) {
            $chunkSize = 1000;
            $offset = 0;
            
            while ($offset < $totalRows) {
                ProgressDisplay::info("📦 Fetching chunk " . (($offset / $chunkSize) + 1) . " (offset: $offset)");
                
                // Fetch a chunk of data
                $sql = "
                    SELECT * FROM `$ubs_table`
                    WHERE UPDATED_ON IS NOT NULL
                    ORDER BY UPDATED_ON ASC
                    LIMIT $chunkSize OFFSET $offset
                ";
                $ubs_data = $db_local->get($sql);
                
                if (empty($ubs_data)) {
                    break; // No more data
                }
                
                // Validate timestamps
                $ubs_data = validateAndFixUpdatedOn($ubs_data);
                
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
        
        // Step 3: Sync icgroup from icitem GROUP values
        ProgressDisplay::info("🔄 Syncing icgroup from icitem GROUP values...");
        syncIcgroupFromIcitem($db_local, $db_remote);
        
        // Get count of synced icgroup records
        $icgroupCountResult = $db_remote->first("SELECT COUNT(*) as total FROM icgroup");
        $icgroupCount = $icgroupCountResult['total'] ?? 0;
        
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
