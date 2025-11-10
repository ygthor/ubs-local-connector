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

    // Group records by operation type for better performance
    $updateRecords = [];
    $insertRecords = [];

    // First, identify which records need updates vs inserts
    $editor = new \XBase\TableEditor($path, [
        'editMode' => \XBase\TableEditor::EDIT_MODE_CLONE,
    ]);

    // Create index of existing records
    $existingRecords = [];
    while ($row = $editor->nextRecord()) {
        $key = getRecordKey($row, $keyField);
        $existingRecords[$key] = $row;
    }
    $editor->close();

    // Categorize records
    foreach ($records as $record) {
        $key = getRecordKey($record, $keyField);
        if (isset($existingRecords[$key])) {
            $updateRecords[] = ['key' => $key, 'record' => $record, 'row' => $existingRecords[$key]];
        } else {
            $insertRecords[] = $record;
        }
    }

    // Process updates in batches
    if (!empty($updateRecords)) {
        ProgressDisplay::info("Processing " . count($updateRecords) . " updates for $table_name");

        for ($i = 0; $i < count($updateRecords); $i += $batchSize) {
            $batch = array_slice($updateRecords, $i, $batchSize);

            $editor = new \XBase\TableEditor($path, [
                'editMode' => \XBase\TableEditor::EDIT_MODE_CLONE,
            ]);

            foreach ($batch as $item) {
                $row = $item['row'];
                $record = $item['record'];

                // Update the record
                updateUbsRecord($editor, $row, $record, $table_name);
            }

            $editor->save()->close();
            $processed += count($batch);
            // ProgressDisplay::display("Updating $table_name", $processed, count($updateRecords));

            gc_collect_cycles();
        }
    }

    // Process inserts in batches
    if (!empty($insertRecords)) {
        ProgressDisplay::info("Processing " . count($insertRecords) . " inserts for $table_name");

        for ($i = 0; $i < count($insertRecords); $i += $batchSize) {
            $batch = array_slice($insertRecords, $i, $batchSize);

            $editor = new \XBase\TableEditor($path, [
                'editMode' => \XBase\TableEditor::EDIT_MODE_CLONE,
            ]);

            foreach ($batch as $record) {
                insertUbsRecord($editor, $record, $table_name);
            }

            $editor->save()->close();
            $processed += count($batch);
            // ProgressDisplay::display("Inserting $table_name", $processed, count($insertRecords));

            gc_collect_cycles();
        }
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
        $columnMap[$column->getName()] = $column;
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

        try {
            $row->set($field, $value);
        } catch (\Throwable $e) {
            // Skip problematic fields
        }
    }

    $editor->writeRecord();
}

function insertUbsRecord($editor, $record, $table_name)
{
    $structure = [];
    foreach ($editor->getColumns() as $column) {
        $structure[strtoupper($column->getName())] = $column->getType();
    }

    $newRow = $editor->appendRecord();

    foreach ($record as $field => $value) {
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
            // Skip problematic fields
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
        SELECT * FROM $alias_table WHERE $column_updated_at >= '$updatedAfter'
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

            // Validate UPDATED_ON fields and convert invalid ones to current date
            if (
                empty($ubs_updated_on) ||
                $ubs_updated_on === '0000-00-00' ||
                $ubs_updated_on === '0000-00-00 00:00:00' ||
                strtotime($ubs_updated_on) === false
            ) {
                $ubs_updated_on = '1970-01-01 00:00:00';
                // dump("Warning: Invalid UPDATED_ON in UBS data: '{$ubs['UPDATED_ON']}' - Using current date: $ubs_updated_on");
            }

            if (
                empty($remote_updated_on) ||
                $remote_updated_on === '0000-00-00' ||
                $remote_updated_on === '0000-00-00 00:00:00' ||
                strtotime($remote_updated_on) === false
            ) {
                $remote_updated_on = date('Y-m-d H:i:s');
                // dump("Warning: Invalid UPDATED_ON in remote data: '{$remote[$column_updated_at]}' - Using current date: $remote_updated_on");
            }

            $ubs_time = strtotime($ubs_updated_on);
            $remote_time = strtotime($remote_updated_on);

            if ($ubs_time > $remote_time) {
                $sync['remote_data'][] = convert($remote_table_name, $ubs, 'to_remote');
            } elseif ($remote_time > $ubs_time) {
                $sync['ubs_data'][] = convert($remote_table_name, $remote, 'to_ubs');
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
