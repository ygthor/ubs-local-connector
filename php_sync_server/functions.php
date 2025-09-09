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
    
    // Set error reporting
    error_reporting(E_ERROR | E_WARNING | E_PARSE);
    ini_set('display_errors', 1);
    
    // Log configuration
    if (function_exists('dump')) {
        dump("🚀 Sync environment initialized:");
        dump("- Memory limit: " . ini_get('memory_limit'));
        dump("- Execution time limit: " . ini_get('max_execution_time'));
        dump("- Garbage collection: " . (ini_get('zend.enable_gc') ? 'Enabled' : 'Disabled'));
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
        echo "\n⚠️  WARNING: " . $message . "\n";
    }
    
    public static function info($message)
    {
        echo "\nℹ️  INFO: " . $message . "\n";
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
        dump("Processed: $processed/$total ($percentage%) - Memory: " . getMemoryUsage()['memory_usage_mb'] . "MB");
    }
    
    return $results;
}

// Batch processing functions for better performance
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
    
    ProgressDisplay::info("Starting batch upsert for $remote_table_name ($totalRecords records)");
    
    for ($i = 0; $i < $totalRecords; $i += $batchSize) {
        $batch = array_slice($records, $i, $batchSize);
        
        // Process each record in the batch
        foreach ($batch as $record) {
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
                $db->update_or_insert($remote_table_name, [$primary_key => $record[$primary_key]], $record);
            }
        }
        
        $processed += count($batch);
        ProgressDisplay::display("Processing $remote_table_name", $processed, $totalRecords);
        
        // Memory cleanup between batches
        if ($i + $batchSize < $totalRecords) {
            gc_collect_cycles();
        }
    }
    
    ProgressDisplay::info("Completed batch upsert for $remote_table_name");
}

function batchUpsertUbs($table, $records, $batchSize = 100)
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
            ProgressDisplay::display("Updating $table_name", $processed, count($updateRecords));
            
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
            ProgressDisplay::display("Inserting $table_name", $processed, count($insertRecords));
            
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
            $composite_keys[] = trim($record[$k] ?? '');
        }
        return implode('|', $composite_keys);
    } else {
        return trim($record[$keyField] ?? '');
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
        
        // Handle boolean fields
        if ($fieldType === 'L') {
            $value = filter_var($value, FILTER_VALIDATE_BOOLEAN);
        }
        
        // Handle date fields
        if ($fieldType === 'D') {
            if (empty($value) || $value === '0000-00-00') {
                $value = "        ";
            } else {
                $timestamp = strtotime($value);
                if ($timestamp !== false) {
                    $value = date('Ymd', $timestamp);
                } else {
                    $value = "        ";
                }
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
            
            if ($structure[$field] == 'L' && empty($value)) {
                $value = false;
            }
            if ($structure[$field] === 'D') {
                $value = date('Ymd', strtotime($value));
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

    $sql = "SELECT * FROM $alias_table WHERE $column_updated_at >= '$updatedAfter'";
    
    // Log memory usage before fetching
    $memoryBefore = getMemoryUsage();
    dump("Memory before fetch: " . $memoryBefore['memory_usage_mb'] . "MB");
    
    $data = $db->get($sql);
    
    // Log memory usage after fetching
    $memoryAfter = getMemoryUsage();
    dump("Memory after fetch: " . $memoryAfter['memory_usage_mb'] . "MB");
    dump("Data rows fetched: " . count($data));
    
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
                $converted[$ubs] = $dataRow[$remote] ?? $remote;
            }
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

    $ubs_map = [];
    $remote_map = [];

    $is_composite_key = is_array($ubs_key);

    // Log initial memory usage
    $memoryStart = getMemoryUsage();
    dump("SyncEntity start - Memory: " . $memoryStart['memory_usage_mb'] . "MB");
    dump("UBS data count: " . count($ubs_data));
    dump("Remote data count: " . count($remote_data));

    if ($remote_table_name == 'artrans') {
        // dd($is_composite_key);
    }

    // Process UBS data in batches to optimize memory
    $ubs_batch_size = 5000;
    for ($i = 0; $i < count($ubs_data); $i += $ubs_batch_size) {
        $batch = array_slice($ubs_data, $i, $ubs_batch_size);
        
        foreach ($batch as $row) {
            if ($is_composite_key) {
                $composite_keys = [];
                foreach ($ubs_key as $k) {
                    $composite_keys[] = $row[$k];
                }
                $composite_keys = implode('|', $composite_keys);
                $ubs_map[$composite_keys] = $row;
            } else {
                $ubs_map[$row[$ubs_key]] = $row;
            }
        }
        
        // Memory optimization between batches
        if ($i + $ubs_batch_size < count($ubs_data)) {
            gc_collect_cycles();
        }
    }

    // Process remote data in batches
    $remote_batch_size = 5000;
    for ($i = 0; $i < count($remote_data); $i += $remote_batch_size) {
        $batch = array_slice($remote_data, $i, $remote_batch_size);
        
        foreach ($batch as $row) {
            $remote_map[$row[$remote_key]] = $row;
        }
        
        // Memory optimization between batches
        if ($i + $remote_batch_size < count($remote_data)) {
            gc_collect_cycles();
        }
    }

    $sync = [
        'remote_data' => [],
        'ubs_data' => [],
    ];

    $all_keys = array_unique(array_merge(array_keys($ubs_map), array_keys($remote_map)));
    dump("Total unique keys to process: " . count($all_keys));

    // Process sync logic in batches
    $sync_batch_size = 1000;
    for ($i = 0; $i < count($all_keys); $i += $sync_batch_size) {
        $key_batch = array_slice($all_keys, $i, $sync_batch_size);
        
        foreach ($key_batch as $key) {
            $ubs = $ubs_map[$key] ?? null;
            $remote = $remote_map[$key] ?? null;

            if ($ubs) {
                $ubs_time = strtotime($ubs['UPDATED_ON'] ?? '1970-01-01');
            }
            if ($remote) {
                $remote_time = strtotime($remote[$column_updated_at] ?? '1970-01-01');
            }

            if ($ubs && !$remote) {
                $sync['remote_data'][] = convert($remote_table_name, $ubs, 'to_remote');
            } elseif (!$ubs && $remote) {
                $sync['ubs_data'][] = convert($remote_table_name, $remote, 'to_ubs');
            } elseif ($ubs && $remote) {
                if ($ubs_time > $remote_time) {
                    $sync['remote_data'][] = convert($remote_table_name, $ubs, 'to_remote');
                } elseif ($remote_time > $ubs_time) {
                    $sync['ubs_data'][] = convert($remote_table_name, $remote, 'to_ubs');
                }
            }
        }
        
        // Memory optimization between batches
        if ($i + $sync_batch_size < count($all_keys)) {
            gc_collect_cycles();
            $memoryCurrent = getMemoryUsage();
            dump("Sync progress: " . round(($i / count($all_keys)) * 100, 2) . "% - Memory: " . $memoryCurrent['memory_usage_mb'] . "MB");
        }
    }

    // Final memory cleanup
    $memoryEnd = getMemoryUsage();
    dump("SyncEntity end - Memory: " . $memoryEnd['memory_usage_mb'] . "MB");
    dump("Sync results - Remote: " . count($sync['remote_data']) . ", UBS: " . count($sync['ubs_data']));

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
            dump("update: $keyValue");
            // dump("$keyValue === $recordKeyValue");
            foreach ($record as $field => $value) {
                if (in_array($field, ['artrans_id'])) {
                    continue;
                }
                // Use the column map to get the column object directly
                // need lowerr case
                $column = $columnMap[strtolower($field)] ?? null;
                if($column == null){
                    continue;
                }
                $fieldType = $column->getType();
                
                // Handle boolean fields
                if ($fieldType === 'L') {
                    $value = filter_var($value, FILTER_VALIDATE_BOOLEAN);
                }

                // Handle date fields
                if ($fieldType === 'D') {
                    if (empty($value) || $value === '0000-00-00') {
                        // Set empty date to 8 spaces
                        $value = "        ";
                    } else {
                        // Ensure a valid timestamp can be created from the value
                        $timestamp = strtotime($value);
                        if ($timestamp !== false) {
                            $value = date('Ymd', $timestamp);
                        } else {
                            // Handle invalid date strings gracefully. 
                            // You might want to log this or set it to an empty date.
                            dump("Warning: Invalid date format for field '$field'. Value: '$value'. Setting to empty date.");
                            $value = "        ";
                        }
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
                    dump($field);
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
        dump('insert');
        $newRow = $editor->appendRecord();

        $new_record = $BASE_RECORD;


        foreach ($record as $field => $value) {
            $new_record[$field] = $value;
        }
        // dump($new_record);
        foreach ($new_record as $field => $value) {
            if(!isset($structure[$field])) continue;
            try {
                if ($value === null) $value = "";

                if ($structure[$field] == 'L' && empty($value)) {
                    $value = false;
                }
                if ($structure[$field] === 'D') {
                    // Normalize to 8-character DBF format
                    $value = date('Ymd', strtotime($value));
                }

                $newRow->set($field, $value);
            } catch (\Throwable $e) {
                var_dump($fieldType);
                var_dump($value);
                dump("$field => $value caused problem");
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
    dump($remote_table_name);
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
        dump("DBF read start - Memory: " . $memoryStart['memory_usage_mb'] . "MB");

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
                dump("DBF read progress: $rowCount rows - Memory: " . $memoryCurrent['memory_usage_mb'] . "MB");
            }
        }

        // Final memory cleanup
        $memoryEnd = getMemoryUsage();
        dump("DBF read end - Total rows: $rowCount - Memory: " . $memoryEnd['memory_usage_mb'] . "MB");

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
