<?php

// ubs table => kanesan table
function tableMapping()
{
    $TABLE_MAPPING = [
        'ubs_ubsacc2015_arcust' => 'customers', // Customer master data (Account Receivable)
        'ubs_ubsacc2015_apvend' => 'vendor', // Vendor master data (Account Payable)

        'ubs_ubsacc2015_arpost' => 'vendor', // Customer invoice posting
        'ubs_ubsacc2015_artran' => 'vendor', // Customer transaction details

        'ubs_ubsacc2015_glbatch' => 'vendor', // GL batch header
        'ubs_ubsacc2015_gldata' => 'vendor', // General Ledger transactions

        'ubs_ubsacc2015_glpost' => 'vendor', // Posted General Ledger entries
        'ubs_ubsacc2015_ictran' => 'vendor', // Inventory transactions

        'ubs_ubsstk2015_apvend' => 'vendor',
        'ubs_ubsstk2015_arcust' => 'vendor',
        'ubs_ubsstk2015_artran' => 'vendor',
        'ubs_ubsstk2015_ictran' => 'vendor',
    ];
    return $TABLE_MAPPING;
}

// Memory management helper functions
function increaseMemoryLimit($limit = '1G')
{
    $currentLimit = ini_get('memory_limit');
    $currentBytes = return_bytes($currentLimit);
    $newBytes = return_bytes($limit);
    
    if ($newBytes > $currentBytes) {
        ini_set('memory_limit', $limit);
        return true;
    }
    return false;
}

function return_bytes($val)
{
    $val = trim($val);
    $last = strtolower($val[strlen($val)-1]);
    $val = (int)$val;
    switch($last) {
        case 'g': $val *= 1024;
        case 'm': $val *= 1024;
        case 'k': $val *= 1024;
    }
    return $val;
}

function getMemoryUsage()
{
    return [
        'memory_usage' => memory_get_usage(true),
        'memory_peak' => memory_get_peak_usage(true),
        'memory_limit' => ini_get('memory_limit')
    ];
}

function logMemoryUsage($context = '')
{
    $usage = getMemoryUsage();
    error_log("Memory Usage [$context]: " . 
              "Current: " . formatBytes($usage['memory_usage']) . 
              ", Peak: " . formatBytes($usage['peak_usage']) . 
              ", Limit: " . $usage['memory_limit']);
}

function formatBytes($bytes, $precision = 2)
{
    $units = array('B', 'KB', 'MB', 'GB', 'TB');
    for ($i = 0; $bytes > 1024 && $i < count($units) - 1; $i++) {
        $bytes /= 1024;
    }
    return round($bytes, $precision) . ' ' . $units[$i];
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

function fetchServerData($table, $updatedAfter = null, $bearerToken = null, $chunkSize = 1000)
{
    // Increase memory limit for large data operations
    increaseMemoryLimit('2G');
    
    $apiUrl = ENV::API_URL . '/api/data_sync';
    $TABLE_MAPPING = tableMapping();
    $alias_table = $TABLE_MAPPING[$table];

    $postData = [
        'table' => $alias_table,
        'chunk_size' => $chunkSize, // Request data in chunks
    ];

    if ($updatedAfter) {
        $postData['updated_after'] = $updatedAfter;
    }

    $ch = curl_init($apiUrl);

    $headers = [
        'Content-Type: application/json',
    ];

    if ($bearerToken) {
        $headers[] = 'Authorization: Bearer ' . $bearerToken;
    }

    curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
    curl_setopt($ch, CURLOPT_POST, true);
    curl_setopt($ch, CURLOPT_HTTPHEADER, $headers);
    curl_setopt($ch, CURLOPT_POSTFIELDS, json_encode($postData));
    curl_setopt($ch, CURLOPT_TIMEOUT, 300); // 5 minutes timeout
    curl_setopt($ch, CURLOPT_CONNECTTIMEOUT, 60); // 1 minute connection timeout

    $response = curl_exec($ch);
    $httpCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);

    if (curl_errno($ch)) {
        throw new Exception('Curl error: ' . curl_error($ch));
    }

    curl_close($ch);

    if ($httpCode !== 200) {
        throw new Exception("API returned HTTP $httpCode: $response");
    }

    $data = json_decode($response, true);
    
    // Log memory usage after API call
    logMemoryUsage("fetchServerData_$table");
    
    return $data;
}

function convert($dataRow, $map, $direction = 'to_remote')
{
    $converted = [];

    foreach ($map as $remote => $ubs) {
        if ($direction === 'to_remote') {
            $converted[$remote] = $ubs ? ($dataRow[$ubs] ?? null) : null;
        } else {
            if ($ubs) {
                $converted[$ubs] = $dataRow[$remote] ?? null;
            }
        }
    }

    return $converted;
}

// Enhanced syncEntity with memory optimization
function syncEntity($entity, $ubs_data, $remote_data, $chunkSize = 500)
{
    // Increase memory limit for sync operations
    increaseMemoryLimit('2G');
    
    $converter = new Converter();
    $map = $converter->map($entity);

    $ubs_key = $map['customer_code'];  // or use dynamic key map config
    $remote_key = 'customer_code';

    $sync = [
        'to_insert_remote' => [],
        'to_update_remote' => [],
        'to_insert_ubs' => [],
        'to_update_ubs' => [],
    ];

    // Process data in chunks to avoid memory issues
    $ubs_chunks = array_chunk($ubs_data, $chunkSize);
    $remote_chunks = array_chunk($remote_data, $chunkSize);
    
    foreach ($ubs_chunks as $ubs_chunk) {
        $ubs_map = [];
        foreach ($ubs_chunk as $row) {
            $ubs_map[$row[$ubs_key]] = $row;
        }
        
        foreach ($remote_chunks as $remote_chunk) {
            $remote_map = [];
            foreach ($remote_chunk as $row) {
                $remote_map[$row[$remote_key]] = $row;
            }
            
            // Process this chunk
            $chunk_sync = processChunk($ubs_map, $remote_map, $map, $ubs_key, $remote_key);
            
            // Merge results
            $sync['to_insert_remote'] = array_merge($sync['to_insert_remote'], $chunk_sync['to_insert_remote']);
            $sync['to_update_remote'] = array_merge($sync['to_update_remote'], $chunk_sync['to_update_remote']);
            $sync['to_insert_ubs'] = array_merge($sync['to_insert_ubs'], $chunk_sync['to_insert_ubs']);
            $sync['to_update_ubs'] = array_merge($sync['to_update_ubs'], $chunk_sync['to_update_ubs']);
            
            // Clear chunk maps to free memory
            unset($ubs_map, $remote_map, $chunk_sync);
            
            // Force garbage collection
            if (function_exists('gc_collect_cycles')) {
                gc_collect_cycles();
            }
        }
    }
    
    logMemoryUsage("syncEntity_$entity");
    return $sync;
}

// Helper function to process data chunks
function processChunk($ubs_map, $remote_map, $map, $ubs_key, $remote_key)
{
    $sync = [
        'to_insert_remote' => [],
        'to_update_remote' => [],
        'to_insert_ubs' => [],
        'to_update_ubs' => [],
    ];

    $all_keys = array_unique(array_merge(array_keys($ubs_map), array_keys($remote_map)));

    foreach ($all_keys as $key) {
        $ubs = $ubs_map[$key] ?? null;
        $remote = $remote_map[$key] ?? null;

        $ubs_time = strtotime($ubs[$map['updated_at']] ?? '1970-01-01');
        $remote_time = strtotime($remote['updated_at'] ?? '1970-01-01');

        if ($ubs && !$remote) {
            $sync['to_insert_remote'][] = convert($ubs, $map, 'to_remote');
        } elseif (!$ubs && $remote) {
            $sync['to_insert_ubs'][] = convert($remote, $map, 'to_ubs');
        } elseif ($ubs && $remote) {
            if ($ubs_time > $remote_time) {
                $sync['to_update_remote'][] = convert($ubs, $map, 'to_remote');
            } elseif ($remote_time > $ubs_time) {
                $sync['to_update_ubs'][] = convert($remote, $map, 'to_ubs');
            }
        }
    }

    return $sync;
}

// Stream-based sync for very large datasets
function streamSyncEntity($entity, $ubs_data, $remote_data, $chunkSize = 100)
{
    increaseMemoryLimit('2G');
    
    $converter = new Converter();
    $map = $converter->map($entity);

    $ubs_key = $map['customer_code'];
    $remote_key = 'customer_code';

    $sync = [
        'to_insert_remote' => [],
        'to_update_remote' => [],
        'to_insert_ubs' => [],
        'to_update_ubs' => [],
    ];

    // Process UBS data in small chunks
    $ubs_iterator = new ArrayIterator($ubs_data);
    $ubs_iterator->rewind();
    
    while ($ubs_iterator->valid()) {
        $ubs_chunk = [];
        $count = 0;
        
        // Build small chunk
        while ($ubs_iterator->valid() && $count < $chunkSize) {
            $ubs_chunk[] = $ubs_iterator->current();
            $ubs_iterator->next();
            $count++;
        }
        
        // Process this chunk against remote data
        $chunk_sync = processChunkAgainstRemote($ubs_chunk, $remote_data, $map, $ubs_key, $remote_key);
        
        // Merge results
        $sync['to_insert_remote'] = array_merge($sync['to_insert_remote'], $chunk_sync['to_insert_remote']);
        $sync['to_update_remote'] = array_merge($sync['to_update_remote'], $chunk_sync['to_update_remote']);
        $sync['to_insert_ubs'] = array_merge($sync['to_insert_ubs'], $chunk_sync['to_insert_ubs']);
        $sync['to_update_ubs'] = array_merge($sync['to_update_ubs'], $chunk_sync['to_update_ubs']);
        
        // Clear chunk to free memory
        unset($ubs_chunk, $chunk_sync);
        
        // Force garbage collection
        if (function_exists('gc_collect_cycles')) {
            gc_collect_cycles();
        }
        
        logMemoryUsage("streamSyncEntity_chunk_$entity");
    }
    
    return $sync;
}

// Process chunk against remote data
function processChunkAgainstRemote($ubs_chunk, $remote_data, $map, $ubs_key, $remote_key)
{
    $sync = [
        'to_insert_remote' => [],
        'to_update_remote' => [],
        'to_insert_ubs' => [],
        'to_update_ubs' => [],
    ];

    foreach ($ubs_chunk as $ubs_row) {
        $key = $ubs_row[$ubs_key];
        
        // Find matching remote row
        $remote_row = null;
        foreach ($remote_data as $remote) {
            if ($remote[$remote_key] === $key) {
                $remote_row = $remote;
                break;
            }
        }
        
        if (!$remote_row) {
            $sync['to_insert_remote'][] = convert($ubs_row, $map, 'to_remote');
        } else {
            $ubs_time = strtotime($ubs_row[$map['updated_at']] ?? '1970-01-01');
            $remote_time = strtotime($remote_row['updated_at'] ?? '1970-01-01');
            
            if ($ubs_time > $remote_time) {
                $sync['to_update_remote'][] = convert($ubs_row, $map, 'to_remote');
            }
        }
    }

    return $sync;
}

function upsertUbs($path, $record)
{
    $path = '/path/to/AR_CUST.DBF';
    $table = new Table($path);
    $table->openWrite();

    $keyField = 'CUSTNO';
    $found = false;

    while ($row = $table->nextRecord()) {
        if (trim($row->get($keyField)) === trim($record[$keyField])) {
            // UPDATE
            foreach ($record as $field => $value) {
                $row->set($field, $value);
            }
            $row->save();
            $found = true;
            break;
        }
    }

    if (!$found) {
        // INSERT
        $table->append($record);
    }

    $table->close();
}

// Configuration function for memory optimization
function getSyncConfig()
{
    return [
        'memory_limit' => '2G',
        'chunk_size' => 500,
        'stream_chunk_size' => 100,
        'enable_gc' => true,
        'log_memory' => true,
        'timeout' => 300
    ];
}