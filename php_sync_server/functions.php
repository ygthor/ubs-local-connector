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
    $apiUrl = ENV::API_URL . '/api/data_sync';

    $TABLE_MAPPING = tableMapping();
    $alias_table = $TABLE_MAPPING[$table];

    $postData = [
        'table' => $alias_table,
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

    $response = curl_exec($ch);
    $httpCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);

    if (curl_errno($ch)) {
        throw new Exception('Curl error: ' . curl_error($ch));
    }

    curl_close($ch);

    if ($httpCode !== 200) {
        throw new Exception("API returned HTTP $httpCode: $response");
    }

    return json_decode($response, true);
}


function convert($remote_table_name, $dataRow, $direction = 'to_remote')
{
    $converted = [];

    $map = Converter::mapColumns($remote_table_name);

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


function syncEntity($entity, $ubs_data, $remote_data)
{
    $remote_table_name = Converter::table_map($entity);
    $remote_key = Converter::primaryKey($remote_table_name);
    $ubs_key = Converter::primaryKey($entity);
    
    $ubs_map = [];
    $remote_map = [];
    
    foreach ($ubs_data as $row) {
        $ubs_map[$row[$ubs_key]] = $row;
    }

    foreach ($remote_data as $row) {
        $remote_map[$row[$remote_key]] = $row;
    }

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

        if($ubs){
            $ubs_time = strtotime($ubs['UPDATED_ON'] ?? '1970-01-01');
        }
        if($remote){
            $remote_time = strtotime($remote['updated_at'] ?? '1970-01-01');
        }
        

        if ($ubs && !$remote) {
            $sync['to_insert_remote'][] = convert($remote_table_name,$ubs, 'to_remote');
        } elseif (!$ubs && $remote) {
            $sync['to_insert_ubs'][] = convert($remote_table_name, $remote, 'to_ubs');
        } elseif ($ubs && $remote) {
            if ($ubs_time > $remote_time) {
                $sync['to_update_remote'][] = convert($remote_table_name, $ubs, 'to_remote');
            } elseif ($remote_time > $ubs_time) {
                $sync['to_update_ubs'][] = convert($remote_table_name, $remote, 'to_ubs');
            }
        }
    }
    return $sync;
}


function upsertUbs($table, $record)
{
    $keyField = Converter::primaryKey($table);

    $arr = parseUbsTable($table);
    $table = $arr['table'];
    $directory = strtoupper($arr['database']);

    $path = "C:/$directory/Sample/{$table}.dbf";
    

    $editor = new \XBase\TableEditor($path, [
        'editMode' => \XBase\TableEditor::EDIT_MODE_CLONE, // safer mode
    ]);

    $found = false;

    // UPDATE IF EXISTS
    while ($row = $editor->nextRecord()) {
        
        $keyValue = trim($row->get($keyField));
        if ($keyValue === trim($record[$keyField])) {
            foreach ($record as $field => $value) {
                $row->set($field, $value);
            }
            
            $editor->writeRecord(); 
            $editor->save(); 

            $found = true;
            break;
        }
    }

    // INSERT IF NOT FOUND
    if (!$found) {
        $newRow = $editor->appendRecord();
        foreach ($record as $field => $value) {
            $newRow->set($field, $value);
        }
        $editor->writeRecord(); // commit the new record
        $editor->save()->close(); 
    }

    
}


function upsertRemote($table, $record){
    $remote_table_name = Converter::table_map($table);
    $primary_key = Converter::primaryKey($remote_table_name);

    $db = new mysql;
    $db->connect_remote();

    $db->update_or_insert($remote_table_name,[$primary_key => $record[$primary_key]],$record);
    
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

        $rows = [];
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
        }

        return [
            'structure' => $structure,
            'rows' => $rows,
        ];
    } catch (Exception $e) {
        throw new Exception("Failed to read DBF file: " . $e->getMessage());
    }
}


function sync_all_dbf_to_local()
{
    $directory_arr = ['UBSACC2015', 'UBSSTK2015'];
    $dbf_arr = [
        // 'arcust',
        // 'apvend',
        // 'artran',
        // 'icarea',
        // 'icitem',
        // 'ictran',
        // 'arpay',
        // 'arpost',
        // 'gldata',
        // 'glbatch',
        // 'glpost',

        'arpso',
    ];

    $db = new mysql;
    $db->connect();

    foreach ($directory_arr as $directory_name) {
        $directory_path = "C:/{$directory_name}/Sample";

        foreach ($dbf_arr as $dbf_name) {
            $file_name = $dbf_name . '.dbf';
            $full_path = $directory_path . '/' . $file_name;

            if (!file_exists($full_path)) {
                echo "Skipping missing file: $full_path\n";
                continue;
            }

            try {
                echo "Processing $full_path...\n";

                $data = read_dbf($full_path); // return ['structure'=>[], 'rows'=>[]]
                $table_name = strtolower("ubs_{$directory_name}_{$dbf_name}");
                

                // Optional: Clean table before insert
                $db->query("DELETE FROM {$table_name}");

                // Insert row by row
                foreach ($data['rows'] as $row) {
                    $db->insert($table_name, $row);
                }

                echo "Inserted " . count($data['rows']) . " rows into {$table_name}<br>\n";
            } catch (Exception $e) {
                echo "Error processing $file_name: " . $e->getMessage() . "\n";
            }
        }
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