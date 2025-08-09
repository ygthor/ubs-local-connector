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

        'ubs_ubsstk2015_artran' => 'artrans',
        'ubs_ubsstk2015_ictran' => 'artrans_items',

        'ubs_ubsstk2015_arpso' => 'orders',
        'ubs_ubsstk2015_icpso' => 'order_items',
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
    $db = new mysql;
    $db->connect_remote();

    $TABLE_MAPPING = tableMapping();
    $alias_table = $TABLE_MAPPING[$table];

    $arr_cloned_ubs = [
        'artrans',
        'artrans_items'
    ];
    if (in_array($alias_table, $arr_cloned_ubs)) {
        $column_updated_at = "UPDATED_ON";
    } else {
        $column_updated_at = "updated_at";
    }

    $sql = "SELECT * FROM $alias_table WHERE $column_updated_at >= '$updatedAfter'";
    $data = $db->get($sql);
    return $data;
}


function convert($remote_table_name, $dataRow, $direction = 'to_remote')
{
    $converted = [];

    $map = Converter::mapColumns($remote_table_name);

    if($map == []){
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
    $remote_table_name = Converter::table_map($entity);
    $remote_key = Converter::primaryKey($remote_table_name);
    $ubs_key = Converter::primaryKey($entity);
    $column_updated_at = Converter::mapUpdatedAtField($remote_table_name);

    $ubs_map = [];
    $remote_map = [];

    $is_composite_key = is_array($ubs_key);
    
    if($remote_table_name == 'artrans'){
        // dd($is_composite_key);
    }
    

    foreach ($ubs_data as $row) {
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

    foreach ($remote_data as $row) {
        $remote_map[$row[$remote_key]] = $row;
    }

    $sync = [
        'remote_data' => [],
        'ubs_data' => [],
    ];

    $all_keys = array_unique(array_merge(array_keys($ubs_map), array_keys($remote_map)));
    
    foreach ($all_keys as $key) {
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
                if (in_array($field, ['DATE', 'PLA_DODATE'])) {
                    $value = date('Ymd', strtotime($value));
                }

                // dump("$field => $value");
                $row->set($field, $value);
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
    $remote_table_name = Converter::table_map($table);
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

    if($remote_table_name == 'artrans' ){
        // dd($record);
        // dd($primary_key);
    }
    
    if(count($record) > 0){
        if($remote_table_name == 'artrans_items'){
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


// function sync_all_dbf_to_local()
// {
//     $directory_arr = ['UBSACC2015', 'UBSSTK2015'];
//     $dbf_arr = [
//         // 'arcust',
//         // 'apvend',
//         // 'artran',
//         // 'icarea',
//         // 'icitem',
//         // 'ictran',
//         // 'arpay',
//         // 'arpost',
//         // 'gldata',
//         // 'glbatch',
//         // 'glpost',

//         'arpso',
//     ];

//     $db = new mysql;
//     $db->connect();

//     foreach ($directory_arr as $directory_name) {
//         $directory_path = "C:/{$directory_name}/Sample";

//         foreach ($dbf_arr as $dbf_name) {
//             $file_name = $dbf_name . '.dbf';
//             $full_path = $directory_path . '/' . $file_name;

//             if (!file_exists($full_path)) {
//                 echo "Skipping missing file: $full_path\n";
//                 continue;
//             }

//             try {
//                 echo "Processing $full_path...\n";

//                 $data = read_dbf($full_path); // return ['structure'=>[], 'rows'=>[]]
//                 $table_name = strtolower("ubs_{$directory_name}_{$dbf_name}");


//                 // Optional: Clean table before insert
//                 $db->query("DELETE FROM {$table_name}");

//                 // Insert row by row
//                 foreach ($data['rows'] as $row) {
//                     $db->insert($table_name, $row);
//                 }

//                 echo "Inserted " . count($data['rows']) . " rows into {$table_name}<br>\n";
//             } catch (Exception $e) {
//                 echo "Error processing $file_name: " . $e->getMessage() . "\n";
//             }
//         }
//     }
// }

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
