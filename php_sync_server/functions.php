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


function syncEntity($entity, $ubs_data, $remote_data)
{
    $converter = new Converter();
    $map = $converter->map($entity);

    $ubs_key = $map['customer_code'];  // or use dynamic key map config
    $remote_key = 'customer_code';

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