<?php
/**
 * Debug why specific orders aren't syncing from Remote to UBS
 * AND why remote updated_at gets overwritten to match created_at
 *
 * Usage:
 *   php test_sync_debug.php S200570
 *   php test_sync_debug.php S200570 --watch    (monitors changes during sync)
 */

include(__DIR__ . '/bootstrap/app.php');

$referenceNo = $argv[1] ?? null;
$watchMode = isset($argv[2]) && $argv[2] === '--watch';

if (!$referenceNo) {
    echo "Usage: php test_sync_debug.php <reference_no> [--watch]\n";
    echo "       --watch: Monitor remote updated_at changes in real-time\n";
    exit(1);
}

echo "=== SYNC DEBUG FOR $referenceNo ===\n";
echo "=== " . date('Y-m-d H:i:s') . " ===\n";

// 0. Check last_synced_at (from sync_logs)
echo "\n[0] Sync metadata:\n";
$db = new mysql;
$lastSynced = $db->first("SELECT synced_at FROM sync_logs ORDER BY synced_at DESC LIMIT 1");
$db->close();
if ($lastSynced) {
    echo "   last_synced_at: " . $lastSynced['synced_at'] . "\n";
} else {
    echo "   last_synced_at: NOT SET\n";
}

// 1. Get Remote data
echo "\n[1] Remote data:\n";
$db = new mysql;
$db->connect_remote();
$refNo = $db->escape($referenceNo);
$remote = $db->first("SELECT reference_no, order_date, updated_at, created_at FROM orders WHERE reference_no='$refNo'");
$db->close();

if (!$remote) {
    echo "   ERROR: Not found in remote!\n";
} else {
    echo "   reference_no: " . $remote['reference_no'] . "\n";
    echo "   order_date: " . ($remote['order_date'] ?? 'NULL') . "\n";
    echo "   updated_at: " . ($remote['updated_at'] ?? 'NULL') . "\n";
    echo "   created_at: " . ($remote['created_at'] ?? 'NULL') . "\n";
}

// 2. Get UBS data
echo "\n[2] UBS data (artran.dbf):\n";
$arr = parseUbsTable('ubs_ubsstk2015_artran');
$directory = strtoupper($arr['database']);
$dbfPath = "C:/$directory/" . ENV::DBF_SUBPATH . "/artran.dbf";
echo "   DBF path: $dbfPath\n";

if (!file_exists($dbfPath)) {
    echo "   ERROR: DBF file not found!\n";
} else {
    $table = new \XBase\TableReader($dbfPath);
    $found = false;
    try {
        while ($record = $table->nextRecord()) {
            $refno = trim($record->get('REFNO') ?? '');
            if ($refno === $referenceNo) {
                $found = true;
                $dateVal = $record->get('DATE');
                $updatedOnVal = $record->get('UPDATED_ON');

                // Handle DateTime objects
                if ($dateVal instanceof DateTime) {
                    $dateVal = $dateVal->format('Y-m-d H:i:s');
                }
                if ($updatedOnVal instanceof DateTime) {
                    $updatedOnVal = $updatedOnVal->format('Y-m-d H:i:s');
                }

                echo "   REFNO: " . $refno . "\n";
                echo "   DATE: " . ($dateVal ?? 'NULL') . "\n";
                echo "   UPDATED_ON: " . ($updatedOnVal ?? 'NULL') . "\n";
                echo "   FPERIOD: " . ($record->get('FPERIOD') ?? 'NULL') . "\n";
                break;
            }
        }
        $table->close();

        if (!$found) {
            echo "   NOT FOUND in artran.dbf\n";
        }
    } catch (Exception $e) {
        echo "   ERROR reading DBF: " . $e->getMessage() . "\n";
    }
}

// 2b. Get LOCAL MySQL data (this is what main.php uses!)
echo "\n[2b] LOCAL MySQL data (ubs_ubsstk2015_artran):\n";
$db = new mysql;
$localRecord = $db->first("SELECT REFNO, DATE, UPDATED_ON, FPERIOD FROM ubs_ubsstk2015_artran WHERE REFNO='$refNo'");
$db->close();

if (!$localRecord) {
    echo "   NOT FOUND in local MySQL!\n";
} else {
    echo "   REFNO: " . $localRecord['REFNO'] . "\n";
    echo "   DATE: " . ($localRecord['DATE'] ?? 'NULL') . "\n";
    echo "   UPDATED_ON: " . ($localRecord['UPDATED_ON'] ?? 'NULL') . "\n";
    echo "   FPERIOD: " . ($localRecord['FPERIOD'] ?? 'NULL') . "\n";
}

// 2c. Get LOCAL MySQL ictran data (order_items)
echo "\n[2c] LOCAL MySQL data (ubs_ubsstk2015_ictran):\n";
$db = new mysql;
$localIctran = $db->get("SELECT REFNO, ITEMCOUNT, UPDATED_ON FROM ubs_ubsstk2015_ictran WHERE REFNO='$refNo' LIMIT 5");
$db->close();

if (empty($localIctran)) {
    echo "   NOT FOUND in local MySQL!\n";
} else {
    foreach ($localIctran as $item) {
        echo "   REFNO: " . $item['REFNO'] . " | ITEMCOUNT: " . $item['ITEMCOUNT'] . " | UPDATED_ON: " . ($item['UPDATED_ON'] ?? 'NULL') . "\n";
    }
}

// 3. Compare timestamps (using LOCAL MySQL - same as main.php)
echo "\n[3] Timestamp comparison (main.php uses LOCAL MySQL!):\n";
if ($remote && isset($remote['updated_at'])) {
    $remote_updated_at = $remote['updated_at'];
    $remote_time = strtotime($remote_updated_at);
    echo "   Remote updated_at: $remote_updated_at (timestamp: $remote_time)\n";

    if ($localRecord) {
        $local_updated_on = $localRecord['UPDATED_ON'] ?? null;

        // Handle invalid timestamps (same logic as syncEntity)
        if (empty($local_updated_on) || $local_updated_on === '0000-00-00' ||
            $local_updated_on === '0000-00-00 00:00:00' || strtotime($local_updated_on) === false) {
            $local_updated_on = '1970-01-01 00:00:00';
        }

        $local_time = strtotime($local_updated_on);
        echo "   Local MySQL UPDATED_ON: $local_updated_on (timestamp: $local_time)\n";

        echo "\n   COMPARISON RESULT (what main.php sees):\n";
        if ($local_time > $remote_time) {
            echo "   -> Local is NEWER - would sync UBS->Remote (NO update to DBF)\n";
        } elseif ($remote_time > $local_time) {
            echo "   <- Remote is NEWER - should sync Remote->UBS\n";
        } else {
            echo "   == EQUAL timestamps - no sync needed\n";
        }
    } else {
        echo "   Record NOT FOUND in local MySQL - should INSERT to UBS\n";
    }
}

// 4. Simulate what convert() does (UBS -> Remote)
echo "\n[4] Simulating convert() UBS->Remote:\n";
if ($localRecord) {
    $simulatedRemote = convert('orders', $localRecord, 'to_remote');
    echo "   Input UPDATED_ON from UBS: " . ($localRecord['UPDATED_ON'] ?? 'NULL') . "\n";
    echo "   Output updated_at for Remote: " . ($simulatedRemote['updated_at'] ?? 'NULL') . "\n";

    if (isset($simulatedRemote['updated_at']) && isset($remote['updated_at'])) {
        if ($simulatedRemote['updated_at'] !== $remote['updated_at']) {
            echo "   ⚠️  WARNING: convert() would OVERWRITE remote updated_at!\n";
            echo "      Current remote: " . $remote['updated_at'] . "\n";
            echo "      Would become:   " . $simulatedRemote['updated_at'] . "\n";
        } else {
            echo "   ✅ OK: Timestamps match\n";
        }
    }
}

// 5. Check if this order would be in the sync chunk
echo "\n[5] Would this order be included in sync?\n";
$last_synced_at = lastSyncAt();
if (empty($last_synced_at)) {
    $last_synced_at = '2025-08-01 00:00:00';
}
$last_synced_at = date('Y-m-d H:i:s', strtotime($last_synced_at.' - 10 minutes'));
echo "   last_synced_at (with 10min buffer): $last_synced_at\n";

if ($localRecord) {
    $local_updated = $localRecord['UPDATED_ON'] ?? '1970-01-01';
    $local_time = strtotime($local_updated);
    $sync_time = strtotime($last_synced_at);

    if ($local_time > $sync_time) {
        echo "   ✅ YES - Local UPDATED_ON ($local_updated) > last_synced_at\n";
        echo "      This order WILL be in UBS chunk and may sync UBS->Remote\n";
    } else {
        echo "   ❌ NO - Local UPDATED_ON ($local_updated) <= last_synced_at\n";
        echo "      This order will NOT be in UBS chunk\n";
    }
}

// 6. Check fetchRemoteDataByKeys query result
echo "\n[6] Testing fetchRemoteDataByKeys (what main.php fetches):\n";
if ($localRecord) {
    $remoteData = fetchRemoteDataByKeys('ubs_ubsstk2015_artran', [$referenceNo], $last_synced_at, null);
    if (empty($remoteData)) {
        echo "   ❌ NO remote data returned (filtered out by timestamp)\n";
        echo "      Remote record exists but updated_at/order_date <= $last_synced_at\n";
    } else {
        echo "   ✅ Remote data returned: " . count($remoteData) . " record(s)\n";
        foreach ($remoteData as $r) {
            echo "      reference_no: " . $r['reference_no'] . "\n";
            echo "      updated_at: " . ($r['updated_at'] ?? 'NULL') . "\n";
            echo "      order_date: " . ($r['order_date'] ?? 'NULL') . "\n";
        }
    }
}

// 7. Simulate syncEntity comparison
echo "\n[7] Simulating syncEntity() comparison:\n";
if ($localRecord && $remote) {
    $ubs_updated_on = $localRecord['UPDATED_ON'] ?? null;
    $remote_updated_at = $remote['updated_at'] ?? null;

    // Same validation as syncEntity
    if (empty($ubs_updated_on) || $ubs_updated_on === '0000-00-00' ||
        $ubs_updated_on === '0000-00-00 00:00:00' || strtotime($ubs_updated_on) === false) {
        $ubs_updated_on = '1970-01-01 00:00:00';
    }
    if (empty($remote_updated_at) || $remote_updated_at === '0000-00-00' ||
        $remote_updated_at === '0000-00-00 00:00:00' || strtotime($remote_updated_at) === false) {
        $remote_updated_at = '1970-01-01 00:00:00';
    }

    $ubs_time = strtotime($ubs_updated_on);
    $remote_time = strtotime($remote_updated_at);

    echo "   UBS UPDATED_ON:    $ubs_updated_on (ts: $ubs_time)\n";
    echo "   Remote updated_at: $remote_updated_at (ts: $remote_time)\n";
    echo "   Difference: " . ($ubs_time - $remote_time) . " seconds\n";

    echo "\n   SYNC DECISION:\n";
    if ($ubs_time > $remote_time) {
        echo "   ⚠️  UBS->Remote (UBS is newer)\n";
        echo "      This will OVERWRITE remote updated_at with UBS UPDATED_ON!\n";
        echo "      Remote updated_at will change from: " . $remote['updated_at'] . "\n";
        echo "      To: " . $localRecord['UPDATED_ON'] . "\n";
    } elseif ($remote_time > $ubs_time) {
        echo "   ✅ Remote->UBS (Remote is newer)\n";
        echo "      Remote updated_at will be preserved\n";
    } else {
        echo "   ⏸️  No sync (timestamps equal)\n";
    }
}

// 8. Check for the specific issue: updated_at == created_at
echo "\n[8] Checking for updated_at == created_at issue:\n";
if ($remote) {
    $created = $remote['created_at'] ?? null;
    $updated = $remote['updated_at'] ?? null;

    if ($created && $updated) {
        $created_ts = strtotime($created);
        $updated_ts = strtotime($updated);
        $diff = abs($updated_ts - $created_ts);

        if ($diff < 2) { // Within 2 seconds = effectively same
            echo "   ⚠️  PROBLEM DETECTED: updated_at ≈ created_at!\n";
            echo "      created_at: $created\n";
            echo "      updated_at: $updated\n";
            echo "      This suggests UBS sync overwrote your manual update.\n";
        } else {
            echo "   ✅ OK: updated_at differs from created_at by " . $diff . " seconds\n";
        }
    }
}

// Watch mode - monitor changes
if ($watchMode) {
    echo "\n[WATCH MODE] Monitoring remote updated_at changes...\n";
    echo "Press Ctrl+C to stop.\n\n";

    $lastUpdatedAt = $remote['updated_at'] ?? null;
    $checkCount = 0;

    while (true) {
        sleep(2);
        $checkCount++;

        $db = new mysql;
        $db->connect_remote();
        $current = $db->first("SELECT updated_at, created_at FROM orders WHERE reference_no='" . $db->escape($referenceNo) . "'");
        $db->close();

        $currentUpdatedAt = $current['updated_at'] ?? null;

        if ($currentUpdatedAt !== $lastUpdatedAt) {
            echo "[" . date('H:i:s') . "] ⚠️  CHANGE DETECTED!\n";
            echo "   Before: $lastUpdatedAt\n";
            echo "   After:  $currentUpdatedAt\n";

            if ($current['created_at'] && abs(strtotime($currentUpdatedAt) - strtotime($current['created_at'])) < 2) {
                echo "   🚨 updated_at now equals created_at! Sync likely overwrote it.\n";
            }

            $lastUpdatedAt = $currentUpdatedAt;
        } else {
            // Show heartbeat every 10 checks
            if ($checkCount % 5 == 0) {
                echo "[" . date('H:i:s') . "] No change (updated_at: $currentUpdatedAt)\n";
            }
        }
    }
}

echo "\n=== END ===\n";
