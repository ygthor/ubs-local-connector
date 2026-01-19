<?php
/**
 * Debug why specific orders aren't syncing from Remote to UBS
 *
 * Usage:
 *   php test_sync_debug.php S200570
 */

include(__DIR__ . '/bootstrap/app.php');

$referenceNo = $argv[1] ?? null;

if (!$referenceNo) {
    echo "Usage: php test_sync_debug.php <reference_no>\n";
    exit(1);
}

echo "=== SYNC DEBUG FOR $referenceNo ===\n";

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

echo "\n=== END ===\n";
