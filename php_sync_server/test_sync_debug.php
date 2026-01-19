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
$dbfPath = env('UBS_PATH') . '/' . strtoupper($arr['database']) . '/' . $arr['table'] . '.dbf';
echo "   DBF path: $dbfPath\n";

if (!file_exists($dbfPath)) {
    echo "   ERROR: DBF file not found!\n";
} else {
    $table = new \XBase\TableReader($dbfPath);
    $found = false;
    while ($record = $table->nextRecord()) {
        if ($record->deleted) continue;
        $refno = trim($record->get('REFNO') ?? '');
        if ($refno === $referenceNo) {
            $found = true;
            echo "   REFNO: " . $refno . "\n";
            echo "   DATE: " . ($record->get('DATE') ?? 'NULL') . "\n";
            echo "   UPDATED_ON: " . ($record->get('UPDATED_ON') ?? 'NULL') . "\n";
            echo "   FPERIOD: " . ($record->get('FPERIOD') ?? 'NULL') . "\n";
            break;
        }
    }
    $table->close();

    if (!$found) {
        echo "   NOT FOUND in artran.dbf\n";
    }
}

// 3. Compare timestamps
echo "\n[3] Timestamp comparison:\n";
if ($remote && isset($remote['updated_at'])) {
    $remote_updated_at = $remote['updated_at'];
    $remote_time = strtotime($remote_updated_at);
    echo "   Remote updated_at: $remote_updated_at (timestamp: $remote_time)\n";

    // Re-read UBS for comparison
    if (file_exists($dbfPath)) {
        $table = new \XBase\TableReader($dbfPath);
        while ($record = $table->nextRecord()) {
            if ($record->deleted) continue;
            $refno = trim($record->get('REFNO') ?? '');
            if ($refno === $referenceNo) {
                $ubs_updated_on = $record->get('UPDATED_ON');

                // Handle invalid timestamps (same logic as syncEntity)
                if (empty($ubs_updated_on) || $ubs_updated_on === '0000-00-00' ||
                    $ubs_updated_on === '0000-00-00 00:00:00' || strtotime($ubs_updated_on) === false) {
                    $ubs_updated_on = '1970-01-01 00:00:00';
                }

                $ubs_time = strtotime($ubs_updated_on);
                echo "   UBS UPDATED_ON: $ubs_updated_on (timestamp: $ubs_time)\n";

                echo "\n   COMPARISON RESULT:\n";
                if ($ubs_time > $remote_time) {
                    echo "   ➡️  UBS is NEWER - would sync UBS→Remote\n";
                } elseif ($remote_time > $ubs_time) {
                    echo "   ⬅️  Remote is NEWER - should sync Remote→UBS\n";
                } else {
                    echo "   ⏸️  EQUAL timestamps - no sync needed\n";
                }
                break;
            }
        }
        $table->close();
    }
}

echo "\n=== END ===\n";
