<?php
/**
 * Test single order sync from remote to UBS
 *
 * Usage:
 *   php test_sync_single.php S200568
 */

include(__DIR__ . '/bootstrap/app.php');

$referenceNo = $argv[1] ?? null;

if (!$referenceNo) {
    echo "Usage: php test_sync_single.php <reference_no>\n";
    exit(1);
}

echo "=== SINGLE ORDER SYNC TEST ===\n";

// 1. Fetch from remote
$db = new mysql;
$db->connect_remote();
$refNo = $db->escape($referenceNo);
$order = $db->first("SELECT * FROM orders WHERE reference_no='$refNo'");
$db->close();

if (!$order) {
    echo "ERROR: Order not found: $referenceNo\n";
    exit(1);
}

echo "\n[1] Fetched from remote:\n";
echo "   reference_no: " . $order['reference_no'] . "\n";
echo "   order_date: " . ($order['order_date'] ?? 'NULL') . "\n";

// 2. Convert to UBS format
$converted = convert('orders', $order, 'to_ubs');
echo "\n[2] Converted data:\n";
echo "   DATE: " . ($converted['DATE'] ?? 'NOT SET') . "\n";
echo "   FPERIOD: " . ($converted['FPERIOD'] ?? 'NOT SET') . "\n";
echo "   REFNO: " . ($converted['REFNO'] ?? 'NOT SET') . "\n";

// 3. Upsert to UBS
echo "\n[3] Upserting to UBS (ubs_ubsstk2015_artran)...\n";
try {
    upsertUbs('ubs_ubsstk2015_artran', $converted);
    echo "   SUCCESS!\n";
} catch (Exception $e) {
    echo "   ERROR: " . $e->getMessage() . "\n";
}

echo "\n=== END ===\n";
