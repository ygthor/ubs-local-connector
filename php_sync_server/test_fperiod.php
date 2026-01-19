<?php
/**
 * Test script for FPERIOD debugging
 *
 * Usage:
 *   php test_fperiod.php INV-001
 */

require_once __DIR__ . '/bootstrap/index.php';
require_once __DIR__ . '/functions.php';

$referenceNo = $argv[1] ?? null;

if (!$referenceNo) {
    echo "Usage: php test_fperiod.php <reference_no>\n";
    echo "Example: php test_fperiod.php INV-001\n";
    exit(1);
}

echo "=== FPERIOD DEBUG TEST ===\n";

$db = new mysql;
$db->connect_remote();
$refNo = $db->escape($referenceNo);
$order = $db->first("SELECT * FROM orders WHERE reference_no='$refNo'");
$db->close();

if (!$order) {
    echo "ERROR: Order not found with reference_no: $referenceNo\n";
    exit(1);
}

echo "\n[1] Order data from DB:\n";
echo "   reference_no: " . $order['reference_no'] . "\n";
echo "   order_date: " . ($order['order_date'] ?? 'NULL') . "\n";

echo "\n[2] extractDate result:\n";
$extractedDate = extractDate($order['order_date'] ?? null);
echo "   $extractedDate\n";

echo "\n[3] getFPeriodFromDate result:\n";
$fperiod = getFPeriodFromDate($extractedDate);
echo "   FPERIOD: $fperiod\n";

echo "\n[4] convert() result:\n";
$converted = convert('orders', $order, 'to_ubs');
echo "   DATE: " . ($converted['DATE'] ?? 'NOT SET') . "\n";
echo "   FPERIOD: " . ($converted['FPERIOD'] ?? 'NOT SET') . "\n";

echo "\n=== END ===\n";
