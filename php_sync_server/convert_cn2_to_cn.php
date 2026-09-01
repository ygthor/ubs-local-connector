<?php
/**
 * Convert CN2 to CN Migration Script for UBS (Sage UBS Accounting / Stock)
 * 
 * Purpose:
 *   Converts all 'CN2' records (manual credit notes created in app) to standard UBS 'CN'
 *   (Credit Note) in UBS DBF files (artran.dbf, ictran.dbf) and local MySQL connector tables.
 * 
 * Usage:
 *   php convert_cn2_to_cn.php                # Normal run (cutoff date default: 2026-07-19)
 *   php convert_cn2_to_cn.php --dry-run      # Preview records to convert without modifying files
 *   php convert_cn2_to_cn.php --date=2026-07-19  # Specify custom cutoff date
 *   php convert_cn2_to_cn.php --force        # Bypass running UBS software check (use with caution)
 */

include(__DIR__ . '/bootstrap/app.php');
include(__DIR__ . '/bootstrap/cache.php');

// Initialize sync environment
initializeSyncEnvironment();

/**
 * Safely get field from XBase row using columnMap
 */
function getFieldSafe($row, $fieldName, $default = null, $columnMap = null) {
    if ($columnMap !== null && !isset($columnMap[strtolower($fieldName)])) {
        return $default;
    }
    try {
        $val = $row->get($fieldName);
        return $val !== null ? $val : $default;
    } catch (\Throwable $e) {
        return $default;
    }
}

/**
 * Safely set field on XBase row using columnMap
 */
function setFieldSafe($row, $fieldName, $value, $columnMap = null) {
    if ($columnMap !== null && !isset($columnMap[strtolower($fieldName)])) {
        return false;
    }
    try {
        $row->set($fieldName, $value);
        return true;
    } catch (\Throwable $e) {
        return false;
    }
}

// Parse command line arguments
$isDryRun = false;
$isForce = false;
$customDate = null;

foreach ($argv as $arg) {
    if ($arg === '--dry-run') {
        $isDryRun = true;
    } elseif ($arg === '--force') {
        $isForce = true;
    } elseif (strpos($arg, '--date=') === 0) {
        $customDate = trim(substr($arg, 7));
    }
}

// Default cutoff date: 19/07/2026 (2026-07-19)
$cutoffDateStr = $customDate ?: '2026-07-19';

// Normalize date formats
$timestamp = strtotime(str_replace('/', '-', $cutoffDateStr));
if ($timestamp === false) {
    ProgressDisplay::error("❌ Invalid cutoff date provided: $cutoffDateStr");
    exit(1);
}

$cutoffDateYmd = date('Y-m-d', $timestamp);      // 2026-07-19
$cutoffDateDbf = date('Ymd', $timestamp);        // 20260719
$cutoffDateDisplay = date('d/m/Y', $timestamp);  // 19/07/2026

echo "\n" . str_repeat('=', 70) . "\n";
echo "  KBS UBS CONNECTOR - CONVERT CN2 TO CN MIGRATION TOOL\n";
echo str_repeat('=', 70) . "\n";
echo "  📅 Cutoff Date: $cutoffDateDisplay ($cutoffDateYmd)\n";
echo "  🎯 Target: Convert all CN2 records on/after $cutoffDateDisplay to TYPE='CN'\n";
if ($isDryRun) {
    echo "  🔍 MODE: DRY-RUN (Preview only, no files will be changed)\n";
} else {
    echo "  ⚡ MODE: LIVE EXECUTION (DBF files and local MySQL will be updated)\n";
}
echo str_repeat('=', 70) . "\n\n";

// Safety checks (only if not dry run)
if (!$isDryRun) {
    // Check if other sync processes are active
    if (isSyncRunning('python')) {
        ProgressDisplay::error("❌ Python sync is currently running. Please wait for it to finish.");
        exit(1);
    }
    if (isSyncRunning('php')) {
        ProgressDisplay::error("❌ PHP sync is currently running. Please wait for it to finish.");
        exit(1);
    }

    // Check if UBS software is running
    if (!$isForce && isUbsRunning()) {
        ProgressDisplay::error("❌ UBS software is currently open! Please close Sage UBS before running migration to prevent DBF corruption.");
        echo "   (If you are sure UBS is closed, run with --force)\n";
        exit(1);
    }

    // Acquire lock
    if (!acquireSyncLock('convert_cn2')) {
        ProgressDisplay::error("❌ Another migration or sync lock is already active.");
        exit(1);
    }

    register_shutdown_function(function() {
        releaseSyncLock('convert_cn2');
    });
}

// --------------------------------------------------------------------------
// 1. Convert artran.dbf (Orders / Header Transactions)
// --------------------------------------------------------------------------
ProgressDisplay::start("Step 1/3: Scanning artran.dbf (Orders)...");

$artranTableInfo = parseUbsTable('ubs_ubsstk2015_artran');
$artranDirectory = strtoupper($artranTableInfo['database']);
$artranPath = "C:/$artranDirectory/" . ENV::DBF_SUBPATH . "/artran.dbf";

if (!file_exists($artranPath)) {
    ProgressDisplay::error("❌ artran.dbf not found at: $artranPath");
    if (!$isDryRun) releaseSyncLock('convert_cn2');
    exit(1);
}

$convertedArtranRefs = [];
$artranModifiedCount = 0;

try {
    if (!$isDryRun) {
        $backupPath = backupDbfFile($artranPath);
        if ($backupPath) {
            ProgressDisplay::info("📦 Backup created: $backupPath");
        }
    }

    $editor = new \XBase\TableEditor($artranPath, [
        'editMode' => \XBase\TableEditor::EDIT_MODE_CLONE
    ]);

    $columns = $editor->getColumns();
    $columnMap = [];
    foreach ($columns as $column) {
        $columnMap[strtolower($column->getName())] = $column;
    }

    while ($row = $editor->nextRecord()) {
        $type = strtoupper(trim(getFieldSafe($row, 'TYPE', '', $columnMap)));
        $refNo = trim(getFieldSafe($row, 'REFNO', '', $columnMap));
        $rawDate = trim(getFieldSafe($row, 'DATE', '', $columnMap));
        
        // Parse row date (can be YYYYMMDD or YYYY-MM-DD or DateTime)
        $rowDateYmd = '';
        if (!empty($rawDate)) {
            $parsedDate = parseDateRobust($rawDate);
            if ($parsedDate) {
                $rowDateYmd = date('Y-m-d', strtotime($parsedDate));
            }
        }

        // Match criteria:
        // 1. TYPE is 'CN2', OR
        // 2. REFNO starts with 'CN2' and TYPE != 'CN'
        $isCn2Type = ($type === 'CN2');
        $isCn2Ref = (strpos(strtoupper($refNo), 'CN2') === 0 && $type !== 'CN');

        if ($isCn2Type || $isCn2Ref) {
            // Check date eligibility (date >= cutoffDate OR created on/after cutoffDate)
            $isEligibleDate = empty($rowDateYmd) || ($rowDateYmd >= $cutoffDateYmd);

            if ($isEligibleDate) {
                $convertedArtranRefs[] = $refNo;
                $grossBil = (float)(getFieldSafe($row, 'GROSS_BIL', 0, $columnMap));
                $grandBil = (float)(getFieldSafe($row, 'GRAND_BIL', getFieldSafe($row, 'GRAND', 0, $columnMap), $columnMap));
                $debitAmt = (float)(getFieldSafe($row, 'DEBITAMT', 0, $columnMap));
                $creditAmt = (float)(getFieldSafe($row, 'CREDITAMT', 0, $columnMap));

                // For Credit Note: Amount should be in CREDITAMT, DEBITAMT should be 0
                $targetCreditAmt = $creditAmt > 0 ? $creditAmt : ($debitAmt > 0 ? $debitAmt : $grandBil);

                echo sprintf(
                    "  -> Found artran: REFNO: %-10s | Date: %-10s | TYPE: %-4s -> CN | CreditAmt: %8.2f\n",
                    $refNo,
                    $rowDateYmd ?: 'N/A',
                    $type ?: 'NONE',
                    $targetCreditAmt
                );

                if (!$isDryRun) {
                    setFieldSafe($row, 'TYPE', 'CN', $columnMap);
                    setFieldSafe($row, 'CREDITAMT', $targetCreditAmt, $columnMap);
                    setFieldSafe($row, 'DEBITAMT', 0, $columnMap);
                    setFieldSafe($row, 'UPDATED_ON', date('Y-m-d H:i:s'), $columnMap);
                    $editor->writeRecord();
                }

                $artranModifiedCount++;
            }
        }
    }

    if (!$isDryRun) {
        if ($artranModifiedCount > 0) {
            $editor->save();
            if (!validateDbfFile($artranPath)) {
                throw new Exception("artran.dbf validation failed after saving!");
            }
            ProgressDisplay::complete("✅ artran.dbf updated successfully ($artranModifiedCount record(s) converted to TYPE='CN')");
        } else {
            ProgressDisplay::info("ℹ️  No CN2 records found in artran.dbf matching criteria");
        }
    } else {
        ProgressDisplay::info("ℹ️  [Dry-Run] artran.dbf: $artranModifiedCount record(s) would be converted to TYPE='CN'");
    }

    $editor->close();
} catch (\Throwable $e) {
    ProgressDisplay::error("❌ Failed processing artran.dbf: " . $e->getMessage());
    if (!$isDryRun) releaseSyncLock('convert_cn2');
    exit(1);
}

// --------------------------------------------------------------------------
// 2. Convert ictran.dbf (Order Items / Line Items)
// --------------------------------------------------------------------------
echo "\n";
ProgressDisplay::start("Step 2/3: Scanning ictran.dbf (Order Line Items)...");

$ictranTableInfo = parseUbsTable('ubs_ubsstk2015_ictran');
$ictranDirectory = strtoupper($ictranTableInfo['database']);
$ictranPath = "C:/$ictranDirectory/" . ENV::DBF_SUBPATH . "/ictran.dbf";

if (!file_exists($ictranPath)) {
    ProgressDisplay::error("❌ ictran.dbf not found at: $ictranPath");
    if (!$isDryRun) releaseSyncLock('convert_cn2');
    exit(1);
}

$ictranModifiedCount = 0;

try {
    if (!$isDryRun) {
        $backupPath = backupDbfFile($ictranPath);
        if ($backupPath) {
            ProgressDisplay::info("📦 Backup created: $backupPath");
        }
    }

    $editor = new \XBase\TableEditor($ictranPath, [
        'editMode' => \XBase\TableEditor::EDIT_MODE_CLONE
    ]);

    $icColumns = $editor->getColumns();
    $icColumnMap = [];
    foreach ($icColumns as $column) {
        $icColumnMap[strtolower($column->getName())] = $column;
    }

    while ($row = $editor->nextRecord()) {
        $type = strtoupper(trim(getFieldSafe($row, 'TYPE', '', $icColumnMap)));
        $refNo = trim(getFieldSafe($row, 'REFNO', '', $icColumnMap));
        $itemCount = trim(getFieldSafe($row, 'ITEMCOUNT', getFieldSafe($row, 'TRANCODE', '', $icColumnMap), $icColumnMap));
        $itemNo = trim(getFieldSafe($row, 'ITEMNO', '', $icColumnMap));
        $rawDate = trim(getFieldSafe($row, 'DATE', '', $icColumnMap));

        $rowDateYmd = '';
        if (!empty($rawDate)) {
            $parsedDate = parseDateRobust($rawDate);
            if ($parsedDate) {
                $rowDateYmd = date('Y-m-d', strtotime($parsedDate));
            }
        }

        // Match criteria:
        // 1. REFNO is in converted artran list, OR
        // 2. TYPE is 'CN2', OR
        // 3. REFNO starts with 'CN2' and TYPE != 'CN'
        $isInArtranList = in_array($refNo, $convertedArtranRefs, true);
        $isCn2Type = ($type === 'CN2');
        $isCn2Ref = (strpos(strtoupper($refNo), 'CN2') === 0 && $type !== 'CN');

        if ($isInArtranList || $isCn2Type || $isCn2Ref) {
            $isEligibleDate = empty($rowDateYmd) || ($rowDateYmd >= $cutoffDateYmd);

            if ($isEligibleDate) {
                echo sprintf(
                    "  -> Found ictran: REFNO: %-10s | Item: %-8s | Prod: %-10s | TYPE: %-4s -> CN\n",
                    $refNo,
                    $itemCount,
                    $itemNo,
                    $type ?: 'NONE'
                );

                if (!$isDryRun) {
                    setFieldSafe($row, 'TYPE', 'CN', $icColumnMap);
                    setFieldSafe($row, 'UPDATED_ON', date('Y-m-d H:i:s'), $icColumnMap);
                    $editor->writeRecord();
                }

                $ictranModifiedCount++;
            }
        }
    }

    if (!$isDryRun) {
        if ($ictranModifiedCount > 0) {
            $editor->save();
            if (!validateDbfFile($ictranPath)) {
                throw new Exception("ictran.dbf validation failed after saving!");
            }
            ProgressDisplay::complete("✅ ictran.dbf updated successfully ($ictranModifiedCount record(s) converted to TYPE='CN')");
        } else {
            ProgressDisplay::info("ℹ️  No CN2 records found in ictran.dbf matching criteria");
        }
    } else {
        ProgressDisplay::info("ℹ️  [Dry-Run] ictran.dbf: $ictranModifiedCount record(s) would be converted to TYPE='CN'");
    }

    $editor->close();
} catch (\Throwable $e) {
    ProgressDisplay::error("❌ Failed processing ictran.dbf: " . $e->getMessage());
    if (!$isDryRun) releaseSyncLock('convert_cn2');
    exit(1);
}

// --------------------------------------------------------------------------
// 3. Convert Local MySQL Connector Tables
// --------------------------------------------------------------------------
echo "\n";
ProgressDisplay::start("Step 3/3: Updating Local MySQL Connector Tables...");

try {
    $db_local = new mysql();
    $db_local->connect();

    // Check artran table
    $mysqlArtranCountSql = "SELECT COUNT(*) as cnt FROM `ubs_ubsstk2015_artran` 
                           WHERE (`DATE` >= '$cutoffDateYmd' OR `CREATED_ON` >= '$cutoffDateYmd' OR `REFNO` LIKE 'CN2%') 
                             AND (`TYPE` = 'CN2' OR `REFNO` LIKE 'CN2%')";
    $mysqlArtranFound = (int)($db_local->first($mysqlArtranCountSql)['cnt'] ?? 0);

    // Check ictran table
    $mysqlIctranCountSql = "SELECT COUNT(*) as cnt FROM `ubs_ubsstk2015_ictran` 
                           WHERE (`DATE` >= '$cutoffDateYmd' OR `CREATED_ON` >= '$cutoffDateYmd' OR `REFNO` LIKE 'CN2%') 
                             AND (`TYPE` = 'CN2' OR `REFNO` LIKE 'CN2%')";
    $mysqlIctranFound = (int)($db_local->first($mysqlIctranCountSql)['cnt'] ?? 0);

    if ($isDryRun) {
        ProgressDisplay::info("ℹ️  [Dry-Run] Local MySQL `ubs_ubsstk2015_artran`: $mysqlArtranFound record(s) would be updated");
        ProgressDisplay::info("ℹ️  [Dry-Run] Local MySQL `ubs_ubsstk2015_ictran`: $mysqlIctranFound record(s) would be updated");
    } else {
        if ($mysqlArtranFound > 0) {
            $updateArtranSql = "UPDATE `ubs_ubsstk2015_artran` 
                               SET `TYPE` = 'CN',
                                   `CREDITAMT` = CASE WHEN (`CREDITAMT` = 0 OR `CREDITAMT` IS NULL) AND `DEBITAMT` > 0 THEN `DEBITAMT` ELSE `CREDITAMT` END,
                                   `DEBITAMT` = 0,
                                   `UPDATED_ON` = NOW()
                               WHERE (`DATE` >= '$cutoffDateYmd' OR `CREATED_ON` >= '$cutoffDateYmd' OR `REFNO` LIKE 'CN2%') 
                                 AND (`TYPE` = 'CN2' OR `REFNO` LIKE 'CN2%')";
            $db_local->query($updateArtranSql);
            ProgressDisplay::complete("✅ Local MySQL `ubs_ubsstk2015_artran` updated ($mysqlArtranFound record(s))");
        } else {
            ProgressDisplay::info("ℹ️  Local MySQL `ubs_ubsstk2015_artran`: No records needed update");
        }

        if ($mysqlIctranFound > 0) {
            $updateIctranSql = "UPDATE `ubs_ubsstk2015_ictran` 
                               SET `TYPE` = 'CN',
                                   `UPDATED_ON` = NOW()
                               WHERE (`DATE` >= '$cutoffDateYmd' OR `CREATED_ON` >= '$cutoffDateYmd' OR `REFNO` LIKE 'CN2%') 
                                 AND (`TYPE` = 'CN2' OR `REFNO` LIKE 'CN2%')";
            $db_local->query($updateIctranSql);
            ProgressDisplay::complete("✅ Local MySQL `ubs_ubsstk2015_ictran` updated ($mysqlIctranFound record(s))");
        } else {
            ProgressDisplay::info("ℹ️  Local MySQL `ubs_ubsstk2015_ictran`: No records needed update");
        }
    }

    $db_local->close();
} catch (\Throwable $e) {
    ProgressDisplay::warning("⚠️  Local MySQL update note: " . $e->getMessage());
}

// Release lock if not dry-run
if (!$isDryRun) {
    releaseSyncLock('convert_cn2');
}

echo "\n" . str_repeat('=', 70) . "\n";
echo "  🎉 MIGRATION SUMMARY\n";
echo str_repeat('=', 70) . "\n";
echo "  • artran.dbf records processed : $artranModifiedCount\n";
echo "  • ictran.dbf records processed : $ictranModifiedCount\n";
echo "  • Cutoff date applied          : $cutoffDateDisplay ($cutoffDateYmd)\n";
if ($isDryRun) {
    echo "  • Result                       : DRY-RUN COMPLETED (no data changed)\n";
    echo "  • To execute live conversion   : Run `php convert_cn2_to_cn.php`\n";
} else {
    echo "  • Result                       : CONVERSION COMPLETED SUCCESSFULLY!\n";
}
echo str_repeat('=', 70) . "\n\n";
