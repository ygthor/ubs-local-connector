<?php
/**
 * DBF File Repair Utility
 * Fixes common DBF file corruption issues like record length mismatches
 * 
 * Usage:
 *   php repair_dbf.php <dbf_file_path> [output_path]
 *   php repair_dbf.php C:/UBSACC2015/KBS/arcust.dbf
 */

function repairDbfFile($inputPath, $outputPath = null)
{
    if (!file_exists($inputPath)) {
        echo "❌ File not found: $inputPath\n";
        return false;
    }
    
    if ($outputPath === null) {
        $outputPath = $inputPath . '.repaired';
    }
    
    echo "🔧 Repairing DBF file: $inputPath\n";
    echo "📁 Output file: $outputPath\n";
    
    try {
        // Create backup
        $backupPath = $inputPath . '.backup.' . date('YmdHis');
        copy($inputPath, $backupPath);
        echo "💾 Created backup: $backupPath\n";
        
        $infile = fopen($inputPath, 'rb');
        $outfile = fopen($outputPath, 'wb');
        
        if (!$infile || !$outfile) {
            echo "❌ Failed to open files\n";
            return false;
        }
        
        // Read header (first 32 bytes)
        $header = fread($infile, 32);
        if (strlen($header) < 32) {
            echo "❌ Invalid DBF file: header too short\n";
            fclose($infile);
            fclose($outfile);
            return false;
        }
        
        // Parse header information
        $numRecords = unpack('V', substr($header, 4, 4))[1]; // Little-endian unsigned long
        $headerLength = unpack('v', substr($header, 8, 2))[1]; // Little-endian unsigned short
        $recordLength = unpack('v', substr($header, 10, 2))[1]; // Little-endian unsigned short
        
        echo "📊 Records: $numRecords, Header length: $headerLength, Record length: $recordLength\n";
        
        // Copy header and field definitions
        rewind($infile);
        $headerAndFields = fread($infile, $headerLength);
        fwrite($outfile, $headerAndFields);
        
        // Process records
        $repairedCount = 0;
        $skippedCount = 0;
        $actualRecordCount = 0;
        
        for ($i = 0; $i < $numRecords; $i++) {
            $recordData = fread($infile, $recordLength);
            
            if (strlen($recordData) == 0) {
                // End of file reached
                echo "⚠️  End of file reached at record $i\n";
                break;
            } elseif (strlen($recordData) < $recordLength) {
                // Record too short - pad with null bytes
                $paddingNeeded = $recordLength - strlen($recordData);
                $recordData = $recordData . str_repeat("\x00", $paddingNeeded);
                $repairedCount++;
                if ($repairedCount <= 5) {
                    echo "🔧 Repaired record $i: padded $paddingNeeded bytes\n";
                }
            } elseif (strlen($recordData) > $recordLength) {
                // Record too long - truncate
                $recordData = substr($recordData, 0, $recordLength);
                $repairedCount++;
                if ($repairedCount <= 5) {
                    echo "🔧 Repaired record $i: truncated " . (strlen($recordData) - $recordLength) . " bytes\n";
                }
            }
            
            // Skip deleted records (marked with 0x2A)
            if (ord($recordData[0]) == 0x2A) {
                $skippedCount++;
                continue;
            }
            
            fwrite($outfile, $recordData);
            $actualRecordCount++;
        }
        
        // Update record count in header if needed
        if ($actualRecordCount != $numRecords) {
            echo "⚠️  Record count mismatch: header says $numRecords, actual is $actualRecordCount\n";
            // Update the record count in the output file
            fseek($outfile, 4);
            fwrite($outfile, pack('V', $actualRecordCount));
        }
        
        fclose($infile);
        fclose($outfile);
        
        echo "✅ Repair completed!\n";
        echo "📊 Repaired records: $repairedCount\n";
        echo "🗑️  Skipped deleted records: $skippedCount\n";
        echo "💾 Repaired file saved as: $outputPath\n";
        
        return true;
        
    } catch (Exception $e) {
        echo "❌ Error repairing file: " . $e->getMessage() . "\n";
        return false;
    }
}

function repairDbfFileInPlace($filePath)
{
    $tempPath = $filePath . '.temp.' . time();
    $success = repairDbfFile($filePath, $tempPath);
    
    if ($success) {
        // Replace original with repaired version
        if (copy($tempPath, $filePath)) {
            unlink($tempPath);
            echo "✅ File repaired and replaced successfully\n";
            return true;
        } else {
            echo "⚠️  Repair successful but failed to replace original file\n";
            echo "💡 Repaired file is at: $tempPath\n";
            return false;
        }
    }
    
    return false;
}

// Command-line usage
if (php_sapi_name() === 'cli' && isset($argv)) {
    if (count($argv) < 2) {
        echo "Usage: php repair_dbf.php <dbf_file_path> [output_path]\n";
        echo "Example: php repair_dbf.php C:/UBSACC2015/KBS/arcust.dbf\n";
        echo "         php repair_dbf.php C:/UBSACC2015/KBS/arcust.dbf C:/UBSACC2015/KBS/arcust_repaired.dbf\n";
        exit(1);
    }
    
    $inputFile = $argv[1];
    $outputFile = isset($argv[2]) ? $argv[2] : null;
    
    $success = repairDbfFile($inputFile, $outputFile);
    
    if ($success) {
        echo "\n🎉 DBF file repair completed successfully!\n";
        echo "💡 You can now try syncing the repaired file.\n";
        exit(0);
    } else {
        echo "\n❌ DBF file repair failed.\n";
        echo "💡 You may need to restore from backup or contact support.\n";
        exit(1);
    }
}

