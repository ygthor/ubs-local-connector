<?php
include(__DIR__ . '/../bootstrap/app.php');
// get artrans without ictrans
// need to resync

// Setup logging
$debugDir = __DIR__ . '/debug';
if (!is_dir($debugDir)) {
    mkdir($debugDir, 0755, true);
}

$timestamp = date('Y-m-d_H-i-s');
$logFile = $debugDir . '/artrans_extract_' . $timestamp . '.txt';

function writeLog($message, $level = 'INFO') {
    global $logFile;
    $logMessage = '[' . date('Y-m-d H:i:s') . '] [' . $level . '] ' . $message . "\n";
    file_put_contents($logFile, $logMessage, FILE_APPEND);
    echo $logMessage;
}

writeLog('Script started', 'INFO');

$db = new mysql();
writeLog('Database connection established', 'INFO');

$sql = "
    SELECT 
        AR.REFNO,
        AR.DATE,
        COUNT(IC.REFNO) AS count_items
    FROM ubs_ubsstk2015_artran AS AR
    LEFT JOIN ubs_ubsstk2015_ictran AS IC ON AR.REFNO = IC.REFNO
    WHERE AR.TYPE NOT IN  ('RC','DO','PR')
    AND AR.REFNO NOT LIKE 'CN %'
    AND AR.REFNO NOT LIKE 'I0%'
    GROUP BY AR.REFNO
    HAVING count_items = 0
    ORDER BY AR.DATE DESC
";

// create a txt file with all REFNO, comma separated.
try {
    writeLog('Executing query to find ARTRANS without items', 'INFO');
    
    // Execute the query - get() returns an array of rows
    $rows = $db->get($sql);
    
    if (empty($rows)) {
        writeLog('No ARTRANS records without items found', 'WARNING');
        exit;
    }
    
    writeLog('Found ' . count($rows) . ' ARTRANS records without items', 'INFO');
    
    // Extract REFNO and DATE values from result array
    $refnos = [];
    $tableData = [];
    foreach ($rows as $row) {
        $refno = $row['REFNO'];
        $date = $row['DATE'] ?? 'N/A';
        $refnos[] = $refno;
        $tableData[] = [
            'REFNO' => $refno,
            'DATE' => $date
        ];
    }
    
    // Create comma-separated string
    $refno_list = implode(',', $refnos);
    
    // Create formatted output with table and comma-separated list
    $output_content = '';
    $output_content .= "═══════════════════════════════════════════════════════════\n";
    $output_content .= "ARTRANS WITHOUT ITEMS - EXTRACTED " . date('Y-m-d H:i:s') . "\n";
    $output_content .= "═══════════════════════════════════════════════════════════\n\n";
    
    // Format table
    $output_content .= "REF NO                    | DATE\n";
    $output_content .= str_repeat("─", 55) . "\n";
    
    foreach ($tableData as $item) {
        $refno = str_pad($item['REFNO'], 25);
        $date = $item['DATE'];
        $output_content .= "$refno | $date\n";
    }
    
    $output_content .= str_repeat("═", 55) . "\n";
    $output_content .= "Total Records: " . count($refnos) . "\n\n";
    
    // Add comma-separated list for easy copying
    $output_content .= "COMMA-SEPARATED LIST (For easy copy-paste):\n";
    $output_content .= str_repeat("─", 55) . "\n";
    $output_content .= $refno_list . "\n";
    $output_content .= str_repeat("═", 55) . "\n";
    
    // Write to file
    $output_file = __DIR__ . '/artrans_without_items.txt';
    $bytes_written = file_put_contents($output_file, $output_content);
    
    if ($bytes_written === false) {
        throw new Exception("Failed to write to file: $output_file");
    }
    
    writeLog('Successfully extracted ' . count($refnos) . ' REFNO values', 'SUCCESS');
    writeLog('Output written to: ' . $output_file, 'SUCCESS');
    
    // Display to console
    echo "\n";
    echo $output_content;
    echo "\n";
    echo "✅ Successfully extracted " . count($refnos) . " REFNO values.\n";
    echo "📁 Output written to: $output_file\n";
    echo "📋 Log written to: $logFile\n";
    
    // Open the output file when complete
    writeLog('Opening output file...', 'INFO');
    if (PHP_OS_FAMILY === 'Darwin') { // macOS
        exec("open '$output_file'");
        writeLog('📂 Output file opened automatically', 'SUCCESS');
    } elseif (PHP_OS_FAMILY === 'Windows') {
        exec('notepad.exe "' . $output_file . '"');
        writeLog('📂 Output file opened automatically', 'SUCCESS');
    } elseif (PHP_OS_FAMILY === 'Linux') {
        exec("xdg-open '$output_file' 2>/dev/null &");
        writeLog('📂 Output file opened automatically', 'SUCCESS');
    }
    
} catch (Exception $e) {
    writeLog('Error: ' . $e->getMessage(), 'ERROR');
    echo "❌ Error: " . $e->getMessage() . "\n";
    exit(1);
}