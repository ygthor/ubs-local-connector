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
        COUNT(IC.REFNO) AS count_items
    FROM ubs_ubsstk2015_artran AS AR
    LEFT JOIN ubs_ubsstk2015_ictran AS IC ON AR.REFNO = IC.REFNO
    GROUP BY AR.REFNO
    HAVING count_items = 0
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
    
    // Extract REFNO values from result array
    $refnos = [];
    foreach ($rows as $row) {
        $refnos[] = $row['REFNO'];
    }
    
    // Create comma-separated string
    $refno_list = implode(',', $refnos);
    
    // Write to file
    $output_file = __DIR__ . '/artrans_without_items.txt';
    $bytes_written = file_put_contents($output_file, $refno_list);
    
    if ($bytes_written === false) {
        throw new Exception("Failed to write to file: $output_file");
    }
    
    writeLog('Successfully extracted ' . count($refnos) . ' REFNO values', 'SUCCESS');
    writeLog('Output written to: ' . $output_file, 'SUCCESS');
    
    echo "✅ Successfully extracted " . count($refnos) . " REFNO values.\n";
    echo "📁 Output written to: $output_file\n";
    echo "📋 Log written to: $logFile\n";
    
    // Open the output file when complete
    writeLog('Opening output file...', 'INFO');
    if (file_exists($output_file)) {
        if (PHP_OS_FAMILY === 'Darwin') {
            // macOS
            exec("open " . escapeshellarg($output_file) . " 2>&1", $output, $return_code);
        } elseif (PHP_OS_FAMILY === 'Windows') {
            // Windows
            exec("start " . escapeshellarg($output_file) . " 2>&1", $output, $return_code);
        } else {
            // Linux
            exec("xdg-open " . escapeshellarg($output_file) . " > /dev/null 2>&1 &", $output, $return_code);
        }
        if ($return_code === 0) {
            writeLog('Output file opened successfully', 'SUCCESS');
        } else {
            writeLog('Failed to open file automatically. File saved at: ' . $output_file, 'WARNING');
        }
    }
    
} catch (Exception $e) {
    writeLog('Error: ' . $e->getMessage(), 'ERROR');
    echo "❌ Error: " . $e->getMessage() . "\n";
    exit(1);
}