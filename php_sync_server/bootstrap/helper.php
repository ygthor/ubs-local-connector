<?php
if (!function_exists('url')) {
    function url($val = '')
    {
        $https = isset($_SERVER['HTTPS']) && $_SERVER['HTTPS'] === 'on' ? "https" : "http";
        $server_url = !isset($_SERVER['HTTP_HOST']) ? ENV::APP_URL : $https . "://" . $_SERVER['HTTP_HOST'];
        if (!empty($val)) {
            $server_url .= $val;
        }
        return $server_url;
    }
}

if (!function_exists('isCli')) {
    function isCli()
    {
        return php_sapi_name() === 'cli' || (defined('STDIN') && STDIN !== null);
    }
}

if (!function_exists('dump')) {

    function dump($v = 'RANDOM_STR')
    {
        if (isCli()) {
            // CMD/CLI display pattern - compact format
            
            if ($v === null) {
                echo 'null';
            } elseif ($v === 'RANDOM_STR') {
                echo randstr();
            } elseif ($v === true) {
                echo 'true';
            } elseif ($v === false) {
                echo 'false';
            } else {
                if (is_array($v)) {
                    $v = (json_encode($v, JSON_PRETTY_PRINT));
                    $v = strip_tags($v);
                    print_r($v);
                } else {
                    print_r($v);
                }
            }
            echo "\n";
        } else {
            // Web display pattern
            echo "<pre style='background:#263238;color:white;padding:10px;margin:20px 0px'>";
            if ($v === null) {
                echo 'null';
            } elseif ($v === 'RANDOM_STR') {
                echo randstr();
            } elseif ($v === true) {
                echo 'true';
            } elseif ($v === false) {
                echo 'false';
            } else {
                if (is_array($v)) {
                    $v = (json_encode($v, JSON_PRETTY_PRINT));
                    $v = strip_tags($v);
                    print_r($v);
                } else {
                    print_r($v);
                }
            }
            echo "</pre>";
        }
    }
}

if (!function_exists('dd')) {
    function dd($v = 'RANDOM_STR')
    {
        if (isCli()) {
            // CMD/CLI display pattern - compact format
            echo "\n[DD] ";
            
            if ($v === null) {
                echo 'null';
            } elseif ($v === 'RANDOM_STR') {
                echo randstr();
            } elseif ($v === true) {
                echo 'true';
            } elseif ($v === false) {
                echo 'false';
            } else {
                if (is_array($v)) {
                    $v = (json_encode($v, JSON_PRETTY_PRINT));
                    $v = strip_tags($v);
                    print_r($v);
                } elseif (is_object($v)) {
                    // Convert boolean properties to strings in the object
                    // $v = convertBooleansToStrings(get_object_vars($v));
                    
                    // JSON encode the object
                    $v = json_encode($v, JSON_PRETTY_PRINT);
                    $v = strip_tags($v);
                    print_r($v);
                } else {
                    print_r($v);
                }
            }
            echo "\n[EXIT]\n";
            exit;
        } else {
            // Web display pattern
            echo "<pre style='background:#000000;color:white;padding:10px;margin:20px 0px'>";
            if ($v === null) {
                echo 'null';
            } elseif ($v === 'RANDOM_STR') {
                echo randstr();
            } elseif ($v === true) {
                echo 'true';
            } elseif ($v === false) {
                echo 'false';
            } else {
                if (is_array($v)) {
                    $v = (json_encode($v, JSON_PRETTY_PRINT));
                    $v = strip_tags($v);
                    print_r($v);
                } elseif (is_object($v)) {
                    // Convert boolean properties to strings in the object
                    // $v = convertBooleansToStrings(get_object_vars($v));
                    
                    // JSON encode the object
                    $v = json_encode($v, JSON_PRETTY_PRINT);
                    $v = strip_tags($v);
                    print_r($v);
                } else {
                    print_r($v);
                }
            }
            echo "</pre>";
            echo "<hr>";
            echo "EXIT";
            echo "<hr>";
            exit;
        }
    }
}

function timestamp(){
    return date('Y-m-d H:i:s');
}

/**
 * Lock file system for preventing concurrent syncs
 * @param string $lockType 'php' or 'python'
 * @return bool True if lock acquired, false if already locked
 */
function acquireSyncLock($lockType = 'php') {
    $lockDir = __DIR__ . '/../locks';
    if (!is_dir($lockDir)) {
        mkdir($lockDir, 0755, true);
    }
    
    $lockFile = $lockDir . '/' . $lockType . '_sync.lock';
    $pidFile = $lockDir . '/' . $lockType . '_sync.pid';
    
    // Check if lock exists and if process is still running
    if (file_exists($lockFile)) {
        $pid = file_get_contents($pidFile);
        if ($pid && isProcessRunning($pid)) {
            return false; // Lock is held by running process
        } else {
            // Stale lock, remove it
            @unlink($lockFile);
            @unlink($pidFile);
        }
    }
    
    // Create lock file
    file_put_contents($lockFile, date('Y-m-d H:i:s'));
    file_put_contents($pidFile, getmypid());
    
    return true;
}

/**
 * Release sync lock
 * @param string $lockType 'php' or 'python'
 */
function releaseSyncLock($lockType = 'php') {
    $lockDir = __DIR__ . '/../locks';
    $lockFile = $lockDir . '/' . $lockType . '_sync.lock';
    $pidFile = $lockDir . '/' . $lockType . '_sync.pid';
    
    @unlink($lockFile);
    @unlink($pidFile);
}

/**
 * Check if a process is still running
 * @param int $pid Process ID
 * @return bool True if process is running
 */
function isProcessRunning($pid) {
    if (PHP_OS_FAMILY === 'Windows') {
        // Windows
        $output = [];
        exec("tasklist /FI \"PID eq $pid\" 2>NUL", $output);
        return !empty($output) && strpos(implode(' ', $output), (string)$pid) !== false;
    } else {
        // Unix/Linux/Mac
        return posix_kill($pid, 0);
    }
}

/**
 * Check if sync is currently running
 * @param string $lockType 'php' or 'python'
 * @return bool True if sync is running
 */
function isSyncRunning($lockType = 'php') {
    $lockDir = __DIR__ . '/../locks';
    $lockFile = $lockDir . '/' . $lockType . '_sync.lock';
    $pidFile = $lockDir . '/' . $lockType . '_sync.pid';
    
    if (!file_exists($lockFile)) {
        return false;
    }
    
    $pid = file_get_contents($pidFile);
    return $pid && isProcessRunning($pid);
}

/**
 * Get sync status
 * @return array Status of both sync types
 */
function getSyncStatus() {
    return [
        'php' => [
            'running' => isSyncRunning('php'),
            'lock_file' => __DIR__ . '/../locks/php_sync.lock',
            'pid_file' => __DIR__ . '/../locks/php_sync.pid',
        ],
        'python' => [
            'running' => isSyncRunning('python'),
            'lock_file' => __DIR__ . '/../locks/python_sync.lock',
            'pid_file' => __DIR__ . '/../locks/python_sync.pid',
        ],
    ];
}

/**
 * ✅ SAFE: Verify indexes exist on table (read-only operation)
 * @param string $table Table name
 * @param string $dbType 'local' or 'remote'
 * @return array Index status information
 */
function verifySyncIndexes($table, $dbType = 'local') {
    try {
        $db = new mysql();
        if ($dbType === 'remote') {
            $db->connect_remote();
        }
        
        // Get table name (handle both local and remote table names)
        $actual_table = $table;
        if ($dbType === 'remote') {
            $actual_table = Converter::table_convert_remote($table);
        }
        
        // Check if UPDATED_ON index exists
        $updated_column = $dbType === 'remote' 
            ? Converter::mapUpdatedAtField($actual_table)
            : 'UPDATED_ON';
        
        $indexCheck = $db->query("
            SHOW INDEXES FROM `$actual_table` 
            WHERE Column_name = '$updated_column'
        ");
        
        $indexes = [];
        while ($row = mysqli_fetch_assoc($indexCheck)) {
            $indexes[] = $row;
        }
        
        $hasUpdatedOnIndex = !empty($indexes);
        
        // Check primary key
        $primaryKeyCheck = $db->query("
            SHOW KEYS FROM `$actual_table` 
            WHERE Key_name = 'PRIMARY'
        ");
        
        $primaryKeys = [];
        while ($row = mysqli_fetch_assoc($primaryKeyCheck)) {
            $primaryKeys[] = $row;
        }
        
        $hasPrimaryKey = !empty($primaryKeys);
        
        return [
            'table' => $actual_table,
            'has_updated_on_index' => $hasUpdatedOnIndex,
            'has_primary_key' => $hasPrimaryKey,
            'updated_on_column' => $updated_column,
            'indexes' => $indexes,
            'primary_keys' => $primaryKeys,
        ];
    } catch (Exception $e) {
        // Safe: Just return error info, don't throw
        return [
            'table' => $table,
            'error' => $e->getMessage(),
            'has_updated_on_index' => false,
            'has_primary_key' => false,
        ];
    }
}

/**
 * ✅ SAFE: Log sync conflicts (read-only logging)
 * @param string $table Table name
 * @param string $key Record key
 * @param string $ubs_timestamp UBS timestamp
 * @param string $remote_timestamp Remote timestamp
 */
function logSyncConflict($table, $key, $ubs_timestamp, $remote_timestamp) {
    $logDir = __DIR__ . '/../logs';
    if (!is_dir($logDir)) {
        mkdir($logDir, 0755, true);
    }
    
    $logFile = $logDir . '/sync_conflicts.log';
    $logEntry = sprintf(
        "[%s] CONFLICT: Table=%s, Key=%s, UBS=%s, Remote=%s\n",
        date('Y-m-d H:i:s'),
        $table,
        $key,
        $ubs_timestamp,
        $remote_timestamp
    );
    
    file_put_contents($logFile, $logEntry, FILE_APPEND);
}

/**
 * ✅ SAFE: Retry operation with exponential backoff
 * @param callable $operation Function to retry
 * @param int $maxRetries Maximum number of retries
 * @param int $baseDelay Base delay in microseconds
 * @return mixed Result of operation
 */
function retryOperation($operation, $maxRetries = 3, $baseDelay = 100000) {
    $attempt = 0;
    $lastException = null;
    
    while ($attempt < $maxRetries) {
        try {
            return $operation();
        } catch (Exception $e) {
            $lastException = $e;
            $attempt++;
            
            if ($attempt >= $maxRetries) {
                ProgressDisplay::warning("Operation failed after $maxRetries attempts: " . $e->getMessage());
                throw $e;
            }
            
            // Exponential backoff: 0.1s, 0.2s, 0.4s, etc.
            $delay = $baseDelay * pow(2, $attempt - 1);
            usleep($delay);
            
            ProgressDisplay::info("Retrying operation (attempt $attempt/$maxRetries) after " . ($delay / 1000000) . "s delay...");
        }
    }
    
    throw $lastException;
}

/**
 * ✅ SAFE: Fetch remote data incrementally by keys (optimized)
 * @param string $table Table name
 * @param array $keys Array of primary keys to fetch (can be simple keys or composite keys in "key1|key2" format)
 * @param string $updatedAfter Optional timestamp filter
 * @return array Remote data
 */
function fetchRemoteDataByKeys($table, $keys, $updatedAfter = null, $resyncDate = null) {
    if (empty($keys)) {
        return [];
    }
    
    try {
        $db = new mysql();
        $db->connect_remote();
        
        $remote_table_name = Converter::table_convert_remote($table);
        $remote_key = Converter::primaryKey($remote_table_name);
        $column_updated_at = Converter::mapUpdatedAtField($remote_table_name);
        
        $is_composite_key = is_array($remote_key);
        
        // ✅ SAFER: For orders (artran), we need to filter out orders without items
        // Build the base SQL with JOIN for orders, or standard SQL for other tables
        $isArtran = ($table === 'ubs_ubsstk2015_artran');
        
        if ($is_composite_key) {
            // Handle composite keys (e.g., REFNO + ITEMCOUNT)
            // Keys are in format "key1|key2"
            $conditions = [];
            foreach ($keys as $key) {
                $keyParts = explode('|', $key);
                if (count($keyParts) === count($remote_key)) {
                    $keyConditions = [];
                    foreach ($remote_key as $index => $keyField) {
                        $keyConditions[] = "`$keyField` = '" . $db->escape($keyParts[$index]) . "'";
                    }
                    $conditions[] = "(" . implode(' AND ', $keyConditions) . ")";
                }
            }
            
            if (empty($conditions)) {
                return [];
            }
            
            if ($isArtran) {
                // For orders: Add JOIN with order_items to filter out orders without items
                // Use GROUP BY to avoid duplicate orders when order has multiple items
                $sql = "SELECT o.* FROM `$remote_table_name` o 
                       INNER JOIN order_items oi ON o.reference_no = oi.reference_no 
                       WHERE " . implode(' OR ', $conditions) . "
                       GROUP BY o.reference_no";
            } else {
                $sql = "SELECT * FROM `$remote_table_name` WHERE " . implode(' OR ', $conditions);
            }
        } else {
            // Handle simple keys
            $escaped_keys = array_map(function($key) use ($db) {
                return "'" . $db->escape($key) . "'";
            }, $keys);
            
            $keys_str = implode(',', $escaped_keys);
            
            if ($isArtran) {
                // For orders: Add JOIN with order_items to filter out orders without items
                // Use GROUP BY to avoid duplicate orders when order has multiple items
                $sql = "SELECT o.* FROM `$remote_table_name` o 
                       INNER JOIN order_items oi ON o.reference_no = oi.reference_no 
                       WHERE o.`$remote_key` IN ($keys_str)
                       GROUP BY o.reference_no";
            } else {
                $sql = "SELECT * FROM `$remote_table_name` WHERE `$remote_key` IN ($keys_str)";
            }
        }
        
        // Resync mode: Filter by DATE(created_at) = date OR DATE(updated_at) = date
        if ($resyncDate) {
            if ($isArtran) {
                // ✅ SAFER: JOIN already added in base SQL, just add date filter
                $sql .= " AND (DATE(o.created_at) = '" . $db->escape($resyncDate) . "' OR DATE(o.updated_at) = '" . $db->escape($resyncDate) . "' OR DATE(o.order_date) = '" . $db->escape($resyncDate) . "')";
            } elseif ($table === 'ubs_ubsstk2015_ictran') {
                // Need to join with orders table to check order_date
                if (strpos($sql, "FROM `$remote_table_name` WHERE") !== false) {
                    $sql = str_replace("FROM `$remote_table_name` WHERE", 
                        "FROM `$remote_table_name` oi INNER JOIN orders o ON oi.reference_no = o.reference_no WHERE", 
                        $sql);
                } elseif (strpos($sql, "FROM `$remote_table_name`") !== false) {
                    $sql = str_replace("FROM `$remote_table_name`", 
                        "FROM `$remote_table_name` oi INNER JOIN orders o ON oi.reference_no = o.reference_no", 
                        $sql);
                }
                $sql = str_replace("SELECT *", "SELECT oi.*", $sql);
                $sql .= " AND (DATE(oi.created_at) = '" . $db->escape($resyncDate) . "' OR DATE(oi.updated_at) = '" . $db->escape($resyncDate) . "' OR DATE(o.order_date) = '" . $db->escape($resyncDate) . "')";
            } else {
                $sql .= " AND (DATE(created_at) = '" . $db->escape($resyncDate) . "' OR DATE(updated_at) = '" . $db->escape($resyncDate) . "')";
            }
        }
        // Normal mode: Check both updated_at AND order_date to catch recent orders
        else {
            // For artran (orders): Check both updated_at AND order_date to catch recent orders
            // This ensures orders with recent order_date but old updated_at are still synced
            // ✅ SAFER: JOIN already added in base SQL, just add date filter if needed
            if ($isArtran) {
                if ($updatedAfter) {
                    $sql .= " AND (o.`$column_updated_at` > '" . $db->escape($updatedAfter) . "' OR o.order_date > '" . $db->escape($updatedAfter) . "')";
                }
            }
            // For ictran (order_items): Check both updated_at AND parent order's order_date
            elseif ($table === 'ubs_ubsstk2015_ictran') {
                // Need to join with orders table to check order_date
                if ($updatedAfter) {
                    // Replace FROM clause to add JOIN (handle both WHERE and no WHERE cases)
                    if (strpos($sql, "FROM `$remote_table_name` WHERE") !== false) {
                        // Has WHERE clause - insert JOIN before WHERE
                        $sql = str_replace("FROM `$remote_table_name` WHERE", 
                            "FROM `$remote_table_name` oi INNER JOIN orders o ON oi.reference_no = o.reference_no WHERE", 
                            $sql);
                    } elseif (strpos($sql, "FROM `$remote_table_name`") !== false) {
                        // No WHERE clause - replace FROM
                        $sql = str_replace("FROM `$remote_table_name`", 
                            "FROM `$remote_table_name` oi INNER JOIN orders o ON oi.reference_no = o.reference_no", 
                            $sql);
                    }
                    
                    // Update SELECT to use oi.* to avoid duplicate columns from JOIN
                    $sql = str_replace("SELECT *", "SELECT oi.*", $sql);
                    
                    // Add date filters
                    $sql .= " AND (oi.`$column_updated_at` > '" . $db->escape($updatedAfter) . "' OR o.order_date > '" . $db->escape($updatedAfter) . "')";
                }
            }
            // For other tables: Use standard updated_at filter
            else {
                if ($updatedAfter) {
                    $sql .= " AND `$column_updated_at` > '" . $db->escape($updatedAfter) . "'";
                }
            }
        }
        
        return $db->get($sql);
    } catch (Exception $e) {
        ProgressDisplay::warning("Error fetching remote data by keys: " . $e->getMessage());
        // Safe fallback: return empty array instead of failing
        return [];
    }
}

/**
 * ✅ SAFE: Execute sync operation with transaction and rollback on error
 * @param callable $operation Sync operation to execute
 * @param bool $useTransactions Whether to use transactions (default: true)
 * @return mixed Result of operation
 */
function executeSyncWithTransaction($operation, $useTransactions = true) {
    $db_local = new mysql();
    $db_remote = new mysql();
    $db_remote->connect_remote();
    
    $localTransactionStarted = false;
    $remoteTransactionStarted = false;
    
    try {
        // Start transactions if enabled
        if ($useTransactions) {
            if ($db_local->beginTransaction()) {
                $localTransactionStarted = true;
            }
            if ($db_remote->beginTransaction()) {
                $remoteTransactionStarted = true;
            }
        }
        
        // Execute the operation
        $result = $operation();
        
        // Commit transactions if started
        if ($useTransactions) {
            if ($localTransactionStarted) {
                $db_local->commit();
            }
            if ($remoteTransactionStarted) {
                $db_remote->commit();
            }
        }
        
        return $result;
        
    } catch (Exception $e) {
        // Rollback on error
        if ($useTransactions) {
            if ($localTransactionStarted) {
                try {
                    $db_local->rollback();
                } catch (Exception $rollbackError) {
                    ProgressDisplay::warning("Error rolling back local transaction: " . $rollbackError->getMessage());
                }
            }
            if ($remoteTransactionStarted) {
                try {
                    $db_remote->rollback();
                } catch (Exception $rollbackError) {
                    ProgressDisplay::warning("Error rolling back remote transaction: " . $rollbackError->getMessage());
                }
            }
        }
        
        ProgressDisplay::error("Sync operation failed, transactions rolled back: " . $e->getMessage());
        throw $e;
    }
}



/**
 * Validates and fixes UPDATED_ON field values
 * @param array $data Array of records to validate
 * @return array Array with validated UPDATED_ON fields
 */
function validateAndFixUpdatedOn($data, $table = null) {
    $currentDate = date('Y-m-d H:i:s');
    
    // For icgroup, preserve NULL values - don't convert them
    $preserveNull = ($table === 'ubs_ubsstk2015_icgroup' || $table === 'icgroup');
    
    foreach ($data as &$record) {
        if (isset($record['UPDATED_ON'])) {
            $updatedOn = $record['UPDATED_ON'];
            
            // For icgroup, preserve NULL values
            if ($preserveNull && $updatedOn === null) {
                continue; // Keep NULL as is
            }
            
            // Check if UPDATED_ON is invalid
            if (empty($updatedOn) || 
                $updatedOn === '0000-00-00' || 
                $updatedOn === '0000-00-00 00:00:00' ||
                strtotime($updatedOn) === false ||
                $updatedOn === null) {
                
                ProgressDisplay::info("Invalid UPDATED_ON detected: '$updatedOn' - Converting to current date: $currentDate");
                $record['UPDATED_ON'] = $currentDate;
            }
        }
    }
    
    return $data;
}

/**
 * Check if UBS software is currently running
 * @return bool True if UBS is running, false otherwise
 */
function isUbsRunning() {
    if (PHP_OS_FAMILY !== 'Windows') {
        // UBS is Windows-only software
        return false;
    }
    
    // Common UBS process names
    $ubsProcesses = [
        'UBS.exe',
        'UBSSTK.exe',
        'UBSSTK2015.exe',
        'UBSSTK*.exe',
        'UBS*.exe'
    ];
    
    foreach ($ubsProcesses as $process) {
        $output = [];
        $command = "tasklist /FI \"IMAGENAME eq $process\" 2>NUL";
        exec($command, $output, $returnCode);
        
        // If output has more than just the header, process is running
        if (count($output) > 1) {
            return true;
        }
    }
    
    // Also check for processes containing "UBS" in the name
    $output = [];
    exec("tasklist /FI \"IMAGENAME eq UBS*.exe\" 2>NUL", $output);
    if (count($output) > 1) {
        return true;
    }
    
    return false;
}

/**
 * Check if a DBF file is currently locked/in use
 * @param string $path Path to DBF file
 * @return bool True if file is locked, false otherwise
 */
function isDbfFileLocked($path) {
    if (!file_exists($path)) {
        return false;
    }
    
    // Try to open file in read-write mode to check if it's locked
    $fp = @fopen($path, 'r+');
    if ($fp === false) {
        // File might be locked or doesn't exist
        return true;
    }
    
    // Try to acquire a shared lock (non-blocking)
    $locked = @flock($fp, LOCK_EX | LOCK_NB, $wouldblock);
    
    if ($locked) {
        // We got the lock, release it immediately
        flock($fp, LOCK_UN);
        fclose($fp);
        return false; // File is not locked
    } else {
        fclose($fp);
        return true; // File is locked or would block
    }
}

/**
 * Create a backup of DBF file before writing
 * @param string $path Path to DBF file
 * @return string|false Backup file path on success, false on failure
 */
function backupDbfFile($path) {
    if (!file_exists($path)) {
        ProgressDisplay::warning("Cannot backup: File does not exist: $path");
        return false;
    }
    
    // Create backup directory if it doesn't exist (unique name to avoid conflicts)
    $backupDir = dirname($path) . '/.backup_ubs_local_connector';
    if (!is_dir($backupDir)) {
        if (!@mkdir($backupDir, 0755, true)) {
            ProgressDisplay::warning("Cannot create backup directory: $backupDir");
            return false;
        }
    }
    
    // Generate backup filename with timestamp
    $timestamp = date('YmdHis');
    $basename = basename($path, '.dbf');
    $backupPath = $backupDir . '/' . $basename . '_' . $timestamp . '.dbf';
    
    // Copy the file
    if (!@copy($path, $backupPath)) {
        ProgressDisplay::warning("Failed to create backup: $path -> $backupPath");
        return false;
    }
    
    // Also backup associated files (.fpt, .cdx if they exist)
    $extensions = ['.fpt', '.cdx', '.idx'];
    foreach ($extensions as $ext) {
        $sourceFile = dirname($path) . '/' . basename($path, '.dbf') . $ext;
        if (file_exists($sourceFile)) {
            $backupFile = $backupDir . '/' . $basename . '_' . $timestamp . $ext;
            @copy($sourceFile, $backupFile);
        }
    }
    
    ProgressDisplay::info("✅ Backup created: " . basename($backupPath));
    
    // Clean up old backups (older than 7 days)
    cleanupOldBackups($backupDir);
    
    return $backupPath;
}

/**
 * Clean up backup files older than 7 days
 * @param string $backupDir Directory containing backup files
 */
function cleanupOldBackups($backupDir) {
    if (!is_dir($backupDir)) {
        return;
    }
    
    $daysToKeep = 7;
    $cutoffTime = time() - ($daysToKeep * 24 * 60 * 60); // 7 days ago
    $deletedCount = 0;
    $deletedSize = 0;
    
    try {
        $files = glob($backupDir . '/*');
        if ($files === false) {
            return;
        }
        
        foreach ($files as $file) {
            if (is_file($file)) {
                $fileTime = filemtime($file);
                if ($fileTime !== false && $fileTime < $cutoffTime) {
                    $fileSize = filesize($file);
                    if (@unlink($file)) {
                        $deletedCount++;
                        $deletedSize += $fileSize;
                    }
                }
            }
        }
        
        if ($deletedCount > 0) {
            $deletedSizeMB = round($deletedSize / 1024 / 1024, 2);
            ProgressDisplay::info("🧹 Cleaned up $deletedCount old backup file(s) (older than $daysToKeep days, freed {$deletedSizeMB}MB)");
        }
    } catch (\Throwable $e) {
        // Silently fail - cleanup shouldn't break the main process
        // ProgressDisplay::warning("Warning: Could not clean up old backups: " . $e->getMessage());
    }
}

/**
 * Clean up all backup directories (for all UBS databases)
 * Called at the start of sync operations to clean old backups
 */
function cleanupAllOldBackups() {
    try {
        // Get all possible UBS database directories
        $databases = ['UBSSTK2015', 'UBSACC2015']; // Add more if needed
        $subpath = ENV::DBF_SUBPATH ?? 'DATA';
        
        $totalDeleted = 0;
        $totalFreed = 0;
        
        foreach ($databases as $db) {
            $backupDir = "C:/$db/$subpath/.backup_ubs_local_connector";
            if (is_dir($backupDir)) {
                $daysToKeep = 7;
                $cutoffTime = time() - ($daysToKeep * 24 * 60 * 60);
                
                $files = glob($backupDir . '/*');
                if ($files !== false) {
                    foreach ($files as $file) {
                        if (is_file($file)) {
                            $fileTime = filemtime($file);
                            if ($fileTime !== false && $fileTime < $cutoffTime) {
                                $fileSize = filesize($file);
                                if (@unlink($file)) {
                                    $totalDeleted++;
                                    $totalFreed += $fileSize;
                                }
                            }
                        }
                    }
                }
            }
        }
        
        if ($totalDeleted > 0) {
            $freedMB = round($totalFreed / 1024 / 1024, 2);
            ProgressDisplay::info("🧹 Cleaned up $totalDeleted old backup file(s) (older than 7 days, freed {$freedMB}MB)");
        }
    } catch (\Throwable $e) {
        // Silently fail - cleanup shouldn't break the main process
    }
}

/**
 * Acquire exclusive lock on DBF file
 * @param string $path Path to DBF file
 * @return resource|false File pointer on success, false on failure
 */
function acquireDbfLock($path) {
    $lockFile = $path . '.lock';
    $fp = @fopen($lockFile, 'w');
    
    if (!$fp) {
        ProgressDisplay::warning("Cannot create lock file: $lockFile");
        return false;
    }
    
    // Try to acquire exclusive lock (non-blocking)
    $locked = @flock($fp, LOCK_EX | LOCK_NB, $wouldblock);
    
    if (!$locked) {
        fclose($fp);
        if ($wouldblock) {
            ProgressDisplay::warning("DBF file is locked by another process: " . basename($path));
        } else {
            ProgressDisplay::warning("Cannot acquire lock on: " . basename($path));
        }
        return false;
    }
    
    return $fp; // Return file pointer to maintain lock
}

/**
 * Release lock on DBF file
 * @param resource $fp File pointer from acquireDbfLock()
 * @return void
 */
function releaseDbfLock($fp) {
    if ($fp && is_resource($fp)) {
        @flock($fp, LOCK_UN);
        @fclose($fp);
    }
}

/**
 * Validate DBF file integrity after write
 * @param string $path Path to DBF file
 * @return bool True if file is valid, false otherwise
 */
function validateDbfFile($path) {
    if (!file_exists($path)) {
        return false;
    }
    
    try {
        $testReader = new \XBase\TableReader($path);
        $testReader->close();
        return true;
    } catch (\Throwable $e) {
        ProgressDisplay::warning("DBF validation failed: " . $e->getMessage());
        return false;
    }
}
