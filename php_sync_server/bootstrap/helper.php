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
 * @param array $keys Array of primary keys to fetch
 * @param string $updatedAfter Optional timestamp filter
 * @return array Remote data
 */
function fetchRemoteDataByKeys($table, $keys, $updatedAfter = null) {
    if (empty($keys)) {
        return [];
    }
    
    try {
        $db = new mysql();
        $db->connect_remote();
        
        $remote_table_name = Converter::table_convert_remote($table);
        $remote_key = Converter::primaryKey($remote_table_name);
        $column_updated_at = Converter::mapUpdatedAtField($remote_table_name);
        
        // Build safe IN clause
        $escaped_keys = array_map(function($key) use ($db) {
            return "'" . $db->escape($key) . "'";
        }, $keys);
        
        $keys_str = implode(',', $escaped_keys);
        
        $sql = "SELECT * FROM `$remote_table_name` WHERE `$remote_key` IN ($keys_str)";
        
        if ($updatedAfter) {
            $sql .= " AND `$column_updated_at` > '" . $db->escape($updatedAfter) . "'";
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
