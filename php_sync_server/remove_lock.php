<?php

/**
 * Remove Sync Lock Script
 * 
 * This script removes sync lock files to allow sync processes to run again.
 * Useful when a sync process crashes or is killed, leaving stale lock files.
 * 
 * Usage: php remove_lock.php [lock_type]
 * 
 * Options:
 *   lock_type  : 'php' or 'python' (default: 'php')
 *                Use 'all' to remove all locks
 * 
 * Examples:
 *   php remove_lock.php          # Remove PHP lock
 *   php remove_lock.php php       # Remove PHP lock
 *   php remove_lock.php python    # Remove Python lock
 *   php remove_lock.php all       # Remove all locks
 */

include(__DIR__ . '/bootstrap/app.php');
include(__DIR__ . '/bootstrap/cache.php');

// Initialize sync environment
initializeSyncEnvironment();

// Get lock type from command line argument
$lockType = $argv[1] ?? 'php';

$lockDir = __DIR__ . '/locks';

// Check if locks directory exists
if (!is_dir($lockDir)) {
    echo "✅ Locks directory does not exist. No locks to remove.\n";
    exit(0);
}

// Function to remove lock for a specific type
function removeLock($lockType, $lockDir) {
    $lockFile = $lockDir . '/' . $lockType . '_sync.lock';
    $pidFile = $lockDir . '/' . $lockType . '_sync.pid';
    
    $removed = false;
    $info = [];
    
    // Check and remove lock file
    if (file_exists($lockFile)) {
        $lockTime = file_get_contents($lockFile);
        $info[] = "Lock created: $lockTime";
        if (@unlink($lockFile)) {
            $info[] = "✅ Removed lock file: $lockFile";
            $removed = true;
        } else {
            $info[] = "❌ Failed to remove lock file: $lockFile";
        }
    } else {
        $info[] = "ℹ️  Lock file does not exist: $lockFile";
    }
    
    // Check and remove PID file
    if (file_exists($pidFile)) {
        $pid = file_get_contents($pidFile);
        $info[] = "PID: $pid";
        
        // Check if process is still running
        if ($pid) {
            if (function_exists('isProcessRunning')) {
                if (isProcessRunning($pid)) {
                    $info[] = "⚠️  WARNING: Process $pid is still running! Lock may be recreated.";
                } else {
                    $info[] = "Process $pid is not running";
                }
            } else {
                $info[] = "⚠️  Note: Cannot verify if process $pid is running (isProcessRunning function not available)";
            }
        }
        
        if (@unlink($pidFile)) {
            $info[] = "✅ Removed PID file: $pidFile";
            $removed = true;
        } else {
            $info[] = "❌ Failed to remove PID file: $pidFile";
        }
    } else {
        $info[] = "ℹ️  PID file does not exist: $pidFile";
    }
    
    return ['removed' => $removed, 'info' => $info];
}

// Process based on lock type
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n";
echo "🔓 Removing Sync Lock(s)\n";
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n";

if ($lockType === 'all') {
    // Remove all locks
    $lockTypes = ['php', 'python'];
    $anyRemoved = false;
    
    foreach ($lockTypes as $type) {
        echo "📦 Processing $type lock...\n";
        $result = removeLock($type, $lockDir);
        
        foreach ($result['info'] as $line) {
            echo "   $line\n";
        }
        
        if ($result['removed']) {
            $anyRemoved = true;
        }
        echo "\n";
    }
    
    if ($anyRemoved) {
        echo "✅ All locks processed. Sync processes can now run.\n";
    } else {
        echo "ℹ️  No locks were found or removed.\n";
    }
} else {
    // Validate lock type
    $validTypes = ['php', 'python'];
    if (!in_array($lockType, $validTypes)) {
        echo "❌ Error: Invalid lock type '$lockType'\n";
        echo "Valid types: " . implode(', ', $validTypes) . ", or 'all'\n";
        exit(1);
    }
    
    // Remove specific lock
    echo "📦 Processing $lockType lock...\n";
    $result = removeLock($lockType, $lockDir);
    
    foreach ($result['info'] as $line) {
        echo "   $line\n";
    }
    
    if ($result['removed']) {
        echo "\n✅ Lock removed successfully. $lockType sync can now run.\n";
    } else {
        echo "\nℹ️  No lock files found for $lockType. Sync may already be available.\n";
    }
}

echo "\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n";







