<?php

use XBase\DataConverter\Field\DBase7\TimestampConverter;

include(__DIR__ . '/bootstrap/app.php');
include(__DIR__ . '/bootstrap/cache.php');

// Initialize sync environment and progress display
initializeSyncEnvironment();
ProgressDisplay::start("🚀 Starting FORCE SYNC: artran & ictran (order_date >= 2025-12-12) - No sync log checks");

// Check if Python sync is running
if (isSyncRunning('python')) {
    ProgressDisplay::error("❌ Python sync is currently running. Please wait for it to complete.");
    exit(1);
}

// Acquire PHP sync lock
if (!acquireSyncLock('php')) {
    ProgressDisplay::error("❌ PHP sync is already running or lock file exists. Please check and remove lock file if needed.");
    exit(1);
}

// Register shutdown function to release lock
register_shutdown_function(function() {
    releaseSyncLock('php');
});

try {
    // Minimum order date for sync
    $minOrderDate = '2025-12-15';
    
    // Use the reusable function
    $result = syncArtranAndIctran(null, null, $minOrderDate);
    
    ProgressDisplay::info("📊 Sync Results:");
    ProgressDisplay::info("  - artran (orders) records synced: " . $result['artran_count']);
    ProgressDisplay::info("  - ictran (order_items) records synced: " . $result['ictran_count']);
    
    if ($result['error']) {
        ProgressDisplay::error("  - Error: " . $result['error']);
    }
    
    
} catch (Exception $e) {
    ProgressDisplay::error("❌ Sync failed: " . $e->getMessage());
    releaseSyncLock('php');
    exit(1);
}

// Release lock
releaseSyncLock('php');

ProgressDisplay::complete("🎉 artran & ictran sync completed successfully!");
