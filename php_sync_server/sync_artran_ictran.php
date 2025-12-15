<?php

use XBase\DataConverter\Field\DBase7\TimestampConverter;

include(__DIR__ . '/bootstrap/app.php');
include(__DIR__ . '/bootstrap/cache.php');

// Initialize sync environment and progress display
initializeSyncEnvironment();
ProgressDisplay::start("🚀 Starting FORCE SYNC: artran & ictran (order_date >= 2025-12-01, skipping DO orders) - No sync log checks");

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
    // Minimum order date for sync (matches Python sync date filter: 2025-12-01)
    // Skip records before this date for better performance
    $minOrderDate = getenv('SKIP_BEFORE_DATE') ?: '2025-12-01'; // Default: 2025-12-01
    
    ProgressDisplay::info("📅 Date filtering: Syncing records with order_date >= $minOrderDate");
    ProgressDisplay::info("⏭️  Skipping all DO type orders (DO normally only insert from UBS to server, not synced back)");
    
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
