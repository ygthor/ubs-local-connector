<?php

use XBase\DataConverter\Field\DBase7\TimestampConverter;

include(__DIR__ . '/bootstrap/app.php');
include(__DIR__ . '/bootstrap/cache.php');

// Initialize sync environment and progress display
initializeSyncEnvironment();
ProgressDisplay::start("🚀 Starting icitem & icgroup Sync Process");

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
    // Use the reusable function
    $result = syncIcitemAndIcgroup();
    
    ProgressDisplay::info("📊 Sync Results:");
    ProgressDisplay::info("  - icitem records synced: " . $result['icitem_count']);
    ProgressDisplay::info("  - icgroup records synced: " . $result['icgroup_count']);
    
    // Log successful sync
    $db = new mysql();
    $db->insert('sync_logs', [
        'synced_at' => date('Y-m-d H:i:s')
    ]);
    
} catch (Exception $e) {
    ProgressDisplay::error("❌ Sync failed: " . $e->getMessage());
    releaseSyncLock('php');
    exit(1);
}

// Release lock
releaseSyncLock('php');

ProgressDisplay::complete("🎉 icitem & icgroup sync completed successfully!");
