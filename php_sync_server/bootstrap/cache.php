<?php

// Simple cache system for sync progress tracking
class SyncCache
{
    private static $cacheFile = __DIR__ . '/../cache/sync_progress.json';
    private static $lockFile = __DIR__ . '/../cache/sync.lock';
    
    public static function init()
    {
        // Create cache directory if it doesn't exist
        $cacheDir = dirname(self::$cacheFile);
        if (!is_dir($cacheDir)) {
            mkdir($cacheDir, 0755, true);
        }
    }
    
    public static function start($table, $totalRecords)
    {
        self::init();
        
        $cache = [
            'table' => $table,
            'total_records' => $totalRecords,
            'processed_records' => 0,
            'current_offset' => 0,
            'started_at' => date('Y-m-d H:i:s'),
            'last_updated' => date('Y-m-d H:i:s'),
            'status' => 'running'
        ];
        
        file_put_contents(self::$cacheFile, json_encode($cache, JSON_PRETTY_PRINT));
        file_put_contents(self::$lockFile, getmypid());
    }
    
    public static function update($processedRecords, $currentOffset)
    {
        if (!file_exists(self::$cacheFile)) {
            return;
        }
        
        $cache = json_decode(file_get_contents(self::$cacheFile), true);
        if ($cache) {
            $cache['processed_records'] = $processedRecords;
            $cache['current_offset'] = $currentOffset;
            $cache['last_updated'] = date('Y-m-d H:i:s');
            
            file_put_contents(self::$cacheFile, json_encode($cache, JSON_PRETTY_PRINT));
        }
    }
    
    public static function complete()
    {
        if (file_exists(self::$cacheFile)) {
            unlink(self::$cacheFile);
        }
        if (file_exists(self::$lockFile)) {
            unlink(self::$lockFile);
        }
    }
    
    public static function getProgress()
    {
        if (!file_exists(self::$cacheFile)) {
            return null;
        }
        
        $cache = json_decode(file_get_contents(self::$cacheFile), true);
        return $cache;
    }
    
    public static function isRunning()
    {
        if (!file_exists(self::$lockFile)) {
            return false;
        }
        
        $pid = file_get_contents(self::$lockFile);
        if (!$pid) {
            return false;
        }
        
        // Check if process is still running (Windows)
        $result = shell_exec("tasklist /FI \"PID eq $pid\" 2>NUL");
        return strpos($result, $pid) !== false;
    }
    
    public static function resume()
    {
        $progress = self::getProgress();
        if (!$progress || self::isRunning()) {
            return null;
        }
        
        // Clean up stale lock file
        if (file_exists(self::$lockFile)) {
            unlink(self::$lockFile);
        }
        
        return $progress;
    }
    
    public static function clear()
    {
        self::complete();
    }
}

// Cache helper functions
function startSyncCache($table, $totalRecords)
{
    SyncCache::start($table, $totalRecords);
}

function updateSyncCache($processedRecords, $currentOffset)
{
    SyncCache::update($processedRecords, $currentOffset);
}

function completeSyncCache()
{
    SyncCache::complete();
}

function getSyncProgress()
{
    return SyncCache::getProgress();
}

function canResumeSync()
{
    return SyncCache::resume();
}

function clearSyncCache()
{
    SyncCache::clear();
}
