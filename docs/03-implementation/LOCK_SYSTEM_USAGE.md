# Lock System Usage Guide

## Overview
The lock system prevents Python and PHP syncs from running simultaneously, which could cause data corruption.

## How It Works

### Automatic Protection
Both Python and PHP syncs automatically:
1. Check if the other sync is running before starting
2. Exit with an error message if the other sync is running
3. Create a lock file when starting
4. Remove the lock file when finished (even on errors/crashes)

### Manual Lock Management

#### Check Sync Status (PHP)
```php
$status = getSyncStatus();
print_r($status);
// Returns:
// [
//     'php' => ['running' => true/false, ...],
//     'python' => ['running' => true/false, ...]
// ]
```

#### Check if Sync is Running (PHP)
```php
if (isSyncRunning('python')) {
    echo "Python sync is running";
}
```

#### Force Release Lock (if needed)
```php
releaseSyncLock('php');  // Release PHP lock
releaseSyncLock('python'); // Release Python lock
```

#### Check Sync Status (Python)
```python
from sync_lock import get_sync_status

status = get_sync_status()
print(status)
# Returns:
# {
#     'python': {'running': True/False, ...},
#     'php': {'running': True/False, ...}
# }
```

#### Check if Sync is Running (Python)
```python
from sync_lock import is_sync_running

if is_sync_running('php'):
    print("PHP sync is running")
```

#### Force Release Lock (if needed)
```python
from sync_lock import release_sync_lock

release_sync_lock('python')  # Release Python lock
release_sync_lock('php')     # Release PHP lock
```

## Lock Files Location
```
ubs-local-connector/
└── php_sync_server/
    └── locks/
        ├── php_sync.lock      # PHP sync lock file
        ├── php_sync.pid        # PHP sync process ID
        ├── python_sync.lock    # Python sync lock file
        └── python_sync.pid     # Python sync process ID
```

## Troubleshooting

### Stale Lock Files
If a sync crashes, lock files might remain. The system automatically detects and removes stale locks, but you can manually remove them:

```bash
# Remove all lock files
rm php_sync_server/locks/*.lock
rm php_sync_server/locks/*.pid
```

### Lock File Not Released
If a lock file is not released after a sync completes:
1. Check if the process is still running: `ps aux | grep python` or `ps aux | grep php`
2. Kill the process if needed: `kill <pid>`
3. Manually remove lock files (see above)

### Permission Issues
If you get permission errors:
```bash
# Make sure locks directory is writable
chmod 755 php_sync_server/locks
```

## Error Messages

### "PHP sync is currently running"
- **Meaning**: PHP sync is already running
- **Solution**: Wait for PHP sync to complete, or check if it's stuck and kill the process

### "Python sync is currently running"
- **Meaning**: Python sync is already running
- **Solution**: Wait for Python sync to complete, or check if it's stuck and kill the process

### "Lock file exists"
- **Meaning**: Lock file exists but process is not running (stale lock)
- **Solution**: System should auto-remove, but you can manually delete lock files

## Best Practices

1. **Don't manually delete lock files** while syncs are running
2. **Check sync status** before manually removing lock files
3. **Monitor sync logs** for lock-related warnings
4. **Use the provided functions** instead of manually manipulating lock files

## Testing

### Test Lock System
```bash
# Terminal 1: Start PHP sync
php php_sync_server/main.php

# Terminal 2: Try to start Python sync (should fail)
python python_sync_local/main.py
# Expected: "❌ PHP sync is currently running"
```

### Test Stale Lock Detection
```bash
# Create a fake lock file
echo "2025-01-15 10:30:45" > php_sync_server/locks/php_sync.lock
echo "99999" > php_sync_server/locks/php_sync.pid

# Try to start PHP sync (should detect stale lock and remove it)
php php_sync_server/main.php
# Expected: Should start successfully after removing stale lock
```
