# DBF Corruption Analysis & Recommendations

## Critical Issues Found

### 1. **No File Locking on DBF Files** ⚠️ HIGH RISK
**Location:** `functions.php::batchUpsertUbs()`

**Problem:**
- The code uses application-level lock files to prevent concurrent PHP syncs
- **BUT** it does NOT use OS-level file locking (`flock`) on the actual DBF files
- If UBS software is running and accessing the same DBF files simultaneously, concurrent writes can corrupt the files

**Current Code:**
```php
$editor = new \XBase\TableEditor($path, [
    'editMode' => $editMode,
]);
// No file locking here!
```

**Risk:** When UBS software writes to a DBF file while PHP sync is also writing, the file structure can become corrupted.

---

### 2. **No Check if UBS Software is Running** ⚠️ HIGH RISK
**Location:** `main.php`, `functions.php::batchUpsertUbs()`

**Problem:**
- Code doesn't verify if UBS software is currently running before writing to DBF files
- UBS software typically locks DBF files when open, but PHP might still be able to write, causing corruption

**Risk:** Writing to DBF files while UBS is open can cause corruption.

---

### 3. **EDIT_MODE_REALTIME Fallback** ⚠️ MEDIUM RISK
**Location:** `functions.php::batchUpsertUbs()` lines 424-428, 537-541, 642-646

**Problem:**
- When `EDIT_MODE_CLONE` fails, code falls back to `EDIT_MODE_REALTIME`
- `REALTIME` mode writes **directly** to the DBF file without creating a temporary copy
- This is dangerous if UBS is also accessing the file

**Current Code:**
```php
if ($editMode === \XBase\TableEditor::EDIT_MODE_CLONE) {
    ProgressDisplay::warning("Clone mode failed, trying realtime mode");
    $editMode = \XBase\TableEditor::EDIT_MODE_REALTIME;
    // ⚠️ This writes directly to file - risky!
}
```

**Risk:** Direct writes without atomic operations can corrupt files if interrupted.

---

### 4. **Multiple TableEditor Instances on Same File** ⚠️ MEDIUM RISK
**Location:** `functions.php::batchUpsertUbs()` lines 489-552, 611-657

**Problem:**
- For each batch, a new `TableEditor` is opened on the same DBF file
- While editors are closed between batches, there's still a window where corruption could occur
- No exclusive locking ensures only one process accesses the file at a time

**Current Code:**
```php
for ($i = 0; $i < count($updateRecords); $i += $batchSize) {
    $batch = array_slice($updateRecords, $i, $batchSize);
    // Opens new editor for each batch
    $batchEditor = new \XBase\TableEditor($path, [
        'editMode' => $editMode,
    ]);
    // ... process batch ...
    $batchEditor->close();
}
```

**Risk:** Multiple open/close cycles increase chance of file corruption.

---

### 5. **No Atomic Writes for DBF Files** ⚠️ MEDIUM RISK
**Location:** `functions.php::batchUpsertUbs()`

**Problem:**
- DBF files don't support transactions like databases
- If `save()` fails after modifications, the file can be left in a corrupted state
- No rollback mechanism for DBF operations

**Current Code:**
```php
if ($editMode === \XBase\TableEditor::EDIT_MODE_CLONE) {
    $batchEditor->save(); // If this fails, file might be corrupted
}
```

**Risk:** Partial writes can corrupt the DBF file structure.

---

### 6. **Error Handling During Save** ⚠️ MEDIUM RISK
**Location:** `functions.php::batchUpsertUbs()` lines 521-523, 626-628

**Problem:**
- If `save()` throws an exception after records are modified, the file might be left in an inconsistent state
- The catch block closes the editor, but modifications might already be partially written

**Risk:** Exception during save can leave file in corrupted state.

---

### 7. **Insufficient Delays Between Operations** ⚠️ LOW RISK
**Location:** `main.php` lines 343, 414

**Problem:**
- Small delays (`usleep(100000)`, `usleep(500000)`) help but don't guarantee exclusive access
- These delays are between chunks/tables, not between individual DBF operations

**Current Code:**
```php
usleep(100000); // 0.1 second delay
usleep(500000); // 0.5 second delay
```

**Risk:** Not sufficient to prevent concurrent access issues.

---

## Recommendations

### Priority 1: Add File Locking (CRITICAL)

Add OS-level file locking using `flock()` before opening DBF files:

```php
function acquireDbfLock($path) {
    $lockFile = $path . '.lock';
    $fp = fopen($lockFile, 'w');
    if (!$fp) {
        throw new Exception("Cannot create lock file: $lockFile");
    }
    
    // Try to acquire exclusive lock (non-blocking)
    if (!flock($fp, LOCK_EX | LOCK_NB, $wouldblock)) {
        if ($wouldblock) {
            fclose($fp);
            throw new Exception("DBF file is locked by another process: $path");
        }
        fclose($fp);
        throw new Exception("Cannot acquire lock on: $path");
    }
    
    return $fp; // Return file pointer to maintain lock
}

function releaseDbfLock($fp) {
    if ($fp) {
        flock($fp, LOCK_UN);
        fclose($fp);
    }
}
```

### Priority 2: Check if UBS is Running

Add a function to check if UBS processes are running:

```php
function isUbsRunning() {
    // Check for common UBS process names
    $ubsProcesses = ['UBS.exe', 'UBSSTK.exe', 'UBSSTK2015.exe'];
    
    foreach ($ubsProcesses as $process) {
        $output = [];
        exec("tasklist /FI \"IMAGENAME eq $process\" 2>NUL", $output);
        if (count($output) > 1) { // More than header line means process found
            return true;
        }
    }
    return false;
}
```

### Priority 3: Always Use CLONE Mode

**Never fallback to REALTIME mode.** If CLONE mode fails, abort the operation:

```php
// Remove REALTIME fallback - always use CLONE mode
$editor = new \XBase\TableEditor($path, [
    'editMode' => \XBase\TableEditor::EDIT_MODE_CLONE, // Only safe mode
]);

// If CLONE fails, don't fallback - abort instead
if (!$editor) {
    throw new Exception("Cannot open DBF file in CLONE mode. UBS may be running or file is locked.");
}
```

### Priority 4: Add Backup Before Write

Create a backup before writing:

```php
function backupDbfFile($path) {
    $backupPath = $path . '.backup.' . date('YmdHis');
    if (!copy($path, $backupPath)) {
        throw new Exception("Cannot create backup: $path");
    }
    return $backupPath;
}
```

### Priority 5: Validate File After Write

Add validation after save:

```php
function validateDbfFile($path) {
    try {
        $testReader = new \XBase\TableReader($path);
        $testReader->close();
        return true;
    } catch (\Throwable $e) {
        return false;
    }
}
```

### Priority 6: Improve Error Handling

Wrap save operations with better error handling:

```php
try {
    if ($editMode === \XBase\TableEditor::EDIT_MODE_CLONE) {
        $batchEditor->save();
        
        // Validate after save
        if (!validateDbfFile($path)) {
            throw new Exception("DBF file validation failed after save");
        }
    }
    $batchEditor->close();
} catch (\Throwable $e) {
    // If save failed, try to restore from backup
    if (isset($backupPath) && file_exists($backupPath)) {
        copy($backupPath, $path);
        ProgressDisplay::warning("Restored DBF file from backup due to save error");
    }
    throw $e;
}
```

---

## Implementation Priority

1. **IMMEDIATE:** Add file locking (Priority 1)
2. **IMMEDIATE:** Check if UBS is running (Priority 2)
3. **HIGH:** Remove REALTIME fallback (Priority 3)
4. **MEDIUM:** Add backup before write (Priority 4)
5. **MEDIUM:** Add validation after write (Priority 5)
6. **LOW:** Improve error handling (Priority 6)

---

## Testing Recommendations

1. Test with UBS software running
2. Test with UBS software closed
3. Test with multiple PHP sync processes (should be prevented by lock files)
4. Test with interrupted writes (kill process during save)
5. Test with corrupted files (verify repair mechanisms work)

---

## Additional Notes

- The `executeSyncWithTransaction()` function only handles MySQL transactions, not DBF operations
- DBF files are binary and don't support transactions
- Always prefer CLONE mode over REALTIME mode for safety
- Consider adding a "maintenance mode" that prevents UBS from accessing files during sync
