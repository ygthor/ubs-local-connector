# Automatic Backup System - How It Works

## 🔄 Backup Flow

```
┌─────────────────────────────────────────────────────────┐
│  batchUpsertUbs() called                                │
└─────────────────┬───────────────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────────────────┐
│  1. Check if UBS is running                             │
│     → If YES: Abort (no backup needed)                  │
└─────────────────┬───────────────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────────────────┐
│  2. Check if DBF file is locked                         │
│     → If YES: Abort (no backup needed)                  │
└─────────────────┬───────────────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────────────────┐
│  3. ✅ CREATE BACKUP (backupDbfFile())                  │
│     ├─ Create .backups/ directory if needed             │
│     ├─ Copy .dbf file                                   │
│     ├─ Copy .fpt file (if exists)                      │
│     ├─ Copy .cdx file (if exists)                      │
│     └─ Copy .idx file (if exists)                       │
└─────────────────┬───────────────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────────────────┐
│  4. Acquire file lock                                   │
└─────────────────┬───────────────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────────────────┐
│  5. Perform DBF write operations                        │
│     ├─ Update records                                   │
│     ├─ Insert records                                  │
│     └─ Validate after save                              │
└─────────────────┬───────────────────────────────────────┘
                  │
        ┌─────────┴─────────┐
        │                   │
        ▼                   ▼
   SUCCESS              ERROR
        │                   │
        │                   ▼
        │         ┌─────────────────────────┐
        │         │ Restore from backup     │
        │         │ copy($backupPath, $path)│
        │         └─────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────────────────┐
│  6. Release file lock (always, even on error)           │
└─────────────────────────────────────────────────────────┘
```

## 📁 Backup Location

### Original File:
```
C:/UBSSTK2015/DBF/icitem.dbf
```

### Backup Directory:
```
C:/UBSSTK2015/DBF/.backups/
```

### Backup Files Created:
```
.backups/
  ├── icitem_20250115143025.dbf    ← Main DBF file
  ├── icitem_20250115143025.fpt    ← Memo file (if exists)
  ├── icitem_20250115143025.cdx    ← Index file (if exists)
  └── icitem_20250115143025.idx    ← Index file (if exists)
```

**Filename Format:** `{table_name}_{YYYYMMDDHHMMSS}.{ext}`

## 🔧 How It Works (Step by Step)

### Step 1: Backup Function Called
```php
// In batchUpsertUbs(), before any write operations:
$backupPath = backupDbfFile($path);
```

### Step 2: Create Backup Directory
```php
$backupDir = dirname($path) . '/.backups';
// Example: C:/UBSSTK2015/DBF/.backups

if (!is_dir($backupDir)) {
    mkdir($backupDir, 0755, true);  // Create if doesn't exist
}
```

### Step 3: Generate Timestamped Filename
```php
$timestamp = date('YmdHis');  // e.g., "20250115143025"
$basename = basename($path, '.dbf');  // e.g., "icitem"
$backupPath = $backupDir . '/' . $basename . '_' . $timestamp . '.dbf';
// Result: C:/UBSSTK2015/DBF/.backups/icitem_20250115143025.dbf
```

### Step 4: Copy Main DBF File
```php
copy($path, $backupPath);  // Copy original to backup location
```

### Step 5: Copy Associated Files
```php
$extensions = ['.fpt', '.cdx', '.idx'];
foreach ($extensions as $ext) {
    $sourceFile = dirname($path) . '/' . basename($path, '.dbf') . $ext;
    if (file_exists($sourceFile)) {
        $backupFile = $backupDir . '/' . $basename . '_' . $timestamp . $ext;
        copy($sourceFile, $backupFile);
    }
}
```

## 🔄 Automatic Restoration

If an error occurs during write operations:

```php
catch (\Throwable $e) {
    // If error occurred and we have a backup, restore it
    if ($backupPath !== null && file_exists($backupPath)) {
        ProgressDisplay::warning("⚠️  Error occurred during DBF write. Attempting to restore from backup...");
        try {
            copy($backupPath, $path);  // Restore original file
            ProgressDisplay::info("✅ DBF file restored from backup");
        } catch (\Throwable $restoreError) {
            ProgressDisplay::error("❌ Failed to restore DBF file from backup");
        }
    }
    throw $e;  // Re-throw original exception
}
```

## 📊 Example Scenario

### Before Write:
```
C:/UBSSTK2015/DBF/
  ├── icitem.dbf    (original, 2MB)
  ├── icitem.fpt    (memo file, 500KB)
  └── icitem.cdx    (index file, 100KB)
```

### After Backup Created:
```
C:/UBSSTK2015/DBF/
  ├── icitem.dbf    (original)
  ├── icitem.fpt    (original)
  ├── icitem.cdx    (original)
  └── .backups/
      ├── icitem_20250115143025.dbf    (backup copy)
      ├── icitem_20250115143025.fpt    (backup copy)
      └── icitem_20250115143025.cdx    (backup copy)
```

### If Error Occurs:
```
1. Error during write → Catch block triggered
2. Check if backup exists → YES
3. Restore: copy(backup, original)
4. Original file restored to pre-write state
5. Exception re-thrown (operation failed, but file is safe)
```

## ⚙️ Configuration

### Backup Directory:
- **Location:** `{DBF_DIRECTORY}/.backups/`
- **Permissions:** `0755` (readable/writable by owner, readable by others)
- **Auto-created:** Yes, if doesn't exist

### Backup Naming:
- **Format:** `{table_name}_{YYYYMMDDHHMMSS}.{ext}`
- **Example:** `icitem_20250115143025.dbf`
- **Timestamp:** Current date/time when backup is created

### Files Backed Up:
1. ✅ `.dbf` - Main database file (always)
2. ✅ `.fpt` - Memo file (if exists)
3. ✅ `.cdx` - Compound index file (if exists)
4. ✅ `.idx` - Index file (if exists)

## 🛡️ Safety Features

1. **Non-blocking:** If backup fails, operation continues with warning
2. **Automatic:** No manual intervention needed
3. **Complete:** Backs up all associated files
4. **Timestamped:** Each backup has unique name (no overwrites)
5. **Auto-restore:** Automatically restores if error occurs

## 💾 Disk Space Considerations

- **Each backup:** ~Size of original file(s)
- **Multiple backups:** Each sync creates new backup (not overwritten)
- **Cleanup:** Manual cleanup recommended (not automatic)

### Example:
If `icitem.dbf` is 2MB and you sync 10 times:
- 10 backups × 2MB = ~20MB in `.backups/` directory

**Recommendation:** Periodically clean old backups or implement auto-cleanup.

## ✅ Benefits

1. **Automatic:** No manual backup needed
2. **Complete:** All related files backed up
3. **Safe:** Original file restored if error occurs
4. **Transparent:** Works silently in background
5. **Reliable:** Uses PHP's native `copy()` function
