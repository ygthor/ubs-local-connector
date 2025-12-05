# UBS Local Connector - Sync Flow Analysis

## Overview
The sync system has **two main components**:

1. **Python Sync Local** (`python_sync_local/`) - Reads UBS DBF files → Local MySQL
2. **PHP Sync Server** (`php_sync_server/`) - Syncs Local MySQL ↔ Remote MySQL

---

## Flow Diagram

```
┌─────────────────┐
│  UBS DBF Files  │
│ (C:/UBSACC2015/ │
│  C:/UBSSTK2015/)│
└────────┬────────┘
         │
         │ Python sync_local
         │ (main.py)
         │
         ▼
┌─────────────────┐
│  Local MySQL    │
│ (ubs_ubsacc2015_│
│  ubs_ubsstk2015_)│
└────────┬────────┘
         │
         │ PHP sync_server
         │ (main.php)
         │
         ▼
┌─────────────────┐
│  Remote MySQL   │
│  (Laravel DB)   │
└─────────────────┘
```

---

## Step 1: Python Sync Local (DBF → Local MySQL)

**File**: `python_sync_local/main.py`

**What it does**:
- Reads DBF files from UBS system directories
- Converts DBF structure to MySQL schema
- Inserts/truncates data into local MySQL tables
- Tables are prefixed: `ubs_ubsacc2015_*` or `ubs_ubsstk2015_*`

**Process**:
1. Reads all DBF files from configured directories
2. For each file:
   - Reads structure and rows
   - Truncates existing table
   - Creates table if not exists
   - Inserts data in chunks (1000 records at a time)

**Status**: ✅ **OK to use** - Handles large datasets, has retry logic, chunking

---

## Step 2: PHP Sync Server (Local MySQL ↔ Remote MySQL)

### Mode 1: Initial Sync (`main_init.php`)

**What it does**:
- One-way sync: Local MySQL → Remote MySQL
- Truncates remote tables first
- Reads ALL data from local MySQL (where UPDATED_ON IS NOT NULL)
- Syncs everything to remote

**Use case**: First-time setup or full reset

**Status**: ✅ **OK to use** - Simple one-way sync, handles chunking

---

### Mode 2: Incremental Sync (`main.php`) ⚠️ **HAS ISSUES**

**What it does**:
- Two-way sync based on `UPDATED_ON` timestamp
- Compares Local MySQL vs Remote MySQL
- Syncs newer data to older site

**Current Flow**:
1. Get `last_synced_at` from `sync_logs` table
2. For each table:
   - Fetch UBS data from local MySQL (WHERE UPDATED_ON > last_synced_at)
   - **Fetch remote data ONCE** (WHERE updated_at >= last_synced_at)
   - Process UBS data in chunks (500 records)
   - For each chunk:
     - Compare with remote data
     - Sync newer records

**Issues Found**:

### ❌ Issue 1: Remote Data Fetched Once Per Table
**Location**: `main.php` line 83

```php
$remote_data = fetchServerData($ubs_table, $last_synced_at);
// This is fetched ONCE, then used for ALL chunks
```

**Problem**:
- Remote data is loaded into memory once per table
- For large tables, this can cause memory issues
- If remote data changes during processing, changes won't be reflected

**Impact**: Medium - Could cause memory issues with very large datasets

### ❌ Issue 2: Inefficient Comparison for Large Datasets
**Location**: `functions.php` line 881-991 (`syncEntity`)

**Problem**:
- All remote data is loaded into memory
- All UBS data (per chunk) is loaded into memory
- Creates key-based arrays for comparison
- For tables with 100k+ records, this uses significant memory

**Impact**: Medium - Memory usage could be optimized

### ✅ Issue 3: Timestamp Comparison Logic is CORRECT
**Location**: `functions.php` lines 944-977

**Status**: ✅ **CORRECT**
- Validates UPDATED_ON fields
- Handles invalid dates (sets to 1970-01-01 or current date)
- Compares timestamps correctly
- Syncs newer to older: ✅ **CORRECT**

```php
if ($ubs_time > $remote_time) {
    // UBS is newer → sync to remote
    $sync['remote_data'][] = convert($remote_table_name, $ubs, 'to_remote');
} elseif ($remote_time > $ubs_time) {
    // Remote is newer → sync to UBS
    $sync['ubs_data'][] = convert($remote_table_name, $remote, 'to_ubs');
}
```

---

## Sync Logic Analysis

### The `syncEntity` Function

**Location**: `functions.php:881-991`

**Logic Flow**:
1. Create key-based arrays for UBS and remote data
2. Get all unique keys from both sources
3. For each key:
   - **Only UBS exists** → Sync to remote
   - **Only Remote exists** → Sync to UBS
   - **Both exist** → Compare `UPDATED_ON` timestamps:
     - If UBS is newer → Sync UBS to remote
     - If Remote is newer → Sync Remote to UBS
     - If equal → No sync (both already in sync)

**Status**: ✅ **LOGIC IS CORRECT**

The comparison logic correctly:
- Handles missing records (one-way sync)
- Compares timestamps to determine which is newer
- Syncs newer data to older site

---

## Recommendations

### ✅ Current Code Status: **MOSTLY OK TO USE**

The sync logic is **correct**, but there are **optimization opportunities**:

### 🔧 Recommended Improvements:

1. **Optimize Remote Data Fetching** (High Priority)
   - Instead of fetching all remote data once, fetch in chunks
   - Or use a more efficient comparison method (e.g., database-level comparison)

2. **Add Indexes** (High Priority)
   - Ensure `UPDATED_ON` columns are indexed in both local and remote MySQL
   - Ensure primary keys are indexed

3. **Memory Optimization** (Medium Priority)
   - For very large tables, consider streaming comparison instead of loading all into memory
   - Add memory monitoring and warnings

4. **Error Handling** (Medium Priority)
   - Add retry logic for failed syncs
   - Add logging for sync conflicts

5. **Performance Monitoring** (Low Priority)
   - Add metrics for sync duration
   - Track sync success/failure rates

---

## Testing Recommendations

Before using in production:

1. **Test with small dataset** (100-1000 records)
2. **Test timestamp comparison**:
   - UBS newer than remote
   - Remote newer than UBS
   - Both equal
   - Invalid timestamps
3. **Test memory usage** with large tables (10k+ records)
4. **Test concurrent updates** (what happens if data changes during sync?)

---

## Summary

| Component | Status | Notes |
|-----------|--------|-------|
| Python Sync Local | ✅ OK | Handles large datasets well |
| PHP Initial Sync | ✅ OK | Simple one-way sync |
| PHP Incremental Sync | ⚠️ OK with caveats | Logic correct, but could be optimized |
| Timestamp Comparison | ✅ CORRECT | Properly compares and syncs newer to older |
| Memory Management | ⚠️ Could be better | Works but could be optimized for very large datasets |

**Overall**: The code is **OK to use** for the described flow**. The sync logic is correct and will properly compare timestamps and sync newer data to older sites. However, for production use with very large datasets, consider the optimization recommendations above.
