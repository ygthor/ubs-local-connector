# Sync Method Improvements

## Current Issues & Optimization Opportunities

### 🔴 Critical Improvements

#### 1. **Remote Data Fetching Strategy** (High Priority)
**Current Issue**: Remote data is fetched once per table and kept in memory for all chunks. For large tables (10k+ records), this:
- Uses significant memory
- May miss updates that happen during sync
- Compares stale remote data with fresh UBS data

**Improvement**: Fetch remote data incrementally or use database-level comparison
```php
// Option A: Fetch remote data in chunks matching UBS chunks
// Option B: Use database-level comparison with JOIN queries
// Option C: Fetch only records that match UBS keys being processed
```

**Impact**: High - Reduces memory usage and improves accuracy

---

#### 2. **Database-Level Comparison** (High Priority)
**Current Issue**: All data is loaded into memory for comparison. For very large tables, this is inefficient.

**Improvement**: Use SQL JOIN queries to compare timestamps at database level
```sql
-- Instead of loading all data, use:
SELECT 
    ubs.*, 
    remote.*,
    CASE 
        WHEN ubs.UPDATED_ON > remote.updated_at THEN 'ubs_newer'
        WHEN remote.updated_at > ubs.UPDATED_ON THEN 'remote_newer'
        ELSE 'equal'
    END as sync_direction
FROM ubs_table ubs
LEFT JOIN remote_table remote ON ubs.key = remote.key
WHERE ubs.UPDATED_ON > '$last_synced_at' 
   OR remote.updated_at > '$last_synced_at'
```

**Impact**: High - Much faster for large datasets, reduces memory usage

---

#### 3. **Transaction Handling** (High Priority)
**Current Issue**: No transaction handling. If sync fails partway, data might be inconsistent.

**Improvement**: Wrap sync operations in transactions
```php
// Start transaction
$db->beginTransaction();
try {
    // Sync operations
    batchUpsertRemote(...);
    batchUpsertUbs(...);
    updateLocalMySQL(...);
    
    // Commit only if all succeed
    $db->commit();
} catch (Exception $e) {
    $db->rollback();
    throw $e;
}
```

**Impact**: High - Ensures data consistency

---

### 🟡 Important Improvements

#### 4. **Conflict Resolution Logging** (Medium Priority)
**Current Issue**: When timestamps are equal, nothing happens. No logging of conflicts.

**Improvement**: Log conflicts and provide resolution options
```php
if ($ubs_time == $remote_time) {
    // Log conflict
    logSyncConflict($table, $key, $ubs_updated_on);
    
    // Option: Use a tie-breaker (e.g., prefer remote, or manual review)
    // For now, skip sync
}
```

**Impact**: Medium - Better visibility into sync issues

---

#### 5. **Incremental Remote Data Fetching** (Medium Priority)
**Current Issue**: All remote data is fetched at once, even if only a few UBS records changed.

**Improvement**: Fetch remote data in chunks or only for matching keys
```php
// Instead of fetching all remote data:
// 1. Get UBS keys from current chunk
// 2. Fetch only matching remote records
$ubs_keys = array_column($ubs_data, $ubs_key);
$remote_data = fetchRemoteDataByKeys($table, $ubs_keys);
```

**Impact**: Medium - Reduces memory and improves performance

---

#### 6. **Index Verification** (Medium Priority)
**Current Issue**: No verification that indexes exist on UPDATED_ON and primary keys.

**Improvement**: Check and create indexes if missing
```php
function ensureSyncIndexes($table) {
    // Check if index exists on UPDATED_ON
    // Create if missing
    // Same for primary keys
}
```

**Impact**: Medium - Significantly improves query performance

---

#### 7. **Retry Logic for Failed Operations** (Medium Priority)
**Current Issue**: If an upsert fails, sync continues. No retry mechanism.

**Improvement**: Add retry logic with exponential backoff
```php
function batchUpsertWithRetry($table, $records, $maxRetries = 3) {
    $attempt = 0;
    while ($attempt < $maxRetries) {
        try {
            batchUpsertRemote($table, $records);
            return; // Success
        } catch (Exception $e) {
            $attempt++;
            if ($attempt >= $maxRetries) throw $e;
            sleep(pow(2, $attempt)); // Exponential backoff
        }
    }
}
```

**Impact**: Medium - Improves reliability

---

### 🟢 Nice-to-Have Improvements

#### 8. **Parallel Table Processing** (Low Priority)
**Current Issue**: Tables are processed sequentially. Independent tables could be processed in parallel.

**Improvement**: Use PHP parallel processing for independent tables
```php
// Process independent tables in parallel
$parallel = new \parallel\Runtime();
$futures = [];
foreach ($independent_tables as $table) {
    $futures[] = $parallel->run(function() use ($table) {
        syncTable($table);
    });
}
```

**Impact**: Low - Faster overall sync, but adds complexity

---

#### 9. **Sync Statistics & Metrics** (Low Priority)
**Current Issue**: Limited statistics about sync performance.

**Improvement**: Track detailed metrics
```php
$syncStats = [
    'tables_processed' => 0,
    'records_synced' => 0,
    'conflicts' => 0,
    'errors' => 0,
    'duration' => 0,
    'memory_peak' => 0,
];
```

**Impact**: Low - Better monitoring and debugging

---

#### 10. **Optimistic Locking** (Low Priority)
**Current Issue**: No protection against concurrent updates to the same record.

**Improvement**: Use version numbers or timestamps for optimistic locking
```php
// Check if record was modified during sync
if ($record['version'] != $current_version) {
    // Conflict detected, resolve
}
```

**Impact**: Low - Prevents race conditions in edge cases

---

## Recommended Implementation Order

### Phase 1: Critical (Do First)
1. ✅ Transaction handling
2. ✅ Database-level comparison for large tables
3. ✅ Incremental remote data fetching

### Phase 2: Important (Do Next)
4. ✅ Index verification
5. ✅ Retry logic
6. ✅ Conflict logging

### Phase 3: Nice-to-Have (Optional)
7. Parallel processing
8. Enhanced metrics
9. Optimistic locking

---

## Code Examples

### Example 1: Database-Level Comparison
```php
function syncEntityOptimized($entity, $last_synced_at) {
    $remote_table_name = Converter::table_convert_remote($entity);
    $remote_key = Converter::primaryKey($remote_table_name);
    $ubs_key = Converter::primaryKey($entity);
    $column_updated_at = Converter::mapUpdatedAtField($remote_table_name);
    
    $db = new mysql();
    $db_remote = new mysql();
    $db_remote->connect_remote();
    
    // Use SQL to compare at database level
    $sql = "
        SELECT 
            ubs.*,
            remote.*,
            CASE 
                WHEN ubs.UPDATED_ON > remote.$column_updated_at THEN 'ubs_newer'
                WHEN remote.$column_updated_at > ubs.UPDATED_ON THEN 'remote_newer'
                ELSE 'equal'
            END as sync_direction
        FROM `$entity` ubs
        LEFT JOIN `$remote_table_name` remote ON ubs.$ubs_key = remote.$remote_key
        WHERE ubs.UPDATED_ON > '$last_synced_at' 
           OR remote.$column_updated_at > '$last_synced_at'
    ";
    
    $results = $db->get($sql);
    
    $sync = ['remote_data' => [], 'ubs_data' => []];
    foreach ($results as $row) {
        if ($row['sync_direction'] == 'ubs_newer') {
            $sync['remote_data'][] = convert($remote_table_name, $row, 'to_remote');
        } elseif ($row['sync_direction'] == 'remote_newer') {
            $sync['ubs_data'][] = convert($remote_table_name, $row, 'to_ubs');
        }
        // 'equal' means no sync needed
    }
    
    return $sync;
}
```

### Example 2: Incremental Remote Data Fetching
```php
function fetchRemoteDataByKeys($table, $keys) {
    if (empty($keys)) return [];
    
    $remote_table_name = Converter::table_convert_remote($table);
    $remote_key = Converter::primaryKey($remote_table_name);
    
    $db = new mysql();
    $db->connect_remote();
    
    // Build WHERE clause for keys
    $keyPlaceholders = implode(',', array_fill(0, count($keys), '?'));
    $sql = "SELECT * FROM $remote_table_name WHERE $remote_key IN ($keyPlaceholders)";
    
    return $db->get($sql, $keys);
}
```

### Example 3: Transaction Wrapper
```php
function syncTableWithTransaction($table) {
    $db = new mysql();
    $db_remote = new mysql();
    $db_remote->connect_remote();
    
    try {
        $db->beginTransaction();
        $db_remote->beginTransaction();
        
        // Perform sync operations
        $syncResult = syncEntity($table, $ubs_data, $remote_data);
        
        if (!empty($syncResult['remote_data'])) {
            batchUpsertRemote($table, $syncResult['remote_data']);
        }
        
        if (!empty($syncResult['ubs_data'])) {
            batchUpsertUbs($table, $syncResult['ubs_data']);
        }
        
        // Commit both transactions
        $db->commit();
        $db_remote->commit();
        
    } catch (Exception $e) {
        $db->rollback();
        $db_remote->rollback();
        throw $e;
    }
}
```

---

## Performance Impact Estimates

| Improvement | Memory Reduction | Speed Improvement | Complexity |
|------------|------------------|-------------------|------------|
| Database-level comparison | 60-80% | 3-5x faster | Medium |
| Incremental remote fetch | 40-60% | 2-3x faster | Low |
| Transaction handling | 0% | 0% | Low |
| Index verification | 0% | 2-10x faster | Low |
| Retry logic | 0% | Better reliability | Low |
| Parallel processing | +20% | 2-4x faster | High |

---

## Testing Recommendations

1. **Test with large datasets** (10k+ records per table)
2. **Test transaction rollback** on errors
3. **Test conflict resolution** with equal timestamps
4. **Benchmark performance** before/after improvements
5. **Test memory usage** with large tables

---

## Conclusion

The most impactful improvements are:
1. **Database-level comparison** - Biggest performance gain
2. **Transaction handling** - Critical for data integrity
3. **Incremental remote fetching** - Reduces memory significantly

These three improvements would make the sync method much more efficient and reliable.
