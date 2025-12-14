# PHP Sync Server - Health Check Report

## ✅ Completed Tasks

### 1. Markdown Files Organization
All markdown documentation files have been moved to the `docs/` folder:
- ✅ `TROUBLESHOOTING_MEMO.md` → `docs/07-troubleshooting/`
- ✅ `BACKUP_SYSTEM_EXPLAINED.md` → `docs/06-php-sync-server/`
- ✅ `DBF_CORRUPTION_ANALYSIS.md` → `docs/06-php-sync-server/`
- ✅ `COMPATIBILITY_CHECK.md` → `docs/06-php-sync-server/`
- ✅ `SAFETY_FEATURES_ADDED.md` → `docs/06-php-sync-server/`

## 🔍 PHP Sync Server Analysis

### Structure Overview
The PHP sync server is located in `php_sync_server/` and handles:
- **Local MySQL ↔ Remote MySQL** synchronization
- **DBF file operations** (reading/writing UBS DBF files)
- **Lock management** to prevent concurrent syncs
- **Transaction support** for data integrity

### Main Entry Points
1. **`main.php`** - Incremental sync (syncs records updated after last sync time)
2. **`main_init.php`** - Initial/full sync (syncs all records)
3. **`sync_artran_ictran.php`** - Force sync for orders and order items
4. **`sync_icitem_icgroup.php`** - Sync for products and product groups

### Key Components
- **`bootstrap/app.php`** - Application initialization
- **`bootstrap/helper.php`** - Helper functions (locks, UBS detection, backups)
- **`functions.php`** - Core sync functions
- **`converter.class.php`** - Table mapping and conversion logic
- **`includes/classes/mysql.class.php`** - Database connection wrapper
- **`includes/classes/core.class.php`** - Core functionality

### Dependencies
- ✅ **Composer**: `hisamu/php-xbase` (v2.2) - For DBF file operations
- ✅ **PHP**: Requires PHP 7.4+ (based on code patterns)
- ✅ **MySQL/MariaDB**: For database operations

## ⚠️ Potential Issues Found

### 1. Missing Remote Database Configuration
**Location:** `env.php.example` vs `mysql.class.php`

**Issue:**
- `mysql.class.php` uses `ENV::REMOTE_DB_HOST`, `ENV::REMOTE_DB_USERNAME`, `ENV::REMOTE_DB_PASSWORD`, `ENV::REMOTE_DB_NAME`
- `env.php.example` only defines local database constants
- Missing remote database configuration in example file

**Recommendation:**
Update `env.php.example` to include:
```php
const REMOTE_DB_HOST = '127.0.0.1';
const REMOTE_DB_PORT = '3306';
const REMOTE_DB_USERNAME = 'root';
const REMOTE_DB_PASSWORD = '';
const REMOTE_DB_NAME = 'remote_db_name';
```

### 2. Error Reporting Suppressed
**Location:** `functions.php::initializeSyncEnvironment()`

**Issue:**
- Error reporting is set to `E_ERROR | E_PARSE` (suppresses warnings)
- `display_errors` is set to `0`
- This may hide important warnings during development

**Recommendation:**
- Consider using environment-based error reporting
- Enable warnings in development, suppress in production

### 3. Hard-coded Date in sync_artran_ictran.php
**Location:** `sync_artran_ictran.php:31`

**Issue:**
- Hard-coded date: `$minOrderDate = '2025-12-14 ';`
- This appears to be a test/development value

**Recommendation:**
- Use a configurable date or calculate dynamically
- Or document this as a temporary override

### 4. DBF Corruption Risks (Documented)
**Location:** See `docs/06-php-sync-server/DBF_CORRUPTION_ANALYSIS.md`

**Status:** ✅ **Already Addressed**
- Safety features have been implemented (see `SAFETY_FEATURES_ADDED.md`)
- UBS detection, file locking, backups, and validation are in place

## ✅ Good Practices Found

1. **Lock System**: Prevents concurrent syncs (both PHP and Python)
2. **Transaction Support**: Uses transactions for data integrity
3. **Error Handling**: Comprehensive try-catch blocks
4. **Progress Display**: Real-time progress feedback
5. **Memory Management**: Sets appropriate memory limits (4G)
6. **Backup System**: Automatic backups before DBF writes
7. **File Locking**: OS-level file locks for DBF operations
8. **UBS Detection**: Checks if UBS software is running before writes

## 📋 Configuration Checklist

Before running the PHP sync server, ensure:

- [ ] `env.php` exists (copy from `env.php.example`)
- [ ] `env.php` contains all required constants:
  - [ ] `DB_HOST`, `DB_USERNAME`, `DB_PASSWORD`, `DB_NAME` (local)
  - [ ] `REMOTE_DB_HOST`, `REMOTE_DB_USERNAME`, `REMOTE_DB_PASSWORD`, `REMOTE_DB_NAME` (remote)
  - [ ] `API_URL`
  - [ ] `DBF_SUBPATH`
- [ ] Composer dependencies installed (`composer install` in `php_sync_server/`)
- [ ] Database connections are accessible
- [ ] DBF file paths are correct (C:/UBSACC2015/, C:/UBSSTK2015/)
- [ ] Lock directory exists and is writable (`php_sync_server/locks/`)
- [ ] Log directory exists and is writable (`php_sync_server/logs/`)

## 🚀 Running the Sync Server

### Incremental Sync
```bash
cd php_sync_server
php main.php
```

### Full/Initial Sync
```bash
cd php_sync_server
php main_init.php
```

### Specific Table Syncs
```bash
# Sync products and groups
php sync_icitem_icgroup.php

# Sync orders and order items
php sync_artran_ictran.php
```

## 📝 Notes

- The sync server uses chunked processing (5000 records per chunk) for performance
- Memory limit is set to 4GB for large datasets
- Execution time is unlimited (`set_time_limit(0)`)
- Lock files prevent concurrent execution
- Automatic cleanup of old backups (7+ days)

## 🔗 Related Documentation

- [Backup System Explained](BACKUP_SYSTEM_EXPLAINED.md)
- [DBF Corruption Analysis](DBF_CORRUPTION_ANALYSIS.md)
- [Safety Features Added](SAFETY_FEATURES_ADDED.md)
- [Compatibility Check](COMPATIBILITY_CHECK.md)

