# UBS Local Connector - Client Deployment Guide

## 📋 Pre-Deployment Checklist

Before deploying to client, ensure:

- [ ] All code changes have been tested
- [ ] Database backups are available
- [ ] Client has UBS system installed and accessible
- [ ] Client has Python 3.x installed
- [ ] Client has PHP 7.4+ installed
- [ ] Client has MySQL/MariaDB installed
- [ ] Network connectivity to remote database is available
- [ ] DBF files are accessible at expected paths

---

## 🚀 Deployment Steps

### Step 1: Backup Existing System

```bash
# Backup database
mysqldump -u [username] -p [database_name] > backup_$(date +%Y%m%d).sql

# Backup existing code (if upgrading)
cp -r ubs-local-connector ubs-local-connector_backup_$(date +%Y%m%d)
```

### Step 2: Copy Files to Client PC

Copy the entire `ubs-local-connector` folder to client PC at:
```
C:\ubs-local-connector\
```

Or any location the client prefers.

### Step 3: Install Python Dependencies

Open Command Prompt or PowerShell **as Administrator**:

```bash
cd C:\ubs-local-connector\python_sync_local

# Install required packages
pip install -r requirements.txt

# Or install individually if requirements.txt doesn't work:
pip install dbfread
pip install requests
pip install python-dotenv
pip install psutil
pip install mysql-connector-python
pip install pymysql
```

**Verify installation:**
```bash
python -c "import dbfread, requests, psutil, mysql.connector; print('All dependencies installed!')"
```

### Step 4: Configure Environment Variables

#### Python Configuration

Create or edit `.env` file in `python_sync_local/`:

```env
# Database Configuration
DB_TYPE=mysql
DB_HOST=localhost
DB_USER=root
DB_PASSWORD=your_password
DB_NAME=your_database_name

# DBF File Path
DBF_SUBPATH=Sample
```

#### PHP Configuration

Edit `php_sync_server/bootstrap/env.php` or create `.env` file:

```php
// Local Database
DB_HOST=localhost
DB_USERNAME=root
DB_PASSWORD=your_password
DB_NAME=your_database_name

// Remote Database
REMOTE_DB_HOST=your_remote_host
REMOTE_DB_USERNAME=your_remote_user
REMOTE_DB_PASSWORD=your_remote_password
REMOTE_DB_NAME=your_remote_database

// DBF Path
DBF_SUBPATH=Sample
```

### Step 5: Create Required Directories

```bash
# Create locks directory (for sync lock files)
mkdir C:\ubs-local-connector\php_sync_server\locks

# Create logs directory (for conflict logs)
mkdir C:\ubs-local-connector\php_sync_server\logs
```

Or let the system create them automatically (they will be created on first run).

### Step 6: Set File Permissions

**Windows:**
- Right-click `php_sync_server/locks` folder → Properties → Security
- Ensure the user running PHP has **Write** permissions
- Same for `php_sync_server/logs` folder

**Linux/Mac:**
```bash
chmod 755 php_sync_server/locks
chmod 755 php_sync_server/logs
```

### Step 7: Verify UBS DBF File Paths

Ensure UBS DBF files are accessible at:
```
C:\UBSACC2015\Sample\
C:\UBSSTK2015\Sample\
```

Or update paths in:
- `python_sync_local/main.py` (line 54)
- `php_sync_server/functions.php` (line 289)

---

## ✅ Post-Deployment Testing

### Test 1: Verify Python Sync

```bash
cd C:\ubs-local-connector\python_sync_local
python main.py
```

**Expected:**
- ✅ Reads DBF files successfully
- ✅ Inserts data into local MySQL
- ✅ No errors

**If errors:**
- Check DBF file paths
- Check database connection
- Check Python dependencies

### Test 2: Verify PHP Sync (Initial)

```bash
cd C:\ubs-local-connector\php_sync_server
php main_init.php
```

**Expected:**
- ✅ Connects to local and remote databases
- ✅ Syncs data from local to remote
- ✅ No errors

**If errors:**
- Check database connections
- Check PHP extensions (mysqli, etc.)
- Check file permissions

### Test 3: Verify PHP Sync (Incremental)

```bash
cd C:\ubs-local-connector\php_sync_server
php main.php
```

**Expected:**
- ✅ Checks for lock files
- ✅ Compares timestamps
- ✅ Syncs only changed records
- ✅ No errors

**If errors:**
- Check lock files (remove if stale)
- Check database connections
- Check sync logs

### Test 4: Verify Lock System

**Test 1 - Prevent Concurrent Syncs:**
```bash
# Terminal 1: Start PHP sync
php main.php

# Terminal 2: Try to start Python sync (should fail)
python main.py
# Expected: "❌ PHP sync is currently running"
```

**Test 2 - Check Sync Status:**
```php
// Create test file: check_status.php
<?php
include('bootstrap/app.php');
$status = getSyncStatus();
print_r($status);
```

Run: `php check_status.php`

---

## 🔧 Configuration Checklist

### Database Configuration

- [ ] Local MySQL connection works
- [ ] Remote MySQL connection works
- [ ] Database user has proper permissions (SELECT, INSERT, UPDATE, DELETE)
- [ ] Database names are correct

### File Paths

- [ ] UBS DBF files are accessible
- [ ] DBF_SUBPATH matches actual folder structure
- [ ] Lock files directory is writable
- [ ] Log files directory is writable

### PHP Configuration

- [ ] PHP version is 7.4 or higher
- [ ] mysqli extension is enabled
- [ ] Memory limit is sufficient (4G recommended)
- [ ] Execution time limit is set (0 = unlimited)

### Python Configuration

- [ ] Python version is 3.6 or higher
- [ ] All dependencies are installed
- [ ] Python can access DBF files
- [ ] Python can connect to MySQL

---

## 📝 Running Syncs

### Initial Setup (First Time)

1. **Run Python Sync** (Reads all UBS data into local MySQL):
   ```bash
   cd python_sync_local
   python main.py
   ```

2. **Run PHP Initial Sync** (Syncs all local data to remote):
   ```bash
   cd php_sync_server
   php main_init.php
   ```

### Regular Operation (Scheduled)

**Option 1: Manual Run**
```bash
# Run Python sync first
cd python_sync_local
python main.py

# Then run PHP sync
cd ../php_sync_server
php main.php
```

**Option 2: Scheduled Task (Windows)**

Create batch file `run_sync.bat`:
```batch
@echo off
cd /d C:\ubs-local-connector\python_sync_local
python main.py
cd ..\php_sync_server
php main.php
```

Schedule using Windows Task Scheduler:
1. Open Task Scheduler
2. Create Basic Task
3. Set trigger (e.g., Daily at 2 AM)
4. Action: Start a program
5. Program: `C:\ubs-local-connector\run_sync.bat`

**Option 3: Cron Job (Linux/Mac)**

```bash
# Edit crontab
crontab -e

# Add line (runs daily at 2 AM)
0 2 * * * cd /path/to/ubs-local-connector/python_sync_local && python main.py && cd ../php_sync_server && php main.php
```

---

## 🐛 Troubleshooting

### Issue: "Python sync is currently running"

**Solution:**
```bash
# Check if process is actually running
# Windows:
tasklist | findstr python

# Linux/Mac:
ps aux | grep python

# If not running, remove lock file:
del php_sync_server\locks\python_sync.lock
del php_sync_server\locks\python_sync.pid
```

### Issue: "PHP sync is currently running"

**Solution:**
```bash
# Check if process is actually running
# Windows:
tasklist | findstr php

# Linux/Mac:
ps aux | grep php

# If not running, remove lock file:
del php_sync_server\locks\php_sync.lock
del php_sync_server\locks\php_sync.pid
```

### Issue: "DBF file not found"

**Solution:**
1. Check DBF file paths in configuration
2. Verify `DBF_SUBPATH` matches actual folder name
3. Check file permissions
4. Ensure UBS system is installed

### Issue: "Database connection failed"

**Solution:**
1. Verify database credentials
2. Check if MySQL service is running
3. Check firewall settings
4. Test connection manually:
   ```bash
   mysql -u [username] -p [database_name]
   ```

### Issue: "Memory limit exceeded"

**Solution:**
1. Increase PHP memory limit in `php.ini`:
   ```ini
   memory_limit = 4G
   ```
2. Or set in code (already done in `functions.php`)

### Issue: "Transaction failed"

**Solution:**
1. Check database supports transactions (InnoDB engine)
2. Check database user has proper permissions
3. Check for deadlocks in database logs
4. System will auto-rollback, but check logs for details

### Issue: "Index verification warnings"

**Solution:**
These are warnings, not errors. Sync will continue, but may be slower.

To create indexes (optional):
```sql
-- For local tables
CREATE INDEX idx_updated_on ON table_name(UPDATED_ON);

-- For remote tables
CREATE INDEX idx_updated_at ON table_name(updated_at);
```

---

## 📊 Monitoring

### Check Sync Status

Create `check_status.php`:
```php
<?php
include('bootstrap/app.php');
$status = getSyncStatus();
echo "PHP Sync Running: " . ($status['php']['running'] ? 'Yes' : 'No') . "\n";
echo "Python Sync Running: " . ($status['python']['running'] ? 'Yes' : 'No') . "\n";
```

### Check Conflict Logs

```bash
# View conflict log
type php_sync_server\logs\sync_conflicts.log

# Or open in text editor
notepad php_sync_server\logs\sync_conflicts.log
```

### Check Sync Logs

```bash
# Check last sync time
mysql -u [username] -p [database_name] -e "SELECT * FROM sync_logs ORDER BY synced_at DESC LIMIT 1;"
```

---

## 🔒 Security Considerations

1. **Database Credentials**
   - Store in `.env` file (not in code)
   - Use strong passwords
   - Limit database user permissions

2. **File Permissions**
   - Lock files should be readable/writable only by sync process
   - Log files should be readable only

3. **Network Security**
   - Use SSL/TLS for remote database connections
   - Use VPN if possible
   - Firewall rules for database access

---

## 📞 Support

If issues persist:

1. **Check Logs:**
   - `php_sync_server/logs/sync_conflicts.log`
   - PHP error logs
   - Python error output

2. **Verify Configuration:**
   - Database connections
   - File paths
   - Permissions

3. **Test Components:**
   - Python sync independently
   - PHP sync independently
   - Database connections

4. **Contact Support:**
   - Provide error messages
   - Provide log files
   - Provide configuration (without passwords)

---

## ✅ Deployment Verification Checklist

After deployment, verify:

- [ ] Python sync runs successfully
- [ ] PHP initial sync runs successfully
- [ ] PHP incremental sync runs successfully
- [ ] Lock system prevents concurrent syncs
- [ ] Transactions work (test with error scenario)
- [ ] Conflict logging works
- [ ] Index verification works
- [ ] Retry logic works (test with network interruption)
- [ ] Scheduled task/cron job is set up
- [ ] Client understands how to run syncs manually
- [ ] Client knows how to check sync status
- [ ] Client knows how to troubleshoot common issues

---

## 📚 Additional Resources

- `SYNC_ANALYSIS.md` - How sync works
- `SYNC_ISSUES_FOUND.md` - Known issues and fixes
- `SAFETY_CHECKS.md` - Safety documentation
- `IMPROVEMENTS_IMPLEMENTED.md` - What was improved
- `LOCK_SYSTEM_USAGE.md` - Lock system guide

---

## 🎯 Quick Start Commands

```bash
# 1. Install Python dependencies
cd python_sync_local
pip install -r requirements.txt

# 2. Configure environment
# Edit .env files with database credentials

# 3. Test Python sync
python main.py

# 4. Test PHP initial sync
cd ../php_sync_server
php main_init.php

# 5. Test PHP incremental sync
php main.php

# 6. Set up scheduled task (optional)
# Use Windows Task Scheduler or cron
```

---

**Last Updated:** 2025-01-15  
**Version:** 2.0 (with safe improvements)
