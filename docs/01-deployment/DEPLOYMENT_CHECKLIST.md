# Deployment Checklist for Client

Use this checklist when deploying to a client.

## Pre-Deployment

- [ ] Code has been tested
- [ ] Database backups are available
- [ ] Client environment information collected:
  - [ ] UBS installation path
  - [ ] Database credentials
  - [ ] Remote database credentials
  - [ ] Python version
  - [ ] PHP version

## Deployment Steps

### 1. File Transfer
- [ ] Copy `ubs-local-connector` folder to client PC
- [ ] Verify all files are present

### 2. Python Setup
- [ ] Install Python dependencies:
  ```bash
  cd python_sync_local
  pip install -r requirements.txt
  ```
- [ ] Verify installation:
  ```bash
  python -c "import dbfread, requests, psutil, mysql.connector; print('OK')"
  ```

### 3. Configuration
- [ ] Create/Edit `python_sync_local/.env`:
  - [ ] DB_HOST
  - [ ] DB_USER
  - [ ] DB_PASSWORD
  - [ ] DB_NAME
  - [ ] DBF_SUBPATH
- [ ] Create/Edit `php_sync_server/bootstrap/env.php`:
  - [ ] Local database credentials
  - [ ] Remote database credentials
  - [ ] DBF_SUBPATH

### 4. Directory Setup
- [ ] Create `php_sync_server/locks` directory (or let it auto-create)
- [ ] Create `php_sync_server/logs` directory (or let it auto-create)
- [ ] Verify write permissions

### 5. Path Verification
- [ ] Verify UBS DBF files are at:
  - [ ] `C:\UBSACC2015\Sample\` (or configured path)
  - [ ] `C:\UBSSTK2015\Sample\` (or configured path)
- [ ] Update paths in code if different

## Testing

### Initial Tests
- [ ] **Test Python Sync:**
  ```bash
  cd python_sync_local
  python main.py
  ```
  - [ ] No errors
  - [ ] Data inserted into local MySQL
  - [ ] All tables synced

- [ ] **Test PHP Initial Sync:**
  ```bash
  cd php_sync_server
  php main_init.php
  ```
  - [ ] No errors
  - [ ] Data synced to remote MySQL
  - [ ] All tables processed

- [ ] **Test PHP Incremental Sync:**
  ```bash
  php main.php
  ```
  - [ ] No errors
  - [ ] Only changed records synced
  - [ ] Lock system works

### Lock System Test
- [ ] **Test Concurrent Prevention:**
  - [ ] Start PHP sync in one terminal
  - [ ] Try to start Python sync in another terminal
  - [ ] Should show error: "PHP sync is currently running"
  - [ ] Stop PHP sync
  - [ ] Python sync should work

### Status Check
- [ ] **Check Sync Status:**
  ```bash
  # Windows
  check_status.bat
  
  # Or create check_status.php and run:
  php check_status.php
  ```
  - [ ] Shows correct status

## Post-Deployment

### Documentation
- [ ] Provide client with:
  - [ ] `QUICK_DEPLOYMENT.md` - Quick reference
  - [ ] `DEPLOYMENT_GUIDE.md` - Full guide
  - [ ] Instructions on how to run syncs manually
  - [ ] Instructions on how to check status

### Scheduling
- [ ] Set up scheduled task (if needed):
  - [ ] Windows Task Scheduler
  - [ ] Or cron job (Linux/Mac)
  - [ ] Test scheduled task runs correctly

### Monitoring
- [ ] Show client how to:
  - [ ] Check sync status
  - [ ] View conflict logs
  - [ ] Check sync logs in database
  - [ ] Troubleshoot common issues

## Client Training

- [ ] Explain what the sync does
- [ ] Show how to run syncs manually
- [ ] Show how to check if syncs are running
- [ ] Explain what to do if sync fails
- [ ] Provide contact information for support

## Final Verification

- [ ] All tests pass
- [ ] Client can run syncs independently
- [ ] Client understands how to check status
- [ ] Client knows how to troubleshoot
- [ ] Scheduled task is set up (if needed)
- [ ] Documentation is provided

## Sign-Off

- [ ] Client confirms sync is working
- [ ] Client confirms they understand how to use it
- [ ] Deployment completed successfully

---

**Date:** _______________  
**Deployed by:** _______________  
**Client:** _______________  
**Signature:** _______________
