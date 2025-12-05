# Quick Deployment Checklist

## ⚡ Fast Deployment (5 Minutes)

### 1. Copy Files
```bash
# Copy entire ubs-local-connector folder to client PC
```

### 2. Install Python Dependencies
```bash
cd python_sync_local
pip install -r requirements.txt
```

### 3. Configure Environment
```bash
# Edit .env files with database credentials
# python_sync_local/.env
# php_sync_server/bootstrap/env.php
```

### 4. Create Directories (Auto-created, but can create manually)
```bash
mkdir php_sync_server\locks
mkdir php_sync_server\logs
```

### 5. Test
```bash
# Test Python sync
cd python_sync_local
python main.py

# Test PHP sync
cd ../php_sync_server
php main.php
```

---

## 🔧 Required Configuration

### Python `.env` file:
```env
DB_TYPE=mysql
DB_HOST=localhost
DB_USER=root
DB_PASSWORD=your_password
DB_NAME=your_database
DBF_SUBPATH=Sample
```

### PHP Configuration:
```php
// Local DB
DB_HOST=localhost
DB_USERNAME=root
DB_PASSWORD=your_password
DB_NAME=your_database

// Remote DB
REMOTE_DB_HOST=your_remote_host
REMOTE_DB_USERNAME=your_remote_user
REMOTE_DB_PASSWORD=your_remote_password
REMOTE_DB_NAME=your_remote_database

// DBF Path
DBF_SUBPATH=Sample
```

---

## ✅ Post-Deployment Test

1. Run Python sync → Should read DBF files
2. Run PHP initial sync → Should sync to remote
3. Run PHP incremental sync → Should sync changes
4. Check lock system → Try running both syncs simultaneously

---

## 🐛 Common Issues

| Issue | Solution |
|-------|----------|
| Lock file error | Delete `php_sync_server/locks/*.lock` and `*.pid` |
| DBF not found | Check `DBF_SUBPATH` matches folder name |
| Database error | Verify credentials and connection |
| Python import error | Run `pip install -r requirements.txt` |

---

## 📞 Need Help?

See `DEPLOYMENT_GUIDE.md` for detailed instructions.
