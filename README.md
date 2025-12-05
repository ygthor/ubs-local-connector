# UBS Local Connector

Sync system for syncing UBS DBF files between local MySQL and remote MySQL databases.

## 📚 Documentation

**All documentation is organized in the [`docs/`](docs/) folder:**

- **[📖 Documentation Index](docs/README.md)** - Start here to find what you need

### Quick Links

**For Deployment:**
- [📦 Deployment Guide](docs/01-deployment/DEPLOYMENT_GUIDE.md) - Complete deployment instructions
- [⚡ Quick Deployment](docs/01-deployment/QUICK_DEPLOYMENT.md) - 5-minute checklist
- [✅ Deployment Checklist](docs/01-deployment/DEPLOYMENT_CHECKLIST.md) - Step-by-step checklist

**For Understanding:**
- [🔍 Sync Analysis](docs/02-analysis/SYNC_ANALYSIS.md) - How the sync system works
- [🐛 Issues Found](docs/02-analysis/SYNC_ISSUES_FOUND.md) - Known issues and fixes
- [💡 Improvements](docs/02-analysis/SYNC_IMPROVEMENTS.md) - Potential improvements

**Implementation Details:**
- [✅ Implemented Improvements](docs/03-implementation/IMPROVEMENTS_IMPLEMENTED.md) - What was implemented
- [🛡️ Safety Checks](docs/03-implementation/SAFETY_CHECKS.md) - Safety documentation
- [🔒 Lock System](docs/03-implementation/LOCK_SYSTEM_USAGE.md) - Lock system guide

## 🚀 Quick Start

### 1. Install Dependencies
```bash
cd python_sync_local
pip install -r requirements.txt
```

### 2. Configure
Edit `.env` files with your database credentials.

### 3. Run Sync
```bash
# Windows (using batch file)
docs/05-scripts/run_sync.bat

# Or manually
cd python_sync_local
python main.py
cd ../php_sync_server
php main.php
```

## 📁 Project Structure

```
ubs-local-connector/
├── python_sync_local/      # Python sync (DBF → Local MySQL)
│   ├── main.py
│   ├── sync_database.py
│   ├── sync_lock.py
│   └── requirements.txt
├── php_sync_server/        # PHP sync (Local MySQL ↔ Remote MySQL)
│   ├── main.php            # Incremental sync
│   ├── main_init.php       # Initial sync
│   ├── functions.php
│   ├── bootstrap/
│   └── locks/              # Lock files (auto-created)
├── docs/                   # All documentation (organized by category)
│   ├── 01-deployment/     # Deployment guides
│   ├── 02-analysis/       # System analysis
│   ├── 03-implementation/ # Implementation details
│   ├── 04-setup/          # Setup guides
│   └── 05-scripts/         # Utility scripts
└── [other files...]
```

## 🔧 Requirements

- Python 3.6+
- PHP 7.4+
- MySQL/MariaDB
- UBS system with DBF files

## 📝 Features

- ✅ Two-way sync based on timestamps
- ✅ Lock system prevents concurrent syncs
- ✅ Transaction support for data integrity
- ✅ Retry logic for reliability
- ✅ Conflict logging
- ✅ Index verification

## 🆘 Support

See [📦 Deployment Guide](docs/01-deployment/DEPLOYMENT_GUIDE.md) for troubleshooting.

## 📄 License

[Your License Here]
