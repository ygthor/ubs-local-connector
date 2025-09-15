# DBF UPDATED_ON Date Fixer

This collection of Python scripts fixes invalid UPDATED_ON dates in DBF files by updating them to the current date.

## 📋 Overview

The scripts loop through all configured DBF tables and:
1. Check each UPDATED_ON field for invalid dates and non-datetime data
2. Convert non-datetime data (strings, numbers, etc.) to null
3. Update invalid datetime/date values to the current datetime
4. Provide detailed progress reporting and summary

## 🚀 Scripts Available

### 1. `fix_updated_on_dates.py` - Interactive Version
- **Purpose**: Interactive script with user confirmation
- **Features**: 
  - Asks for user confirmation before proceeding
  - Shows detailed progress for first 5 fixes per table
  - Comprehensive reporting
- **Usage**: `python fix_updated_on_dates.py`

### 2. `fix_updated_on_dates_auto.py` - Automated Version
- **Purpose**: Automated script without user interaction
- **Features**:
  - Runs automatically without confirmation
  - Suitable for batch processing or automation
  - Same functionality as interactive version
- **Usage**: `python fix_updated_on_dates_auto.py`

### 3. `run_date_fixer.py` - Simple Runner
- **Purpose**: Simple wrapper to run the interactive version
- **Usage**: `python run_date_fixer.py`

## 📊 Tables Processed

The scripts process the following DBF tables:

### UBSACC2015 Directory:
- `arcust.dbf`
- `apvend.dbf`
- `arpay.dbf`
- `arpost.dbf`
- `gldata.dbf`
- `glbatch.dbf`
- `glpost.dbf`

### UBSSTK2015 Directory:
- `icitem.dbf`
- `artran.dbf`
- `ictran.dbf`
- `arpso.dbf`
- `icpso.dbf`

## 🔍 Data Processing Logic

The script handles different data types in datetime fields:

### **Valid Data Types:**
- `datetime` objects (proper datetime with time)
- `date` objects (date only, no time)  
- `None` values (null)

### **Invalid Data Types (converted to null):**
- String values (e.g., "invalid_date", "0000-00-00")
- Numeric values (e.g., 0, 12345)
- Any other non-datetime data types

### **Invalid DateTime Values (fixed to current datetime):**
- Years before 1900 or after 2100
- `1900-01-01` (common default invalid date)
- `1970-01-01` (Unix epoch, often used as default)

## 📁 File Paths

The scripts look for DBF files in:
```
C:/UBSACC2015/{DBF_SUBPATH}/
C:/UBSSTK2015/{DBF_SUBPATH}/
```

Where `DBF_SUBPATH` is read from environment variables (default: "Sample")

## 🛠️ Requirements

Make sure you have the required dependencies installed:
```bash
pip install -r requirement.txt
```

Required packages:
- `dbf`
- `python-dotenv`
- `datetime` (built-in)

## 📈 Sample Output

```
🚀 Starting DBF UPDATED_ON Date Fixing Process
============================================================
📋 Found 12 tables to process:
  📁 UBSACC2015: arcust, apvend, arpay, arpost, gldata, glbatch, glpost
  📁 UBSSTK2015: icitem, artran, ictran, arpso, icpso

📂 Processing directory: UBSACC2015
----------------------------------------
[1/12] Processing UBSACC2015_arcust
🔍 Processing UBSACC2015_arcust...
📁 File: C:/UBSACC2015/Sample/arcust.dbf
📊 Found 1,250 records to process
  🔄 Converted to null record 1: 'invalid_date' (str) → None
  🔄 Converted to null record 3: '0000-00-00' (str) → None
  🔄 Converted to null record 5: '12345' (int) → None
  🔧 Fixed record 7: '1900-01-01 00:00:00' → '2024-12-20 15:30:45'
  🔧 Fixed record 9: '1970-01-01 00:00:00' → '2024-12-20 15:30:45'
  ... (showing first 5 conversions and fixes)
✅ Completed UBSACC2015_arcust: 1,250 records processed, 45 dates fixed, 23 converted to null

============================================================
📊 PROCESSING SUMMARY
============================================================
⏱️  Total Time: 12.34 seconds
📋 Tables Processed: 12
📄 Total Records Processed: 15,000
🔧 Total Dates Fixed: 234
🔄 Total Converted to Null: 156
📈 Fix Rate: 1.56%
📈 Null Conversion Rate: 1.04%

✅ Successfully processed 390 UPDATED_ON fields!
🔧 Fixed 234 invalid dates
🔄 Converted 156 non-datetime values to null
🎯 All UPDATED_ON fields now contain valid datetime data or null.
============================================================
```

## ⚠️ Important Notes

1. **Backup**: Always backup your DBF files before running the fixer
2. **Permissions**: Ensure the script has write permissions to the DBF files
3. **File Access**: Make sure DBF files are not locked by other applications
4. **Environment**: Set the `DBF_SUBPATH` environment variable if needed

## 🔧 Environment Variables

Create a `.env` file in the script directory:
```
DBF_SUBPATH=Sample
```

Or set it in your system environment.

## 🚨 Error Handling

The scripts include comprehensive error handling:
- Skips non-existent files
- Continues processing if individual records fail
- Reports warnings for problematic records
- Provides detailed error messages

## 📝 Logging

The scripts provide real-time progress updates:
- Table-by-table progress
- Record count updates
- Fix count updates
- Final summary report

## 🎯 Use Cases

- **Data Migration**: Fix invalid dates before data migration
- **Data Validation**: Ensure all UPDATED_ON fields have valid dates
- **Automated Maintenance**: Run as part of regular data maintenance
- **Pre-Sync Cleanup**: Clean data before synchronization processes
