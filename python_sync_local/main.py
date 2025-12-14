from utils import read_dbf, sync_to_server, test_server_response
from sync_database import create_sync_logs_table, sync_to_database
from sync_lock import acquire_sync_lock, release_sync_lock, is_sync_running
import os
import sys
import time
import atexit


def main():
    # Check if PHP sync is running
    if is_sync_running('php'):
        print("❌ PHP sync is currently running. Please wait for it to complete.")
        sys.exit(1)
    
    # Acquire Python sync lock
    if not acquire_sync_lock('python'):
        print("❌ Python sync is already running or lock file exists. Please check and remove lock file if needed.")
        sys.exit(1)
    
    # Register cleanup function to release lock on exit
    atexit.register(lambda: release_sync_lock('python'))
    
    try:
        create_sync_logs_table()

        # test_server_response()
        sync_all()
    except Exception as e:
        print(f"❌ Sync failed: {e}")
        release_sync_lock('python')
        sys.exit(1)
    finally:
        # Ensure lock is released
        release_sync_lock('python')

    # while True:
    #     # Call your function or logic here
    #     print("Running task...")
    #     sync_all()
    #     # Wait 5 seconds
    #     print("Waiting ... ... ...")
    #     time.sleep(30)

    # single_sync()


def sync_all():
    start_time = time.time()
    print("🚀 Starting FAST DBF to MySQL sync...")
    
    grouped_dbfs = {
        "UBSACC2015": [
            "arcust",
            "apvend",
            "arpay",
            "arpost",
            "gldata",
            "glbatch",
            "glpost",
        ],
        "UBSSTK2015": [
            "icitem",
            "icgroup",
            "artran",
            "ictran",
        ],
    }

    dbf_subpath=os.getenv("DBF_SUBPATH", "Sample")
    total_files = sum(len(dbf_list) for dbf_list in grouped_dbfs.values())
    processed_files = 0

    for directory_name, dbf_list in grouped_dbfs.items():
        directory_path = f"C:/{directory_name}/"+dbf_subpath
        for dbf_name in dbf_list:
            file_name = dbf_name + ".dbf"
            full_path = os.path.join(directory_path, file_name)
            
            # Check if file exists before processing
            if not os.path.exists(full_path):
                print(f"⚠️  File {full_path} not found, skipping...")
                continue
                
            try:
                file_start = time.time()
                print(f"📁 [{processed_files+1}/{total_files}] Processing {file_name}...")
                
                data = read_dbf(full_path)
                
                # Check if we got valid data
                if not data or not data.get('structure') or not data.get('rows'):
                    print(f"⚠️  No data in {file_name}, skipping...")
                    continue
                
                # Filter artran and ictran: Skip records with DATE <= 2025-12-12
                if dbf_name in ['artran', 'ictran']:
                    cutoff_date_str = '20251212'  # YYYYMMDD format (DBF date format)
                    original_count = len(data['rows'])
                    
                    # Filter rows based on DATE field
                    filtered_rows = []
                    for row in data['rows']:
                        date_value = row.get('DATE')
                        should_skip = False
                        
                        if date_value:
                            try:
                                # DBF DATE fields are typically in YYYYMMDD string format
                                date_str = str(date_value).strip()
                                
                                # Handle YYYYMMDD format (8 digits)
                                if len(date_str) >= 8 and date_str[:8].isdigit():
                                    # Compare as string (YYYYMMDD format allows string comparison)
                                    if date_str[:8] <= cutoff_date_str:
                                        should_skip = True
                                # Handle YYYY-MM-DD format
                                elif '-' in date_str and len(date_str) >= 10:
                                    date_parts = date_str[:10].split('-')
                                    if len(date_parts) == 3:
                                        date_comp = ''.join(date_parts)
                                        if date_comp.isdigit() and date_comp <= cutoff_date_str:
                                            should_skip = True
                            except Exception:
                                # If date parsing fails, include the record (safer)
                                pass
                        
                        if not should_skip:
                            filtered_rows.append(row)
                    
                    data['rows'] = filtered_rows
                    filtered_count = len(filtered_rows)
                    skipped_count = original_count - filtered_count
                    
                    if skipped_count > 0:
                        print(f"⏭️  Skipped {skipped_count} record(s) with DATE <= 2025-12-12 ({filtered_count} remaining)")
                    
                sync_to_database(file_name, data, directory_name)
                
                file_time = time.time() - file_start
                print(f"✅ {file_name} completed in {file_time:.2f}s ({len(data['rows'])} records)")
                
            except Exception as e:
                print(f"❌ Error processing {file_name}: {e}")
                continue
            
            processed_files += 1
    
    total_time = time.time() - start_time
    print(f"\n🎉 SYNC COMPLETED!")
    print(f"⏱️  Total time: {total_time:.2f} seconds")
    print(f"📊 Files processed: {processed_files}/{total_files}")
    print(f"⚡ Average per file: {total_time/processed_files:.2f}s" if processed_files > 0 else "")


def single_sync():
    directory_name = "UBSACC2015"
    directory_path = f"C:/{directory_name}/Sample"
    file_name = "arcust.dbf"
    full_path = os.path.join(directory_path, file_name)
    data = read_dbf(full_path)
    print(data)
    sync_to_database(file_name, data, directory_name)


def sync_icgroup_only():
    """
    Sync only icgroup.dbf from UBSSTK2015 directory to local MySQL
    Creates/updates the ubs_ubsstk2015_icgroup table
    """
    import mysql.connector
    
    start_time = time.time()
    print("🚀 Starting icgroup DBF to MySQL sync...")
    
    # Show database connection info
    db_host = os.getenv("DB_HOST", "localhost")
    db_user = os.getenv("DB_USER", "root")
    db_name = os.getenv("DB_NAME", "your_database")
    print(f"📊 Database: {db_name} @ {db_host} (user: {db_user})")
    
    directory_name = "UBSSTK2015"
    dbf_subpath = os.getenv("DBF_SUBPATH", "Sample")
    directory_path = f"C:/{directory_name}/{dbf_subpath}"
    file_name = "icgroup.dbf"
    full_path = os.path.join(directory_path, file_name)
    
    # Check if file exists
    if not os.path.exists(full_path):
        print(f"❌ File {full_path} not found!")
        print(f"💡 Please check if the file exists at: {full_path}")
        return False
    
    try:
        print(f"📁 Processing {file_name}...")
        
        # Read DBF file
        data = read_dbf(full_path)
        
        # Check if we got valid data
        if not data or not data.get('structure') or not data.get('rows'):
            print(f"⚠️  No data in {file_name}, skipping...")
            return False
        
        # Sync to database
        sync_to_database(file_name, data, directory_name)
        
        # Verify table was created
        table_name = "ubs_ubsstk2015_icgroup"
        try:
            connection = mysql.connector.connect(
                host=db_host,
                user=db_user,
                password=os.getenv("DB_PASSWORD", ""),
                database=db_name
            )
            cursor = connection.cursor()
            
            # Check if table exists
            cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
            table_exists = cursor.fetchone()
            
            if table_exists:
                # Get record count
                cursor.execute(f"SELECT COUNT(*) FROM `{table_name}`")
                count = cursor.fetchone()[0]
                print(f"✅ Table '{table_name}' verified in database '{db_name}'")
                print(f"📊 Record count: {count} records")
            else:
                print(f"⚠️  WARNING: Table '{table_name}' not found in database '{db_name}'")
                print(f"💡 Please check if you're looking at the correct database")
            
            cursor.close()
            connection.close()
            
        except Exception as verify_error:
            print(f"⚠️  Could not verify table creation: {verify_error}")
        
        file_time = time.time() - start_time
        record_count = len(data['rows']) if data.get('rows') else 0
        print(f"✅ {file_name} completed in {file_time:.2f}s ({record_count} records)")
        print(f"📊 Table 'ubs_ubsstk2015_icgroup' created/updated in database '{db_name}'")
        
        return True
        
    except Exception as e:
        print(f"❌ Error processing {file_name}: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    main()
