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
        print("❌ PHP sync is currently running. Please wait for it to complete.", flush=True)
        sys.exit(1)
    
    # Acquire Python sync lock
    if not acquire_sync_lock('python'):
        print("❌ Python sync is already running or lock file exists. Please check and remove lock file if needed.", flush=True)
        sys.exit(1)
    
    # Register cleanup function to release lock on exit
    atexit.register(lambda: release_sync_lock('python'))
    
    try:
        create_sync_logs_table()

        # test_server_response()
        sync_all()
    except Exception as e:
        print(f"❌ Sync failed: {e}", flush=True)
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
    print("🚀 Starting FAST DBF to MySQL sync...", flush=True)
    
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
                print(f"⚠️  File {full_path} not found, skipping...", flush=True)
                continue
                
            try:
                file_start = time.time()
                print(f"📁 [{processed_files+1}/{total_files}] Processing {file_name}...", flush=True)
                
                # Define progress callback for this file
                def progress_callback(records_read, status_message):
                    print(status_message, flush=True)
                
                print(f"🔍 Reading DBF file: {file_name}...", flush=True)
                data = read_dbf(full_path, progress_callback=progress_callback)
                
                # Check if we got valid data
                if not data or not data.get('structure') or not data.get('rows'):
                    print(f"⚠️  No data in {file_name}, skipping...", flush=True)
                    continue
                
                original_record_count = len(data['rows'])
                print(f"📊 Read {original_record_count:,} records from {file_name}", flush=True)
                
                # Filter artran: Apply different rules for DO and INV types
                # - DO with DATE <= 2025-12-12: Keep only latest per agent_no
                # - DO with DATE > 2025-12-12: Keep ALL (future dates allowed)
                # - INV with DATE <= 2025-12-12: Skip ALL
                # - INV with DATE > 2025-12-12: Keep ALL (future dates allowed)
                if dbf_name == 'artran':
                    print(f"🔍 Filtering artran records...", flush=True)
                    cutoff_date_str = '20251212'  # YYYYMMDD format (DBF date format)
                    original_count = len(data['rows'])
                    
                    # Separate DO and INV records
                    do_records = []  # DO records with date <= cutoff (will filter to latest per agent)
                    other_records = []  # All other records: DO/INV after cutoff, other types, latest DO per agent
                    
                    for row in data['rows']:
                        date_value = row.get('DATE')
                        type_value = str(row.get('TYPE', '')).strip().upper()
                        agent_no = str(row.get('AGENNO', '')).strip()
                        refno = str(row.get('REFNO', '')).strip()
                        
                        # Parse date
                        date_str = None
                        if date_value:
                            try:
                                date_str = str(date_value).strip()
                                # Handle YYYYMMDD format (8 digits)
                                if len(date_str) >= 8 and date_str[:8].isdigit():
                                    date_str = date_str[:8]
                                # Handle YYYY-MM-DD format
                                elif '-' in date_str and len(date_str) >= 10:
                                    date_parts = date_str[:10].split('-')
                                    if len(date_parts) == 3:
                                        date_str = ''.join(date_parts)
                            except Exception:
                                pass
                        
                        # Check if date is <= cutoff
                        is_old_date = date_str and date_str <= cutoff_date_str
                        
                        if type_value == 'DO' and is_old_date:
                            # DO with date <= 2025-12-12: keep for filtering to latest per agent
                            do_records.append({
                                'row': row,
                                'date_str': date_str,
                                'agent_no': agent_no,
                                'refno': refno
                            })
                        elif type_value == 'INV' and is_old_date:
                            # INV with date <= 2025-12-12: skip all
                            continue
                        else:
                            # Keep all: DO/INV with date > 2025-12-12, other types, or records without date
                            other_records.append(row)
                    
                    # For DO records with date <= 2025-12-12: Keep only latest per agent_no
                    # (DO records with date > 2025-12-12 are already in other_records and will be kept)
                    if do_records:
                        # Group by agent_no and find latest (by date DESC, then refno DESC)
                        from collections import defaultdict
                        agent_latest = defaultdict(lambda: {'date': '', 'refno': '', 'row': None})
                        
                        for do_rec in do_records:
                            agent = do_rec['agent_no']
                            date = do_rec['date_str'] or ''
                            refno = do_rec['refno']
                            
                            # Compare: latest date, then latest refno (both descending)
                            current = agent_latest[agent]
                            if (date > current['date'] or 
                                (date == current['date'] and refno > current['refno'])):
                                agent_latest[agent] = {
                                    'date': date,
                                    'refno': refno,
                                    'row': do_rec['row']
                                }
                        
                        # Add latest DO records per agent (only for dates <= 2025-12-12)
                        for agent, latest in agent_latest.items():
                            if latest['row']:
                                other_records.append(latest['row'])
                        
                        do_kept = len(agent_latest)
                        do_skipped = len(do_records) - do_kept
                        if do_skipped > 0:
                            print(f"⏭️  DO (date <= 2025-12-12): Kept {do_kept} latest record(s) per agent, skipped {do_skipped} older DO(s)", flush=True)
                    
                    data['rows'] = other_records
                    filtered_count = len(other_records)
                    skipped_count = original_count - filtered_count
                    
                    if skipped_count > 0:
                        print(f"⏭️  Filtered {skipped_count:,} record(s) ({filtered_count:,} remaining)", flush=True)
                    print(f"✅ Filtering complete: {filtered_count:,} records to sync", flush=True)
                
                # Filter ictran: Keep all records
                # Orphaned items (where parent order was deleted) will be cleaned up by database cleanup script
                elif dbf_name == 'ictran':
                    # Note: ictran items are linked to artran via REFNO
                    # Since we filter artran above, some ictran items may become orphaned
                    # These will be cleaned up by the database cleanup script that deletes:
                    # DELETE oi FROM order_items oi LEFT JOIN orders o ON oi.reference_no = o.reference_no 
                    # WHERE o.reference_no IS NULL
                    pass
                
                record_count_to_sync = len(data['rows'])
                print(f"💾 Syncing {record_count_to_sync:,} records to database...", flush=True)
                sync_to_database(file_name, data, directory_name)
                
                file_time = time.time() - file_start
                print(f"✅ {file_name} completed in {file_time:.2f}s ({record_count_to_sync:,} records)", flush=True)
                
            except Exception as e:
                print(f"❌ Error processing {file_name}: {e}", flush=True)
                import traceback
                traceback.print_exc()
                continue
            
            processed_files += 1
    
    total_time = time.time() - start_time
    print(f"\n🎉 SYNC COMPLETED!", flush=True)
    print(f"⏱️  Total time: {total_time:.2f} seconds", flush=True)
    print(f"📊 Files processed: {processed_files}/{total_files}", flush=True)
    print(f"⚡ Average per file: {total_time/processed_files:.2f}s" if processed_files > 0 else "", flush=True)


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
