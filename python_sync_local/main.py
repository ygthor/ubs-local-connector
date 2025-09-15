from utils import read_dbf, sync_to_server, test_server_response
from sync_database import create_sync_logs_table, sync_to_database
import os
import time


def main():

    create_sync_logs_table()

    # test_server_response()
    sync_all()

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
            "artran",
            "ictran",
            "arpso",
            "icpso",
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


if __name__ == "__main__":
    main()
