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
            
            # "icarea",
            "icitem",

            "artran",
            "ictran",

            "arpso",
            "icpso",
        ],
    }

    dbf_subpath=os.getenv("DBF_SUBPATH", "Sample")

    for directory_name, dbf_list in grouped_dbfs.items():
        directory_path = f"C:/{directory_name}/"+dbf_subpath
        for dbf_name in dbf_list:
            file_name = dbf_name + ".dbf"
            full_path = os.path.join(directory_path, file_name)
            
            # Check if file exists before processing
            if not os.path.exists(full_path):
                print(f"Warning: File {full_path} does not exist, skipping...")
                continue
                
            try:
                print(f"Processing {file_name}...")
                data = read_dbf(full_path)
                
                # Check if we got valid data
                if not data or not data.get('structure') or not data.get('rows'):
                    print(f"Warning: No valid data found in {file_name}, skipping...")
                    continue
                    
                sync_to_database(file_name, data, directory_name)
            except Exception as e:
                print(f"Error processing {file_name}: {e}")
                # Continue with next file instead of stopping
                continue


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
