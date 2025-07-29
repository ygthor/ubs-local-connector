from utils import read_dbf, sync_to_server, test_server_response
from sync_database import sync_to_database
import os
import time

def main():
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
    directory_arr = ['UBSACC2015', 'UBSSTK2015']
    dbf_arr = [
        'arcust',
        'apvend',
        'artran',

        'icarea'
        'icitem',
        'ictran',
        

        'arpay',
        'arpost',
        'gldata',
        'glbatch',
        'glpost',
        'arpso',
        'icpso'
    ]

    for directory_name in directory_arr:
        directory_path = f'C:/{directory_name}/Sample'
        for dbf_name in dbf_arr:
            file_name = dbf_name + '.dbf'
            full_path = os.path.join(directory_path, file_name)
            try:
                data = read_dbf(full_path)
                sync_to_database(file_name, data, directory_name)
            except Exception as e:
                print(f"Error processing {file_name}:", e)


def single_sync():
    directory_name = 'UBSACC2015'
    directory_path = f'C:/{directory_name}/Sample'
    file_name = 'arcust.dbf'
    full_path = os.path.join(directory_path, file_name)
    data = read_dbf(full_path)
    print(data)
    sync_to_database(file_name, data, directory_name)


if __name__ == "__main__":
    main()
