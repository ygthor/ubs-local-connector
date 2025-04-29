import dbf
import os
import requests
from dbfread import DBF
from dotenv import load_dotenv
import datetime

load_dotenv()  # Load environment variables from .env file

def update_dbf_record(file_path, key_field, key_value, target_field, new_value):
    """
    Updates a field in a DBF file where a specific field matches a given value.

    :param file_path: Full path to the .dbf file
    :param key_field: Field to match on (e.g., 'name')
    :param key_value: Value to match (e.g., 'Alice')
    :param target_field: Field to update (e.g., 'balance')
    :param new_value: New value to set
    """
    table = dbf.Table(file_path)
    table.open(mode=dbf.READ_WRITE)

    updated = False
    for record in table:
        if str(record[key_field]).strip() == str(key_value).strip():
            with record:
                record[target_field] = new_value
            print(f"Record updated: {record}")
            updated = True

    table.close()

    if not updated:
        print("No matching record found.")

def serialize_record(record):
    """Convert record fields to JSON-serializable values."""
    return {
        key: (
            value.isoformat() if isinstance(value, (datetime.date, datetime.datetime))
            else value
        )
        for key, value in record.items()
    }

def read_dbf(dbf_file_path):
    try:
        dbf = DBF(dbf_file_path, load=True)
        fields = [
            {
                "name": field.name,
                "type": field.type,
                "size": field.length,
                "decs": field.decimal_count
            }
            for field in dbf.fields
        ]

        data = [serialize_record(record) for record in dbf]

        return {
            "structure": fields,
            "rows": data
        }
    except Exception as e:
        print(f"Error reading DBF file: {e}")
        raise

def test_server_response():
    url = os.getenv("SERVER_URL") + "/api/test/response"
    try:
        response = requests.post(url)
        print("Response Data:", response.json())
    except Exception as e:
        print("Error fetching data:", e)


def sync_to_server(filename, data, directory):
    url = os.getenv("SERVER_URL") + "/api/sync/local"
    try:
        response = requests.post(
            url, json={"directory": directory, "filename": filename, "data": data}
        )
        print("Response Data:", response.json())
    except Exception as e:
        print("Error syncing data:", e)
