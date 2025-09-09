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
    serialized = {}
    for key, value in record.items():
        try:
            # Handle datetime objects
            if isinstance(value, (datetime.date, datetime.datetime)):
                serialized[key] = value.isoformat()
            # Handle None values
            elif value is None:
                serialized[key] = None
            # Handle string values that might contain null bytes
            elif isinstance(value, str):
                # Strip null bytes and whitespace
                cleaned_value = value.replace('\x00', '').strip()
                if cleaned_value == '':
                    serialized[key] = None
                else:
                    serialized[key] = cleaned_value
            # Handle bytes objects (which might contain null bytes)
            elif isinstance(value, bytes):
                # Convert bytes to string, remove null bytes, and strip
                try:
                    str_value = value.decode('utf-8', errors='ignore')
                    cleaned_value = str_value.replace('\x00', '').strip()
                    if cleaned_value == '':
                        serialized[key] = None
                    else:
                        # Try to convert to float if it looks like a number
                        try:
                            serialized[key] = float(cleaned_value)
                        except ValueError:
                            serialized[key] = cleaned_value
                except:
                    serialized[key] = None
            # Handle numeric values
            elif isinstance(value, (int, float)):
                serialized[key] = value
            # Handle other types by converting to string
            else:
                serialized[key] = str(value)
        except Exception as e:
            # If any conversion fails, set to None
            print(f"Warning: Could not serialize field '{key}' with value '{value}': {e}")
            serialized[key] = None
    
    return serialized

def read_dbf(dbf_file_path):
    try:
        # Use a custom approach to handle null bytes by reading raw data
        import struct
        
        with open(dbf_file_path, 'rb') as f:
            # Read DBF header
            header = f.read(32)
            if len(header) < 32:
                raise Exception("Invalid DBF file: header too short")
            
            # Get number of records and header length
            num_records = struct.unpack('<I', header[4:8])[0]
            header_length = struct.unpack('<H', header[8:10])[0]
            record_length = struct.unpack('<H', header[10:12])[0]
            
            # Read field definitions
            fields = []
            field_defs_length = header_length - 32 - 1  # -1 for terminator
            field_defs_data = f.read(field_defs_length)
            
            pos = 0
            while pos < len(field_defs_data) and field_defs_data[pos] != 0x0D:
                field_def = field_defs_data[pos:pos+32]
                if len(field_def) == 32:
                    field_name = field_def[:11].decode('ascii').rstrip('\x00')
                    field_type = chr(field_def[11])
                    field_length = field_def[16]
                    field_decimal = field_def[17]
                    
                    fields.append({
                        "name": field_name,
                        "type": field_type,
                        "size": field_length,
                        "decs": field_decimal
                    })
                pos += 32
            
            # Skip terminator
            f.read(1)
            
            # Read records
            data = []
            for i in range(num_records):
                try:
                    # Read record
                    record_data = f.read(record_length)
                    if len(record_data) != record_length:
                        break
                    
                    # Skip deleted flag
                    if record_data[0] == 0x2A:  # Deleted record
                        continue
                    
                    # Parse record fields
                    record = {}
                    pos = 1  # Skip deletion flag
                    
                    for field in fields:
                        field_data = record_data[pos:pos+field['size']]
                        pos += field['size']
                        
                        # Handle null bytes in field data
                        try:
                            if field['type'] in ['N', 'F']:  # Numeric fields
                                # Remove null bytes and try to convert
                                cleaned_data = field_data.replace(b'\x00', b'').strip()
                                if cleaned_data:
                                    try:
                                        value = float(cleaned_data.decode('ascii'))
                                        record[field['name']] = value
                                    except (ValueError, UnicodeDecodeError):
                                        record[field['name']] = None
                                else:
                                    record[field['name']] = None
                            elif field['type'] == 'C':  # Character fields
                                # Remove null bytes and decode
                                cleaned_data = field_data.replace(b'\x00', b'').strip()
                                if cleaned_data:
                                    try:
                                        value = cleaned_data.decode('ascii', errors='ignore')
                                        record[field['name']] = value if value else None
                                    except UnicodeDecodeError:
                                        record[field['name']] = None
                                else:
                                    record[field['name']] = None
                            elif field['type'] == 'D':  # Date fields
                                # Handle date fields
                                try:
                                    date_str = field_data.decode('ascii').strip()
                                    if date_str and date_str != '00000000':
                                        record[field['name']] = date_str
                                    else:
                                        record[field['name']] = None
                                except UnicodeDecodeError:
                                    record[field['name']] = None
                            else:
                                # Handle other field types
                                try:
                                    value = field_data.decode('ascii', errors='ignore').strip()
                                    record[field['name']] = value if value else None
                                except UnicodeDecodeError:
                                    record[field['name']] = None
                        except Exception as field_error:
                            print(f"Warning: Could not process field '{field['name']}' in record {i}: {field_error}")
                            record[field['name']] = None
                    
                    # Serialize the record
                    serialized_record = serialize_record(record)
                    data.append(serialized_record)
                    
                except Exception as record_error:
                    print(f"Warning: Could not process record {i}: {record_error}")
                    continue
            
            return {
                "structure": fields,
                "rows": data
            }
            
    except Exception as e:
        print(f"Error reading DBF file {dbf_file_path}: {e}")
        # Fallback to dbfread if custom method fails
        try:
            print(f"Trying fallback method with dbfread for {dbf_file_path}")
            dbf_table = DBF(dbf_file_path, load=True, ignore_missing_memofile=True, char_decode_errors='ignore')
            
            fields = [
                {
                    "name": field.name,
                    "type": field.type,
                    "size": field.length,
                    "decs": field.decimal_count
                }
                for field in dbf_table.fields
            ]

            data = []
            for i, record in enumerate(dbf_table):
                try:
                    serialized_record = serialize_record(record)
                    data.append(serialized_record)
                except Exception as e:
                    print(f"Warning: Could not serialize record {i} in {dbf_file_path}: {e}")
                    continue

            return {
                "structure": fields,
                "rows": data
            }
        except Exception as fallback_error:
            print(f"Fallback method also failed for {dbf_file_path}: {fallback_error}")
            return {
                "structure": [],
                "rows": []
            }

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
