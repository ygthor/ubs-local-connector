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
                    if cleaned_value == '' or cleaned_value == '00000000':
                        serialized[key] = None
                    else:
                        # For date fields, don't try to convert to numbers
                        if any(date_field in key.upper() for date_field in ['DATE', 'SODATE', 'EXPDATE', 'UPDATED_ON', 'CREATED_ON', 'GSTDATE', 'CR_AP_DATE', 'DUEDATE']):
                            serialized[key] = cleaned_value
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

def read_dbf_original(dbf_file_path):
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
                    # Safely convert field type to character, handling invalid values
                    field_type = chr(field_def[11]) if field_def[11] > 0 else '?'
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
                        # Handle records with incorrect length - pad with null bytes or truncate
                        if len(record_data) < record_length:
                            # Pad with null bytes if too short
                            record_data = record_data + b'\x00' * (record_length - len(record_data))
                        else:
                            # Truncate if too long
                            record_data = record_data[:record_length]
                        print(f"Warning: Record {i} had incorrect length ({len(record_data)} vs expected {record_length}), adjusted")
                    
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
                            elif field['type'] == 'T':  # Timestamp fields
                                # Handle timestamp fields (8 bytes: 4 bytes date + 4 bytes time)
                                try:
                                    if len(field_data) >= 8:
                                        # Extract date part (first 4 bytes)
                                        date_bytes = field_data[:4]
                                        # Extract time part (next 4 bytes)
                                        time_bytes = field_data[4:8]
                                        
                                        # Convert date bytes to date
                                        if date_bytes != b'\x00\x00\x00\x00':
                                            date_int = int.from_bytes(date_bytes, byteorder='little')
                                            # DBF date format: days since 1/1/4713 BC
                                            # Convert to Python date
                                            import datetime
                                            base_date = datetime.date(4713, 1, 1)
                                            try:
                                                date_obj = base_date + datetime.timedelta(days=date_int)
                                                record[field['name']] = date_obj
                                            except (ValueError, OverflowError):
                                                record[field['name']] = None
                                        else:
                                            record[field['name']] = None
                                        
                                        # Convert time bytes to time (if needed)
                                        if time_bytes != b'\x00\x00\x00\x00' and record[field['name']] is not None:
                                            time_int = int.from_bytes(time_bytes, byteorder='little')
                                            # Convert milliseconds to time components
                                            milliseconds = time_int
                                            seconds = milliseconds // 1000
                                            milliseconds = milliseconds % 1000
                                            
                                            hours = seconds // 3600
                                            minutes = (seconds % 3600) // 60
                                            seconds = seconds % 60
                                            
                                            try:
                                                time_obj = datetime.time(hours, minutes, seconds, milliseconds * 1000)
                                                # Combine date and time
                                                if isinstance(record[field['name']], datetime.date):
                                                    record[field['name']] = datetime.datetime.combine(record[field['name']], time_obj)
                                            except (ValueError, OverflowError):
                                                pass  # Keep just the date
                                    else:
                                        record[field['name']] = None
                                except (ValueError, OverflowError, UnicodeDecodeError):
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
                    # Suppress warnings for date field parsing issues - data is still processed
                    if "invalid literal for int()" not in str(e):
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

def read_dbf(dbf_file_path):
    """
    Read DBF file using the dbf library for proper timestamp handling - ENHANCED VERSION
    """
    try:
        # Use dbf library for proper timestamp field handling
        table = dbf.Table(dbf_file_path)
        table.open(mode=dbf.READ_ONLY)
        
        # Get field structure
        fields = []
        for field in table.field_names:
            field_info = table.field_info(field)
            # Safely convert field type to character, handling invalid values
            field_type_char = chr(field_info.field_type) if field_info.field_type > 0 else '?'
            fields.append({
                "name": field,
                "type": field_type_char,
                "size": field_info.length,
                "decs": field_info.decimal
            })
        
        # Read records - ORIGINAL WORKING METHOD
        data = []
        error_count = 0
        max_errors = 100  # Limit consecutive errors to prevent infinite loops
        
        for record in table:
            # Check if record is deleted (handle both methods)
            try:
                if hasattr(record, 'is_deleted') and record.is_deleted():
                    continue
            except:
                # If is_deleted() method doesn't exist or fails, continue
                pass
                
            record_data = {}
            for field_name in table.field_names:
                try:
                    value = getattr(record, field_name)
                    record_data[field_name] = value
                except Exception as e:
                    # Check if this is the ordinal error from dbf library
                    if "ordinal must be >= 1" in str(e):
                        # Field has invalid field type in DBF file, skip it silently
                        record_data[field_name] = None
                        continue
                    # Handle specific binary conversion issues for date fields
                    if "invalid literal for int()" in str(e) and any(date_field in field_name.upper() for date_field in ['DATE', 'SODATE', 'EXPDATE', 'UPDATED_ON', 'CREATED_ON', 'GSTDATE', 'CR_AP_DATE', 'DUEDATE']):
                        # For date fields with binary issues, try to get raw data
                        try:
                            # Get field info to determine field type
                            field_info = table.field_info(field_name)
                            # Safely convert field type to character, handling invalid values
                            field_type = chr(field_info.field_type) if field_info.field_type > 0 else '?'
                            
                            if field_type == 'D':  # Date field
                                # Try to read as raw bytes and handle null bytes
                                raw_value = getattr(record, field_name, None)
                                if raw_value is not None:
                                    # If it's bytes, clean it
                                    if isinstance(raw_value, bytes):
                                        cleaned_value = raw_value.replace(b'\x00', b'').strip()
                                        if cleaned_value and cleaned_value != b'00000000':
                                            record_data[field_name] = cleaned_value.decode('ascii', errors='ignore')
                                        else:
                                            record_data[field_name] = None
                                    else:
                                        record_data[field_name] = raw_value
                                else:
                                    record_data[field_name] = None
                            else:
                                record_data[field_name] = None
                        except:
                            record_data[field_name] = None
                    else:
                        print(f"Warning: Could not read field '{field_name}': {e}")
                        record_data[field_name] = None
            
            # Serialize the record - ORIGINAL WORKING METHOD
            try:
                serialized_record = serialize_record(record_data)
                data.append(serialized_record)
                error_count = 0  # Reset error count on successful record
            except Exception as serialize_error:
                error_count += 1
                if error_count > max_errors:
                    print(f"Too many consecutive errors ({error_count}), stopping processing to prevent infinite loop")
                    break
                print(f"Warning: Could not serialize record: {serialize_error}")
                continue
        
        table.close()
        
        return {
            "structure": fields,
            "rows": data
        }
        
    except Exception as e:
        error_msg = str(e)
        print(f"Error reading DBF file with dbf library {dbf_file_path}: {e}")
        
        # Check if it's a record length mismatch error
        if "record data is not the correct length" in error_msg:
            print("⚠️  Detected record length mismatch - this is common with corrupted DBF files")
            print("🔄 Attempting to read with enhanced error handling...")
        
        # Fallback to original method with enhanced error handling
        print("Falling back to original method with enhanced error handling...")
        return read_dbf_original(dbf_file_path)

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
