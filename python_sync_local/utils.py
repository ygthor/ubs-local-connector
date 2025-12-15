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

def should_skip_record_by_date(record_data, cutoff_date_str, date_field_names=None):
    """
    Check if a record should be skipped based on date field.
    Returns True if record should be skipped (date is before cutoff), False otherwise.
    
    Args:
        record_data: Dictionary of record field values
        cutoff_date_str: Cutoff date in YYYYMMDD format (e.g., '20251201')
        date_field_names: List of date field names to check in priority order. 
                         If None, checks 'DATE' first, then other common date fields.
    
    Returns:
        True if record should be skipped, False if it should be kept
    """
    if cutoff_date_str is None:
        return False
    
    if date_field_names is None:
        # Priority order: check 'DATE' first (most common primary date field)
        # Then check other date fields if DATE is not available
        date_field_names = ['DATE', 'SODATE', 'EXPDATE', 'GSTDATE', 'CR_AP_DATE', 'DUEDATE', 'UPDATED_ON', 'CREATED_ON']
    
    def parse_date_value(date_value):
        """Parse date value to YYYYMMDD format string, or None if parsing fails."""
        if date_value is None:
            return None
        
        try:
            if isinstance(date_value, (datetime.date, datetime.datetime)):
                # Convert datetime/date objects to YYYYMMDD format
                return date_value.strftime('%Y%m%d')
            elif isinstance(date_value, str):
                date_str = date_value.strip()
                # Handle YYYYMMDD format (8 digits)
                if len(date_str) >= 8 and date_str[:8].isdigit():
                    return date_str[:8]
                # Handle YYYY-MM-DD format
                elif '-' in date_str and len(date_str) >= 10:
                    date_parts = date_str[:10].split('-')
                    if len(date_parts) == 3:
                        return ''.join(date_parts)
            elif isinstance(date_value, bytes):
                # Handle bytes date values
                try:
                    date_str = date_value.decode('ascii', errors='ignore').strip()
                    if len(date_str) >= 8 and date_str[:8].isdigit():
                        return date_str[:8]
                except:
                    pass
        except Exception:
            pass
        
        return None
    
    # Check date fields in priority order
    # If primary date field (DATE) is found and is before cutoff, skip
    # If no date field found or date is >= cutoff, keep the record
    for field_name in date_field_names:
        date_value = record_data.get(field_name)
        if date_value is None:
            continue
        
        date_str = parse_date_value(date_value)
        if date_str and len(date_str) >= 8 and date_str[:8].isdigit():
            # If date is before cutoff, skip this record
            if date_str[:8] < cutoff_date_str:
                return True
            # If date is >= cutoff, keep the record (don't skip)
            # For primary date field (DATE), we can return immediately
            if field_name == 'DATE':
                return False
    
    # If no valid date field found, keep the record (don't skip - be conservative)
    return False


def serialize_record_fast(record_data, date_fields_cache=None):
    """
    Optimized version of serialize_record - inlined and faster
    Reduces function call overhead and string operations
    """
    if date_fields_cache is None:
        # Cache date field names for faster lookup
        date_fields_cache = frozenset(['DATE', 'SODATE', 'EXPDATE', 'UPDATED_ON', 'CREATED_ON', 'GSTDATE', 'CR_AP_DATE', 'DUEDATE'])
    
    serialized = {}
    for key, value in record_data.items():
        # Fast path for None
        if value is None:
            serialized[key] = None
            continue
        
        # Fast path for datetime objects
        if isinstance(value, (datetime.date, datetime.datetime)):
            serialized[key] = value.isoformat()
            continue
        
        # Fast path for numeric values
        if isinstance(value, (int, float)):
            serialized[key] = value
            continue
        
        # Handle strings - optimized
        if isinstance(value, str):
            # Only process if contains null bytes or needs cleaning
            if '\x00' in value:
                cleaned = value.replace('\x00', '').strip()
                serialized[key] = None if cleaned == '' else cleaned
            else:
                cleaned = value.strip()
                serialized[key] = None if cleaned == '' else cleaned
            continue
        
        # Handle bytes - optimized
        if isinstance(value, bytes):
            try:
                str_value = value.decode('utf-8', errors='ignore')
                cleaned = str_value.replace('\x00', '').strip()
                if cleaned == '' or cleaned == '00000000':
                    serialized[key] = None
                else:
                    # Check if it's a date field - use cached lookup
                    key_upper = key.upper()
                    is_date_field = any(df in key_upper for df in date_fields_cache)
                    if is_date_field:
                        serialized[key] = cleaned
                    else:
                        # Try to convert to float if numeric
                        try:
                            serialized[key] = float(cleaned)
                        except ValueError:
                            serialized[key] = cleaned
            except:
                serialized[key] = None
            continue
        
        # Fallback for other types
        serialized[key] = str(value)
    
    return serialized


def read_dbf(dbf_file_path, progress_callback=None, skip_before_date=None):
    """
    Read DBF file using the dbf library for proper timestamp handling - OPTIMIZED VERSION
    with progress reporting support and performance improvements
    
    Args:
        dbf_file_path: Path to the DBF file
        progress_callback: Optional callback function(records_read, status_message) called periodically
        skip_before_date: Optional date string in YYYYMMDD format (e.g., '20251201'). 
                         Records with date fields before this date will be skipped early for better performance.
    """
    import sys
    import time
    
    try:
        # Get file size for progress estimation
        file_size = os.path.getsize(dbf_file_path) if os.path.exists(dbf_file_path) else 0
        file_size_mb = file_size / (1024 * 1024) if file_size > 0 else 0
        
        if file_size_mb > 0:
            print(f"📊 File size: {file_size_mb:.2f} MB", flush=True)
        print(f"🔍 Opening DBF file...", flush=True)
        
        # Use dbf library for proper timestamp field handling
        table = dbf.Table(dbf_file_path)
        table.open(mode=dbf.READ_ONLY)
        
        # Get field structure
        print(f"📋 Reading field structure...", flush=True)
        fields = []
        field_names = table.field_names  # Cache field names to avoid repeated lookups
        date_fields_cache = frozenset(['DATE', 'SODATE', 'EXPDATE', 'UPDATED_ON', 'CREATED_ON', 'GSTDATE', 'CR_AP_DATE', 'DUEDATE'])
        
        # Pre-build field info cache for faster access
        field_info_cache = {}
        for field in field_names:
            field_info = table.field_info(field)
            field_type_char = chr(field_info.field_type) if field_info.field_type > 0 else '?'
            fields.append({
                "name": field,
                "type": field_type_char,
                "size": field_info.length,
                "decs": field_info.decimal
            })
            field_info_cache[field] = {
                'type': field_type_char,
                'info': field_info
            }
        
        print(f"✅ Found {len(fields)} fields, starting to read records...", flush=True)
        
        # Date filtering setup
        if skip_before_date:
            print(f"⏭️  Date filtering enabled: Skipping records before {skip_before_date}", flush=True)
        
        # Read records - OPTIMIZED VERSION
        data = []
        error_count = 0
        max_errors = 100  # Limit consecutive errors to prevent infinite loops
        skipped_count = 0  # Track skipped records for reporting
        
        # Progress reporting variables
        records_read = 0
        last_progress_time = time.time()
        progress_interval = 2.0  # Report progress every 2 seconds
        progress_record_interval = 5000  # Report progress every 5000 records (increased for better performance)
        
        start_time = time.time()
        
        # Optimized record reading loop
        for record in table:
            # Check if record is deleted - optimized (check once per record)
            try:
                if hasattr(record, 'is_deleted') and record.is_deleted():
                    continue
            except:
                pass
            
            # Build record data - optimized: cache field_names to avoid repeated lookups
            record_data = {}
            for field_name in field_names:
                try:
                    value = getattr(record, field_name)
                    record_data[field_name] = value
                except Exception as e:
                    error_str = str(e)
                    # Check if this is the ordinal error from dbf library
                    if "ordinal must be >= 1" in error_str:
                        record_data[field_name] = None
                        continue
                    # Handle specific binary conversion issues for date fields
                    if "invalid literal for int()" in error_str:
                        key_upper = field_name.upper()
                        if any(df in key_upper for df in date_fields_cache):
                            # For date fields with binary issues, try to get raw data
                            try:
                                field_type = field_info_cache[field_name]['type']
                                if field_type == 'D':  # Date field
                                    raw_value = getattr(record, field_name, None)
                                    if raw_value is not None:
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
                            record_data[field_name] = None
                    else:
                        # Only print warning for non-expected errors
                        if error_count < 10:  # Limit warning spam
                            print(f"Warning: Could not read field '{field_name}': {e}", flush=True)
                        record_data[field_name] = None
            
            # Early date filtering - skip old records before expensive serialization
            if skip_before_date:
                # Convert date_fields_cache frozenset to list for the function
                date_field_list = list(date_fields_cache) if date_fields_cache else None
                if should_skip_record_by_date(record_data, skip_before_date, date_field_list):
                    skipped_count += 1
                    # Report skipped count periodically
                    if skipped_count % 10000 == 0:
                        print(f"⏭️  Skipped {skipped_count:,} records before {skip_before_date}", flush=True)
                    continue
            
            # Serialize the record - using optimized fast version
            try:
                serialized_record = serialize_record_fast(record_data, date_fields_cache)
                data.append(serialized_record)
                error_count = 0  # Reset error count on successful record
                
                records_read += 1
                
                # Progress reporting: every N records or every few seconds
                if records_read % progress_record_interval == 0:
                    current_time = time.time()
                    elapsed = current_time - start_time
                    rate = records_read / elapsed if elapsed > 0 else 0
                    status = f"📥 Reading records: {records_read:,} read ({rate:.0f} records/sec)"
                    
                    if progress_callback:
                        progress_callback(records_read, status)
                    else:
                        print(status, flush=True)
                    
                    last_progress_time = current_time
                elif time.time() - last_progress_time >= progress_interval:
                    current_time = time.time()
                    elapsed = current_time - start_time
                    rate = records_read / elapsed if elapsed > 0 else 0
                    status = f"📥 Reading records: {records_read:,} read ({rate:.0f} records/sec)"
                    
                    if progress_callback:
                        progress_callback(records_read, status)
                    else:
                        print(status, flush=True)
                    
                    last_progress_time = current_time
                    
            except Exception as serialize_error:
                error_count += 1
                if error_count > max_errors:
                    print(f"Too many consecutive errors ({error_count}), stopping processing to prevent infinite loop", flush=True)
                    break
                if error_count <= 10:  # Limit error spam
                    print(f"Warning: Could not serialize record: {serialize_error}", flush=True)
                continue
        
        # Final progress report
        elapsed_total = time.time() - start_time
        rate_total = records_read / elapsed_total if elapsed_total > 0 else 0
        print(f"✅ Read {records_read:,} records in {elapsed_total:.2f}s ({rate_total:.0f} records/sec)", flush=True)
        if skip_before_date and skipped_count > 0:
            print(f"⏭️  Skipped {skipped_count:,} records before {skip_before_date} (performance optimization)", flush=True)
        
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
    """
    Sync data to remote server/UBS.
    Automatically skips DO type orders (DO normally only insert from UBS to server, not synced back).
    """
    # Filter out DO type orders for artran (for remote sync to UBS)
    if filename.lower() == 'artran.dbf' and data and data.get('rows'):
        original_count = len(data['rows'])
        filtered_rows = []
        do_skipped = 0
        
        for row in data['rows']:
            type_value = str(row.get('TYPE', '')).strip().upper()
            if type_value == 'DO':
                do_skipped += 1
                continue
            filtered_rows.append(row)
        
        if do_skipped > 0:
            print(f"⏭️  Skipped {do_skipped:,} DO type orders when syncing to remote (DO normally only insert from UBS to server)", flush=True)
            data['rows'] = filtered_rows
    
    url = os.getenv("SERVER_URL") + "/api/sync/local"
    try:
        response = requests.post(
            url, json={"directory": directory, "filename": filename, "data": data}
        )
        print("Response Data:", response.json())
    except Exception as e:
        print("Error syncing data:", e)
