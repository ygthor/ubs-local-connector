#!/usr/bin/env python3
"""
ULTRA-FAST DBF to MySQL Sync
Optimized for speed - should complete within 1 minute
"""

import os
import time
import mysql.connector
from mysql.connector import Error
import dbf
import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def ultra_fast_read_dbf(dbf_file_path):
    """
    Ultra-fast DBF reading with minimal processing
    """
    try:
        table = dbf.Table(dbf_file_path)
        # Handle memo file issues
        try:
            table.open(mode=dbf.READ_ONLY)
        except Exception as memo_error:
            if "memo file" in str(memo_error).lower():
                print(f"⚠️  Memo file issue in {os.path.basename(dbf_file_path)}, skipping...")
                return {"structure": [], "rows": []}
            else:
                raise memo_error
        
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
        
        # ULTRA-FAST: Read all records at once
        data = []
        for record in table:
            # Skip deleted records check for speed
                
            record_data = {}
            for field_name in table.field_names:
                try:
                    value = getattr(record, field_name)
                    # MINIMAL PROCESSING - only essential conversions
                    if isinstance(value, (datetime.date, datetime.datetime)):
                        record_data[field_name] = value.isoformat()
                    elif value is None:
                        record_data[field_name] = None
                    elif isinstance(value, str):
                        # Only clean if necessary
                        if '\x00' in value:
                            record_data[field_name] = value.replace('\x00', '').strip() or None
                        else:
                            record_data[field_name] = value.strip() or None
                    elif isinstance(value, bytes):
                        try:
                            str_value = value.decode('utf-8', errors='ignore')
                            record_data[field_name] = str_value.replace('\x00', '').strip() or None
                        except:
                            record_data[field_name] = None
                    else:
                        record_data[field_name] = value
                except:
                    record_data[field_name] = None
            
            data.append(record_data)
        
        table.close()
        return {"structure": fields, "rows": data}
        
    except Exception as e:
        print(f"Error reading {dbf_file_path}: {e}")
        return {"structure": [], "rows": []}

def ultra_fast_mysql_sync(table_name, structures, rows):
    """
    Ultra-fast MySQL sync with robust packet size handling
    """
    connection = mysql.connector.connect(
        host=os.getenv("DB_HOST", "localhost"),
        user=os.getenv("DB_USER", "root"),
        password=os.getenv("DB_PASSWORD", ""),
        database=os.getenv("DB_NAME", "your_database"),
        autocommit=False,
        use_unicode=True,
        charset='utf8mb4',
        # Performance optimizations
        sql_mode=''
    )
    
    cursor = connection.cursor()

    try:
        # Set MySQL session variables for better performance
        cursor.execute("SET SESSION sql_mode = ''")
        
        # Truncate table
        cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
        if cursor.fetchone():
            cursor.execute(f"TRUNCATE TABLE `{table_name}`")
        
        # Create table
        create_table_sql = generate_ultra_fast_create_table(table_name, structures)
        cursor.execute(create_table_sql)
        
        # SIMPLE BULK INSERT - no complex adaptive logic
        if rows:
            # Prepare insert SQL
            columns = [f"`{struct['name']}`" for struct in structures]
            placeholders = ", ".join(["%s"] * len(columns))
            insert_sql = f"INSERT INTO `{table_name}` ({', '.join(columns)}) VALUES ({placeholders})"
            
            # Simple chunking - fixed size to avoid hanging
            chunk_size = 500  # Fixed chunk size
            
            for i in range(0, len(rows), chunk_size):
                chunk = rows[i:i + chunk_size]
                batch_data = []
                
                # Prepare chunk data with size limits
                for row in chunk:
                    row_values = []
                    for struct in structures:
                        value = row.get(struct['name'])
                        # Truncate very long strings to prevent packet issues
                        if isinstance(value, str) and len(value) > 5000:
                            value = value[:5000]
                        row_values.append(value)
                    batch_data.append(row_values)
                
                # Insert chunk - simple approach
                try:
                    cursor.executemany(insert_sql, batch_data)
                except mysql.connector.Error as e:
                    if "packet" in str(e).lower() or "1153" in str(e):
                        # If packet error, try smaller chunks
                        smaller_chunk_size = 50
                        for j in range(0, len(batch_data), smaller_chunk_size):
                            smaller_chunk = batch_data[j:j + smaller_chunk_size]
                            cursor.executemany(insert_sql, smaller_chunk)
                    else:
                        raise e
        
        connection.commit()
        
    finally:
        cursor.close()
        connection.close()

def generate_ultra_fast_create_table(table_name, structures):
    """
    Generate optimized CREATE TABLE SQL
    """
    columns = []
    
    for structure in structures:
        name = structure['name']
        type_code = structure['type']
        size = structure.get('size')
        decs = structure.get('decs')
        
        # Simplified type mapping for speed
        if type_code in ['D', 'T']:  # Date/DateTime
            column_def = f"`{name}` VARCHAR(255)"
        elif type_code == 'C':  # Character
            column_def = f"`{name}` TEXT" if size and size > 255 else f"`{name}` VARCHAR({size or 255})"
        elif type_code == 'N':  # Numeric
            column_def = f"`{name}` DECIMAL({size or 10}, {decs})" if decs and decs > 0 else f"`{name}` INT"
        elif type_code == 'F':  # Float
            column_def = f"`{name}` FLOAT"
        elif type_code == 'L':  # Logical
            column_def = f"`{name}` BOOLEAN"
        else:
            column_def = f"`{name}` TEXT"
            
        columns.append(column_def)
    
    return f"CREATE TABLE IF NOT EXISTS `{table_name}` ({', '.join(columns)}) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"

def ultra_fast_sync():
    """
    Ultra-fast sync of all DBF files
    """
    start_time = time.time()
    print("🚀 ULTRA-FAST DBF to MySQL sync starting...")
    
    # Define files to sync
    files_to_sync = [
        ("UBSACC2015", "arcust"),
        ("UBSACC2015", "apvend"),
        ("UBSACC2015", "arpay"),
        ("UBSACC2015", "arpost"),
        ("UBSACC2015", "gldata"),
        ("UBSACC2015", "glbatch"),
        ("UBSACC2015", "glpost"),
        ("UBSSTK2015", "icitem"),
        ("UBSSTK2015", "artran"),
        ("UBSSTK2015", "ictran"),
        ("UBSSTK2015", "arpso"),
        ("UBSSTK2015", "icpso"),
    ]
    
    dbf_subpath = os.getenv("DBF_SUBPATH", "Sample")
    processed_files = 0
    
    for directory_name, dbf_name in files_to_sync:
        file_name = dbf_name + ".dbf"
        # Try both paths: Sample/TESTMODE and Sample
        full_path = f"C:/{directory_name}/{dbf_subpath}/TESTMODE/{file_name}"
        if not os.path.exists(full_path):
            full_path = f"C:/{directory_name}/{dbf_subpath}/{file_name}"
        
        if not os.path.exists(full_path):
            print(f"⚠️  {file_name} not found, skipping...")
            continue
            
        try:
            file_start = time.time()
            print(f"📁 [{processed_files+1}/{len(files_to_sync)}] {file_name}...")
            
            # Read DBF
            data = ultra_fast_read_dbf(full_path)
            
            if not data.get('rows'):
                print(f"⚠️  No data in {file_name}")
                continue
            
            # Generate table name
            table_name = f"ubs_{directory_name.lower()}_{dbf_name}"
            
            # Sync to MySQL
            ultra_fast_mysql_sync(table_name, data['structure'], data['rows'])
            
            file_time = time.time() - file_start
            print(f"✅ {file_name} ({len(data['rows'])} records) - {file_time:.2f}s")
            
        except Exception as e:
            print(f"❌ Error: {file_name} - {e}")
            continue
        
        processed_files += 1
    
    total_time = time.time() - start_time
    print(f"\n🎉 ULTRA-FAST SYNC COMPLETED!")
    print(f"⏱️  Total time: {total_time:.2f} seconds")
    print(f"📊 Files processed: {processed_files}/{len(files_to_sync)}")
    print(f"⚡ Speed: {processed_files/total_time:.1f} files/second" if total_time > 0 else "")

if __name__ == "__main__":
    ultra_fast_sync()
