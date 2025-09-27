#!/usr/bin/env python3
"""
ULTRA-FAST MySQL Import for Large Datasets (50k+ records)
Optimized for maximum speed with minimal memory usage
"""

import os
import mysql.connector
from mysql.connector import Error
import time
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def ultra_fast_mysql_import(table_name, structures, rows):
    """
    Ultra-fast MySQL import optimized for large datasets (50k+ records)
    Uses LOAD DATA INFILE for maximum speed
    """
    try:
        connection = mysql.connector.connect(
            host=os.getenv("DB_HOST", "localhost"),
            user=os.getenv("DB_USER", "root"),
            password=os.getenv("DB_PASSWORD", ""),
            database=os.getenv("DB_NAME", "your_database"),
            autocommit=True,  # Enable autocommit for LOAD DATA
            use_unicode=True,
            charset='utf8mb4',
            connection_timeout=60,
            sql_mode='NO_AUTO_VALUE_ON_ZERO'
        )
        
        cursor = connection.cursor()
        
        # Set timeout settings
        cursor.execute("SET SESSION wait_timeout = 28800")
        cursor.execute("SET SESSION interactive_timeout = 28800")
        cursor.execute("SET SESSION net_read_timeout = 600")
        cursor.execute("SET SESSION net_write_timeout = 600")
        
        print(f"🚀 ULTRA-FAST Import: {len(rows):,} records to {table_name}")
        start_time = time.time()
        
        # Method 1: Try LOAD DATA INFILE (fastest method)
        try:
            import_csv_method(table_name, structures, rows, cursor)
            method_used = "CSV Import"
        except Exception as csv_error:
            print(f"⚠️  CSV method failed: {csv_error}")
            print("🔄 Falling back to chunked batch insert...")
            
            # Method 2: Chunked batch insert (fallback)
            chunked_batch_method(table_name, structures, rows, cursor)
            method_used = "Chunked Batch"
        
        elapsed_time = time.time() - start_time
        records_per_second = len(rows) / elapsed_time if elapsed_time > 0 else 0
        
        print(f"✅ Import completed!")
        print(f"📊 Method: {method_used}")
        print(f"⏱️  Time: {elapsed_time:.2f} seconds")
        print(f"🚀 Speed: {records_per_second:,.0f} records/second")
        
        cursor.close()
        connection.close()
        
    except Exception as e:
        print(f"❌ Ultra-fast import failed: {e}")
        raise e

def import_csv_method(table_name, structures, rows, cursor):
    """
    Use CSV file import method - fastest for large datasets
    """
    import csv
    import tempfile
    
    # Create temporary CSV file
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv', newline='') as csvfile:
        writer = csv.writer(csvfile)
        
        # Write data to CSV
        for row in rows:
            csv_row = []
            for struct in structures:
                value = row.get(struct['name'])
                if value is None:
                    csv_row.append('')
                elif isinstance(value, str):
                    # Escape quotes and newlines for CSV
                    value = value.replace('"', '""').replace('\n', '\\n').replace('\r', '\\r')
                    csv_row.append(f'"{value}"')
                else:
                    csv_row.append(str(value))
            writer.writerow(csv_row)
        
        csv_filename = csvfile.name
    
    try:
        # Truncate table
        cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
        result = cursor.fetchone()
        if result:
            cursor.execute(f"TRUNCATE TABLE `{table_name}`")
        
        # Create table if not exists
        create_table_sql = generate_mysql_create_table(table_name, structures)
        cursor.execute(create_table_sql)
        
        # Use LOAD DATA INFILE for ultra-fast import
        load_data_sql = f"""
        LOAD DATA INFILE '{csv_filename}'
        INTO TABLE `{table_name}`
        FIELDS TERMINATED BY ','
        ENCLOSED BY '"'
        LINES TERMINATED BY '\\n'
        IGNORE 0 ROWS
        """
        
        cursor.execute(load_data_sql)
        print(f"📁 CSV import completed: {len(rows):,} records")
        
    finally:
        # Clean up temporary file
        try:
            os.unlink(csv_filename)
        except:
            pass

def chunked_batch_method(table_name, structures, rows, cursor):
    """
    Chunked batch insert method - optimized for large datasets
    """
    # Truncate table
    cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
    result = cursor.fetchone()
    if result:
        cursor.execute(f"TRUNCATE TABLE `{table_name}`")
    
    # Create table if not exists
    create_table_sql = generate_mysql_create_table(table_name, structures)
    cursor.execute(create_table_sql)
    
    insert_sql = generate_mysql_insert_sql(table_name, structures)
    
    # Optimized chunk size based on record count
    total_rows = len(rows)
    if total_rows > 100000:
        chunk_size = 5000  # Larger chunks for very large datasets
    elif total_rows > 10000:
        chunk_size = 2000
    else:
        chunk_size = 1000
    
    processed_rows = 0
    
    for i in range(0, total_rows, chunk_size):
        chunk_rows = rows[i:i + chunk_size]
        batch_data = []
        
        # Prepare chunk data
        for row in chunk_rows:
            row_values = []
            for struct in structures:
                value = row.get(struct['name'])
                if isinstance(value, str) and len(value) > 10000:
                    value = value[:10000]
                row_values.append(value)
            batch_data.append(row_values)
        
        # Insert chunk
        cursor.executemany(insert_sql, batch_data)
        processed_rows += len(chunk_rows)
        
        # Progress feedback
        progress = (processed_rows / total_rows) * 100
        print(f"📈 Progress: {processed_rows:,}/{total_rows:,} ({progress:.1f}%)")

def generate_mysql_create_table(table_name, structures):
    """Generate MySQL CREATE TABLE statement"""
    columns = []
    for struct in structures:
        col_name = struct['name']
        col_type = struct['type']
        col_size = struct['size']
        
        if col_type == 'C':  # Character
            mysql_type = f"VARCHAR({col_size})"
        elif col_type == 'N':  # Numeric
            decimals = struct.get('decs', 0)
            if decimals > 0:
                mysql_type = f"DECIMAL({col_size},{decimals})"
            else:
                mysql_type = f"INT({col_size})"
        elif col_type == 'D':  # Date
            mysql_type = "DATE"
        elif col_type == 'L':  # Logical
            mysql_type = "BOOLEAN"
        else:
            mysql_type = "TEXT"
        
        columns.append(f"`{col_name}` {mysql_type}")
    
    return f"CREATE TABLE IF NOT EXISTS `{table_name}` ({', '.join(columns)})"

def generate_mysql_insert_sql(table_name, structures):
    """Generate MySQL INSERT statement"""
    columns = [f"`{struct['name']}`" for struct in structures]
    placeholders = ["%s"] * len(structures)
    
    return f"INSERT INTO `{table_name}` ({', '.join(columns)}) VALUES ({', '.join(placeholders)})"

if __name__ == "__main__":
    # Test with sample data
    test_structures = [
        {'name': 'id', 'type': 'N', 'size': 10, 'decs': 0},
        {'name': 'name', 'type': 'C', 'size': 50, 'decs': 0},
        {'name': 'date', 'type': 'D', 'size': 8, 'decs': 0}
    ]
    
    test_rows = [
        {'id': 1, 'name': 'Test 1', 'date': '2024-01-01'},
        {'id': 2, 'name': 'Test 2', 'date': '2024-01-02'},
    ]
    
    ultra_fast_mysql_import("test_table", test_structures, test_rows)
