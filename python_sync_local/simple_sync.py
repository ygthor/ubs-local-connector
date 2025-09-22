#!/usr/bin/env python3
"""
ULTRA-SIMPLE DBF to MySQL Sync - No Hanging
"""

import os
import time
import mysql.connector
import dbf
import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def simple_read_dbf(dbf_file_path):
    """Simple DBF reading - minimal processing"""
    try:
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
        
        # Read records - minimal processing
        data = []
        for record in table:
            record_data = {}
            for field_name in table.field_names:
                try:
                    value = getattr(record, field_name)
                    # Minimal conversion
                    if isinstance(value, (datetime.date, datetime.datetime)):
                        record_data[field_name] = value.isoformat()
                    elif isinstance(value, str):
                        record_data[field_name] = value.strip() or None
                    elif isinstance(value, bytes):
                        try:
                            record_data[field_name] = value.decode('utf-8', errors='ignore').strip() or None
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

def simple_mysql_sync(table_name, structures, rows):
    """Simple MySQL sync - no complex logic"""
    try:
        connection = mysql.connector.connect(
            host=os.getenv("DB_HOST", "localhost"),
            user=os.getenv("DB_USER", "root"),
            password=os.getenv("DB_PASSWORD", ""),
            database=os.getenv("DB_NAME", "your_database"),
            autocommit=True,  # Auto commit for speed
            use_unicode=True,
            charset='utf8mb4'
        )
        
        cursor = connection.cursor()
        
        # Truncate table
        cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
        if cursor.fetchone():
            cursor.execute(f"TRUNCATE TABLE `{table_name}`")
        
        # Create table
        create_table_sql = generate_simple_create_table(table_name, structures)
        cursor.execute(create_table_sql)
        
        # Simple insert - one by one to avoid packet issues
        if rows:
            columns = [f"`{struct['name']}`" for struct in structures]
            placeholders = ", ".join(["%s"] * len(columns))
            insert_sql = f"INSERT INTO `{table_name}` ({', '.join(columns)}) VALUES ({placeholders})"
            
            for row in rows:
                row_values = []
                for struct in structures:
                    value = row.get(struct['name'])
                    # Truncate long strings
                    if isinstance(value, str) and len(value) > 1000:
                        value = value[:1000]
                    row_values.append(value)
                
                cursor.execute(insert_sql, row_values)
        
        cursor.close()
        connection.close()
        
    except Exception as e:
        print(f"MySQL error: {e}")

def generate_simple_create_table(table_name, structures):
    """Generate simple CREATE TABLE SQL"""
    columns = []
    
    for structure in structures:
        name = structure['name']
        type_code = structure['type']
        size = structure.get('size')
        
        # Simple type mapping
        if type_code in ['D', 'T']:  # Date/DateTime
            column_def = f"`{name}` VARCHAR(255)"
        elif type_code == 'C':  # Character
            column_def = f"`{name}` TEXT"
        elif type_code == 'N':  # Numeric
            column_def = f"`{name}` DECIMAL(15,2)"
        elif type_code == 'F':  # Float
            column_def = f"`{name}` FLOAT"
        elif type_code == 'L':  # Logical
            column_def = f"`{name}` BOOLEAN"
        else:
            column_def = f"`{name}` TEXT"
            
        columns.append(column_def)
    
    return f"CREATE TABLE IF NOT EXISTS `{table_name}` ({', '.join(columns)}) ENGINE=InnoDB"

def simple_sync():
    """Simple sync - no hanging"""
    start_time = time.time()
    print("🚀 SIMPLE DBF to MySQL sync starting...")
    
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
        # Try both paths
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
            data = simple_read_dbf(full_path)
            
            if not data.get('rows'):
                print(f"⚠️  No data in {file_name}")
                continue
            
            # Generate table name
            table_name = f"ubs_{directory_name.lower()}_{dbf_name}"
            
            # Sync to MySQL
            simple_mysql_sync(table_name, data['structure'], data['rows'])
            
            file_time = time.time() - file_start
            print(f"✅ {file_name} ({len(data['rows'])} records) - {file_time:.2f}s")
            
        except Exception as e:
            print(f"❌ Error: {file_name} - {e}")
            continue
        
        processed_files += 1
    
    total_time = time.time() - start_time
    print(f"\n🎉 SIMPLE SYNC COMPLETED!")
    print(f"⏱️  Total time: {total_time:.2f} seconds")
    print(f"📊 Files processed: {processed_files}/{len(files_to_sync)}")

if __name__ == "__main__":
    simple_sync()
