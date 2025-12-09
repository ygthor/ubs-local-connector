#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Script to create ubs_ubsstk2015_icgroup table based on icgroup.dbf structure
Usage: python create_icgroup_table.py
"""

import mysql.connector
import os
import sys
from dotenv import load_dotenv
from utils import read_dbf

load_dotenv()

def generate_mysql_create_table(table_name, structures):
    """
    Generate MySQL CREATE TABLE SQL based on DBF structure
    """
    columns = []
    
    for structure in structures:
        name = structure['name']
        type_code = structure['type']
        size = structure.get('size')
        decs = structure.get('decs')
        
        if type_code == 'D':  # Date
            column_def = f"`{name}` VARCHAR(255)"
        elif type_code == 'T':  # DateTime/Timestamp
            column_def = f"`{name}` VARCHAR(255)"
        elif type_code == 'C':  # Character/String
            if size and size > 255:
                column_def = f"`{name}` TEXT"
            else:
                column_def = f"`{name}` VARCHAR({size or 255})"
        elif type_code == 'N':  # Numeric
            if decs and decs > 0:
                column_def = f"`{name}` DECIMAL({size or 10}, {decs})"
            else:
                column_def = f"`{name}` INT"
        elif type_code == 'F':  # Float
            column_def = f"`{name}` FLOAT"
        elif type_code == 'L':  # Logical/Boolean
            column_def = f"`{name}` BOOLEAN"
        else:
            column_def = f"`{name}` TEXT"  # fallback
            
        columns.append(column_def)
    
    return f"""
    CREATE TABLE IF NOT EXISTS `{table_name}` (
        {', '.join(columns)}
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """

def main():
    print("🚀 Creating icgroup table from DBF structure...")
    
    # Get DBF file path
    directory_name = "UBSSTK2015"
    dbf_subpath = os.getenv("DBF_SUBPATH", "Sample")
    directory_path = f"C:/{directory_name}/{dbf_subpath}"
    file_name = "icgroup.dbf"
    full_path = os.path.join(directory_path, file_name)
    
    # Check if file exists
    if not os.path.exists(full_path):
        print(f"❌ File {full_path} not found!")
        print(f"💡 Please check if the file exists at: {full_path}")
        sys.exit(1)
    
    # Get database connection info
    db_host = os.getenv("DB_HOST", "localhost")
    db_user = os.getenv("DB_USER", "root")
    db_password = os.getenv("DB_PASSWORD", "")
    db_name = os.getenv("DB_NAME", "your_database")
    table_name = "ubs_ubsstk2015_icgroup"
    
    print(f"📊 Database: {db_name} @ {db_host} (user: {db_user})")
    print(f"📁 Reading DBF structure from: {full_path}")
    
    try:
        # Read DBF structure
        print("📖 Reading DBF file structure...")
        data = read_dbf(full_path)
        
        if not data or not data.get('structure'):
            print(f"❌ Could not read DBF structure from {file_name}")
            sys.exit(1)
        
        structures = data['structure']
        print(f"✅ Found {len(structures)} fields in DBF file:")
        for struct in structures:
            print(f"   - {struct['name']} ({struct['type']}, size: {struct.get('size', 'N/A')})")
        
        # Generate CREATE TABLE SQL
        print(f"\n🔨 Generating CREATE TABLE SQL...")
        create_table_sql = generate_mysql_create_table(table_name, structures)
        
        print(f"\n📝 SQL to execute:")
        print("-" * 60)
        print(create_table_sql)
        print("-" * 60)
        
        # Connect to MySQL
        print(f"\n🔌 Connecting to MySQL...")
        connection = mysql.connector.connect(
            host=db_host,
            user=db_user,
            password=db_password,
            database=db_name
        )
        
        if not connection.is_connected():
            print("❌ Failed to connect to MySQL")
            sys.exit(1)
        
        print(f"✅ Connected to MySQL")
        
        cursor = connection.cursor()
        
        try:
            # Check if table exists
            cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
            table_exists = cursor.fetchone()
            
            if table_exists:
                print(f"\n⚠️  Table '{table_name}' already exists!")
                response = input("   Do you want to DROP and recreate it? (yes/no): ")
                if response.lower() == 'yes':
                    print(f"🗑️  Dropping existing table...")
                    cursor.execute(f"DROP TABLE `{table_name}`")
                    connection.commit()
                    print(f"✅ Table dropped")
                else:
                    print(f"ℹ️  Keeping existing table. Exiting.")
                    cursor.close()
                    connection.close()
                    sys.exit(0)
            
            # Create table
            print(f"\n🔨 Creating table '{table_name}'...")
            cursor.execute(create_table_sql)
            connection.commit()
            print(f"✅ Table '{table_name}' created successfully!")
            
            # Show table structure
            cursor.execute(f"DESCRIBE `{table_name}`")
            columns = cursor.fetchall()
            print(f"\n📋 Table structure ({len(columns)} columns):")
            for col in columns:
                print(f"   - {col[0]} ({col[1]})")
            
            # Get record count from DBF
            rows = data.get('rows', [])
            record_count = len(rows) if rows else 0
            print(f"\n📊 DBF file contains {record_count} records")
            
            if record_count > 0:
                response = input(f"\n   Do you want to insert {record_count} records now? (yes/no): ")
                if response.lower() == 'yes':
                    from sync_database import sync_to_mysql
                    print(f"\n📥 Inserting records...")
                    sync_to_mysql(table_name, structures, rows)
                    print(f"✅ Records inserted successfully!")
                else:
                    print(f"ℹ️  Table created. You can sync data later using: python sync_icgroup.py")
            
        finally:
            cursor.close()
            connection.close()
        
        print(f"\n🎉 Done! Table '{table_name}' is ready in database '{db_name}'")
        
    except mysql.connector.Error as e:
        print(f"❌ MySQL Error: {e}")
        print(f"   Error Code: {e.errno}")
        print(f"   SQL State: {e.sqlstate}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
