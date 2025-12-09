#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Script to check if ubs_ubsstk2015_icgroup table exists and show its details
Usage: python check_icgroup_table.py
"""

import mysql.connector
import os
from dotenv import load_dotenv

load_dotenv()

def check_table():
    db_host = os.getenv("DB_HOST", "localhost")
    db_user = os.getenv("DB_USER", "root")
    db_password = os.getenv("DB_PASSWORD", "")
    db_name = os.getenv("DB_NAME", "your_database")
    table_name = "ubs_ubsstk2015_icgroup"
    
    print(f"🔍 Checking for table '{table_name}'...")
    print(f"📊 Database: {db_name} @ {db_host} (user: {db_user})")
    print("-" * 60)
    
    try:
        connection = mysql.connector.connect(
            host=db_host,
            user=db_user,
            password=db_password,
            database=db_name
        )
        cursor = connection.cursor()
        
        # Check if database exists
        cursor.execute("SHOW DATABASES")
        databases = [db[0] for db in cursor.fetchall()]
        
        if db_name not in databases:
            print(f"❌ Database '{db_name}' does not exist!")
            print(f"📋 Available databases: {', '.join(databases)}")
            return
        
        # Check if table exists
        cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
        table_exists = cursor.fetchone()
        
        if not table_exists:
            print(f"❌ Table '{table_name}' does NOT exist in database '{db_name}'")
            
            # Show all tables that start with 'ubs_ubsstk2015'
            cursor.execute("SHOW TABLES LIKE 'ubs_ubsstk2015%'")
            similar_tables = cursor.fetchall()
            
            if similar_tables:
                print(f"\n📋 Similar tables found:")
                for table in similar_tables:
                    print(f"   - {table[0]}")
            else:
                print(f"\n📋 No tables starting with 'ubs_ubsstk2015' found")
                cursor.execute("SHOW TABLES")
                all_tables = cursor.fetchall()
                if all_tables:
                    print(f"\n📋 All tables in database '{db_name}':")
                    for table in all_tables[:20]:  # Show first 20
                        print(f"   - {table[0]}")
                    if len(all_tables) > 20:
                        print(f"   ... and {len(all_tables) - 20} more")
        else:
            print(f"✅ Table '{table_name}' EXISTS in database '{db_name}'")
            
            # Get record count
            cursor.execute(f"SELECT COUNT(*) FROM `{table_name}`")
            count = cursor.fetchone()[0]
            print(f"📊 Record count: {count} records")
            
            # Show table structure
            cursor.execute(f"DESCRIBE `{table_name}`")
            columns = cursor.fetchall()
            print(f"\n📋 Table structure ({len(columns)} columns):")
            for col in columns:
                print(f"   - {col[0]} ({col[1]})")
            
            # Show first few records
            if count > 0:
                cursor.execute(f"SELECT * FROM `{table_name}` LIMIT 5")
                records = cursor.fetchall()
                column_names = [desc[0] for desc in cursor.description]
                print(f"\n📋 First {min(5, count)} records:")
                for i, record in enumerate(records, 1):
                    print(f"   Record {i}:")
                    for j, col_name in enumerate(column_names):
                        print(f"      {col_name}: {record[j]}")
        
        cursor.close()
        connection.close()
        
    except mysql.connector.Error as e:
        print(f"❌ Database error: {e}")
        print(f"\n💡 Troubleshooting:")
        print(f"   1. Check if database '{db_name}' exists")
        print(f"   2. Check if user '{db_user}' has access")
        print(f"   3. Check your .env file settings")
        print(f"   4. Verify MySQL is running")
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    check_table()
