#!/usr/bin/env python3
"""
Update MySQL timeout settings
"""

import os
import mysql.connector
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def update_mysql_timeouts():
    """Update MySQL timeout settings for better DBF sync performance"""
    try:
        connection = mysql.connector.connect(
            host=os.getenv("DB_HOST", "localhost"),
            user=os.getenv("DB_USER", "root"),
            password=os.getenv("DB_PASSWORD", ""),
            database=os.getenv("DB_NAME", "your_database")
        )
        
        cursor = connection.cursor()
        
        print("🔧 Updating MySQL Timeout Settings...")
        print("=" * 50)
        
        # Update timeout settings
        timeout_updates = {
            'wait_timeout': 28800,        # 8 hours
            'interactive_timeout': 28800,  # 8 hours  
            'net_read_timeout': 600,      # 10 minutes
            'net_write_timeout': 600,     # 10 minutes
            'max_allowed_packet': 1073741824  # 1GB
        }
        
        for var, value in timeout_updates.items():
            try:
                cursor.execute(f"SET GLOBAL {var} = {value}")
                print(f"✅ Updated {var} to {value}")
            except mysql.connector.Error as e:
                if "Access denied" in str(e):
                    print(f"⚠️  Cannot update {var} - insufficient privileges")
                    print(f"   You may need to run this as MySQL root user")
                else:
                    print(f"❌ Error updating {var}: {e}")
        
        # Also set session-level timeouts
        print("\n🔧 Setting Session-Level Timeouts...")
        session_updates = {
            'wait_timeout': 28800,
            'interactive_timeout': 28800,
            'net_read_timeout': 600,
            'net_write_timeout': 600
        }
        
        for var, value in session_updates.items():
            try:
                cursor.execute(f"SET SESSION {var} = {value}")
                print(f"✅ Set session {var} to {value}")
            except mysql.connector.Error as e:
                print(f"❌ Error setting session {var}: {e}")
        
        print("\n📋 Manual Configuration Instructions:")
        print("If you have access to MySQL configuration file (my.cnf or my.ini), add these lines:")
        print("[mysqld]")
        print("wait_timeout = 28800")
        print("interactive_timeout = 28800") 
        print("net_read_timeout = 600")
        print("net_write_timeout = 600")
        print("max_allowed_packet = 1G")
        print("\nThen restart MySQL service.")
        
        cursor.close()
        connection.close()
        
    except Exception as e:
        print(f"❌ Error updating MySQL settings: {e}")

if __name__ == "__main__":
    update_mysql_timeouts()
