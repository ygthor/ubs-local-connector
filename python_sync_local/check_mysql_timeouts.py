#!/usr/bin/env python3
"""
Check MySQL timeout settings
"""

import os
import mysql.connector
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def check_mysql_timeouts():
    """Check current MySQL timeout settings"""
    try:
        connection = mysql.connector.connect(
            host=os.getenv("DB_HOST", "localhost"),
            user=os.getenv("DB_USER", "root"),
            password=os.getenv("DB_PASSWORD", ""),
            database=os.getenv("DB_NAME", "your_database")
        )
        
        cursor = connection.cursor()
        
        # Check timeout-related variables
        timeout_vars = [
            'wait_timeout',
            'interactive_timeout', 
            'net_read_timeout',
            'net_write_timeout',
            'connect_timeout',
            'max_allowed_packet'
        ]
        
        print("🔍 Current MySQL Timeout Settings:")
        print("=" * 50)
        
        for var in timeout_vars:
            cursor.execute(f"SHOW VARIABLES LIKE '{var}'")
            result = cursor.fetchone()
            if result:
                value = result[1]
                if 'timeout' in var:
                    # Convert seconds to hours for display
                    if value.isdigit():
                        hours = int(value) / 3600
                        print(f"{var:20}: {value} seconds ({hours:.1f} hours)")
                    else:
                        print(f"{var:20}: {value}")
                else:
                    print(f"{var:20}: {value}")
        
        print("\n📊 Recommendations:")
        print("- wait_timeout should be >= 28800 (8 hours)")
        print("- interactive_timeout should be >= 28800 (8 hours)")
        print("- net_read_timeout should be >= 600 (10 minutes)")
        print("- net_write_timeout should be >= 600 (10 minutes)")
        
        cursor.close()
        connection.close()
        
    except Exception as e:
        print(f"❌ Error checking MySQL settings: {e}")

if __name__ == "__main__":
    check_mysql_timeouts()
