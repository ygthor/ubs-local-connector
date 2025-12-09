import os
import sqlite3
import mysql.connector
from mysql.connector import Error
import pymysql

def safe_execute(cursor, query, params=None):
    """Safely execute a query and consume all results to avoid 'Unread result found' errors"""
    cursor.execute(query, params)
    # For queries that don't return results, just execute them
    # For queries that do return results, fetchone() or fetchall() will be called separately

def sync_to_database(filename, data, directory):
    """
    Create table and insert data directly to database - OPTIMIZED for large datasets
    """
    import time
    
    try:
        # Extract data components
        structures = data['structure']
        rows = data['rows']
        
        # Generate table name
        filename_base = filename.split('.')[0]
        prefix = "ubs"
        directory_name = directory.lower()
        table_name = f"{prefix}_{directory_name}_{filename_base}"
        
        # Performance monitoring
        sync_start = time.time()
        record_count = len(rows) if rows else 0
        
        # Choose your database type
        db_type = os.getenv("DB_TYPE", "mysql")  # mysql, sqlite, postgresql
        
        # Use ultra-fast method for large datasets
        if db_type == "mysql" and record_count > 10000:
            print(f"🚀 Large dataset detected ({record_count:,} records) - using ultra-fast import")
            from ultra_fast_import import ultra_fast_mysql_import
            ultra_fast_mysql_import(table_name, structures, rows)
        elif db_type == "mysql":
            sync_to_mysql(table_name, structures, rows)
        elif db_type == "sqlite":
            sync_to_sqlite(table_name, structures, rows)
        elif db_type == "postgresql":
            sync_to_postgresql(table_name, structures, rows)
        
        # Performance metrics
        sync_time = time.time() - sync_start
        if record_count > 0:
            records_per_second = record_count / sync_time
            print(f"📊 Performance: {records_per_second:.0f} records/sec ({sync_time:.2f}s for {record_count} records)")
            
    except mysql.connector.Error as e:
        print(f"❌ MySQL Error syncing data: {e}")
        print(f"   Error Code: {e.errno}")
        print(f"   SQL State: {e.sqlstate}")
        import traceback
        traceback.print_exc()
        raise
    except Exception as e:
        print(f"❌ Error syncing data: {e}")
        import traceback
        traceback.print_exc()
        raise

def sync_to_mysql(table_name, structures, rows):
    """
    Create table and insert data in MySQL - OPTIMIZED VERSION with batch operations and retry logic
    """
    import time
    
    max_retries = 3
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            # Get connection parameters
            db_host = os.getenv("DB_HOST", "localhost")
            db_user = os.getenv("DB_USER", "root")
            db_password = os.getenv("DB_PASSWORD", "")
            db_name = os.getenv("DB_NAME", "your_database")
            
            print(f"🔌 Connecting to MySQL: {db_name} @ {db_host} (user: {db_user})")
            
            # Optimize connection settings for bulk operations
            connection = mysql.connector.connect(
                host=db_host,
                user=db_user,
                password=db_password,
                database=db_name,
                autocommit=False,  # Disable autocommit for better performance
                use_unicode=True,
                charset='utf8mb4',
                # Add timeout settings to prevent connection drops
                connection_timeout=60,
                # Optimize for bulk operations
                sql_mode='NO_AUTO_VALUE_ON_ZERO',
                # Prevent "Unread result found" errors
                consume_results=True,
                init_command="SET SESSION sql_mode='NO_AUTO_VALUE_ON_ZERO'; SET SESSION wait_timeout=28800; SET SESSION interactive_timeout=28800;"
            )
            
            # Verify connection is actually working
            if not connection.is_connected():
                raise mysql.connector.Error("Connection established but not connected")
            
            print(f"✅ MySQL connection verified and ready")
            
            cursor = connection.cursor(buffered=True)

            try:
                # Test connection with a simple query
                cursor.execute("SELECT 1")
                cursor.fetchone()
                
                # Set additional timeout settings to prevent connection drops
                cursor.execute("SET SESSION wait_timeout = 28800")  # 8 hours
                cursor.execute("SET SESSION interactive_timeout = 28800")  # 8 hours
                cursor.execute("SET SESSION net_read_timeout = 600")  # 10 minutes
                cursor.execute("SET SESSION net_write_timeout = 600")  # 10 minutes
                
                print(f"✅ MySQL session configured")
                
                # Truncate table to remove old data
                cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
                result = cursor.fetchone()
                if result:
                    cursor.execute(f"TRUNCATE TABLE `{table_name}`")
                
                # Create table if not exists
                create_table_sql = generate_mysql_create_table(table_name, structures)
                cursor.execute(create_table_sql)
                
                # Insert data - OPTIMIZED CHUNKED METHOD for large datasets
                if rows:
                    insert_sql = generate_mysql_insert_sql(table_name, structures)
                    
                    # Process in smaller chunks to avoid memory issues and timeouts
                    chunk_size = 1000  # Process 1000 records at a time
                    total_rows = len(rows)
                    processed_rows = 0
                    
                    print(f"📊 Processing {total_rows:,} records in chunks of {chunk_size:,}...")
                    
                    for i in range(0, total_rows, chunk_size):
                        chunk_rows = rows[i:i + chunk_size]
                        batch_data = []
                        
                        # Prepare chunk data
                        for row in chunk_rows:
                            row_values = []
                            for struct in structures:
                                value = row.get(struct['name'])
                                # Fix packet size issue - truncate very long strings
                                if isinstance(value, str) and len(value) > 10000:
                                    value = value[:10000]  # Truncate to 10KB
                                row_values.append(value)
                            batch_data.append(row_values)
                        
                        # Insert chunk
                        try:
                            cursor.executemany(insert_sql, batch_data)
                            connection.commit()  # Commit each chunk
                            processed_rows += len(chunk_rows)
                            
                            # Progress feedback
                            progress = (processed_rows / total_rows) * 100
                            print(f"📈 Progress: {processed_rows:,}/{total_rows:,} ({progress:.1f}%)")
                            
                        except mysql.connector.Error as e:
                            if "packet" in str(e).lower() or "1153" in str(e):
                                # If packet error, truncate more aggressively and retry
                                batch_data = []
                                for row in chunk_rows:
                                    row_values = []
                                    for struct in structures:
                                        value = row.get(struct['name'])
                                        if isinstance(value, str) and len(value) > 1000:
                                            value = value[:1000]  # Truncate to 1KB
                                        row_values.append(value)
                                    batch_data.append(row_values)
                                cursor.executemany(insert_sql, batch_data)
                                connection.commit()
                                processed_rows += len(chunk_rows)
                                print(f"📈 Progress: {processed_rows:,}/{total_rows:,} ({progress:.1f}%) - Retried with smaller chunks")
                            else:
                                raise e
                    
                    print(f"✅ Successfully processed {processed_rows:,} records")
                
            finally:
                cursor.close()
                connection.close()
            
            # If we get here, the operation was successful
            break
            
        except mysql.connector.Error as e:
            retry_count += 1
            print(f"❌ MySQL Error (attempt {retry_count}/{max_retries}): {e}")
            print(f"   Error Code: {e.errno}")
            print(f"   SQL State: {e.sqlstate}")
            
            if "Lost connection" in str(e) and retry_count < max_retries:
                print(f"⚠️  Connection lost, retrying ({retry_count}/{max_retries})...")
                time.sleep(2)  # Wait before retry
                continue
            elif retry_count >= max_retries:
                print(f"❌ Max retries reached. Connection failed.")
                raise e
            else:
                raise e
        except Exception as e:
            print(f"❌ Unexpected error: {e}")
            import traceback
            traceback.print_exc()
            raise e

def sync_to_sqlite(table_name, structures, rows):
    """
    Create table and insert data in SQLite - OPTIMIZED VERSION
    """
    db_path = os.getenv("SQLITE_DB_PATH", "database.db")
    connection = sqlite3.connect(db_path)
    cursor = connection.cursor()
    
    try:
        # Create table if not exists
        create_table_sql = generate_sqlite_create_table(table_name, structures)
        cursor.execute(create_table_sql)
        
        # Insert data - OPTIMIZED BATCH METHOD
        if rows:
            insert_sql = generate_sqlite_insert_sql(table_name, structures)
            
            # Prepare all rows data in batch
            batch_data = []
            for row in rows:
                row_values = [row.get(struct['name']) for struct in structures]
                batch_data.append(row_values)
            
            # Use executemany for batch insert - MUCH FASTER!
            cursor.executemany(insert_sql, batch_data)
        
        connection.commit()
        
    finally:
        cursor.close()
        connection.close()

def generate_mysql_create_table(table_name, structures):
    """
    Generate MySQL CREATE TABLE SQL
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
    )
    """

def generate_sqlite_create_table(table_name, structures):
    """
    Generate SQLite CREATE TABLE SQL
    """
    columns = []
    
    for structure in structures:
        name = structure['name']
        type_code = structure['type']
        size = structure.get('size')
        decs = structure.get('decs')
        
        if type_code == 'D':  # Date
            column_def = f"[{name}] TEXT"
        elif type_code == 'T':  # DateTime/Timestamp
            column_def = f"[{name}] TEXT"
        elif type_code == 'C':  # Character/String
            column_def = f"[{name}] TEXT"
        elif type_code == 'N':  # Numeric
            if decs and decs > 0:
                column_def = f"[{name}] REAL"
            else:
                column_def = f"[{name}] INTEGER"
        elif type_code == 'F':  # Float
            column_def = f"[{name}] REAL"
        elif type_code == 'L':  # Logical/Boolean
            column_def = f"[{name}] INTEGER"  # SQLite doesn't have native boolean
        else:
            column_def = f"[{name}] TEXT"  # fallback
            
        columns.append(column_def)
    
    return f"""
    CREATE TABLE IF NOT EXISTS [{table_name}] (
        {', '.join(columns)}
    )
    """

def generate_mysql_insert_sql(table_name, structures):
    """
    Generate MySQL INSERT SQL
    """
    columns = [f"`{struct['name']}`" for struct in structures]
    placeholders = ["%s"] * len(structures)
    
    return f"""
    INSERT INTO `{table_name}` ({', '.join(columns)}) 
    VALUES ({', '.join(placeholders)})
    """

def generate_sqlite_insert_sql(table_name, structures):
    """
    Generate SQLite INSERT SQL
    """
    columns = [f"[{struct['name']}]" for struct in structures]
    placeholders = ["?"] * len(structures)
    
    return f"""
    INSERT INTO [{table_name}] ({', '.join(columns)}) 
    VALUES ({', '.join(placeholders)})
    """

def sync_to_postgresql(table_name, structures, rows):
    """
    Create table and insert data in PostgreSQL - OPTIMIZED VERSION
    """
    try:
        import psycopg2
    except ImportError:
        print("❌ psycopg2 library not installed. Install with: pip install psycopg2-binary")
        raise ImportError("psycopg2 library is required for PostgreSQL support")
    
    connection = psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", ""),
        database=os.getenv("DB_NAME", "your_database"),
        port=os.getenv("DB_PORT", "5432")
    )
    
    cursor = connection.cursor()
    
    try:
        # Create table if not exists
        create_table_sql = generate_postgresql_create_table(table_name, structures)
        cursor.execute(create_table_sql)
        
        # Insert data - OPTIMIZED BATCH METHOD
        if rows:
            insert_sql = generate_postgresql_insert_sql(table_name, structures)
            
            # Prepare all rows data in batch
            batch_data = []
            for row in rows:
                row_values = [row.get(struct['name']) for struct in structures]
                batch_data.append(row_values)
            
            # Use executemany for batch insert - MUCH FASTER!
            cursor.executemany(insert_sql, batch_data)
        
        connection.commit()
        
    finally:
        cursor.close()
        connection.close()

def generate_postgresql_create_table(table_name, structures):
    """
    Generate PostgreSQL CREATE TABLE SQL
    """
    columns = []
    
    for structure in structures:
        name = structure['name']
        type_code = structure['type']
        size = structure.get('size')
        decs = structure.get('decs')
        
        if type_code == 'D':  # Date
            column_def = f'"{name}" VARCHAR(255)'
        elif type_code == 'T':  # DateTime/Timestamp
            column_def = f'"{name}" VARCHAR(255)'
        elif type_code == 'C':  # Character/String
            if size and size > 255:
                column_def = f'"{name}" TEXT'
            else:
                column_def = f'"{name}" VARCHAR({size or 255})'
        elif type_code == 'N':  # Numeric
            if decs and decs > 0:
                column_def = f'"{name}" DECIMAL({size or 10}, {decs})'
            else:
                column_def = f'"{name}" INTEGER'
        elif type_code == 'F':  # Float
            column_def = f'"{name}" REAL'
        elif type_code == 'L':  # Logical/Boolean
            column_def = f'"{name}" BOOLEAN'
        else:
            column_def = f'"{name}" TEXT'  # fallback
            
        columns.append(column_def)
    
    return f'''
    CREATE TABLE IF NOT EXISTS "{table_name}" (
        {', '.join(columns)}
    )
    '''

def generate_postgresql_insert_sql(table_name, structures):
    """
    Generate PostgreSQL INSERT SQL
    """
    columns = [f'"{struct["name"]}"' for struct in structures]
    placeholders = ["%s"] * len(structures)
    
    return f'''
    INSERT INTO "{table_name}" ({', '.join(columns)}) 
    VALUES ({', '.join(placeholders)})
    '''

def create_sync_logs_table():
    try:
        connection = mysql.connector.connect(
            host=os.getenv("DB_HOST", "localhost"),
            user=os.getenv("DB_USER", "root"),
            password=os.getenv("DB_PASSWORD", ""),
            database=os.getenv("DB_NAME", "your_database"),
        )

        cursor = connection.cursor()

        create_table_sql = """
        CREATE TABLE IF NOT EXISTS `sync_logs` (
            `id` INT AUTO_INCREMENT PRIMARY KEY,
            `synced_at` DATETIME NOT NULL
        );
        """

        cursor.execute(create_table_sql)
        connection.commit()
        print("Table 'sync_logs' created or already exists.")

    except Error as e:
        print(f"Error creating table: {e}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

# Example usage:
# Replace your original sync_to_server function call with:
# sync_to_database(filename, data, directory)