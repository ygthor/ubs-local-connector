import os
import sqlite3
import mysql.connector
from mysql.connector import Error
import pymysql

def sync_to_database(filename, data, directory):
    """
    Create table and insert data directly to database
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
        
        if db_type == "mysql":
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
            
        # Reduced logging for speed - only show on errors
        
    except Exception as e:
        print(f"Error syncing data: {e}")

def sync_to_mysql(table_name, structures, rows):
    """
    Create table and insert data in MySQL - OPTIMIZED VERSION with batch operations
    """
    # Optimize connection settings for bulk operations
    connection = mysql.connector.connect(
        host=os.getenv("DB_HOST", "localhost"),
        user=os.getenv("DB_USER", "root"),
        password=os.getenv("DB_PASSWORD", ""),
        database=os.getenv("DB_NAME", "your_database"),
        autocommit=False,  # Disable autocommit for better performance
        use_unicode=True,
        charset='utf8mb4',
        # Optimize for bulk operations
        sql_mode='NO_AUTO_VALUE_ON_ZERO',
        init_command="SET SESSION sql_mode='NO_AUTO_VALUE_ON_ZERO'"
    )
    
    cursor = connection.cursor()

    try:
        # Truncate table to remove old data
        cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
        if cursor.fetchone():
            cursor.execute(f"TRUNCATE TABLE `{table_name}`")
        
        # Create table if not exists
        create_table_sql = generate_mysql_create_table(table_name, structures)
        cursor.execute(create_table_sql)
        
        # Insert data - OPTIMIZED BATCH METHOD
        if rows:
            insert_sql = generate_mysql_insert_sql(table_name, structures)
            
            # Prepare all rows data in batch
            batch_data = []
            for row in rows:
                row_values = []
                for struct in structures:
                    value = row.get(struct['name'])
                    # Fix packet size issue - truncate very long strings
                    if isinstance(value, str) and len(value) > 10000:
                        value = value[:10000]  # Truncate to 10KB
                    row_values.append(value)
                batch_data.append(row_values)
            
            # Use executemany for batch insert - MUCH FASTER!
            try:
                cursor.executemany(insert_sql, batch_data)
            except mysql.connector.Error as e:
                if "packet" in str(e).lower() or "1153" in str(e):
                    # If packet error, truncate more aggressively and retry
                    batch_data = []
                    for row in rows:
                        row_values = []
                        for struct in structures:
                            value = row.get(struct['name'])
                            if isinstance(value, str) and len(value) > 1000:
                                value = value[:1000]  # Truncate to 1KB
                            row_values.append(value)
                        batch_data.append(row_values)
                    cursor.executemany(insert_sql, batch_data)
                else:
                    raise e
        
        # Single commit for all operations - much faster
        connection.commit()
        
    finally:
        cursor.close()
        connection.close()

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
    import psycopg2
    
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
            host="localhost",
            user="root",
            password="",  # Replace with your actual password
            database="ubs_data"
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