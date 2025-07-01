import os
import sqlite3
import mysql.connector
from mysql.connector import Error
import pymysql

def sync_to_database(filename, data, directory):
    """
    Create table and insert data directly to database
    """
    try:
        # Extract data components
        structures = data['structure']
        rows = data['rows']
        
        # Generate table name
        filename_base = filename.split('.')[0]
        prefix = "ubs"
        directory_name = directory.lower()
        table_name = f"{prefix}_{directory_name}_{filename_base}"
        
        # Choose your database type
        db_type = os.getenv("DB_TYPE", "mysql")  # mysql, sqlite, postgresql
        
        if db_type == "mysql":
            sync_to_mysql(table_name, structures, rows)
        elif db_type == "sqlite":
            sync_to_sqlite(table_name, structures, rows)
        elif db_type == "postgresql":
            sync_to_postgresql(table_name, structures, rows)
            
        print(f"Successfully synced {filename} to table {table_name}")
        
    except Exception as e:
        print(f"Error syncing data: {e}")

def sync_to_mysql(table_name, structures, rows):
    """
    Create table and insert data in MySQL
    """
    connection = mysql.connector.connect(
        host=os.getenv("DB_HOST", "localhost"),
        user=os.getenv("DB_USER", "root"),
        password=os.getenv("DB_PASSWORD", ""),
        database=os.getenv("DB_NAME", "your_database")
    )
    
    cursor = connection.cursor()
    
    try:
        # Create table if not exists
        create_table_sql = generate_mysql_create_table(table_name, structures)
        cursor.execute(create_table_sql)
        
        # Insert data
        if rows:
            insert_sql = generate_mysql_insert_sql(table_name, structures)
            for row in rows:
                # Prepare row data in correct order
                row_values = [row.get(struct['name']) for struct in structures]
                cursor.execute(insert_sql, row_values)
        
        connection.commit()
        
    finally:
        cursor.close()
        connection.close()

def sync_to_sqlite(table_name, structures, rows):
    """
    Create table and insert data in SQLite
    """
    db_path = os.getenv("SQLITE_DB_PATH", "database.db")
    connection = sqlite3.connect(db_path)
    cursor = connection.cursor()
    
    try:
        # Create table if not exists
        create_table_sql = generate_sqlite_create_table(table_name, structures)
        cursor.execute(create_table_sql)
        
        # Insert data
        if rows:
            insert_sql = generate_sqlite_insert_sql(table_name, structures)
            for row in rows:
                row_values = [row.get(struct['name']) for struct in structures]
                cursor.execute(insert_sql, row_values)
        
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
    Create table and insert data in PostgreSQL
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
        
        # Insert data
        if rows:
            insert_sql = generate_postgresql_insert_sql(table_name, structures)
            for row in rows:
                row_values = [row.get(struct['name']) for struct in structures]
                cursor.execute(insert_sql, row_values)
        
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

# Example usage:
# Replace your original sync_to_server function call with:
# sync_to_database(filename, data, directory)