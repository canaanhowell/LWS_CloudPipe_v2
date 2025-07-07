#!/usr/bin/env python3
"""
Recreate Snowflake tables using original CSV column names from mapping files
This ensures the Snowflake tables match the CSV headers exactly.
"""
import os
import sys
import glob
import json
from pathlib import Path
import snowflake.connector
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

def load_settings():
    """Load settings from settings.json"""
    try:
        with open('settings.json', 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print("settings.json not found")
        sys.exit(1)

def get_snowflake_connection():
    """Get Snowflake connection using settings.json"""
    settings = load_settings()
    private_key_path = settings.get('SNOWFLAKE_PRIVATE_KEY_PATH', 'config_files/snowflake_private_key.txt')
    with open(private_key_path, 'rb') as key_file:
        p_key = serialization.load_pem_private_key(
            key_file.read(),
            password=None,
            backend=default_backend()
        )
    pkb = p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )
    conn = snowflake.connector.connect(
        account=settings['SNOWFLAKE_ACCOUNT'],
        user=settings['SNOWFLAKE_USER'],
        private_key=pkb,
        warehouse=settings['SNOWFLAKE_WAREHOUSE'],
        database=settings['SNOWFLAKE_DATABASE']
    )
    return conn

def create_table_with_original_names(cursor, mapping):
    """Create table using original CSV column names"""
    table_full = mapping.get('table_name')
    columns = mapping['column_mappings']
    
    if not table_full or not columns:
        print(f"[ERROR] Invalid mapping: {mapping}")
        return False
    
    # Parse database, schema, and table name
    table_parts = table_full.split('.')
    if len(table_parts) == 3:
        database, schema, table_name = table_parts
    else:
        print(f"[ERROR] Invalid table name format: {table_full}")
        return False
    
    # Set context
    cursor.execute(f"USE DATABASE {database}")
    cursor.execute(f"USE SCHEMA {schema}")
    
    # Build columns using original CSV names
    col_defs = []
    for col_key, col_info in columns.items():
        original_name = col_info.get('original_name', col_key)
        snowflake_type = col_info.get('snowflake_type', 'VARCHAR(255)')
        # Use original name in quotes to handle special characters
        col_defs.append(f'"{original_name}" {snowflake_type}')
    
    create_sql = f'CREATE OR REPLACE TABLE {table_name} (\n    ' + ',\n    '.join(col_defs) + '\n)'
    
    print(f"[INFO] Creating table {database}.{schema}.{table_name} with original column names...")
    print(f"[INFO] SQL: {create_sql}")
    
    try:
        cursor.execute(create_sql)
        print(f"[SUCCESS] Created {database}.{schema}.{table_name}")
        return True
    except Exception as e:
        print(f"[ERROR] Failed to create {database}.{schema}.{table_name}: {e}")
        return False

def main():
    """Main function to recreate tables with original column names"""
    print("Starting table recreation with original CSV column names...")
    
    # Load table mapping to get the correct table names
    with open('config_files/table_mapping.json', 'r') as f:
        table_mapping = json.load(f)
    
    # Create a mapping of table names to their mapping files
    table_to_mapping_file = {}
    logs_dir = Path(__file__).parent.parent / 'logs'
    mapping_files = glob.glob(str(logs_dir / 'column_type_mapping_*.json'))
    
    for mf in mapping_files:
        with open(mf, 'r', encoding='utf-8') as f:
            mapping = json.load(f)
            table_name = mapping.get('table_name')
            if table_name:
                table_to_mapping_file[table_name] = mf
    
    if not mapping_files:
        print("No mapping files found in logs directory.")
        return 1
    
    conn = get_snowflake_connection()
    cursor = conn.cursor()
    success = 0
    fail = 0
    
    # Process tables in the order specified in table_mapping.json
    for table_config in table_mapping:
        table_name = table_config['snowflake_table']
        
        if table_name in table_to_mapping_file:
            mapping_file = table_to_mapping_file[table_name]
            print(f"\n[INFO] Processing {table_name} from {mapping_file}")
            
            with open(mapping_file, 'r', encoding='utf-8') as f:
                mapping = json.load(f)
            
            if create_table_with_original_names(cursor, mapping):
                success += 1
            else:
                fail += 1
        else:
            print(f"[WARNING] No mapping file found for {table_name}")
            fail += 1
    
    cursor.close()
    conn.close()
    
    print(f"\nSummary: {success} tables recreated, {fail} failed.")
    return 0 if fail == 0 else 1

if __name__ == '__main__':
    sys.exit(main()) 