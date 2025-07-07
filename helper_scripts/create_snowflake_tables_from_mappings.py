#!/usr/bin/env python3
"""
One-time script to create Snowflake tables from all column_type_mapping_*.json files in the logs directory.
- Reads each mapping file
- Connects to Snowflake
- Issues CREATE TABLE statements for each mapping
- Does NOT load any data
- Logs results to console
"""
import os
import sys
import glob
import json
from pathlib import Path
import snowflake.connector
from getpass import getpass
import base64
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

def load_settings():
    # Try project root, then config_files
    root = Path(__file__).parent.parent
    settings_paths = [root / 'settings.json', root / 'config_files' / 'settings.json']
    for path in settings_paths:
        if path.exists():
            with open(path, 'r', encoding='utf-8') as f:
                return json.load(f)
    return {}

def get_snowflake_connection():
    settings = load_settings()
    account = os.environ.get('SNOWFLAKE_ACCOUNT') or settings.get('SNOWFLAKE_ACCOUNT') or input('Snowflake account: ')
    user = os.environ.get('SNOWFLAKE_USER') or settings.get('SNOWFLAKE_USER') or input('Snowflake user: ')
    warehouse = os.environ.get('SNOWFLAKE_WAREHOUSE') or settings.get('SNOWFLAKE_WAREHOUSE') or input('Snowflake warehouse: ')
    database = os.environ.get('SNOWFLAKE_DATABASE') or settings.get('SNOWFLAKE_DATABASE') or input('Default database: ')
    schema = os.environ.get('SNOWFLAKE_SCHEMA') or settings.get('SNOWFLAKE_SCHEMA') or 'PUBLIC'
    key_path = os.environ.get('SNOWFLAKE_PRIVATE_KEY_PATH') or settings.get('SNOWFLAKE_PRIVATE_KEY_PATH') or str(Path(__file__).parent.parent / 'config_files' / 'snowflake_private_key.txt')
    with open(key_path, 'r') as f:
        p_key = serialization.load_pem_private_key(
            f.read().encode('utf-8'),
            password=None,
            backend=default_backend()
        )
    pkb = p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )
    return snowflake.connector.connect(
        user=user,
        private_key=pkb,
        account=account,
        warehouse=warehouse,
        database=database,
        schema=schema
    )

def create_table_from_mapping(cursor, mapping):
    db_schema = mapping.get('database_schema')
    table_full = mapping.get('table_name')
    columns = mapping['column_mappings']
    if not db_schema or not table_full or not columns:
        print(f"[ERROR] Invalid mapping: {mapping}")
        return False
    # Parse database and schema
    db_parts = db_schema.split('.')
    if len(db_parts) == 2:
        database, schema = db_parts
    else:
        database, schema = db_schema, 'PUBLIC'
    # Table name is last part
    table_name = table_full.split('.')[-1]
    # Set context
    cursor.execute(f"USE DATABASE {database}")
    cursor.execute(f"USE SCHEMA {schema}")
    # Build columns
    col_defs = []
    for col, meta in columns.items():
        col_defs.append(f'"{col}" {meta["snowflake_type"]}')
    create_sql = f'CREATE OR REPLACE TABLE {table_name} (\n    ' + ',\n    '.join(col_defs) + '\n)'
    print(f"[INFO] Creating table {database}.{schema}.{table_name} ...")
    try:
        cursor.execute(create_sql)
        print(f"[SUCCESS] Created {database}.{schema}.{table_name}")
        return True
    except Exception as e:
        print(f"[ERROR] Failed to create {database}.{schema}.{table_name}: {e}")
        return False

def main():
    logs_dir = Path(__file__).parent.parent / 'logs'
    mapping_files = glob.glob(str(logs_dir / 'column_type_mapping_*.json'))
    if not mapping_files:
        print("No mapping files found in logs directory.")
        return 1
    conn = get_snowflake_connection()
    cursor = conn.cursor()
    success = 0
    fail = 0
    for mf in mapping_files:
        with open(mf, 'r', encoding='utf-8') as f:
            mapping = json.load(f)
        if create_table_from_mapping(cursor, mapping):
            success += 1
        else:
            fail += 1
    cursor.close()
    conn.close()
    print(f"\nSummary: {success} tables created, {fail} failed.")
    return 0 if fail == 0 else 1

if __name__ == '__main__':
    sys.exit(main()) 