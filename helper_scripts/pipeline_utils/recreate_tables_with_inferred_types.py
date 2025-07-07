#!/usr/bin/env python3
"""
Script to recreate all Snowflake tables using inferred column types
and exact CSV column names to ensure perfect data loading compatibility.
"""
import os
import sys
import json
import pandas as pd
import snowflake.connector
from azure.storage.blob import BlobServiceClient
from datetime import datetime
import io

# Set project root as two levels up from this script
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..'))
def get_abs_path(rel_path):
    return os.path.abspath(os.path.join(os.path.dirname(__file__), rel_path))

def infer_type(col):
    """Infer Snowflake column type based on column name keywords"""
    col_lower = col.lower()
    date_keywords = ['date', 'created', 'modified', 'updated', 'at']
    timestamp_keywords = ['time', 'timestamp']
    if any(kw in col_lower for kw in timestamp_keywords):
        return 'TIMESTAMP'
    elif any(kw in col_lower for kw in date_keywords):
        return 'DATE'
    elif any(kw in col_lower for kw in ['amount', 'cost', 'price', 'total', 'qty', 'number']):
        return 'FLOAT'
    elif 'id' in col_lower:
        return 'VARCHAR(255)'
    else:
        return 'VARCHAR(255)'

def clean_column_name_for_sql(column_name):
    """
    Clean column names for Snowflake SQL while preserving original case and spacing.
    Handle special characters that need to be quoted in Snowflake.
    Replace any non-ASCII characters (such as emojis) with an underscore '_'.
    """
    # Remove quotes if present
    cleaned = column_name.strip().strip('"\'')
    # Replace non-ASCII characters with '_'
    cleaned = ''.join(c if ord(c) < 128 else '_' for c in cleaned)
    # Check if column name needs to be quoted (contains spaces, special chars, or is reserved word)
    needs_quotes = (
        ' ' in cleaned or 
        any(char in cleaned for char in ['(', ')', '-', '/', '?', '&', '#', '$', '%', '@', '!', '^', '*', '+', '=', '[', ']', '{', '}', '|', '\\', ';', ':', '"', "'", ',', '.', '<', '>']) or
        cleaned.upper() in ['SELECT', 'FROM', 'WHERE', 'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'DROP', 'TABLE', 'COLUMN', 'ORDER', 'GROUP', 'BY', 'HAVING', 'UNION', 'JOIN', 'LEFT', 'RIGHT', 'INNER', 'OUTER', 'ON', 'AS', 'AND', 'OR', 'NOT', 'NULL', 'TRUE', 'FALSE', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END', 'DISTINCT', 'COUNT', 'SUM', 'AVG', 'MIN', 'MAX']
    )
    if needs_quotes:
        # Escape any existing quotes and wrap in double quotes
        escaped = cleaned.replace('"', '""')
        return f'"{escaped}"'
    else:
        return cleaned

def get_snowflake_connection(settings):
    """Create and return a Snowflake connection"""
    # Force private key path to project config_files directory for Azure compatibility.
    private_key_path = get_abs_path('../../config_files/snowflake_private_key.txt')
    
    # Load private key in PEM format
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.backends import default_backend
    
    with open(private_key_path, 'r') as f:
        p_key = serialization.load_pem_private_key(
            f.read().encode(),
            password=None,
            backend=default_backend()
        )
    
    pkb = p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )
    
    conn = snowflake.connector.connect(
        user=settings['SNOWFLAKE_USER'],
        account=settings['SNOWFLAKE_ACCOUNT'],
        private_key=pkb,
        warehouse=settings['SNOWFLAKE_WAREHOUSE'],
        database=settings['SNOWFLAKE_DATABASE']
    )
    return conn

def main():
    print("\nRecreating Snowflake Tables with Inferred Types\n" + "="*60)
    
    try:
        # Load configuration
        settings_path = get_abs_path('../../settings.json')
        with open(settings_path, 'r') as f:
            settings = json.load(f)
        
        mapping_path = get_abs_path('../../config_files/table_mapping.json')
        with open(mapping_path, 'r') as f:
            table_mapping = json.load(f)
        
        # Setup Azure connection
        connection_string = settings['AZURE_STORAGE_CONNECTION_STRING']
        container_name = settings['BLOB_CONTAINER']
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        
        # Setup Snowflake connection
        conn = get_snowflake_connection(settings)
        cursor = conn.cursor()
        
        # Prepare DDL statements
        ddl_statements = []
        ddl_statements.append("-- Snowflake Table Recreation DDL")
        ddl_statements.append(f"-- Generated on: {datetime.now().isoformat()}")
        ddl_statements.append("-- Based on Azure CSV column analysis")
        ddl_statements.append("")
        
        for mapping in table_mapping:
            table_name = mapping['snowflake_table']
            azure_csv_name = mapping['azure_csv_name']
            csv_blob_name = f"{azure_csv_name}.csv"
            
            print(f"\nProcessing: {table_name}")
            print(f"CSV Source: {csv_blob_name}")
            
            try:
                # Download and analyze CSV
                container_client = blob_service_client.get_container_client(container_name)
                blob_client = container_client.get_blob_client(csv_blob_name)
                
                if not blob_client.exists():
                    print(f"  [ERROR] Blob {csv_blob_name} not found")
                    continue
                
                download_stream = blob_client.download_blob()
                csv_bytes = download_stream.readall()
                df = pd.read_csv(io.StringIO(csv_bytes.decode('utf-8')))
                
                # Set database and schema context
                if '.' in table_name:
                    db_schema_parts = table_name.split('.')
                    if len(db_schema_parts) >= 2:
                        database = db_schema_parts[0]
                        schema = db_schema_parts[1]
                        simple_table_name = db_schema_parts[2] if len(db_schema_parts) > 2 else db_schema_parts[1]
                        
                        cursor.execute(f"USE DATABASE {database}")
                        cursor.execute(f"USE SCHEMA {schema}")
                        print(f"  Set context: DATABASE={database}, SCHEMA={schema}")
                    else:
                        simple_table_name = table_name
                else:
                    simple_table_name = table_name
                
                # Generate DDL
                ddl_statements.append(f"-- Table: {table_name}")
                ddl_statements.append(f"-- Source CSV: {csv_blob_name}")
                ddl_statements.append(f"USE DATABASE {database};")
                ddl_statements.append(f"USE SCHEMA {schema};")
                ddl_statements.append(f"DROP TABLE IF EXISTS {simple_table_name};")
                
                # Build CREATE TABLE statement
                create_parts = [f"CREATE TABLE {simple_table_name} ("]
                column_definitions = []
                
                for col in df.columns:
                    inferred_type = infer_type(col)
                    sql_column_name = clean_column_name_for_sql(col)
                    column_definitions.append(f"    {sql_column_name} {inferred_type}")
                
                create_parts.append(",\n".join(column_definitions))
                create_parts.append(");")
                
                create_statement = "\n".join(create_parts)
                ddl_statements.append(create_statement)
                ddl_statements.append("")
                
                # Execute DDL
                print(f"  Dropping existing table: {simple_table_name}")
                cursor.execute(f"DROP TABLE IF EXISTS {simple_table_name}")
                
                print(f"  Creating table: {simple_table_name}")
                cursor.execute(create_statement)
                
                print(f"  ✅ Successfully recreated table: {table_name}")
                print(f"  Columns: {len(df.columns)}")
                
                # Show column mapping
                print(f"  Column mapping preview:")
                for i, col in enumerate(df.columns[:5]):  # Show first 5 columns
                    inferred_type = infer_type(col)
                    sql_name = clean_column_name_for_sql(col)
                    print(f"    '{col}' -> {sql_name} ({inferred_type})")
                if len(df.columns) > 5:
                    print(f"    ... and {len(df.columns) - 5} more columns")
                
            except Exception as e:
                print(f"  [ERROR] Failed to process {table_name}: {str(e)}")
                import traceback
                traceback.print_exc()
        
        # Save DDL statements
        os.makedirs(get_abs_path('../logs'), exist_ok=True)
        ddl_file = get_abs_path('../logs/recreate_tables_ddl.sql')
        with open(ddl_file, 'w', encoding='utf-8') as f:
            f.write('\n'.join(ddl_statements))
        
        print(f"\n💾 DDL statements saved to: {ddl_file}")
        print(f"✅ All tables recreated successfully!")
        
        # Close connection
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main() 