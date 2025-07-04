#!/usr/bin/env python3
"""
Script to recreate Snowflake tables with maximum VARCHAR lengths to handle all data
This script will:
1. Drop existing tables
2. Recreate tables with maximum VARCHAR lengths (16,777,216 characters)
3. Handle all data edge cases
4. Reload data from Azure CSV files
5. Verify data integrity
"""

import os
import sys
import json
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import base64
import re
from datetime import datetime
from azure.storage.blob import BlobServiceClient

# Add helper_scripts/Utils to path for logger import
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'helper_scripts', 'Utils'))
from logger import pipeline_logger

def clean_column_name(column_name):
    """
    Clean column names to be Snowflake-compatible
    Replace spaces, special characters, and make them valid identifiers
    """
    # Remove or replace problematic characters
    cleaned = column_name.strip()
    
    # Replace spaces and special characters with underscores
    cleaned = re.sub(r'[^a-zA-Z0-9_]', '_', cleaned)
    
    # Remove multiple consecutive underscores
    cleaned = re.sub(r'_+', '_', cleaned)
    
    # Remove leading/trailing underscores
    cleaned = cleaned.strip('_')
    
    # Ensure it starts with a letter or underscore
    if cleaned and not cleaned[0].isalpha() and cleaned[0] != '_':
        cleaned = 'COL_' + cleaned
    
    # If empty after cleaning, use a default name
    if not cleaned:
        cleaned = 'UNNAMED_COLUMN'
    
    return cleaned

def get_snowflake_connection(settings):
    """Create and return a Snowflake connection"""
    # Load private key for Snowflake
    private_key_path = os.path.join(os.path.dirname(__file__), '..', settings['SNOWFLAKE_PRIVATE_KEY_PATH'])
    
    # Load private key (base64 DER format)
    with open(private_key_path, "r") as key_file:
        key_content = key_file.read().strip()
    
    # Decode base64 to get DER bytes
    pkb = base64.b64decode(key_content)
    
    snowflake_config = {
        'user': settings['SNOWFLAKE_USER'],
        'account': settings['SNOWFLAKE_ACCOUNT'],
        'private_key': pkb,
        'warehouse': settings['SNOWFLAKE_WAREHOUSE'],
        'database': settings['SNOWFLAKE_DATABASE'],
        'schema': 'PUBLIC'  # Default schema
    }
    
    return snowflake.connector.connect(**snowflake_config)

def list_azure_blobs(blob_service_client, container_name):
    """List all blobs in the Azure container"""
    try:
        container_client = blob_service_client.get_container_client(container_name)
        blobs = []
        for blob in container_client.list_blobs():
            blobs.append(blob.name)
        return blobs, None
    except Exception as e:
        return None, f"Error listing blobs: {str(e)}"

def find_matching_blob(blob_name, available_blobs):
    """Find a matching blob name, handling case sensitivity and extensions"""
    # Try exact match first
    if blob_name in available_blobs:
        return blob_name
    
    # Try with .csv extension
    csv_name = f"{blob_name}.csv"
    if csv_name in available_blobs:
        return csv_name
    
    # Try case-insensitive match
    blob_name_lower = blob_name.lower()
    for blob in available_blobs:
        if blob.lower() == blob_name_lower:
            return blob
        if blob.lower() == f"{blob_name_lower}.csv":
            return blob
    
    return None

def load_csv_from_azure(blob_service_client, container_name, blob_name):
    """Download and load CSV from Azure Blob Storage"""
    try:
        container_client = blob_service_client.get_container_client(container_name)
        blob_client = container_client.get_blob_client(blob_name)
        
        # Check if blob exists
        if not blob_client.exists():
            return None, f"Blob {blob_name} not found in container {container_name}"
        
        # Download the CSV from Azure
        download_stream = blob_client.download_blob()
        df = pd.read_csv(download_stream)
        
        return df, None
    except Exception as e:
        return None, f"Error downloading {blob_name}: {str(e)}"

def drop_table(cursor, table_name, database_schema):
    """Drop a table if it exists"""
    try:
        # Parse database and schema
        if '.' in database_schema:
            db_schema_parts = database_schema.split('.')
            if len(db_schema_parts) >= 2:
                database = db_schema_parts[0]
                schema = db_schema_parts[1]
                cursor.execute(f"USE DATABASE {database}")
                cursor.execute(f"USE SCHEMA {schema}")
        else:
            database = database_schema
            schema = 'PUBLIC'
            cursor.execute(f"USE DATABASE {database}")
            cursor.execute(f"USE SCHEMA {schema}")
        
        # Extract simple table name
        simple_table_name = table_name.split('.')[-1]
        
        # Drop table if exists
        cursor.execute(f"DROP TABLE IF EXISTS {simple_table_name}")
        pipeline_logger.log("RECREATE_FINAL", f"🗑️ Dropped table {simple_table_name}", "INFO")
        return True, None
    except Exception as e:
        return False, f"Error dropping table {table_name}: {str(e)}"

def create_table_with_max_varchar(cursor, table_name, database_schema, df):
    """Create table with maximum VARCHAR lengths to handle all data"""
    try:
        # Parse database and schema
        if '.' in database_schema:
            db_schema_parts = database_schema.split('.')
            if len(db_schema_parts) >= 2:
                database = db_schema_parts[0]
                schema = db_schema_parts[1]
                cursor.execute(f"USE DATABASE {database}")
                cursor.execute(f"USE SCHEMA {schema}")
        else:
            database = database_schema
            schema = 'PUBLIC'
            cursor.execute(f"USE DATABASE {database}")
            cursor.execute(f"USE SCHEMA {schema}")
        
        # Extract simple table name
        simple_table_name = table_name.split('.')[-1]
        
        # Build CREATE TABLE statement with maximum VARCHAR lengths
        columns_sql = []
        for col in df.columns:
            cleaned_col = clean_column_name(col)
            
            # Analyze the column to determine appropriate type
            col_lower = col.lower()
            sample_data = df[col].dropna().head(100)  # Sample for analysis
            
            # Determine column type based on content and name
            if any(keyword in col_lower for keyword in ['amount', 'price', 'cost', 'budget', 'total', 'sum', 'count', 'number', 'id', 'quantity', 'qty']):
                # Numeric columns - check if actually numeric
                try:
                    pd.to_numeric(sample_data, errors='raise')
                    if sample_data.dtype in ['int64', 'int32']:
                        snowflake_type = "NUMBER(15)"
                    else:
                        snowflake_type = "FLOAT"
                except:
                    # If not numeric, use maximum VARCHAR
                    snowflake_type = "VARCHAR(16777216)"
            else:
                # All other columns use maximum VARCHAR to handle any length
                snowflake_type = "VARCHAR(16777216)"
            
            columns_sql.append(f'"{cleaned_col}" {snowflake_type}')
        
        create_table_sql = f"""
        CREATE TABLE {simple_table_name} (
            {', '.join(columns_sql)}
        )
        """
        
        pipeline_logger.log("RECREATE_FINAL", f"🏗️ Creating table {simple_table_name} with {len(columns_sql)} columns", "INFO")
        pipeline_logger.log("RECREATE_FINAL", f"📝 CREATE TABLE SQL: {create_table_sql[:200]}...", "DEBUG")
        
        cursor.execute(create_table_sql)
        pipeline_logger.log("RECREATE_FINAL", f"✅ Successfully created table {simple_table_name}", "INFO")
        return True, simple_table_name, None
    except Exception as e:
        return False, None, f"Error creating table {table_name}: {str(e)}"

def load_data_to_snowflake(conn, df, table_name):
    """Load DataFrame data into Snowflake table with proper column mapping"""
    try:
        # Clean column names in DataFrame to match Snowflake table
        df_renamed = df.copy()
        df_renamed.columns = [clean_column_name(col) for col in df.columns]
        
        # Convert all columns to string initially to avoid data type issues during load
        for col in df_renamed.columns:
            df_renamed[col] = df_renamed[col].astype(str)
        
        # Replace 'nan' strings with None
        df_renamed = df_renamed.replace('nan', None)
        
        # Log before loading
        cursor = conn.cursor()
        cursor.execute("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA()")
        db_ctx, schema_ctx = cursor.fetchone()
        pipeline_logger.log("RECREATE_FINAL", f"Context before write_pandas: DB={db_ctx}, SCHEMA={schema_ctx}", "DEBUG")
        pipeline_logger.log("RECREATE_FINAL", f"write_pandas target: {table_name}", "DEBUG")
        
        # Load data using write_pandas
        success, nchunks, nrows, output = write_pandas(
            conn, 
            df_renamed, 
            table_name,
            auto_create_table=False,
            overwrite=False
        )
        
        pipeline_logger.log("RECREATE_FINAL", f"write_pandas output: success={success}, nchunks={nchunks}, nrows={nrows}", "DEBUG")
        
        if success:
            return True, nrows, None
        else:
            return False, 0, "write_pandas returned False"
    except Exception as e:
        return False, 0, f"Error loading data: {str(e)}"

def verify_data_integrity(cursor, table_name, database_schema, expected_row_count):
    """Verify that the loaded data matches expectations"""
    try:
        # Parse database and schema
        if '.' in database_schema:
            db_schema_parts = database_schema.split('.')
            if len(db_schema_parts) >= 2:
                database = db_schema_parts[0]
                schema = db_schema_parts[1]
                cursor.execute(f"USE DATABASE {database}")
                cursor.execute(f"USE SCHEMA {schema}")
        else:
            database = database_schema
            schema = 'PUBLIC'
            cursor.execute(f"USE DATABASE {database}")
            cursor.execute(f"USE SCHEMA {schema}")
        
        # Extract simple table name
        simple_table_name = table_name.split('.')[-1]
        
        # Get actual row count
        cursor.execute(f"SELECT COUNT(*) FROM {simple_table_name}")
        actual_row_count = cursor.fetchone()[0]
        
        # Get column count
        cursor.execute(f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{simple_table_name}'")
        column_count = cursor.fetchone()[0]
        
        # Check if row counts match
        row_count_match = actual_row_count == expected_row_count
        match_percentage = (actual_row_count / expected_row_count * 100) if expected_row_count > 0 else 0
        
        verification_result = {
            'table_name': simple_table_name,
            'expected_rows': expected_row_count,
            'actual_rows': actual_row_count,
            'row_count_match': row_count_match,
            'match_percentage': match_percentage,
            'column_count': column_count,
            'verification_passed': row_count_match
        }
        
        if row_count_match:
            pipeline_logger.log("RECREATE_FINAL", f"✅ Data integrity verified for {simple_table_name}: {actual_row_count} rows", "INFO")
        else:
            pipeline_logger.log("RECREATE_FINAL", f"⚠️ Data integrity warning for {simple_table_name}: expected {expected_row_count}, got {actual_row_count}", "WARNING")
        
        return verification_result
    except Exception as e:
        pipeline_logger.log("RECREATE_FINAL", f"❌ Error verifying data integrity for {table_name}: {str(e)}", "ERROR")
        return {
            'table_name': table_name.split('.')[-1],
            'expected_rows': expected_row_count,
            'actual_rows': 0,
            'row_count_match': False,
            'match_percentage': 0,
            'column_count': 0,
            'verification_passed': False,
            'error': str(e)
        }

def recreate_tables_with_max_varchar():
    """Main function to recreate all tables with maximum VARCHAR lengths"""
    
    pipeline_logger.log("RECREATE_FINAL", "🚀 Starting table recreation with maximum VARCHAR lengths", "INFO")
    
    try:
        # Load settings
        pipeline_logger.log("RECREATE_FINAL", "🔑 Loading credentials", "INFO")
        with open('../config_files/settings.json', 'r') as f:
            settings = json.load(f)
        
        # Load table mapping
        pipeline_logger.log("RECREATE_FINAL", "📋 Loading table mapping configuration", "INFO")
        with open('../config_files/table_mapping.json', 'r') as f:
            table_mapping = json.load(f)
        
        # Azure Blob Storage connection
        connection_string = settings['AZURE_STORAGE_CONNECTION_STRING']
        container_name = settings['BLOB_CONTAINER']
        
        pipeline_logger.log("RECREATE_FINAL", f"📦 Connecting to Azure Blob Storage: {container_name}", "INFO")
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        
        # List available blobs in Azure container
        available_blobs, error = list_azure_blobs(blob_service_client, container_name)
        if error:
            pipeline_logger.log("RECREATE_FINAL", f"❌ Failed to list Azure blobs: {error}", "ERROR")
            raise Exception(f"Failed to list Azure blobs: {error}")
        
        pipeline_logger.log("RECREATE_FINAL", f"📦 Found {len(available_blobs)} blobs in container", "INFO")
        
        # Connect to Snowflake
        pipeline_logger.log("RECREATE_FINAL", "❄️ Connecting to Snowflake", "INFO")
        conn = get_snowflake_connection(settings)
        cursor = conn.cursor()
        
        # Track results
        results = []
        total_tables = len(table_mapping)
        
        for i, mapping in enumerate(table_mapping, 1):
            table_name = mapping['snowflake_table']
            azure_csv_name = mapping['azure_csv_name']
            database_schema = mapping['snowflake_database']
            
            pipeline_logger.log("RECREATE_FINAL", f"🔄 Processing table {i}/{total_tables}: {table_name}", "INFO")
            pipeline_logger.log_progress("RECREATE_FINAL", i, total_tables, f"Processing {table_name}")
            
            result = {
                'table_name': table_name,
                'azure_csv_name': azure_csv_name,
                'database_schema': database_schema,
                'status': 'pending',
                'error': None,
                'rows_loaded': 0,
                'verification': None
            }
            
            try:
                # Find matching blob in Azure
                matching_blob = find_matching_blob(azure_csv_name, available_blobs)
                if not matching_blob:
                    result['status'] = 'failed'
                    result['error'] = f"Could not find matching blob for {azure_csv_name}"
                    pipeline_logger.log("RECREATE_FINAL", f"❌ No matching blob found for {azure_csv_name}", "ERROR")
                    results.append(result)
                    continue
                
                pipeline_logger.log("RECREATE_FINAL", f"✅ Found matching blob: {matching_blob}", "INFO")
                
                # Download CSV from Azure
                pipeline_logger.log("RECREATE_FINAL", f"⬇️ Downloading {matching_blob} from Azure", "INFO")
                df, error = load_csv_from_azure(blob_service_client, container_name, matching_blob)
                
                if error:
                    result['status'] = 'failed'
                    result['error'] = error
                    pipeline_logger.log("RECREATE_FINAL", f"❌ Failed to download {matching_blob}: {error}", "ERROR")
                    results.append(result)
                    continue
                
                expected_row_count = len(df)
                pipeline_logger.log("RECREATE_FINAL", f"📊 Downloaded DataFrame shape: {df.shape}", "INFO")
                
                # Drop existing table
                pipeline_logger.log("RECREATE_FINAL", f"🗑️ Dropping existing table: {table_name}", "INFO")
                success, error = drop_table(cursor, table_name, database_schema)
                if not success:
                    result['status'] = 'failed'
                    result['error'] = error
                    pipeline_logger.log("RECREATE_FINAL", f"❌ Failed to drop table {table_name}: {error}", "ERROR")
                    results.append(result)
                    continue
                
                # Create table with maximum VARCHAR lengths
                pipeline_logger.log("RECREATE_FINAL", f"🏗️ Creating table with max VARCHAR: {table_name}", "INFO")
                success, simple_table_name, error = create_table_with_max_varchar(cursor, table_name, database_schema, df)
                if not success:
                    result['status'] = 'failed'
                    result['error'] = error
                    pipeline_logger.log("RECREATE_FINAL", f"❌ Failed to create table {table_name}: {error}", "ERROR")
                    results.append(result)
                    continue
                
                # Load data to Snowflake
                pipeline_logger.log("RECREATE_FINAL", f"📤 Loading data into {simple_table_name}", "INFO")
                success, rows_loaded, error = load_data_to_snowflake(conn, df, simple_table_name)
                if not success:
                    result['status'] = 'failed'
                    result['error'] = error
                    pipeline_logger.log("RECREATE_FINAL", f"❌ Failed to load data into {table_name}: {error}", "ERROR")
                    results.append(result)
                    continue
                
                result['rows_loaded'] = rows_loaded
                pipeline_logger.log("RECREATE_FINAL", f"✅ Successfully loaded {rows_loaded} rows into {simple_table_name}", "INFO")
                
                # Verify data integrity
                pipeline_logger.log("RECREATE_FINAL", f"🔍 Verifying data integrity for {simple_table_name}", "INFO")
                verification = verify_data_integrity(cursor, table_name, database_schema, expected_row_count)
                result['verification'] = verification
                
                if verification['verification_passed']:
                    result['status'] = 'success'
                    pipeline_logger.log("RECREATE_FINAL", f"🎉 Successfully recreated and verified {simple_table_name}", "INFO")
                else:
                    result['status'] = 'warning'
                    pipeline_logger.log("RECREATE_FINAL", f"⚠️ Table recreated but verification failed for {simple_table_name}", "WARNING")
                
            except Exception as e:
                result['status'] = 'failed'
                result['error'] = str(e)
                pipeline_logger.log("RECREATE_FINAL", f"❌ Error processing {table_name}: {str(e)}", "ERROR")
            
            results.append(result)
        
        # Close connections
        cursor.close()
        conn.close()
        
        # Log final results
        successful = sum(1 for r in results if r['status'] == 'success')
        warnings = sum(1 for r in results if r['status'] == 'warning')
        failed = sum(1 for r in results if r['status'] == 'failed')
        total_rows_loaded = sum(r['rows_loaded'] for r in results if r['status'] in ['success', 'warning'])
        
        pipeline_logger.log("RECREATE_FINAL", f"🎉 Table recreation completed! Success: {successful}, Warnings: {warnings}, Failed: {failed}, Total rows loaded: {total_rows_loaded}", "INFO")
        
        # Save detailed results
        results_file = "../logs/recreate_tables_final_results.json"
        with open(results_file, 'w') as f:
            json.dump({
                'timestamp': datetime.now().isoformat(),
                'total_tables': total_tables,
                'successful': successful,
                'warnings': warnings,
                'failed': failed,
                'total_rows_loaded': total_rows_loaded,
                'results': results
            }, f, indent=2)
        
        pipeline_logger.log("RECREATE_FINAL", f"💾 Recreation results saved to: {results_file}", "INFO")
        
        # Log JSON summary
        pipeline_logger.log_json("RECREATE_FINAL", {
            'total_tables': total_tables,
            'successful': successful,
            'warnings': warnings,
            'failed': failed,
            'total_rows_loaded': total_rows_loaded,
            'completion_percentage': ((successful + warnings) / total_tables * 100) if total_tables > 0 else 0
        }, "INFO")
        
        return results
        
    except Exception as e:
        pipeline_logger.log("RECREATE_FINAL", f"❌ Critical error in recreate_tables_with_max_varchar: {str(e)}", "ERROR")
        import traceback
        traceback.print_exc()
        raise

if __name__ == "__main__":
    recreate_tables_with_max_varchar() 