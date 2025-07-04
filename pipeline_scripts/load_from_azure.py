#!/usr/bin/env python3
"""
Script to load all cleaned CSV files from Azure Blob Storage to their corresponding Snowflake tables
This script uses the table_mapping.json configuration to determine which files to load
"""

import os
import sys
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import json
import re
from datetime import datetime
from azure.storage.blob import BlobServiceClient
import base64

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

def create_snowflake_table(cursor, table_name, df, database_schema):
    """Create Snowflake table with appropriate schema and return fully qualified table name"""
    try:
        # Parse database and schema from the mapping
        if '.' in database_schema:
            db_schema_parts = database_schema.split('.')
            if len(db_schema_parts) >= 2:
                database = db_schema_parts[0]
                schema = db_schema_parts[1]
                # Use the database and schema
                cursor.execute(f"USE DATABASE {database}")
                cursor.execute(f"USE SCHEMA {schema}")
        else:
            database = database_schema
            schema = 'PUBLIC'
            cursor.execute(f"USE DATABASE {database}")
            cursor.execute(f"USE SCHEMA {schema}")
        
        # Extract just the table name (last part after the dot)
        simple_table_name = table_name.split('.')[-1]
        
        # Log current context
        cursor.execute("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA()")
        db_ctx, schema_ctx = cursor.fetchone()
        pipeline_logger.log("LOAD_FROM_AZURE", f"Context before CREATE TABLE: DB={db_ctx}, SCHEMA={schema_ctx}", "DEBUG")
        
        # Generate CREATE TABLE statement
        columns_sql = []
        for col in df.columns:
            columns_sql.append(f'"{col}" VARCHAR')
        
        create_table_sql = f"""
        CREATE OR REPLACE TABLE {simple_table_name} (
            {', '.join(columns_sql)}
        )
        """
        
        pipeline_logger.log("LOAD_FROM_AZURE", f"CREATE TABLE SQL: {create_table_sql[:200]}...", "DEBUG")
        cursor.execute(create_table_sql)
        
        # Log after creation
        cursor.execute("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA()")
        db_ctx2, schema_ctx2 = cursor.fetchone()
        pipeline_logger.log("LOAD_FROM_AZURE", f"Context after CREATE TABLE: DB={db_ctx2}, SCHEMA={schema_ctx2}", "DEBUG")
        
        # Return the simple table name for write_pandas (it will use current context)
        return True, None, simple_table_name
    except Exception as e:
        return False, f"Error creating table {table_name}: {str(e)}", None

def load_data_to_snowflake(conn, df, table_name):
    """Load DataFrame data into Snowflake table using current context"""
    try:
        # Convert all columns to string to avoid data type issues
        for col in df.columns:
            df[col] = df[col].astype(str)
        # Replace 'nan' strings with None
        df = df.replace('nan', None)
        
        # Log before loading
        cursor = conn.cursor()
        cursor.execute("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA()")
        db_ctx, schema_ctx = cursor.fetchone()
        pipeline_logger.log("LOAD_FROM_AZURE", f"Context before write_pandas: DB={db_ctx}, SCHEMA={schema_ctx}", "DEBUG")
        pipeline_logger.log("LOAD_FROM_AZURE", f"write_pandas target: {table_name}", "DEBUG")
        
        # Load data using write_pandas
        success, nchunks, nrows, output = write_pandas(
            conn, 
            df, 
            table_name,
            auto_create_table=False,
            overwrite=False
        )
        
        pipeline_logger.log("LOAD_FROM_AZURE", f"write_pandas output: success={success}, nchunks={nchunks}, nrows={nrows}, output={output}", "DEBUG")
        
        if success:
            return True, nrows, None
        else:
            return False, 0, "write_pandas returned False"
    except Exception as e:
        return False, 0, f"Error loading data: {str(e)}"

def verify_data_load(cursor, table_name, expected_rows):
    """Verify that data was loaded correctly"""
    try:
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        actual_count = cursor.fetchone()[0]
        
        # Show sample data
        cursor.execute(f"SELECT * FROM {table_name} LIMIT 3")
        sample_data = cursor.fetchall()
        
        return {
            'actual_count': actual_count,
            'expected_count': expected_rows,
            'match': actual_count == expected_rows,
            'sample_data': sample_data[0] if sample_data else None
        }
    except Exception as e:
        return {
            'error': f"Error verifying data: {str(e)}",
            'actual_count': 0,
            'expected_count': expected_rows,
            'match': False
        }

def load_from_azure():
    """Load all cleaned CSV files from Azure to their corresponding Snowflake tables"""
    
    pipeline_logger.log("LOAD_FROM_AZURE", "🚀 Starting Azure to Snowflake data load pipeline", "INFO")
    
    try:
        # Load settings
        pipeline_logger.log("LOAD_FROM_AZURE", "🔑 Loading Azure and Snowflake credentials", "INFO")
        with open('../config_files/settings.json', 'r') as f:
            settings = json.load(f)
        
        # Load table mapping
        pipeline_logger.log("LOAD_FROM_AZURE", "📋 Loading table mapping configuration", "INFO")
        with open('../config_files/table_mapping.json', 'r') as f:
            table_mapping = json.load(f)
        
        # Azure Blob Storage connection
        connection_string = settings['AZURE_STORAGE_CONNECTION_STRING']
        container_name = settings['BLOB_CONTAINER']
        
        pipeline_logger.log("LOAD_FROM_AZURE", f"📦 Connecting to Azure Blob Storage: {container_name}", "INFO")
        
        # Create blob service client
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        
        # List available blobs in Azure container
        pipeline_logger.log("LOAD_FROM_AZURE", "📋 Listing available blobs in Azure container", "INFO")
        available_blobs, error = list_azure_blobs(blob_service_client, container_name)
        
        if error:
            pipeline_logger.log("LOAD_FROM_AZURE", f"❌ Failed to list Azure blobs: {error}", "ERROR")
            raise Exception(f"Failed to list Azure blobs: {error}")
        
        pipeline_logger.log("LOAD_FROM_AZURE", f"📦 Found {len(available_blobs)} blobs in container", "INFO")
        pipeline_logger.log("LOAD_FROM_AZURE", f"📋 Available blobs: {available_blobs[:10]}...", "INFO")
        
        # Connect to Snowflake
        pipeline_logger.log("LOAD_FROM_AZURE", "❄️ Connecting to Snowflake", "INFO")
        conn = get_snowflake_connection(settings)
        cursor = conn.cursor()
        
        # Track results
        results = []
        total_tables = len(table_mapping)
        
        for i, mapping in enumerate(table_mapping, 1):
            table_name = mapping['snowflake_table']
            azure_csv_name = mapping['azure_csv_name']
            database_schema = mapping['snowflake_database']
            expected_rows = int(mapping['estimated_row_count'])
            
            pipeline_logger.log("LOAD_FROM_AZURE", f"📊 Processing table {i}/{total_tables}: {table_name}", "INFO")
            pipeline_logger.log_progress("LOAD_FROM_AZURE", i, total_tables, f"Processing {table_name}")
            
            result = {
                'table_name': table_name,
                'azure_csv_name': azure_csv_name,
                'status': 'pending',
                'error': None,
                'rows_loaded': 0,
                'verification': None
            }
            
            try:
                # Find matching blob in Azure
                pipeline_logger.log("LOAD_FROM_AZURE", f"🔍 Looking for {azure_csv_name} in Azure container", "INFO")
                matching_blob = find_matching_blob(azure_csv_name, available_blobs)
                
                if not matching_blob:
                    result['status'] = 'failed'
                    result['error'] = f"Could not find matching blob for {azure_csv_name} in available blobs"
                    pipeline_logger.log("LOAD_FROM_AZURE", f"❌ No matching blob found for {azure_csv_name}", "ERROR")
                    results.append(result)
                    continue
                
                pipeline_logger.log("LOAD_FROM_AZURE", f"✅ Found matching blob: {matching_blob}", "INFO")
                
                # Download CSV from Azure
                pipeline_logger.log("LOAD_FROM_AZURE", f"⬇️ Downloading {matching_blob} from Azure", "INFO")
                df, error = load_csv_from_azure(blob_service_client, container_name, matching_blob)
                
                if error:
                    result['status'] = 'failed'
                    result['error'] = error
                    pipeline_logger.log("LOAD_FROM_AZURE", f"❌ Failed to download {matching_blob}: {error}", "ERROR")
                    results.append(result)
                    continue
                
                pipeline_logger.log("LOAD_FROM_AZURE", f"📊 Downloaded DataFrame shape: {df.shape}", "INFO")
                
                # Clean column names
                column_mapping = {}
                cleaned_columns = []
                
                for j, col in enumerate(df.columns):
                    cleaned_col = clean_column_name(col)
                    # Handle duplicates by adding index
                    if cleaned_col in cleaned_columns:
                        cleaned_col = f"{cleaned_col}_{j}"
                    cleaned_columns.append(cleaned_col)
                    column_mapping[col] = cleaned_col
                
                # Rename columns
                df.columns = cleaned_columns
                
                pipeline_logger.log("LOAD_FROM_AZURE", f"🧹 Cleaned {len(df.columns)} columns for {table_name}", "INFO")
                
                # Save column mapping for reference
                mapping_file = f"../logs/column_mapping_{table_name.replace('.', '_')}.json"
                os.makedirs("../logs", exist_ok=True)
                with open(mapping_file, 'w') as f:
                    json.dump(column_mapping, f, indent=2)
                
                # Create Snowflake table
                pipeline_logger.log("LOAD_FROM_AZURE", f"🏗️ Creating Snowflake table: {table_name}", "INFO")
                success, error, simple_table_name = create_snowflake_table(cursor, table_name, df, database_schema)
                if not success:
                    result['status'] = 'failed'
                    result['error'] = error
                    pipeline_logger.log("LOAD_FROM_AZURE", f"❌ Failed to create table {table_name}: {error}", "ERROR")
                    results.append(result)
                    continue
                # Load data to Snowflake
                pipeline_logger.log("LOAD_FROM_AZURE", f"📤 Loading data into {simple_table_name}", "INFO")
                success, rows_loaded, error = load_data_to_snowflake(conn, df, simple_table_name)
                if not success:
                    result['status'] = 'failed'
                    result['error'] = error
                    pipeline_logger.log("LOAD_FROM_AZURE", f"❌ Failed to load data into {simple_table_name}: {error}", "ERROR")
                    results.append(result)
                    continue
                result['rows_loaded'] = rows_loaded
                
                # Verify the data load
                pipeline_logger.log("LOAD_FROM_AZURE", f"🔍 Verifying data load for {table_name}", "INFO")
                verification = verify_data_load(cursor, simple_table_name, expected_rows)
                result['verification'] = verification
                
                if verification['match']:
                    result['status'] = 'success'
                    pipeline_logger.log("LOAD_FROM_AZURE", f"✅ Successfully loaded {table_name}: {rows_loaded} rows", "INFO")
                else:
                    result['status'] = 'warning'
                    pipeline_logger.log("LOAD_FROM_AZURE", f"⚠️ Row count mismatch for {table_name}: Expected {expected_rows}, got {verification['actual_count']}", "WARNING")
                
            except Exception as e:
                result['status'] = 'failed'
                result['error'] = str(e)
                pipeline_logger.log("LOAD_FROM_AZURE", f"❌ Error processing {table_name}: {str(e)}", "ERROR")
            
            results.append(result)
        
        # Close connections
        cursor.close()
        conn.close()
        
        # Log final results
        successful = sum(1 for r in results if r['status'] == 'success')
        failed = sum(1 for r in results if r['status'] == 'failed')
        warnings = sum(1 for r in results if r['status'] == 'warning')
        
        pipeline_logger.log("LOAD_FROM_AZURE", f"🎉 Pipeline completed! Success: {successful}, Failed: {failed}, Warnings: {warnings}", "INFO")
        
        # Save detailed results
        results_file = "../logs/load_from_azure_results.json"
        with open(results_file, 'w') as f:
            json.dump({
                'timestamp': datetime.now().isoformat(),
                'total_tables': total_tables,
                'successful': successful,
                'failed': failed,
                'warnings': warnings,
                'results': results
            }, f, indent=2)
        
        pipeline_logger.log("LOAD_FROM_AZURE", f"💾 Detailed results saved to: {results_file}", "INFO")
        
        # Log JSON summary
        pipeline_logger.log_json("LOAD_FROM_AZURE", {
            'total_tables': total_tables,
            'successful': successful,
            'failed': failed,
            'warnings': warnings,
            'completion_percentage': (successful / total_tables * 100) if total_tables > 0 else 0
        }, "INFO")
        
        return results
        
    except Exception as e:
        pipeline_logger.log("LOAD_FROM_AZURE", f"❌ Critical error in load_from_azure: {str(e)}", "ERROR")
        import traceback
        traceback.print_exc()
        raise

if __name__ == "__main__":
    load_from_azure() 