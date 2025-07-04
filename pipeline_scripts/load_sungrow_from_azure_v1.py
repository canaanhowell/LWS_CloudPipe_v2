#!/usr/bin/env python3
"""
Script to load the correct LWS.PUBLIC.SUNGROW.csv from Azure Blob Storage
This script will load the cleaned/processed version from Azure, not the raw local file
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

def load_sungrow_from_azure():
    """Load the correct LWS.PUBLIC.SUNGROW.csv from Azure Blob Storage"""
    
    print("🚀 Starting Sungrow data load from Azure Blob Storage")
    
    try:
        # Load settings
        print("🔑 Loading Azure and Snowflake credentials")
        with open('config_files/settings.json', 'r') as f:
            settings = json.load(f)
        
        # Azure Blob Storage connection
        connection_string = settings['AZURE_STORAGE_CONNECTION_STRING']
        container_name = settings['BLOB_CONTAINER']
        blob_name = "LWS.PUBLIC.SUNGROW.csv"
        
        print(f"📦 Connecting to Azure Blob Storage: {container_name}/{blob_name}")
        
        # Create blob service client
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        container_client = blob_service_client.get_container_client(container_name)
        blob_client = container_client.get_blob_client(blob_name)
        
        # Download the CSV from Azure
        print("⬇️ Downloading CSV from Azure Blob Storage")
        download_stream = blob_client.download_blob()
        df = pd.read_csv(download_stream)
        
        print(f"📊 Downloaded DataFrame shape: {df.shape}")
        print(f"📋 Columns count: {len(df.columns)}")
        
        # Check if this matches expected row count from table mapping
        expected_rows = 1450  # from table_mapping.json
        actual_rows = len(df)
        print(f"🔍 Row count check - Expected: {expected_rows}, Actual: {actual_rows}")
        
        if actual_rows != expected_rows:
            print(f"⚠️ Warning: Row count mismatch! Expected {expected_rows}, got {actual_rows}")
        
        # Create a mapping of original to cleaned column names
        column_mapping = {}
        cleaned_columns = []
        
        for i, col in enumerate(df.columns):
            cleaned_col = clean_column_name(col)
            # Handle duplicates by adding index
            if cleaned_col in cleaned_columns:
                cleaned_col = f"{cleaned_col}_{i}"
            cleaned_columns.append(cleaned_col)
            column_mapping[col] = cleaned_col
        
        # Rename columns
        df.columns = cleaned_columns
        
        print(f"🧹 Cleaned columns count: {len(df.columns)}")
        print(f"📝 Column mapping created with {len(column_mapping)} columns")
        
        # Show first few cleaned column names
        print("🔍 First 10 cleaned column names:")
        for i, col in enumerate(cleaned_columns[:10]):
            print(f"  {i+1}. {col}")
        
        # Save column mapping for reference
        mapping_file = "logs/column_mapping_sungrow_azure.json"
        os.makedirs("logs", exist_ok=True)
        with open(mapping_file, 'w') as f:
            json.dump(column_mapping, f, indent=2)
        print(f"💾 Column mapping saved to: {mapping_file}")
        
        # Convert all columns to string to avoid data type issues
        for col in df.columns:
            df[col] = df[col].astype(str)
        
        # Replace 'nan' strings with None
        df = df.replace('nan', None)
        
        # Load private key for Snowflake
        private_key_path = settings['SNOWFLAKE_PRIVATE_KEY_PATH']
        import base64
        
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
        
        # Connect to Snowflake
        print("❄️ Connecting to Snowflake (key pair auth)")
        conn = snowflake.connector.connect(**snowflake_config)
        
        # Create table with cleaned column names
        table_name = "SUNGROW"
        
        # Generate CREATE TABLE statement
        columns_sql = []
        for col in df.columns:
            columns_sql.append(f'"{col}" VARCHAR')
        
        create_table_sql = f"""
        CREATE OR REPLACE TABLE {table_name} (
            {', '.join(columns_sql)}
        )
        """
        
        print("🏗️ Creating Snowflake table with cleaned column names")
        print(f"📝 CREATE TABLE SQL (first 500 chars): {create_table_sql[:500]}...")
        
        cursor = conn.cursor()
        cursor.execute(create_table_sql)
        print("✅ Table created successfully")
        
        # Load data using write_pandas
        print("📤 Loading data into Snowflake table")
        success, nchunks, nrows, _ = write_pandas(
            conn, 
            df, 
            table_name,
            auto_create_table=False,
            overwrite=True
        )
        
        if success:
            print(f"✅ Data loaded successfully! Rows: {nrows}, Chunks: {nchunks}")
            
            # Verify the data was loaded
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cursor.fetchone()[0]
            print(f"🔍 Verified row count in Snowflake: {count}")
            
            # Show sample data
            cursor.execute(f"SELECT * FROM {table_name} LIMIT 3")
            sample_data = cursor.fetchall()
            print(f"📄 Sample data from Snowflake (first row): {sample_data[0] if sample_data else 'No data'}")
            
        else:
            print("❌ Failed to load data into Snowflake")
        
        cursor.close()
        conn.close()
        
        print("🎉 Sungrow data load from Azure completed successfully!")
        
    except Exception as e:
        print(f"❌ Error in load_sungrow_from_azure: {str(e)}")
        import traceback
        traceback.print_exc()
        raise

if __name__ == "__main__":
    load_sungrow_from_azure() 