#!/usr/bin/env python3
"""
Script to create intelligent column type mappings for CSV files
This script analyzes each cleaned CSV and maps columns to appropriate Snowflake data types
based on column names, keywords, and data patterns
"""

import os
import sys
import json
import pandas as pd
import re
import base64
import snowflake.connector
from datetime import datetime
from azure.storage.blob import BlobServiceClient
import numpy as np

# Add helper_scripts/Utils to path for logger import
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'Utils'))
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

def analyze_column_type(column_name, sample_data, max_sample_size=1000):
    """
    Analyze a column to determine the appropriate Snowflake data type
    based on column name keywords and data patterns
    """
    # Convert to lowercase for keyword matching
    col_lower = column_name.lower()
    
    # Sample the data for analysis (avoid processing entire dataset)
    if len(sample_data) > max_sample_size:
        sample = sample_data.sample(n=max_sample_size, random_state=42)
    else:
        sample = sample_data
    
    # Remove null/empty values for analysis
    non_null_sample = sample.dropna()
    
    # Default to VARCHAR
    snowflake_type = "VARCHAR"
    precision = None
    scale = None
    
    # Check for DATE keywords and patterns
    date_keywords = ['date', 'created', 'modified', 'updated', 'timestamp', 'time']
    if any(keyword in col_lower for keyword in date_keywords):
        # Check if data looks like dates
        if len(non_null_sample) > 0:
            # Try to parse as date
            try:
                pd.to_datetime(non_null_sample, errors='raise')
                snowflake_type = "DATE"
            except:
                # If date parsing fails, check for timestamp patterns
                try:
                    pd.to_datetime(non_null_sample, format='%Y-%m-%d %H:%M:%S', errors='raise')
                    snowflake_type = "TIMESTAMP"
                except:
                    pass
    
    # Check for NUMBER/AMOUNT keywords
    number_keywords = ['amount', 'price', 'cost', 'budget', 'total', 'sum', 'count', 'number', 'id', 'quantity', 'qty']
    if any(keyword in col_lower for keyword in number_keywords):
        if len(non_null_sample) > 0:
            # Try to convert to numeric
            try:
                numeric_data = pd.to_numeric(non_null_sample, errors='raise')
                
                # Check if it's integer or decimal
                if numeric_data.dtype in ['int64', 'int32']:
                    snowflake_type = "NUMBER"
                    # Determine precision based on max value
                    max_val = abs(numeric_data.max())
                    if max_val < 10:
                        precision = 1
                    elif max_val < 100:
                        precision = 2
                    elif max_val < 1000:
                        precision = 4
                    elif max_val < 10000:
                        precision = 5
                    elif max_val < 100000:
                        precision = 6
                    elif max_val < 1000000:
                        precision = 7
                    else:
                        precision = 10
                else:
                    # It's a decimal/float
                    snowflake_type = "FLOAT"
                    
            except:
                # If conversion fails, keep as VARCHAR
                pass
    
    # Check for BOOLEAN keywords
    boolean_keywords = ['flag', 'is_', 'has_', 'active', 'enabled', 'status', 'boolean', 'bool']
    if any(keyword in col_lower for keyword in boolean_keywords):
        if len(non_null_sample) > 0:
            # Check if data looks like boolean
            unique_values = non_null_sample.astype(str).str.lower().unique()
            if len(unique_values) <= 4:  # Small number of unique values
                boolean_values = ['true', 'false', '1', '0', 'yes', 'no', 'y', 'n']
                if all(val in boolean_values for val in unique_values):
                    snowflake_type = "BOOLEAN"
    
    # Check for specific patterns in data
    if len(non_null_sample) > 0:
        # Check for email patterns
        if 'email' in col_lower or 'mail' in col_lower:
            email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
            if all(re.match(email_pattern, str(val)) for val in non_null_sample.head(10)):
                snowflake_type = "VARCHAR(255)"
        
        # Check for phone patterns
        elif 'phone' in col_lower or 'tel' in col_lower:
            snowflake_type = "VARCHAR(20)"
        
        # Check for URL patterns
        elif 'url' in col_lower or 'link' in col_lower or 'website' in col_lower:
            snowflake_type = "VARCHAR(500)"
        
        # Check for name patterns
        elif 'name' in col_lower:
            snowflake_type = "VARCHAR(100)"
        
        # Check for description patterns
        elif 'description' in col_lower or 'desc' in col_lower or 'notes' in col_lower:
            snowflake_type = "VARCHAR(1000)"
        
        # Check for code/ID patterns
        elif 'code' in col_lower or 'id' in col_lower:
            # Check if it's numeric
            try:
                pd.to_numeric(non_null_sample, errors='raise')
                snowflake_type = "NUMBER"
                precision = 10
            except:
                snowflake_type = "VARCHAR(50)"
    
    # Build the final type string
    if snowflake_type == "NUMBER" and precision:
        return f"NUMBER({precision})"
    elif snowflake_type == "VARCHAR" and "(" not in snowflake_type:
        # Default VARCHAR length based on data
        if len(non_null_sample) > 0:
            max_length = non_null_sample.astype(str).str.len().max()
            if max_length <= 50:
                return "VARCHAR(50)"
            elif max_length <= 100:
                return "VARCHAR(100)"
            elif max_length <= 255:
                return "VARCHAR(255)"
            elif max_length <= 500:
                return "VARCHAR(500)"
            else:
                return "VARCHAR(1000)"
        else:
            return "VARCHAR(255)"
    else:
        return snowflake_type

def create_column_type_mapping():
    """Create intelligent column type mappings for all CSV files"""
    
    pipeline_logger.log("COLUMN_MAPPING", "🔍 Starting intelligent column type mapping", "INFO")
    
    try:
        # Load settings
        pipeline_logger.log("COLUMN_MAPPING", "🔑 Loading Azure credentials", "INFO")
        with open('../config_files/settings.json', 'r') as f:
            settings = json.load(f)
        
        # Load table mapping
        pipeline_logger.log("COLUMN_MAPPING", "📋 Loading table mapping configuration", "INFO")
        with open('../config_files/table_mapping.json', 'r') as f:
            table_mapping = json.load(f)
        
        # Azure Blob Storage connection
        connection_string = settings['AZURE_STORAGE_CONNECTION_STRING']
        container_name = settings['BLOB_CONTAINER']
        
        pipeline_logger.log("COLUMN_MAPPING", f"📦 Connecting to Azure Blob Storage: {container_name}", "INFO")
        
        # Create blob service client
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        
        # List available blobs in Azure container
        pipeline_logger.log("COLUMN_MAPPING", "📋 Listing available blobs in Azure container", "INFO")
        available_blobs, error = list_azure_blobs(blob_service_client, container_name)
        
        if error:
            pipeline_logger.log("COLUMN_MAPPING", f"❌ Failed to list Azure blobs: {error}", "ERROR")
            raise Exception(f"Failed to list Azure blobs: {error}")
        
        pipeline_logger.log("COLUMN_MAPPING", f"📦 Found {len(available_blobs)} blobs in container", "INFO")
        
        # Track results
        results = []
        total_tables = len(table_mapping)
        
        for i, mapping in enumerate(table_mapping, 1):
            table_name = mapping['snowflake_table']
            azure_csv_name = mapping['azure_csv_name']
            database_schema = mapping['snowflake_database']
            
            pipeline_logger.log("COLUMN_MAPPING", f"🔍 Analyzing table {i}/{total_tables}: {table_name}", "INFO")
            pipeline_logger.log_progress("COLUMN_MAPPING", i, total_tables, f"Analyzing {table_name}")
            
            result = {
                'table_name': table_name,
                'azure_csv_name': azure_csv_name,
                'database_schema': database_schema,
                'status': 'pending',
                'error': None,
                'column_mappings': {},
                'total_columns': 0
            }
            
            try:
                # Find matching blob in Azure
                pipeline_logger.log("COLUMN_MAPPING", f"🔍 Looking for {azure_csv_name} in Azure container", "INFO")
                matching_blob = find_matching_blob(azure_csv_name, available_blobs)
                
                if not matching_blob:
                    result['status'] = 'failed'
                    result['error'] = f"Could not find matching blob for {azure_csv_name} in available blobs"
                    pipeline_logger.log("COLUMN_MAPPING", f"❌ No matching blob found for {azure_csv_name}", "ERROR")
                    results.append(result)
                    continue
                
                pipeline_logger.log("COLUMN_MAPPING", f"✅ Found matching blob: {matching_blob}", "INFO")
                
                # Download CSV from Azure
                pipeline_logger.log("COLUMN_MAPPING", f"⬇️ Downloading {matching_blob} from Azure", "INFO")
                df, error = load_csv_from_azure(blob_service_client, container_name, matching_blob)
                
                if error:
                    result['status'] = 'failed'
                    result['error'] = error
                    pipeline_logger.log("COLUMN_MAPPING", f"❌ Failed to download {matching_blob}: {error}", "ERROR")
                    results.append(result)
                    continue
                
                pipeline_logger.log("COLUMN_MAPPING", f"📊 Downloaded DataFrame shape: {df.shape}", "INFO")
                
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
                
                # Analyze each column for type mapping
                type_mapping = {}
                for original_col, cleaned_col in column_mapping.items():
                    pipeline_logger.log("COLUMN_MAPPING", f"🔍 Analyzing column: {original_col} -> {cleaned_col}", "DEBUG")
                    
                    # Analyze the column type
                    snowflake_type = analyze_column_type(original_col, df[original_col])
                    type_mapping[cleaned_col] = {
                        'original_name': original_col,
                        'snowflake_type': snowflake_type,
                        'sample_values': df[original_col].dropna().head(3).tolist()
                    }
                    
                    pipeline_logger.log("COLUMN_MAPPING", f"📝 {cleaned_col}: {snowflake_type}", "DEBUG")
                
                result['column_mappings'] = type_mapping
                result['total_columns'] = len(type_mapping)
                result['status'] = 'success'
                
                pipeline_logger.log("COLUMN_MAPPING", f"✅ Successfully analyzed {len(type_mapping)} columns for {table_name}", "INFO")
                
                # Save individual table mapping
                mapping_file = f"../logs/column_type_mapping_{table_name.replace('.', '_')}.json"
                os.makedirs("../logs", exist_ok=True)
                with open(mapping_file, 'w') as f:
                    json.dump({
                        'table_name': table_name,
                        'azure_csv_name': azure_csv_name,
                        'database_schema': database_schema,
                        'timestamp': datetime.now().isoformat(),
                        'column_mappings': type_mapping
                    }, f, indent=2)
                
                pipeline_logger.log("COLUMN_MAPPING", f"💾 Column type mapping saved to: {mapping_file}", "INFO")
                
            except Exception as e:
                result['status'] = 'failed'
                result['error'] = str(e)
                pipeline_logger.log("COLUMN_MAPPING", f"❌ Error analyzing {table_name}: {str(e)}", "ERROR")
            
            results.append(result)
        
        # Save comprehensive results
        comprehensive_file = "../logs/comprehensive_column_type_mapping.json"
        with open(comprehensive_file, 'w') as f:
            json.dump({
                'timestamp': datetime.now().isoformat(),
                'total_tables': total_tables,
                'results': results
            }, f, indent=2)
        
        # Log final results
        successful = sum(1 for r in results if r['status'] == 'success')
        failed = sum(1 for r in results if r['status'] == 'failed')
        total_columns = sum(r['total_columns'] for r in results if r['status'] == 'success')
        
        pipeline_logger.log("COLUMN_MAPPING", f"🎉 Column type mapping completed! Success: {successful}, Failed: {failed}, Total columns analyzed: {total_columns}", "INFO")
        pipeline_logger.log("COLUMN_MAPPING", f"💾 Comprehensive mapping saved to: {comprehensive_file}", "INFO")
        
        # Log JSON summary
        pipeline_logger.log_json("COLUMN_MAPPING", {
            'total_tables': total_tables,
            'successful': successful,
            'failed': failed,
            'total_columns': total_columns,
            'completion_percentage': (successful / total_tables * 100) if total_tables > 0 else 0
        }, "INFO")
        
        return results
        
    except Exception as e:
        pipeline_logger.log("COLUMN_MAPPING", f"❌ Critical error in create_column_type_mapping: {str(e)}", "ERROR")
        import traceback
        traceback.print_exc()
        raise

if __name__ == "__main__":
    create_column_type_mapping() 