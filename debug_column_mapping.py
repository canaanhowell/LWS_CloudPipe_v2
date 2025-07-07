#!/usr/bin/env python3
"""
Script to create column mappings between Azure CSV files and Snowflake tables
This will show the exact column name transformations and any mismatches
"""

import os
import sys
import pandas as pd
import snowflake.connector
import json
import re
from datetime import datetime
from azure.storage.blob import BlobServiceClient

# Robust logger import for local and container/module execution
try:
    from helper_scripts.Utils.logger import pipeline_logger
except ImportError:
    try:
        from logger import pipeline_logger
    except ImportError:
        import sys, os
        sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'helper_scripts', 'Utils')))
        from logger import pipeline_logger

# Helper to resolve paths relative to this script
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
def get_abs_path(rel_path):
    return os.path.abspath(os.path.join(SCRIPT_DIR, rel_path))

def clean_column_name(column_name):
    """
    Clean column names to be Snowflake-compatible
    Preserve original case and spacing to match existing Snowflake tables
    """
    # Remove quotes if present
    cleaned = column_name.strip().strip('"\'')
    
    # Replace problematic characters that cause SQL errors
    # Replace forward slashes with underscores
    cleaned = cleaned.replace('/', '_')
    # Replace parentheses with underscores
    cleaned = cleaned.replace('(', '_').replace(')', '_')
    # Replace question marks with underscores
    cleaned = cleaned.replace('?', '_')
    # Replace hyphens with underscores (but preserve existing ones)
    cleaned = cleaned.replace('-', '_')
    
    # Remove multiple consecutive underscores
    cleaned = re.sub(r'_+', '_', cleaned)
    
    # Remove leading/trailing underscores
    cleaned = cleaned.strip('_')
    
    # If empty after cleaning, use a default name
    if not cleaned:
        cleaned = 'UNNAMED_COLUMN'
    
    # Check if it's a reserved word and add prefix if needed
    reserved_words = ['SELECT', 'FROM', 'WHERE', 'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'DROP', 'TABLE', 'COLUMN']
    if cleaned.upper() in reserved_words:
        cleaned = 'COL_' + cleaned
    
    # Return the cleaned column name
    return cleaned

def get_snowflake_connection(settings):
    """Create and return a Snowflake connection"""
    # Force private key path to project config_files directory for Azure compatibility.
    private_key_path = get_abs_path('config_files/snowflake_private_key.txt')
    # Load private key in PEM format
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.backends import default_backend
    with open(private_key_path, 'r') as f:
        p_key = serialization.load_pem_private_key(
            f.read().encode('utf-8'),
            password=None,
            backend=default_backend()
        )
    # Convert to bytes format that Snowflake expects
    pkb = p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )
    snowflake_config = {
        'user': settings['SNOWFLAKE_USER'],
        'account': settings['SNOWFLAKE_ACCOUNT'],
        'private_key': pkb,
        'warehouse': settings['SNOWFLAKE_WAREHOUSE'],
        'database': settings['SNOWFLAKE_DATABASE'],
        'schema': 'PUBLIC'  # Default schema
    }
    return snowflake.connector.connect(**snowflake_config)

def get_snowflake_columns(cursor, table_name, database_schema):
    """Get column information from Snowflake table"""
    try:
        # Set database and schema context
        if '.' in database_schema:
            db_schema_parts = database_schema.split('.')
            if len(db_schema_parts) >= 2:
                database = db_schema_parts[0]
                schema = db_schema_parts[1]
                cursor.execute(f"USE DATABASE {database}")
                cursor.execute(f"USE SCHEMA {schema}")
        
        # Get table columns
        simple_table_name = table_name.split('.')[-1]
        cursor.execute(f"DESCRIBE TABLE {simple_table_name}")
        columns = cursor.fetchall()
        
        # Extract column names and types
        snowflake_columns = []
        for col in columns:
            col_name = col[0]  # Column name
            col_type = col[1]  # Data type
            snowflake_columns.append({
                'name': col_name,
                'type': col_type,
                'safe_name': clean_column_name(col_name)
            })
        
        return snowflake_columns
    except Exception as e:
        pipeline_logger.log("COLUMN_MAPPING", f"Error getting Snowflake columns for {table_name}: {str(e)}", "ERROR")
        return []

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

def create_column_mapping():
    """Create column mappings between Azure CSVs and Snowflake tables"""
    
    pipeline_logger.log("COLUMN_MAPPING", "🔍 Starting column mapping analysis", "INFO")
    
    try:
        # Load settings
        settings_path = get_abs_path('config_files/settings.json')
        with open(settings_path, 'r') as f:
            settings = json.load(f)
        
        # Load table mapping
        mapping_path = get_abs_path('config_files/table_mapping.json')
        with open(mapping_path, 'r') as f:
            table_mapping = json.load(f)
        
        # Azure Blob Storage connection
        connection_string = settings['AZURE_STORAGE_CONNECTION_STRING']
        container_name = settings['BLOB_CONTAINER']
        
        # Create blob service client
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        
        # Connect to Snowflake
        conn = get_snowflake_connection(settings)
        cursor = conn.cursor()
        
        # Track all mappings
        all_mappings = []
        
        for mapping in table_mapping:
            table_name = mapping['snowflake_table']
            azure_csv_name = mapping['azure_csv_name']
            database_schema = mapping['snowflake_database']
            
            pipeline_logger.log("COLUMN_MAPPING", f"📊 Analyzing: {table_name} <- {azure_csv_name}", "INFO")
            
            # Get Snowflake columns
            snowflake_columns = get_snowflake_columns(cursor, table_name, database_schema)
            
            # Get Azure CSV columns
            # Try exact name, then with .csv extension if not found
            df, error = load_csv_from_azure(blob_service_client, container_name, azure_csv_name)
            if df is None and not azure_csv_name.lower().endswith('.csv'):
                # Try with .csv extension
                df, error = load_csv_from_azure(blob_service_client, container_name, azure_csv_name + '.csv')
                if df is not None:
                    pipeline_logger.log("COLUMN_MAPPING", f"[Fallback] Loaded CSV with .csv extension: {azure_csv_name + '.csv'}", "INFO")
                    azure_csv_name = azure_csv_name + '.csv'
            if df is None:
                pipeline_logger.log("COLUMN_MAPPING", f"❌ Could not load CSV {azure_csv_name}: {error}", "ERROR")
                continue
            
            # Process CSV columns
            csv_columns = []
            for i, col in enumerate(df.columns):
                cleaned_col = clean_column_name(col)
                csv_columns.append({
                    'original_name': col,
                    'cleaned_name': cleaned_col,
                    'index': i
                })
            
            # Create mapping analysis
            mapping_analysis = {
                'table_name': table_name,
                'azure_csv_name': azure_csv_name,
                'database_schema': database_schema,
                'csv_columns': csv_columns,
                'snowflake_columns': snowflake_columns,
                'mapping_analysis': {
                    'csv_count': len(csv_columns),
                    'snowflake_count': len(snowflake_columns),
                    'exact_matches': [],
                    'cleaned_matches': [],
                    'missing_in_snowflake': [],
                    'missing_in_csv': [],
                    'unmatched_csv': [],
                    'unmatched_snowflake': []
                }
            }
            
            # Analyze mappings
            csv_cleaned_names = [col['cleaned_name'] for col in csv_columns]
            snowflake_names = [col['name'] for col in snowflake_columns]
            snowflake_safe_names = [col['safe_name'] for col in snowflake_columns]
            
            # Find exact matches
            for csv_col in csv_columns:
                if csv_col['cleaned_name'] in snowflake_names:
                    mapping_analysis['mapping_analysis']['exact_matches'].append({
                        'csv_original': csv_col['original_name'],
                        'csv_cleaned': csv_col['cleaned_name'],
                        'snowflake_name': csv_col['cleaned_name']
                    })
                elif csv_col['cleaned_name'] in snowflake_safe_names:
                    # Find the original Snowflake column name
                    for sf_col in snowflake_columns:
                        if sf_col['safe_name'] == csv_col['cleaned_name']:
                            mapping_analysis['mapping_analysis']['cleaned_matches'].append({
                                'csv_original': csv_col['original_name'],
                                'csv_cleaned': csv_col['cleaned_name'],
                                'snowflake_name': sf_col['name'],
                                'snowflake_type': sf_col['type']
                            })
                            break
                else:
                    mapping_analysis['mapping_analysis']['missing_in_snowflake'].append({
                        'csv_original': csv_col['original_name'],
                        'csv_cleaned': csv_col['cleaned_name']
                    })
            
            # Find Snowflake columns not in CSV
            matched_snowflake_names = set()
            for match in mapping_analysis['mapping_analysis']['exact_matches']:
                matched_snowflake_names.add(match['snowflake_name'])
            for match in mapping_analysis['mapping_analysis']['cleaned_matches']:
                matched_snowflake_names.add(match['snowflake_name'])
            
            for sf_col in snowflake_columns:
                if sf_col['name'] not in matched_snowflake_names:
                    mapping_analysis['mapping_analysis']['missing_in_csv'].append({
                        'snowflake_name': sf_col['name'],
                        'snowflake_type': sf_col['type']
                    })
            
            all_mappings.append(mapping_analysis)
            
            # Print summary for this table
            analysis = mapping_analysis['mapping_analysis']
            pipeline_logger.log("COLUMN_MAPPING", f"📋 {table_name} Summary:", "INFO")
            pipeline_logger.log("COLUMN_MAPPING", f"   CSV columns: {analysis['csv_count']}", "INFO")
            pipeline_logger.log("COLUMN_MAPPING", f"   Snowflake columns: {analysis['snowflake_count']}", "INFO")
            pipeline_logger.log("COLUMN_MAPPING", f"   Exact matches: {len(analysis['exact_matches'])}", "INFO")
            pipeline_logger.log("COLUMN_MAPPING", f"   Cleaned matches: {len(analysis['cleaned_matches'])}", "INFO")
            pipeline_logger.log("COLUMN_MAPPING", f"   Missing in Snowflake: {len(analysis['missing_in_snowflake'])}", "INFO")
            pipeline_logger.log("COLUMN_MAPPING", f"   Missing in CSV: {len(analysis['missing_in_csv'])}", "INFO")
        
        # Close connections
        cursor.close()
        conn.close()
        
        # Save detailed mapping report
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_file = get_abs_path(f'logs/column_mapping_report_{timestamp}.json')
        os.makedirs(get_abs_path('logs'), exist_ok=True)
        
        with open(report_file, 'w') as f:
            json.dump(all_mappings, f, indent=2, default=str)
        
        # Create human-readable summary
        summary_file = get_abs_path(f'logs/column_mapping_summary_{timestamp}.txt')
        with open(summary_file, 'w', encoding='utf-8', errors='replace') as f:
            f.write("=" * 80 + "\n")
            f.write("COLUMN MAPPING ANALYSIS REPORT\n")
            f.write("=" * 80 + "\n")
            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            
            for mapping in all_mappings:
                f.write(f"TABLE: {mapping['table_name']}\n")
                f.write(f"Azure CSV: {mapping['azure_csv_name']}\n")
                f.write(f"Database/Schema: {mapping['database_schema']}\n")
                f.write("-" * 60 + "\n")
                
                analysis = mapping['mapping_analysis']
                f.write(f"CSV Columns: {analysis['csv_count']}\n")
                f.write(f"Snowflake Columns: {analysis['snowflake_count']}\n")
                f.write(f"Exact Matches: {len(analysis['exact_matches'])}\n")
                f.write(f"Cleaned Matches: {len(analysis['cleaned_matches'])}\n")
                f.write(f"Missing in Snowflake: {len(analysis['missing_in_snowflake'])}\n")
                f.write(f"Missing in CSV: {len(analysis['missing_in_csv'])}\n\n")
                
                if analysis['exact_matches']:
                    f.write("EXACT MATCHES:\n")
                    for match in analysis['exact_matches']:
                        f.write(f"  CSV: '{match['csv_original']}' -> Snowflake: '{match['snowflake_name']}'\n")
                    f.write("\n")
                
                if analysis['cleaned_matches']:
                    f.write("CLEANED MATCHES:\n")
                    for match in analysis['cleaned_matches']:
                        f.write(f"  CSV: '{match['csv_original']}' -> Cleaned: '{match['csv_cleaned']}' -> Snowflake: '{match['snowflake_name']}' ({match['snowflake_type']})\n")
                    f.write("\n")
                
                if analysis['missing_in_snowflake']:
                    f.write("MISSING IN SNOWFLAKE:\n")
                    for missing in analysis['missing_in_snowflake']:
                        f.write(f"  CSV: '{missing['csv_original']}' -> Cleaned: '{missing['csv_cleaned']}'\n")
                    f.write("\n")
                
                if analysis['missing_in_csv']:
                    f.write("MISSING IN CSV:\n")
                    for missing in analysis['missing_in_csv']:
                        f.write(f"  Snowflake: '{missing['snowflake_name']}' ({missing['snowflake_type']})\n")
                    f.write("\n")
                
                f.write("=" * 80 + "\n\n")
        
        pipeline_logger.log("COLUMN_MAPPING", f"✅ Column mapping analysis completed!", "INFO")
        pipeline_logger.log("COLUMN_MAPPING", f"📄 Detailed report: {report_file}", "INFO")
        pipeline_logger.log("COLUMN_MAPPING", f"📄 Summary report: {summary_file}", "INFO")
        
        return all_mappings
        
    except Exception as e:
        pipeline_logger.log("COLUMN_MAPPING", f"❌ Error in column mapping analysis: {str(e)}", "ERROR")
        return None

if __name__ == "__main__":
    # Utility: List all blobs in the Azure container
    try:
        settings_path = get_abs_path('config_files/settings.json')
        with open(settings_path, 'r') as f:
            settings = json.load(f)
        connection_string = settings['AZURE_STORAGE_CONNECTION_STRING']
        container_name = settings['BLOB_CONTAINER']
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        container_client = blob_service_client.get_container_client(container_name)
        print("\nAvailable blobs in Azure container:")
        for blob in container_client.list_blobs():
            print(f"  - {blob.name}")
        print("\n--- End of blob list ---\n")
    except Exception as e:
        print(f"[ERROR] Could not list blobs: {e}")
    # Now run the mapping
    create_column_mapping() 