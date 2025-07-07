#!/usr/bin/env python3
"""
Generate CSV comparison of columns between CSV files and Snowflake tables
"""

import json
import os
import sys
import re
import csv
from datetime import datetime
from typing import Dict, List, Set, Tuple
from pathlib import Path

# Add the project root to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from helper_scripts.Utils.logger import log
import snowflake.connector
from azure.storage.blob import BlobServiceClient
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

def load_settings():
    """Load settings from settings.json"""
    try:
        with open('settings.json', 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        log("COLUMN_COMPARISON", "settings.json not found", "ERROR")
        sys.exit(1)

def load_table_mapping():
    """Load table mapping from config_files/table_mapping.json"""
    with open('config_files/table_mapping.json', 'r') as f:
        return json.load(f)

def get_snowflake_connection():
    """Get Snowflake connection using flat settings.json keys and a decoded private key."""
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

def get_azure_blob_service_client():
    """Get Azure Blob Service Client using flat settings.json keys"""
    settings = load_settings()
    connection_string = settings['AZURE_STORAGE_CONNECTION_STRING']
    return BlobServiceClient.from_connection_string(connection_string)

def get_snowflake_columns(cursor, database: str, schema: str, table_name: str) -> Dict[str, str]:
    """Get column information from Snowflake table"""
    query = f"""
    SELECT COLUMN_NAME, DATA_TYPE 
    FROM {database}.INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table_name}'
    ORDER BY ORDINAL_POSITION
    """
    cursor.execute(query)
    return {row[0]: row[1] for row in cursor.fetchall()}

def get_csv_headers(blob_service_client, container_name: str, blob_name: str) -> List[str]:
    """Get CSV headers from Azure blob"""
    try:
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
        stream = blob_client.download_blob()
        # Read first line (header) and decode
        header_line = stream.readall().decode('utf-8').splitlines()[0]
        return [col.strip() for col in header_line.split(',')]
    except Exception as e:
        log("COLUMN_COMPARISON", f"Error reading CSV headers from {blob_name}: {str(e)}", "ERROR")
        return []

def analyze_column_comparison(cursor, database: str, schema: str, table_name: str, csv_columns: List[str]) -> List[Dict]:
    """Analyze column comparison between CSV and Snowflake table"""
    snowflake_columns = get_snowflake_columns(cursor, database, schema, table_name)
    
    # Build safe column name mappings
    def safe(col):
        return re.sub(r'[^a-zA-Z0-9_]', '_', col).lower()
    
    snowflake_safe_set = set(safe(col) for col in snowflake_columns.keys())
    csv_safe_map = {safe(col): col for col in csv_columns}
    
    comparison_data = []
    
    # Add CSV columns
    for csv_col in csv_columns:
        safe_csv_col = safe(csv_col)
        snowflake_col = None
        snowflake_type = None
        status = "MISSING_IN_SNOWFLAKE"
        
        if safe_csv_col in snowflake_safe_set:
            # Find the actual Snowflake column name
            for sf_col in snowflake_columns.keys():
                if safe(sf_col) == safe_csv_col:
                    snowflake_col = sf_col
                    snowflake_type = snowflake_columns[sf_col]
                    status = "MATCH"
                    break
        
        comparison_data.append({
            'table_name': f"{database}.{schema}.{table_name}",
            'csv_column': csv_col,
            'snowflake_column': snowflake_col or "",
            'snowflake_type': snowflake_type or "",
            'status': status,
            'source': 'CSV'
        })
    
    # Add Snowflake-only columns
    for sf_col in snowflake_columns.keys():
        safe_sf_col = safe(sf_col)
        if safe_sf_col not in csv_safe_map:
            comparison_data.append({
                'table_name': f"{database}.{schema}.{table_name}",
                'csv_column': "",
                'snowflake_column': sf_col,
                'snowflake_type': snowflake_columns[sf_col],
                'status': "EXTRA_IN_SNOWFLAKE",
                'source': 'SNOWFLAKE'
            })
    
    return comparison_data

def generate_comparison_csv(comparison_data: List[Dict], output_filename: str):
    """Generate CSV file with column comparison data"""
    fieldnames = ['table_name', 'csv_column', 'snowflake_column', 'snowflake_type', 'status', 'source']
    
    with open(output_filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for row in comparison_data:
            writer.writerow(row)

def main():
    """Main function to generate column comparison CSV"""
    log("COLUMN_COMPARISON", "Starting column comparison analysis", "INFO")
    
    # Load settings and mapping
    settings = load_settings()
    table_mapping = load_table_mapping()
    
    # Get connections
    try:
        snowflake_conn = get_snowflake_connection()
        cursor = snowflake_conn.cursor()
        log("COLUMN_COMPARISON", "Successfully connected to Snowflake", "INFO")
    except Exception as e:
        log("COLUMN_COMPARISON", f"Failed to connect to Snowflake: {str(e)}", "ERROR")
        return False
    
    try:
        blob_service_client = get_azure_blob_service_client()
        log("COLUMN_COMPARISON", "Successfully connected to Azure Blob Storage", "INFO")
    except Exception as e:
        log("COLUMN_COMPARISON", f"Failed to connect to Azure Blob Storage: {str(e)}", "ERROR")
        return False
    
    all_comparison_data = []
    successful_tables = 0
    failed_tables = 0
    
    for mapping in table_mapping:
        database, schema, table_name = mapping['snowflake_table'].split('.')
        blob_name = mapping['azure_csv_name'] + '.csv'
        
        log("COLUMN_COMPARISON", f"Analyzing table: {database}.{schema}.{table_name}", "INFO")
        
        try:
            # Get CSV headers
            csv_columns = get_csv_headers(blob_service_client, settings['BLOB_CONTAINER'], blob_name)
            if not csv_columns:
                log("COLUMN_COMPARISON", f"Failed to get CSV headers for {blob_name}", "ERROR")
                failed_tables += 1
                continue
            
            # Analyze column comparison
            comparison_data = analyze_column_comparison(cursor, database, schema, table_name, csv_columns)
            all_comparison_data.extend(comparison_data)
            
            # Log summary
            matches = sum(1 for row in comparison_data if row['status'] == 'MATCH')
            missing = sum(1 for row in comparison_data if row['status'] == 'MISSING_IN_SNOWFLAKE')
            extra = sum(1 for row in comparison_data if row['status'] == 'EXTRA_IN_SNOWFLAKE')
            
            log("COLUMN_COMPARISON", f"Table {table_name}: {matches} matches, {missing} missing, {extra} extra", "INFO")
            successful_tables += 1
            
        except Exception as e:
            log("COLUMN_COMPARISON", f"Error analyzing {database}.{schema}.{table_name}: {str(e)}", "ERROR")
            failed_tables += 1
    
    # Generate CSV file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_filename = f"column_comparison_{timestamp}.csv"
    
    generate_comparison_csv(all_comparison_data, output_filename)
    
    log("COLUMN_COMPARISON", f"Generated comparison CSV: {output_filename}", "INFO")
    log("COLUMN_COMPARISON", f"Analysis completed: {successful_tables} successful, {failed_tables} failed", "INFO")
    
    # Print summary
    print(f"\nColumn Comparison Summary:")
    print(f"Total rows in CSV: {len(all_comparison_data)}")
    print(f"Tables processed: {successful_tables}")
    print(f"Output file: {output_filename}")
    
    cursor.close()
    snowflake_conn.close()
    
    return successful_tables > 0

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 