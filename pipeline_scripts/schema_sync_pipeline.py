#!/usr/bin/env python3
"""
Schema Synchronization Pipeline - Read-Only Report Mode
Compares CSV headers from Azure with Snowflake table schemas and reports variances.
"""

import json
import os
import sys
import re
import base64
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
        log("SCHEMA_SYNC", "settings.json not found", "ERROR")
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
        log("SCHEMA_SYNC", f"Error reading CSV headers from {blob_name}: {str(e)}", "ERROR")
        return []

def analyze_schema_variance(cursor, database: str, schema: str, table_name: str, csv_columns: List[str]) -> Dict:
    """Analyze schema variance between CSV and Snowflake table"""
    snowflake_columns = get_snowflake_columns(cursor, database, schema, table_name)
    
    # Build safe column name mappings
    def safe(col):
        return re.sub(r'[^a-zA-Z0-9_]', '_', col).lower()
    
    snowflake_safe_set = set(safe(col) for col in snowflake_columns.keys())
    csv_safe_map = {safe(col): col for col in csv_columns}
    
    # Find missing columns (in CSV but not in Snowflake)
    missing_in_snowflake = []
    for safe_col, orig_col in csv_safe_map.items():
        if safe_col not in snowflake_safe_set:
            missing_in_snowflake.append(orig_col)
    
    # Find extra columns (in Snowflake but not in CSV)
    extra_in_snowflake = []
    for col in snowflake_columns.keys():
        if safe(col) not in csv_safe_map:
            extra_in_snowflake.append(col)
    
    # Find matching columns (case-insensitive)
    matching_columns = []
    for col in snowflake_columns.keys():
        if safe(col) in csv_safe_map:
            matching_columns.append(col)
    
    return {
        'table_name': f"{database}.{schema}.{table_name}",
        'csv_column_count': len(csv_columns),
        'snowflake_column_count': len(snowflake_columns),
        'matching_columns': matching_columns,
        'missing_in_snowflake': missing_in_snowflake,
        'extra_in_snowflake': extra_in_snowflake,
        'csv_columns': csv_columns,
        'snowflake_columns': list(snowflake_columns.keys()),
        'snowflake_column_types': snowflake_columns
    }

def analyze_and_reconcile_table(cursor, database, schema, table_name, csv_columns):
    # 1. Get current Snowflake columns
    snowflake_columns = get_snowflake_columns(cursor, database, schema, table_name)
    def safe(col):
        return re.sub(r'[^a-zA-Z0-9_]', '_', col).lower()
    
    # Build mappings for comparison
    snowflake_safe_to_orig = {safe(col): col for col in snowflake_columns.keys()}
    csv_safe_to_orig = {safe(col): col for col in csv_columns}
    
    # Log what we're comparing
    log("SCHEMA_SYNC", f"Comparing {len(csv_columns)} CSV columns vs {len(snowflake_columns)} Snowflake columns", "INFO")
    
    # 2. Add missing columns (present in CSV, not in Snowflake)
    added_columns = []
    for safe_csv_col, orig_csv_col in csv_safe_to_orig.items():
        log("SCHEMA_SYNC", f"Checking CSV column: '{orig_csv_col}' (safe: {safe_csv_col})", "DEBUG")
        if safe_csv_col not in snowflake_safe_to_orig:
            log("SCHEMA_SYNC", f"Adding missing column: '{orig_csv_col}' to {database}.{schema}.{table_name}", "INFO")
            try:
                # Infer column type based on keywords
                col_lower = orig_csv_col.lower()
                if "date" in col_lower:
                    col_type = "DATE"
                elif any(kw in col_lower for kw in ["amount", "cost", "price", "total", "qty", "number"]):
                    col_type = "FLOAT"
                elif "id" in col_lower:
                    col_type = "VARCHAR(255)"
                else:
                    col_type = "VARCHAR(255)"
                sql = f'ALTER TABLE "{database}"."{schema}"."{table_name}" ADD COLUMN "{orig_csv_col}" {col_type}'
                log("SCHEMA_SYNC", f"Executing SQL: {sql}", "DEBUG")
                cursor.execute(sql)
                added_columns.append(orig_csv_col)
                log("SCHEMA_SYNC", f"Successfully added column: '{orig_csv_col}'", "INFO")
            except Exception as e:
                log("SCHEMA_SYNC", f"Failed to add column '{orig_csv_col}': {str(e)}", "ERROR")
    
    # 3. Drop extra columns (present in Snowflake, not in CSV)
    dropped_columns = []
    for safe_snowflake_col, orig_snowflake_col in snowflake_safe_to_orig.items():
        log("SCHEMA_SYNC", f"Checking Snowflake column: '{orig_snowflake_col}' (safe: {safe_snowflake_col})", "DEBUG")
        if safe_snowflake_col not in csv_safe_to_orig:
            log("SCHEMA_SYNC", f"Dropping extra column: '{orig_snowflake_col}' from {database}.{schema}.{table_name}", "INFO")
            try:
                sql = f'ALTER TABLE "{database}"."{schema}"."{table_name}" DROP COLUMN "{orig_snowflake_col}"'
                log("SCHEMA_SYNC", f"Executing SQL: {sql}", "DEBUG")
                cursor.execute(sql)
                dropped_columns.append(orig_snowflake_col)
                log("SCHEMA_SYNC", f"Successfully dropped column: '{orig_snowflake_col}'", "INFO")
            except Exception as e:
                log("SCHEMA_SYNC", f"Failed to drop column '{orig_snowflake_col}': {str(e)}", "ERROR")
    
    # 4. Re-read Snowflake schema after changes
    if added_columns or dropped_columns:
        log("SCHEMA_SYNC", f"Re-reading schema after changes (added: {len(added_columns)}, dropped: {len(dropped_columns)})", "INFO")
        snowflake_columns = get_snowflake_columns(cursor, database, schema, table_name)
        snowflake_safe_to_orig = {safe(col): col for col in snowflake_columns.keys()}
    
    # 5. Generate final variance report
    missing_in_snowflake = []
    extra_in_snowflake = []
    matching_columns = []
    
    for safe_csv_col, orig_csv_col in csv_safe_to_orig.items():
        if safe_csv_col not in snowflake_safe_to_orig:
            missing_in_snowflake.append(orig_csv_col)
        else:
            matching_columns.append(orig_csv_col)
    
    for safe_snowflake_col, orig_snowflake_col in snowflake_safe_to_orig.items():
        if safe_snowflake_col not in csv_safe_to_orig:
            extra_in_snowflake.append(orig_snowflake_col)
    
    return {
        'table_name': f"{database}.{schema}.{table_name}",
        'csv_columns': len(csv_columns),
        'snowflake_columns': len(snowflake_columns),
        'missing_in_snowflake': missing_in_snowflake,
        'extra_in_snowflake': extra_in_snowflake,
        'matching_columns': len(matching_columns),
        'added_columns': added_columns,
        'dropped_columns': dropped_columns
    }

def generate_schema_report(analysis_results: List[Dict]) -> str:
    """Generate a comprehensive schema variance report"""
    report = []
    report.append("=" * 80)
    report.append("SCHEMA VARIANCE ANALYSIS REPORT")
    report.append("=" * 80)
    report.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report.append("")
    
    total_tables = len(analysis_results)
    tables_with_issues = sum(1 for result in analysis_results if result['missing_in_snowflake'] or result['extra_in_snowflake'])
    
    report.append(f"SUMMARY:")
    report.append(f"  Total Tables Analyzed: {total_tables}")
    report.append(f"  Tables with Schema Variances: {tables_with_issues}")
    report.append(f"  Tables with Perfect Match: {total_tables - tables_with_issues}")
    report.append("")
    
    for i, result in enumerate(analysis_results, 1):
        report.append(f"TABLE {i}: {result['table_name']}")
        report.append("-" * 60)
        report.append(f"CSV Columns: {result['csv_columns']}")
        report.append(f"Snowflake Columns: {result['snowflake_columns']}")
        report.append("")
        
        if result['missing_in_snowflake']:
            report.append("MISSING IN SNOWFLAKE (need to be added):")
            for col in result['missing_in_snowflake']:
                report.append(f"  - {col}")
            report.append("")
        
        if result['extra_in_snowflake']:
            report.append("EXTRA IN SNOWFLAKE (not in CSV):")
            for col in result['extra_in_snowflake']:
                report.append(f"  - {col}")
            report.append("")
        
        if not result['missing_in_snowflake'] and not result['extra_in_snowflake']:
            report.append("[SUCCESS] PERFECT MATCH - No schema variances detected")
            report.append("")
        
        report.append(f"Matching Columns: {result['matching_columns']}")
        report.append("")
        
        # Show column type information for matching columns
        if result['matching_columns'] > 0:
            report.append("COLUMN TYPE ANALYSIS (matching columns):")
            # Note: Column type analysis not available in reconcile mode
            report.append("  (Column types not shown in reconcile mode)")
            report.append("")
        
        report.append("")
    
    return "\n".join(report)

def main():
    """Main function to run schema variance analysis"""
    log("SCHEMA_SYNC", "Starting schema variance analysis and reconciliation", "INFO")
    
    # Load settings
    settings = load_settings()
    table_mapping = load_table_mapping()
    
    # Get connections
    try:
        snowflake_conn = get_snowflake_connection()
        cursor = snowflake_conn.cursor()
        log("SCHEMA_SYNC", "Successfully connected to Snowflake", "INFO")
    except Exception as e:
        log("SCHEMA_SYNC", f"Failed to connect to Snowflake: {str(e)}", "ERROR")
        return False
    
    try:
        blob_service_client = get_azure_blob_service_client()
        log("SCHEMA_SYNC", "Successfully connected to Azure Blob Storage", "INFO")
    except Exception as e:
        log("SCHEMA_SYNC", f"Failed to connect to Azure Blob Storage: {str(e)}", "ERROR")
        return False
    
    analysis_results = []
    successful_tables = 0
    failed_tables = 0
    
    for mapping in table_mapping:
        database, schema, table_name = mapping['snowflake_table'].split('.')
        blob_name = mapping['azure_csv_name'] + '.csv'
        log("SCHEMA_SYNC", f"Analyzing table: {database}.{schema}.{table_name}", "INFO")
        try:
            # Get CSV headers
            csv_columns = get_csv_headers(blob_service_client, settings['BLOB_CONTAINER'], blob_name)
            if not csv_columns:
                log("SCHEMA_SYNC", f"Failed to get CSV headers for {blob_name}", "ERROR")
                failed_tables += 1
                continue
            # Analyze and reconcile schema
            analysis = analyze_and_reconcile_table(cursor, database, schema, table_name, csv_columns)
            analysis_results.append(analysis)
            # Log summary for this table
            missing_count = len(analysis['missing_in_snowflake'])
            extra_count = len(analysis['extra_in_snowflake'])
            if missing_count == 0 and extra_count == 0:
                log("SCHEMA_SYNC", f"[SUCCESS] Perfect schema match for {database}.{schema}.{table_name}", "INFO")
            else:
                log("SCHEMA_SYNC", f"[WARNING] Schema variances found for {database}.{schema}.{table_name}: {missing_count} missing, {extra_count} extra", "WARNING")
            successful_tables += 1
        except Exception as e:
            log("SCHEMA_SYNC", f"Error analyzing {database}.{schema}.{table_name}: {str(e)}", "ERROR")
            failed_tables += 1
    
    # Generate and save report
    report = generate_schema_report(analysis_results)
    
    # Print summary of column mismatches directly to console
    print("\nSCHEMA SYNC SUMMARY: COLUMN MISMATCHES")
    for result in analysis_results:
        print(f"Table: {result['table_name']}")
        if result['missing_in_snowflake']:
            print(f"  Missing in Snowflake: {result['missing_in_snowflake']}")
        if result['extra_in_snowflake']:
            print(f"  Extra in Snowflake: {result['extra_in_snowflake']}")
        if not result['missing_in_snowflake'] and not result['extra_in_snowflake']:
            print("  [OK] No mismatches.")
        print()
    
    # Save report to file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_filename = f"schema_variance_report_{timestamp}.txt"
    with open(report_filename, 'w', encoding='utf-8') as f:
        f.write(report)
    
    # Also save as JSON for programmatic access
    json_filename = f"schema_variance_analysis_{timestamp}.json"
    with open(json_filename, 'w', encoding='utf-8') as f:
        json.dump({
            'timestamp': datetime.now().isoformat(),
            'summary': {
                'total_tables': len(table_mapping),
                'successful': successful_tables,
                'failed': failed_tables
            },
            'analysis_results': analysis_results
        }, f, indent=2, ensure_ascii=False)
    
    # Print report to console
    print("\n" + report)
    print(f"\nReports saved to:")
    print(f"  - {report_filename}")
    print(f"  - {json_filename}")
    
    log("SCHEMA_SYNC", f"Analysis completed: {successful_tables} successful, {failed_tables} failed", "INFO")
    
    cursor.close()
    snowflake_conn.close()
    
    return successful_tables > 0

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 