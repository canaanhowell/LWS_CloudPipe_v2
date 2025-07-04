#!/usr/bin/env python3
"""
Success verification script for load_from_azure.py
This script verifies that all CSV files were loaded correctly to their corresponding Snowflake tables
"""

import os
import sys
import json
import pandas as pd
import snowflake.connector
import base64
from datetime import datetime

# Add helper_scripts/Utils to path for logger import
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'helper_scripts', 'Utils'))
from logger import pipeline_logger

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

def verify_table_exists(cursor, table_name, database_schema):
    """Verify that a table exists in Snowflake"""
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
        
        # Check if table exists
        cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
        tables = cursor.fetchall()
        
        return len(tables) > 0
    except Exception as e:
        pipeline_logger.log("VERIFY_LOAD", f"Error checking table {table_name}: {str(e)}", "ERROR")
        return False

def verify_table_data(cursor, table_name, expected_rows):
    """Verify table data count and sample data"""
    try:
        # Get row count
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        actual_count = cursor.fetchone()[0]
        
        # Get sample data
        cursor.execute(f"SELECT * FROM {table_name} LIMIT 3")
        sample_data = cursor.fetchall()
        
        # Get column count
        cursor.execute(f"DESCRIBE TABLE {table_name}")
        columns = cursor.fetchall()
        column_count = len(columns)
        
        return {
            'exists': True,
            'actual_count': actual_count,
            'expected_count': expected_rows,
            'count_match': actual_count == expected_rows,
            'column_count': column_count,
            'sample_data': sample_data[0] if sample_data else None,
            'has_data': actual_count > 0
        }
    except Exception as e:
        return {
            'exists': False,
            'error': str(e),
            'actual_count': 0,
            'expected_count': expected_rows,
            'count_match': False,
            'column_count': 0,
            'sample_data': None,
            'has_data': False
        }

def verify_load_from_azure():
    """Verify that all CSV files were loaded correctly to their corresponding Snowflake tables"""
    
    pipeline_logger.log("VERIFY_LOAD", "🔍 Starting verification of Azure to Snowflake data load", "INFO")
    
    try:
        # Load settings
        pipeline_logger.log("VERIFY_LOAD", "🔑 Loading Snowflake credentials", "INFO")
        with open('../config_files/settings.json', 'r') as f:
            settings = json.load(f)
        
        # Load table mapping
        pipeline_logger.log("VERIFY_LOAD", "📋 Loading table mapping configuration", "INFO")
        with open('../config_files/table_mapping.json', 'r') as f:
            table_mapping = json.load(f)
        
        # Connect to Snowflake
        pipeline_logger.log("VERIFY_LOAD", "❄️ Connecting to Snowflake", "INFO")
        conn = get_snowflake_connection(settings)
        cursor = conn.cursor()
        
        # Track verification results
        verification_results = []
        total_tables = len(table_mapping)
        
        for i, mapping in enumerate(table_mapping, 1):
            table_name = mapping['snowflake_table']
            azure_csv_name = mapping['azure_csv_name']
            database_schema = mapping['snowflake_database']
            expected_rows = int(mapping['estimated_row_count'])
            
            pipeline_logger.log("VERIFY_LOAD", f"🔍 Verifying table {i}/{total_tables}: {table_name}", "INFO")
            pipeline_logger.log_progress("VERIFY_LOAD", i, total_tables, f"Verifying {table_name}")
            
            result = {
                'table_name': table_name,
                'azure_csv_name': azure_csv_name,
                'database_schema': database_schema,
                'expected_rows': expected_rows,
                'verification': None
            }
            
            # Verify table exists
            table_exists = verify_table_exists(cursor, table_name, database_schema)
            
            if not table_exists:
                result['verification'] = {
                    'exists': False,
                    'error': f"Table {table_name} not found in Snowflake",
                    'actual_count': 0,
                    'expected_count': expected_rows,
                    'count_match': False,
                    'column_count': 0,
                    'sample_data': None,
                    'has_data': False
                }
                pipeline_logger.log("VERIFY_LOAD", f"❌ Table {table_name} not found", "ERROR")
            else:
                # Verify table data
                data_verification = verify_table_data(cursor, table_name, expected_rows)
                result['verification'] = data_verification
                
                if data_verification['count_match']:
                    pipeline_logger.log("VERIFY_LOAD", f"✅ {table_name}: {data_verification['actual_count']} rows (matches expected)", "INFO")
                elif data_verification['has_data']:
                    pipeline_logger.log("VERIFY_LOAD", f"⚠️ {table_name}: {data_verification['actual_count']} rows (expected {expected_rows})", "WARNING")
                else:
                    pipeline_logger.log("VERIFY_LOAD", f"❌ {table_name}: No data found", "ERROR")
            
            verification_results.append(result)
        
        # Close connections
        cursor.close()
        conn.close()
        
        # Calculate summary statistics
        tables_exist = sum(1 for r in verification_results if r['verification']['exists'])
        count_matches = sum(1 for r in verification_results if r['verification'].get('count_match', False))
        has_data = sum(1 for r in verification_results if r['verification'].get('has_data', False))
        
        # Determine overall success
        overall_success = tables_exist == total_tables and count_matches == total_tables
        
        pipeline_logger.log("VERIFY_LOAD", f"📊 Verification Summary:", "INFO")
        pipeline_logger.log("VERIFY_LOAD", f"   Total tables: {total_tables}", "INFO")
        pipeline_logger.log("VERIFY_LOAD", f"   Tables exist: {tables_exist}", "INFO")
        pipeline_logger.log("VERIFY_LOAD", f"   Row counts match: {count_matches}", "INFO")
        pipeline_logger.log("VERIFY_LOAD", f"   Tables have data: {has_data}", "INFO")
        pipeline_logger.log("VERIFY_LOAD", f"   Overall success: {'✅ YES' if overall_success else '❌ NO'}", "INFO")
        
        # Save verification results
        verification_file = "../logs/verify_load_from_azure_results.json"
        with open(verification_file, 'w') as f:
            json.dump({
                'timestamp': datetime.now().isoformat(),
                'overall_success': overall_success,
                'total_tables': total_tables,
                'tables_exist': tables_exist,
                'count_matches': count_matches,
                'has_data': has_data,
                'verification_results': verification_results
            }, f, indent=2)
        
        pipeline_logger.log("VERIFY_LOAD", f"💾 Verification results saved to: {verification_file}", "INFO")
        
        # Log JSON summary
        pipeline_logger.log_json("VERIFY_LOAD", {
            'overall_success': overall_success,
            'total_tables': total_tables,
            'tables_exist': tables_exist,
            'count_matches': count_matches,
            'has_data': has_data,
            'success_percentage': (count_matches / total_tables * 100) if total_tables > 0 else 0
        }, "INFO")
        
        if overall_success:
            pipeline_logger.log("VERIFY_LOAD", "🎉 SUCCESS: All tables loaded correctly with matching row counts!", "INFO")
        else:
            pipeline_logger.log("VERIFY_LOAD", "⚠️ WARNING: Some tables may not have loaded correctly", "WARNING")
        
        return {
            'overall_success': overall_success,
            'verification_results': verification_results
        }
        
    except Exception as e:
        pipeline_logger.log("VERIFY_LOAD", f"❌ Critical error in verification: {str(e)}", "ERROR")
        import traceback
        traceback.print_exc()
        raise

if __name__ == "__main__":
    verify_load_from_azure() 