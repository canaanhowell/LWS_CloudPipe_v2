#!/usr/bin/env python3
"""
Script to truncate all tables that were loaded from Azure
This prepares the tables for reloading with proper column types
"""

import os
import sys
import json
import snowflake.connector
import base64
from datetime import datetime
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

# Add helper_scripts/Utils to path for logger import
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'Utils'))
from logger import pipeline_logger

def get_snowflake_connection(settings):
    """Create and return a Snowflake connection"""
    # Load private key for Snowflake (PEM format)
    private_key_path = settings['SNOWFLAKE_PRIVATE_KEY_PATH']
    with open(private_key_path, 'r') as f:
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
    
    snowflake_config = {
        'user': settings['SNOWFLAKE_USER'],
        'account': settings['SNOWFLAKE_ACCOUNT'],
        'private_key': pkb,
        'warehouse': settings['SNOWFLAKE_WAREHOUSE'],
        'database': settings['SNOWFLAKE_DATABASE'],
        'schema': 'PUBLIC'  # Default schema
    }
    
    return snowflake.connector.connect(**snowflake_config)

def truncate_all_tables():
    """Truncate all tables that were loaded from Azure"""
    
    pipeline_logger.log("TRUNCATE_TABLES", "🗑️ Starting table truncation process", "INFO")
    
    try:
        # Load settings from config file
        settings_path = os.path.join('config_files', 'settings.json')
        with open(settings_path, 'r') as f:
            settings = json.load(f)
        
        # Load table mapping configuration
        mapping_path = os.path.join('config_files', 'table_mapping.json')
        with open(mapping_path, 'r') as f:
            table_mapping = json.load(f)
        
        # Connect to Snowflake
        pipeline_logger.log("TRUNCATE_TABLES", "❄️ Connecting to Snowflake", "INFO")
        conn = get_snowflake_connection(settings)
        cursor = conn.cursor()
        
        # Track results
        results = []
        total_tables = len(table_mapping)
        
        for i, mapping in enumerate(table_mapping, 1):
            table_name = mapping['snowflake_table']
            database_schema = mapping['snowflake_database']
            
            pipeline_logger.log("TRUNCATE_TABLES", f"🗑️ Truncating table {i}/{total_tables}: {table_name}", "INFO")
            pipeline_logger.log_progress("TRUNCATE_TABLES", i, total_tables, f"Truncating {table_name}")
            
            result = {
                'table_name': table_name,
                'database_schema': database_schema,
                'status': 'pending',
                'error': None,
                'rows_truncated': 0
            }
            
            try:
                # Parse database and schema
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
                
                # Get row count before truncation
                simple_table_name = table_name.split('.')[-1]
                cursor.execute(f"SELECT COUNT(*) FROM {simple_table_name}")
                row_count = cursor.fetchone()[0]
                
                pipeline_logger.log("TRUNCATE_TABLES", f"📊 Table {simple_table_name} has {row_count} rows", "INFO")
                
                # Truncate the table
                cursor.execute(f"TRUNCATE TABLE {simple_table_name}")
                
                # Verify truncation
                cursor.execute(f"SELECT COUNT(*) FROM {simple_table_name}")
                new_row_count = cursor.fetchone()[0]
                
                if new_row_count == 0:
                    result['status'] = 'success'
                    result['rows_truncated'] = row_count
                    pipeline_logger.log("TRUNCATE_TABLES", f"✅ Successfully truncated {simple_table_name}: {row_count} rows removed", "INFO")
                else:
                    result['status'] = 'failed'
                    result['error'] = f"Table still has {new_row_count} rows after truncation"
                    pipeline_logger.log("TRUNCATE_TABLES", f"❌ Failed to truncate {simple_table_name}: {result['error']}", "ERROR")
                
            except Exception as e:
                result['status'] = 'failed'
                result['error'] = str(e)
                pipeline_logger.log("TRUNCATE_TABLES", f"❌ Error truncating {table_name}: {str(e)}", "ERROR")
            
            results.append(result)
        
        # Close connections
        cursor.close()
        conn.close()
        
        # Log final results
        successful = sum(1 for r in results if r['status'] == 'success')
        failed = sum(1 for r in results if r['status'] == 'failed')
        total_rows_truncated = sum(r['rows_truncated'] for r in results if r['status'] == 'success')
        
        pipeline_logger.log("TRUNCATE_TABLES", f"🎉 Truncation completed! Success: {successful}, Failed: {failed}, Total rows removed: {total_rows_truncated}", "INFO")
        
        # Save detailed results
        results_file = "../logs/truncate_tables_results.json"
        with open(results_file, 'w') as f:
            json.dump({
                'timestamp': datetime.now().isoformat(),
                'total_tables': total_tables,
                'successful': successful,
                'failed': failed,
                'total_rows_truncated': total_rows_truncated,
                'results': results
            }, f, indent=2)
        
        pipeline_logger.log("TRUNCATE_TABLES", f"💾 Truncation results saved to: {results_file}", "INFO")
        
        # Log JSON summary
        pipeline_logger.log_json("TRUNCATE_TABLES", {
            'total_tables': total_tables,
            'successful': successful,
            'failed': failed,
            'total_rows_truncated': total_rows_truncated,
            'completion_percentage': (successful / total_tables * 100) if total_tables > 0 else 0
        }, "INFO")
        
        return results
        
    except Exception as e:
        pipeline_logger.log("TRUNCATE_TABLES", f"❌ Critical error in truncate_all_tables: {str(e)}", "ERROR")
        import traceback
        traceback.print_exc()
        raise

if __name__ == "__main__":
    truncate_all_tables() 