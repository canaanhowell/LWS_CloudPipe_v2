#!/usr/bin/env python3
"""
Script to check column types in Snowflake tables before and after running load_from_azure.py
This will help identify if column types are being changed to VARCHAR
"""

import os
import sys
import json
import snowflake.connector
from datetime import datetime

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

def get_table_column_types(cursor, table_name, database_schema):
    """Get column types for a specific table"""
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
        column_types = {}
        for col in columns:
            col_name = col[0]  # Column name
            col_type = col[1]  # Data type
            column_types[col_name] = col_type
        
        return column_types
    except Exception as e:
        pipeline_logger.log("COLUMN_TYPES", f"Error getting column types for {table_name}: {str(e)}", "ERROR")
        return {}

def check_column_types():
    """Check column types before and after running load_from_azure.py"""
    
    pipeline_logger.log("COLUMN_TYPES", "🔍 Starting column type analysis", "INFO")
    
    try:
        # Load settings
        settings_path = get_abs_path('config_files/settings.json')
        with open(settings_path, 'r') as f:
            settings = json.load(f)
        
        # Load table mapping
        mapping_path = get_abs_path('config_files/table_mapping.json')
        with open(mapping_path, 'r') as f:
            table_mapping = json.load(f)
        
        # Connect to Snowflake
        conn = get_snowflake_connection(settings)
        cursor = conn.cursor()
        
        # Check column types BEFORE running load_from_azure.py
        pipeline_logger.log("COLUMN_TYPES", "📊 Checking column types BEFORE load_from_azure.py", "INFO")
        before_types = {}
        
        for mapping in table_mapping:
            table_name = mapping['snowflake_table']
            database_schema = mapping['snowflake_database']
            
            pipeline_logger.log("COLUMN_TYPES", f"🔍 Checking {table_name}", "INFO")
            column_types = get_table_column_types(cursor, table_name, database_schema)
            before_types[table_name] = column_types
            
            # Count VARCHAR columns
            varchar_count = sum(1 for col_type in column_types.values() if 'VARCHAR' in col_type.upper())
            total_count = len(column_types)
            pipeline_logger.log("COLUMN_TYPES", f"   {table_name}: {varchar_count}/{total_count} VARCHAR columns", "INFO")
        
        # Now run load_from_azure.py
        pipeline_logger.log("COLUMN_TYPES", "🚀 Running load_from_azure.py...", "INFO")
        import subprocess
        result = subprocess.run([sys.executable, 'pipeline_scripts/load_from_azure.py'], 
                              capture_output=True, text=True, cwd='.')
        
        if result.returncode == 0:
            pipeline_logger.log("COLUMN_TYPES", "✅ load_from_azure.py completed successfully", "INFO")
        else:
            pipeline_logger.log("COLUMN_TYPES", f"❌ load_from_azure.py failed: {result.stderr}", "ERROR")
        
        # Check column types AFTER running load_from_azure.py
        pipeline_logger.log("COLUMN_TYPES", "📊 Checking column types AFTER load_from_azure.py", "INFO")
        after_types = {}
        
        for mapping in table_mapping:
            table_name = mapping['snowflake_table']
            database_schema = mapping['snowflake_database']
            
            pipeline_logger.log("COLUMN_TYPES", f"🔍 Checking {table_name}", "INFO")
            column_types = get_table_column_types(cursor, table_name, database_schema)
            after_types[table_name] = column_types
            
            # Count VARCHAR columns
            varchar_count = sum(1 for col_type in column_types.values() if 'VARCHAR' in col_type.upper())
            total_count = len(column_types)
            pipeline_logger.log("COLUMN_TYPES", f"   {table_name}: {varchar_count}/{total_count} VARCHAR columns", "INFO")
        
        # Close connections
        cursor.close()
        conn.close()
        
        # Compare before and after
        pipeline_logger.log("COLUMN_TYPES", "🔍 Comparing column types before and after", "INFO")
        
        for table_name in before_types:
            before = before_types[table_name]
            after = after_types[table_name]
            
            pipeline_logger.log("COLUMN_TYPES", f"📋 {table_name} Analysis:", "INFO")
            
            # Check if any column types changed
            changed_columns = []
            for col_name in before:
                if col_name in after:
                    if before[col_name] != after[col_name]:
                        changed_columns.append({
                            'column': col_name,
                            'before': before[col_name],
                            'after': after[col_name]
                        })
            
            if changed_columns:
                pipeline_logger.log("COLUMN_TYPES", f"   ❌ {len(changed_columns)} column types changed:", "WARNING")
                for change in changed_columns:
                    pipeline_logger.log("COLUMN_TYPES", f"      {change['column']}: {change['before']} -> {change['after']}", "WARNING")
            else:
                pipeline_logger.log("COLUMN_TYPES", f"   ✅ No column types changed", "INFO")
        
        # Save detailed report
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_file = get_abs_path(f'logs/column_types_report_{timestamp}.json')
        os.makedirs(get_abs_path('logs'), exist_ok=True)
        
        report = {
            'timestamp': timestamp,
            'before_types': before_types,
            'after_types': after_types,
            'load_from_azure_output': result.stdout if result.returncode == 0 else result.stderr
        }
        
        with open(report_file, 'w') as f:
            json.dump(report, f, indent=2, default=str)
        
        pipeline_logger.log("COLUMN_TYPES", f"📄 Detailed report saved to: {report_file}", "INFO")
        
        return report
        
    except Exception as e:
        pipeline_logger.log("COLUMN_TYPES", f"❌ Error in column type analysis: {str(e)}", "ERROR")
        return None

if __name__ == "__main__":
    check_column_types() 