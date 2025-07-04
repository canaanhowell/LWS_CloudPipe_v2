#!/usr/bin/env python3
"""
LWS CloudPipe v2 - SUNGROW Data Loader

This script specifically loads the LWS.PUBLIC.SUNGROW.csv from Azure Blob Storage 
into the matching Snowflake table using COPY INTO commands.
"""

import os
import sys
import json
import base64
import pandas as pd
from pathlib import Path
from typing import Dict, Any
import snowflake.connector

# Add helper_scripts to path
sys.path.append(str(Path(__file__).parent.parent / "helper_scripts" / "Utils"))
from logger import pipeline_logger, log

class SungrowDataLoader:
    def __init__(self):
        """Initialize the SUNGROW data loader."""
        self.base_dir = Path(__file__).parent.parent
        self.config_dir = self.base_dir / "config_files"
        self.credentials = self.load_credentials()
        
        # Initialize results tracking
        self.results = {
            "start_time": None,
            "end_time": None,
            "status": "failed",
            "error": None,
            "rows_loaded": 0,
            "columns_loaded": 0
        }
        
        log("SUNGROW_LOADER", "SUNGROW data loader initialized", "INFO")
    
    def load_credentials(self) -> Dict[str, Any]:
        """Load credentials from settings.json."""
        cred_file = self.base_dir / "settings.json"
        if not cred_file.exists():
            log("SUNGROW_LOADER", "Missing settings.json file!", "ERROR")
            return {}
        with open(cred_file, "r") as f:
            return json.load(f)
    
    def get_snowflake_connection(self):
        """Get Snowflake connection."""
        try:
            account = self.credentials.get("SNOWFLAKE_ACCOUNT")
            user = self.credentials.get("SNOWFLAKE_USER")
            warehouse = self.credentials.get("SNOWFLAKE_WAREHOUSE")
            database = self.credentials.get("SNOWFLAKE_DATABASE")
            private_key_path = self.config_dir / "snowflake_private_key.txt"
            
            if not all([account, user, warehouse, database]) or not private_key_path.exists():
                log("SUNGROW_LOADER", "Missing Snowflake configuration", "ERROR")
                return None
            
            # Read private key
            with open(private_key_path, 'r') as f:
                private_key = f.read().strip()
            
            # Connect to Snowflake
            conn = snowflake.connector.connect(
                account=account,
                user=user,
                private_key=private_key,
                warehouse=warehouse,
                database=database
            )
            
            log("SUNGROW_LOADER", "Successfully connected to Snowflake", "INFO")
            return conn
            
        except Exception as e:
            log("SUNGROW_LOADER", f"Failed to connect to Snowflake: {str(e)}", "ERROR")
            return None
    
    def create_file_format_if_not_exists(self, cursor):
        """Create standard CSV file format if it doesn't exist."""
        try:
            # Check if file format exists
            cursor.execute("""
                SELECT COUNT(*) 
                FROM LWS.INFORMATION_SCHEMA.FILE_FORMATS 
                WHERE FILE_FORMAT_NAME = 'CSV_STANDARD' 
                AND FILE_FORMAT_SCHEMA = 'PUBLIC'
            """)
            
            if cursor.fetchone()[0] == 0:
                # Create file format
                cursor.execute("""
                    CREATE OR REPLACE FILE FORMAT LWS.PUBLIC.CSV_STANDARD
                    TYPE = CSV
                    FIELD_DELIMITER = ','
                    RECORD_DELIMITER = '\\n'
                    SKIP_HEADER = 1
                    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
                    TRIM_SPACE = TRUE
                    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
                    EMPTY_FIELD_AS_NULL = TRUE
                    NULL_IF = ('NULL', 'null', '')
                """)
                log("SUNGROW_LOADER", "Created file format LWS.PUBLIC.CSV_STANDARD", "INFO")
            else:
                log("SUNGROW_LOADER", "File format LWS.PUBLIC.CSV_STANDARD already exists", "INFO")
                
        except Exception as e:
            log("SUNGROW_LOADER", f"Error creating file format: {str(e)}", "ERROR")
            raise
    
    def check_table_exists(self, cursor) -> bool:
        """Check if the SUNGROW table exists in Snowflake."""
        try:
            cursor.execute("""
                SELECT COUNT(*) 
                FROM LWS.INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_NAME = 'SUNGROW' 
                AND TABLE_SCHEMA = 'PUBLIC'
            """)
            return cursor.fetchone()[0] > 0
        except Exception as e:
            log("SUNGROW_LOADER", f"Error checking table existence: {str(e)}", "ERROR")
            return False
    
    def create_table_from_azure_csv(self, cursor) -> bool:
        """Create the SUNGROW table based on CSV schema from Azure Blob Storage."""
        try:
            # Get Azure Blob Storage client
            from azure.storage.blob import BlobServiceClient
            
            connection_string = self.credentials.get("AZURE_STORAGE_CONNECTION_STRING")
            container_name = self.credentials.get("BLOB_CONTAINER", "pbi25")
            
            if not connection_string:
                log("SUNGROW_LOADER", "Missing Azure Storage connection string", "ERROR")
                return False
            
            # Get CSV from Azure
            blob_service_client = BlobServiceClient.from_connection_string(connection_string)
            blob_client = blob_service_client.get_blob_client(container=container_name, blob="LWS.PUBLIC.SUNGROW.csv")
            
            # Download first few rows to infer schema
            stream = blob_client.download_blob()
            csv_content = stream.readall().decode('utf-8')
            
            # Read first few lines to get headers
            lines = csv_content.split('\n')
            if len(lines) < 2:
                log("SUNGROW_LOADER", "CSV file LWS.PUBLIC.SUNGROW.csv is empty or invalid", "ERROR")
                return False
            
            headers = lines[0].split(',')
            sample_data = lines[1:6]  # Get a few sample rows
            
            # Create DataFrame to infer types
            import io
            sample_df = pd.read_csv(io.StringIO('\n'.join([lines[0]] + sample_data)))
            
            # Generate CREATE TABLE statement
            create_table_sql = "CREATE OR REPLACE TABLE LWS.PUBLIC.SUNGROW (\n"
            columns = []
            
            for col_name, dtype in sample_df.dtypes.items():
                # Map pandas dtypes to Snowflake types
                if 'int' in str(dtype):
                    snowflake_type = 'NUMBER'
                elif 'float' in str(dtype):
                    snowflake_type = 'FLOAT'
                elif 'datetime' in str(dtype):
                    snowflake_type = 'TIMESTAMP'
                elif 'bool' in str(dtype):
                    snowflake_type = 'BOOLEAN'
                else:
                    snowflake_type = 'VARCHAR'
                
                # Clean column name
                clean_col_name = col_name.strip().replace(' ', '_').replace('-', '_')
                columns.append(f"    {clean_col_name} {snowflake_type}")
            
            create_table_sql += ',\n'.join(columns) + '\n)'
            
            # Execute CREATE TABLE
            cursor.execute(create_table_sql)
            log("SUNGROW_LOADER", "Created table LWS.PUBLIC.SUNGROW", "INFO")
            return True
            
        except Exception as e:
            log("SUNGROW_LOADER", f"Error creating table from CSV schema: {str(e)}", "ERROR")
            return False
    
    def load_data_using_copy_into(self, cursor) -> bool:
        """Load data from Azure Blob Storage into Snowflake table using COPY INTO."""
        try:
            # Set context
            cursor.execute("USE DATABASE LWS")
            cursor.execute("USE SCHEMA PUBLIC")
            
            # Get Azure storage account from connection string
            connection_string = self.credentials.get("AZURE_STORAGE_CONNECTION_STRING")
            storage_account = None
            storage_key = None
            for part in connection_string.split(';'):
                if part.startswith('AccountName='):
                    storage_account = part.split('=')[1]
                elif part.startswith('AccountKey='):
                    storage_key = part.split('=')[1]
            
            if not storage_account or not storage_key:
                log("SUNGROW_LOADER", "Could not extract storage account or key from connection string", "ERROR")
                return False
            
            # Create a temporary stage for this load
            stage_name = "TEMP_SUNGROW_STAGE"
            
            # Create stage
            cursor.execute(f"""
                CREATE OR REPLACE TEMPORARY STAGE {stage_name}
                URL = 'azure://{storage_account}.blob.core.windows.net/pbi25/'
                CREDENTIALS = (
                    AZURE_STORAGE_ACCOUNT = '{storage_account}',
                    AZURE_STORAGE_KEY = '{storage_key}'
                )
            """)
            
            # Execute COPY INTO command
            copy_sql = f"""
            COPY INTO LWS.PUBLIC.SUNGROW
            FROM @{stage_name}
            FILES = ('LWS.PUBLIC.SUNGROW.csv')
            FILE_FORMAT = LWS.PUBLIC.CSV_STANDARD
            ON_ERROR = CONTINUE
            VALIDATION_MODE = RETURN_ERRORS
            FORCE = TRUE
            """
            
            log("SUNGROW_LOADER", "Executing COPY INTO for SUNGROW table", "INFO")
            cursor.execute(copy_sql)
            
            # Get load results
            result = cursor.fetchone()
            if result:
                log("SUNGROW_LOADER", f"COPY INTO completed: {result}", "INFO")
            
            # Clean up temporary stage
            cursor.execute(f"DROP STAGE IF EXISTS {stage_name}")
            
            return True
            
        except Exception as e:
            log("SUNGROW_LOADER", f"Error loading data into SUNGROW table: {str(e)}", "ERROR")
            return False
    
    def verify_data_load(self, cursor) -> Dict[str, Any]:
        """Verify that data was loaded successfully."""
        try:
            # Get row count
            cursor.execute("SELECT COUNT(*) FROM LWS.PUBLIC.SUNGROW")
            row_count = cursor.fetchone()[0]
            
            # Get column count
            cursor.execute("""
                SELECT COUNT(*) 
                FROM LWS.INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_NAME = 'SUNGROW' 
                AND TABLE_SCHEMA = 'PUBLIC'
            """)
            column_count = cursor.fetchone()[0]
            
            # Get sample data
            cursor.execute("SELECT * FROM LWS.PUBLIC.SUNGROW LIMIT 5")
            sample_rows = cursor.fetchall()
            
            return {
                "row_count": row_count,
                "column_count": column_count,
                "status": "success" if row_count > 0 else "warning",
                "sample_rows": len(sample_rows)
            }
            
        except Exception as e:
            log("SUNGROW_LOADER", f"Error verifying data load: {str(e)}", "ERROR")
            return {
                "row_count": 0,
                "column_count": 0,
                "status": "error",
                "error": str(e)
            }
    
    def run_loader(self) -> Dict[str, Any]:
        """Run the complete SUNGROW data loading process."""
        self.results["start_time"] = pd.Timestamp.now().isoformat()
        
        log("SUNGROW_LOADER", "Starting SUNGROW data loading process", "INFO")
        
        # Connect to Snowflake
        conn = self.get_snowflake_connection()
        if not conn:
            self.results["error"] = "Failed to connect to Snowflake"
            return self.results
        
        cursor = conn.cursor()
        
        try:
            # Create file format if needed
            self.create_file_format_if_not_exists(cursor)
            
            # Check if table exists, create if not
            if not self.check_table_exists(cursor):
                if not self.create_table_from_azure_csv(cursor):
                    raise Exception("Failed to create table from CSV schema")
            
            # Load data into table
            if not self.load_data_using_copy_into(cursor):
                raise Exception("Failed to load data into table")
            
            # Verify data load
            verification = self.verify_data_load(cursor)
            self.results.update(verification)
            self.results["status"] = "success"
            
            log("SUNGROW_LOADER", f"Successfully loaded SUNGROW data: {verification['row_count']} rows", "INFO")
            
        except Exception as e:
            self.results["error"] = str(e)
            log("SUNGROW_LOADER", f"Failed to load SUNGROW data: {str(e)}", "ERROR")
        
        finally:
            cursor.close()
            conn.close()
        
        # Log final results
        self.results["end_time"] = pd.Timestamp.now().isoformat()
        
        # Log results to JSON
        pipeline_logger.log_json("SUNGROW_LOADER", self.results)
        
        return self.results

def main():
    """Main entry point for the SUNGROW data loader."""
    try:
        loader = SungrowDataLoader()
        results = loader.run_loader()
        
        # Print summary
        print("\n" + "="*60)
        print("SUNGROW DATA LOADER SUMMARY")
        print("="*60)
        print(f"Start Time: {results['start_time']}")
        print(f"End Time: {results['end_time']}")
        print(f"Status: {results['status']}")
        print(f"Rows Loaded: {results['row_count']}")
        print(f"Columns Loaded: {results['column_count']}")
        
        if results.get("error"):
            print(f"Error: {results['error']}")
        
        if results.get("sample_rows"):
            print(f"Sample Rows Retrieved: {results['sample_rows']}")
        
        print("="*60)
        
        return 0 if results["status"] == "success" else 1
        
    except Exception as e:
        log("SUNGROW_LOADER", f"Critical loader error: {str(e)}", "CRITICAL")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 