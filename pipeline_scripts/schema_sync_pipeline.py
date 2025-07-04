#!/usr/bin/env python3
"""
LWS CloudPipe v2 - Schema Synchronization Pipeline

This script loads data from Azure Blob Storage into Snowflake tables using COPY INTO commands.
It handles schema synchronization and data loading for all configured tables.
"""

import os
import sys
import json
import base64
import io
import pandas as pd
from pathlib import Path
from typing import Dict, List, Optional, Any
import snowflake.connector
from snowflake.connector.errors import ProgrammingError

# Add helper_scripts to path
sys.path.append(str(Path(__file__).parent.parent / "helper_scripts" / "Utils"))
from logger import pipeline_logger, log

class SchemaSyncPipeline:
    def __init__(self):
        """Initialize the schema synchronization pipeline."""
        self.base_dir = Path(__file__).parent.parent
        self.config_dir = self.base_dir / "config_files"
        self.credentials = self.load_credentials()
        self.table_mapping = self.load_table_mapping()
        
        # Initialize results tracking
        self.results = {
            "start_time": None,
            "end_time": None,
            "tables_processed": 0,
            "tables_successful": 0,
            "tables_failed": 0,
            "details": []
        }
        
        log("SCHEMA_SYNC", "Schema synchronization pipeline initialized", "INFO")
    
    def load_credentials(self) -> Dict[str, Any]:
        """Load credentials from settings.json."""
        cred_file = self.base_dir / "settings.json"
        if not cred_file.exists():
            log("SCHEMA_SYNC", "Missing settings.json file!", "ERROR")
            return {}
        with open(cred_file, "r") as f:
            return json.load(f)
    
    def load_table_mapping(self) -> List[Dict[str, Any]]:
        """Load table mapping configuration."""
        mapping_path = self.base_dir / "config_files" / "table_mapping.json"
        if not mapping_path.exists():
            log("SCHEMA_SYNC", "Missing table_mapping.json file!", "ERROR")
            return []
        with open(mapping_path, "r", encoding="utf-8") as f:
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
                log("SCHEMA_SYNC", "Missing Snowflake configuration", "ERROR")
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
            
            log("SCHEMA_SYNC", "Successfully connected to Snowflake", "INFO")
            return conn
            
        except Exception as e:
            log("SCHEMA_SYNC", f"Failed to connect to Snowflake: {str(e)}", "ERROR")
            return None
    
    def create_file_format_if_not_exists(self, cursor, database: str, schema: str):
        """Create standard CSV file format if it doesn't exist."""
        try:
            # Check if file format exists
            cursor.execute(f"""
                SELECT COUNT(*) 
                FROM {database}.INFORMATION_SCHEMA.FILE_FORMATS 
                WHERE FILE_FORMAT_NAME = 'CSV_STANDARD' 
                AND FILE_FORMAT_SCHEMA = '{schema}'
            """)
            
            if cursor.fetchone()[0] == 0:
                # Create file format
                cursor.execute(f"""
                    CREATE OR REPLACE FILE FORMAT {database}.{schema}.CSV_STANDARD
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
                log("SCHEMA_SYNC", f"Created file format {database}.{schema}.CSV_STANDARD", "INFO")
            else:
                log("SCHEMA_SYNC", f"File format {database}.{schema}.CSV_STANDARD already exists", "INFO")
                
        except Exception as e:
            log("SCHEMA_SYNC", f"Error creating file format: {str(e)}", "ERROR")
            raise
    
    def create_stage_if_not_exists(self, cursor, database: str, schema: str, table_name: str):
        """Create external stage for Azure Blob Storage if it doesn't exist."""
        try:
            stage_name = f"{database}_{schema}_{table_name}_STAGE"
            full_stage_name = f"{database}.{schema}.{stage_name}"
            
            # Check if stage exists
            cursor.execute(f"""
                SELECT COUNT(*) 
                FROM {database}.INFORMATION_SCHEMA.STAGES 
                WHERE STAGE_NAME = '{stage_name}' 
                AND STAGE_SCHEMA = '{schema}'
            """)
            
            if cursor.fetchone()[0] == 0:
                # Get Azure storage credentials from connection string
                connection_string = self.credentials.get("AZURE_STORAGE_CONNECTION_STRING")
                container_name = self.credentials.get("BLOB_CONTAINER", "pbi25")
                
                if not connection_string:
                    log("SCHEMA_SYNC", "Missing Azure Storage connection string", "ERROR")
                    return None
                
                # Extract storage account name and key from connection string
                storage_account = None
                storage_key = None
                for part in connection_string.split(';'):
                    if part.startswith('AccountName='):
                        storage_account = part.split('=')[1]
                    elif part.startswith('AccountKey='):
                        storage_key = part.split('=')[1]
                
                if not storage_account or not storage_key:
                    log("SCHEMA_SYNC", "Could not extract storage account or key from connection string", "ERROR")
                    return None
                
                # Create stage using connection string
                cursor.execute(f"""
                    CREATE OR REPLACE STAGE {full_stage_name}
                    URL = 'azure://{storage_account}.blob.core.windows.net/{container_name}/'
                    CREDENTIALS = (
                        AZURE_STORAGE_ACCOUNT = '{storage_account}',
                        AZURE_STORAGE_KEY = '{storage_key}'
                    )
                """)
                log("SCHEMA_SYNC", f"Created stage {full_stage_name}", "INFO")
            else:
                log("SCHEMA_SYNC", f"Stage {full_stage_name} already exists", "INFO")
            
            return full_stage_name
            
        except Exception as e:
            log("SCHEMA_SYNC", f"Error creating stage: {str(e)}", "ERROR")
            return None
    
    def check_table_exists(self, cursor, database: str, schema: str, table_name: str) -> bool:
        """Check if a table exists in Snowflake."""
        try:
            cursor.execute(f"""
                SELECT COUNT(*) 
                FROM {database}.INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_NAME = '{table_name}' 
                AND TABLE_SCHEMA = '{schema}'
            """)
            return cursor.fetchone()[0] > 0
        except Exception as e:
            log("SCHEMA_SYNC", f"Error checking table existence: {str(e)}", "ERROR")
            return False
    
    def create_table_from_csv_schema(self, cursor, database: str, schema: str, table_name: str, csv_blob_name: str) -> bool:
        """Create a table based on CSV schema from Azure Blob Storage."""
        try:
            # Get Azure Blob Storage client
            from azure.storage.blob import BlobServiceClient
            
            connection_string = self.credentials.get("AZURE_STORAGE_CONNECTION_STRING")
            container_name = self.credentials.get("BLOB_CONTAINER", "pbi25")
            
            if not connection_string:
                log("SCHEMA_SYNC", "Missing Azure Storage connection string", "ERROR")
                return False
            
            # Get CSV from Azure
            blob_service_client = BlobServiceClient.from_connection_string(connection_string)
            blob_client = blob_service_client.get_blob_client(container=container_name, blob=csv_blob_name)
            
            # Download first few rows to infer schema
            stream = blob_client.download_blob()
            csv_content = stream.readall().decode('utf-8')
            
            # Read first few lines to get headers
            lines = csv_content.split('\n')
            if len(lines) < 2:
                log("SCHEMA_SYNC", f"CSV file {csv_blob_name} is empty or invalid", "ERROR")
                return False
            
            headers = lines[0].split(',')
            sample_data = lines[1:6]  # Get a few sample rows
            
            # Create DataFrame to infer types
            sample_df = pd.read_csv(io.StringIO('\n'.join([lines[0]] + sample_data)))
            
            # Generate CREATE TABLE statement
            create_table_sql = f"CREATE OR REPLACE TABLE {database}.{schema}.{table_name} (\n"
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
            log("SCHEMA_SYNC", f"Created table {database}.{schema}.{table_name}", "INFO")
            return True
            
        except Exception as e:
            log("SCHEMA_SYNC", f"Error creating table from CSV schema: {str(e)}", "ERROR")
            return False
    
    def load_data_into_table(self, cursor, database: str, schema: str, table_name: str, stage_name: str, csv_blob_name: str) -> bool:
        """Load data from Azure Blob Storage into Snowflake table using COPY INTO."""
        try:
            # Set context
            cursor.execute(f"USE DATABASE {database}")
            cursor.execute(f"USE SCHEMA {schema}")
            
            # Execute COPY INTO command
            copy_sql = f"""
            COPY INTO {database}.{schema}.{table_name}
            FROM @{stage_name}
            FILES = ('{csv_blob_name}')
            FILE_FORMAT = {database}.{schema}.CSV_STANDARD
            ON_ERROR = CONTINUE
            VALIDATION_MODE = RETURN_ERRORS
            FORCE = TRUE
            """
            
            log("SCHEMA_SYNC", f"Executing COPY INTO for {table_name}", "INFO")
            cursor.execute(copy_sql)
            
            # Get load results
            result = cursor.fetchone()
            if result:
                log("SCHEMA_SYNC", f"COPY INTO completed for {table_name}: {result}", "INFO")
            
            return True
            
        except Exception as e:
            log("SCHEMA_SYNC", f"Error loading data into {table_name}: {str(e)}", "ERROR")
            return False
    
    def verify_data_load(self, cursor, database: str, schema: str, table_name: str) -> Dict[str, Any]:
        """Verify that data was loaded successfully."""
        try:
            # Get row count
            cursor.execute(f"SELECT COUNT(*) FROM {database}.{schema}.{table_name}")
            row_count = cursor.fetchone()[0]
            
            # Get column count
            cursor.execute(f"""
                SELECT COUNT(*) 
                FROM {database}.INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_NAME = '{table_name}' 
                AND TABLE_SCHEMA = '{schema}'
            """)
            column_count = cursor.fetchone()[0]
            
            return {
                "table_name": table_name,
                "row_count": row_count,
                "column_count": column_count,
                "status": "success" if row_count > 0 else "warning"
            }
            
        except Exception as e:
            log("SCHEMA_SYNC", f"Error verifying data load for {table_name}: {str(e)}", "ERROR")
            return {
                "table_name": table_name,
                "row_count": 0,
                "column_count": 0,
                "status": "error",
                "error": str(e)
            }
    
    def process_table(self, cursor, mapping: Dict[str, Any]) -> Dict[str, Any]:
        """Process a single table mapping."""
        table_result = {
            "table_name": mapping["snowflake_table"],
            "status": "failed",
            "error": None,
            "row_count": 0,
            "column_count": 0
        }
        
        try:
            # Parse table information
            table_parts = mapping["snowflake_table"].split(".")
            if len(table_parts) != 3:
                raise ValueError(f"Invalid table name format: {mapping['snowflake_table']}")
            
            database, schema, table_name = table_parts
            
            log("SCHEMA_SYNC", f"Processing table: {database}.{schema}.{table_name}", "INFO")
            
            # Create file format if needed
            self.create_file_format_if_not_exists(cursor, database, schema)
            
            # Create stage if needed
            stage_name = self.create_stage_if_not_exists(cursor, database, schema, table_name)
            if not stage_name:
                raise Exception("Failed to create stage")
            
            # Check if table exists, create if not
            if not self.check_table_exists(cursor, database, schema, table_name):
                csv_blob_name = f"{mapping['azure_csv_name']}.csv"
                if not self.create_table_from_csv_schema(cursor, database, schema, table_name, csv_blob_name):
                    raise Exception("Failed to create table from CSV schema")
            
            # Load data into table
            csv_blob_name = f"{mapping['azure_csv_name']}.csv"
            if not self.load_data_into_table(cursor, database, schema, table_name, stage_name, csv_blob_name):
                raise Exception("Failed to load data into table")
            
            # Verify data load
            verification = self.verify_data_load(cursor, database, schema, table_name)
            table_result.update(verification)
            table_result["status"] = "success"
            
            log("SCHEMA_SYNC", f"Successfully processed {table_name}: {verification['row_count']} rows", "INFO")
            
        except Exception as e:
            table_result["error"] = str(e)
            log("SCHEMA_SYNC", f"Failed to process {mapping['snowflake_table']}: {str(e)}", "ERROR")
        
        return table_result
    
    def run_pipeline(self) -> Dict[str, Any]:
        """Run the complete schema synchronization pipeline."""
        self.results["start_time"] = pd.Timestamp.now().isoformat()
        
        log("SCHEMA_SYNC", "Starting schema synchronization pipeline", "INFO")
        
        # Connect to Snowflake
        conn = self.get_snowflake_connection()
        if not conn:
            self.results["error"] = "Failed to connect to Snowflake"
            return self.results
        
        cursor = conn.cursor()
        
        try:
            # Process each table in the mapping
            for mapping in self.table_mapping:
                self.results["tables_processed"] += 1
                
                table_result = self.process_table(cursor, mapping)
                self.results["details"].append(table_result)
                
                if table_result["status"] == "success":
                    self.results["tables_successful"] += 1
                else:
                    self.results["tables_failed"] += 1
                
                log("SCHEMA_SYNC", f"Processed {self.results['tables_processed']}/{len(self.table_mapping)} tables", "INFO")
            
            # Log final results
            self.results["end_time"] = pd.Timestamp.now().isoformat()
            
            log("SCHEMA_SYNC", f"Pipeline completed: {self.results['tables_successful']}/{self.results['tables_processed']} tables successful", "INFO")
            
            # Log results to JSON
            pipeline_logger.log_json("SCHEMA_SYNC", self.results)
            
        except Exception as e:
            log("SCHEMA_SYNC", f"Critical pipeline error: {str(e)}", "ERROR")
            self.results["error"] = str(e)
        
        finally:
            cursor.close()
            conn.close()
        
        return self.results

def main():
    """Main entry point for the schema synchronization pipeline."""
    try:
        pipeline = SchemaSyncPipeline()
        results = pipeline.run_pipeline()
        
        # Print summary
        print("\n" + "="*60)
        print("SCHEMA SYNCHRONIZATION PIPELINE SUMMARY")
        print("="*60)
        print(f"Start Time: {results['start_time']}")
        print(f"End Time: {results['end_time']}")
        print(f"Tables Processed: {results['tables_processed']}")
        print(f"Successful: {results['tables_successful']}")
        print(f"Failed: {results['tables_failed']}")
        
        if results.get("error"):
            print(f"Pipeline Error: {results['error']}")
        
        print("\nTable Results:")
        for detail in results["details"]:
            status_symbol = "✅" if detail["status"] == "success" else "❌"
            print(f"  {status_symbol} {detail['table_name']}: {detail['row_count']} rows, {detail['column_count']} columns")
            if detail.get("error"):
                print(f"    Error: {detail['error']}")
        
        print("="*60)
        
        return 0 if results["tables_successful"] > 0 else 1
        
    except Exception as e:
        log("SCHEMA_SYNC", f"Critical pipeline error: {str(e)}", "CRITICAL")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 