#!/usr/bin/env python3
"""
LWS CloudPipe v2 - Simple SUNGROW Data Loader

This script downloads the LWS.PUBLIC.SUNGROW.csv from Azure Blob Storage 
and loads it directly into Snowflake using pandas, avoiding complex stage creation.
"""

import os
import sys
import json
import base64
import pandas as pd
from pathlib import Path
from typing import Dict, Any
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

# Add helper_scripts to path
sys.path.append(str(Path(__file__).parent.parent / "helper_scripts" / "Utils"))
from logger import pipeline_logger, log

class SimpleSungrowLoader:
    def __init__(self):
        """Initialize the simple SUNGROW data loader."""
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
        
        log("SIMPLE_SUNGROW_LOADER", "Simple SUNGROW data loader initialized", "INFO")
    
    def load_credentials(self) -> Dict[str, Any]:
        """Load credentials from settings.json."""
        cred_file = self.base_dir / "settings.json"
        if not cred_file.exists():
            log("SIMPLE_SUNGROW_LOADER", "Missing settings.json file!", "ERROR")
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
                log("SIMPLE_SUNGROW_LOADER", "Missing Snowflake configuration", "ERROR")
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
            
            log("SIMPLE_SUNGROW_LOADER", "Successfully connected to Snowflake", "INFO")
            return conn
            
        except Exception as e:
            log("SIMPLE_SUNGROW_LOADER", f"Failed to connect to Snowflake: {str(e)}", "ERROR")
            return None
    
    def download_csv_from_azure(self) -> pd.DataFrame:
        """Download the SUNGROW CSV from Azure Blob Storage."""
        try:
            from azure.storage.blob import BlobServiceClient
            
            connection_string = self.credentials.get("AZURE_STORAGE_CONNECTION_STRING")
            container_name = self.credentials.get("BLOB_CONTAINER", "pbi25")
            
            if not connection_string:
                log("SIMPLE_SUNGROW_LOADER", "Missing Azure Storage connection string", "ERROR")
                return None
            
            # Get CSV from Azure
            blob_service_client = BlobServiceClient.from_connection_string(connection_string)
            blob_client = blob_service_client.get_blob_client(container=container_name, blob="LWS.PUBLIC.SUNGROW.csv")
            
            # Download CSV content
            stream = blob_client.download_blob()
            csv_content = stream.readall().decode('utf-8')
            
            # Convert to DataFrame
            import io
            df = pd.read_csv(io.StringIO(csv_content), dtype=str)
            
            # Clean data to avoid type conflicts
            for col in df.columns:
                df[col] = df[col].astype(str)
                df[col] = df[col].replace('nan', None)
            
            log("SIMPLE_SUNGROW_LOADER", f"Downloaded CSV from Azure: {len(df)} rows, {len(df.columns)} columns", "INFO")
            return df
            
        except Exception as e:
            log("SIMPLE_SUNGROW_LOADER", f"Error downloading CSV from Azure: {str(e)}", "ERROR")
            return None
    
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
            log("SIMPLE_SUNGROW_LOADER", f"Error checking table existence: {str(e)}", "ERROR")
            return False
    
    def create_table_from_dataframe(self, cursor, df: pd.DataFrame) -> bool:
        """Create the SUNGROW table based on DataFrame schema."""
        try:
            # Generate CREATE TABLE statement
            create_table_sql = "CREATE OR REPLACE TABLE LWS.PUBLIC.SUNGROW (\n"
            columns = []
            
            for col_name in df.columns:
                # All columns as VARCHAR to avoid type issues
                clean_col_name = col_name.strip().replace(' ', '_').replace('-', '_').replace(':', '_').replace('.', '_')
                if not clean_col_name or not clean_col_name[0].isalpha() or not clean_col_name.replace('_', '').isalnum():
                    clean_col_name = '_' + ''.join([c if c.isalnum() or c == '_' else '_' for c in clean_col_name])
                columns.append(f"    {clean_col_name} VARCHAR")
            
            create_table_sql += ',\n'.join(columns) + '\n)'
            
            # Execute CREATE TABLE
            cursor.execute(create_table_sql)
            log("SIMPLE_SUNGROW_LOADER", "Created table LWS.PUBLIC.SUNGROW", "INFO")
            return True
            
        except Exception as e:
            log("SIMPLE_SUNGROW_LOADER", f"Error creating table from DataFrame schema: {str(e)}", "ERROR")
            return False
    
    def load_dataframe_to_snowflake(self, conn, df: pd.DataFrame) -> bool:
        """Load DataFrame directly into Snowflake using write_pandas."""
        try:
            # Set context
            cursor = conn.cursor()
            cursor.execute("USE DATABASE LWS")
            cursor.execute("USE SCHEMA PUBLIC")
            
            # Load data using write_pandas
            success, nchunks, nrows, _ = write_pandas(
                conn, 
                df, 
                'SUNGROW',
                auto_create_table=False,  # We'll create the table manually
                overwrite=True
            )
            
            if success:
                log("SIMPLE_SUNGROW_LOADER", f"Successfully loaded {nrows} rows into SUNGROW table", "INFO")
                return True
            else:
                log("SIMPLE_SUNGROW_LOADER", "Failed to load data using write_pandas", "ERROR")
                return False
                
        except Exception as e:
            log("SIMPLE_SUNGROW_LOADER", f"Error loading DataFrame to Snowflake: {str(e)}", "ERROR")
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
            log("SIMPLE_SUNGROW_LOADER", f"Error verifying data load: {str(e)}", "ERROR")
            return {
                "row_count": 0,
                "column_count": 0,
                "status": "error",
                "error": str(e)
            }
    
    def run_loader(self) -> Dict[str, Any]:
        """Run the complete SUNGROW data loading process."""
        self.results["start_time"] = pd.Timestamp.now().isoformat()
        
        log("SIMPLE_SUNGROW_LOADER", "Starting simple SUNGROW data loading process", "INFO")
        
        # Download CSV from Azure
        df = self.download_csv_from_azure()
        if df is None:
            self.results["error"] = "Failed to download CSV from Azure"
            return self.results
        
        # Sanitize DataFrame column names to match Snowflake
        def sanitize_col(col):
            col = col.strip().replace(' ', '_').replace('-', '_').replace(':', '_').replace('.', '_')
            if not col or not col[0].isalpha() or not col.replace('_', '').isalnum():
                col = '_' + ''.join([c if c.isalnum() or c == '_' else '_' for c in col])
            return col
        df.columns = [sanitize_col(c) for c in df.columns]
        
        # Connect to Snowflake
        conn = self.get_snowflake_connection()
        if not conn:
            self.results["error"] = "Failed to connect to Snowflake"
            return self.results
        
        cursor = conn.cursor()
        
        try:
            # Drop the table if it exists to avoid type conflicts
            cursor.execute("DROP TABLE IF EXISTS LWS.PUBLIC.SUNGROW")
            
            # Check if table exists, create if not
            if not self.check_table_exists(cursor):
                if not self.create_table_from_dataframe(cursor, df):
                    raise Exception("Failed to create table from DataFrame schema")
            
            # Load data into table
            if not self.load_dataframe_to_snowflake(conn, df):
                raise Exception("Failed to load data into table")
            
            # Verify data load
            verification = self.verify_data_load(cursor)
            self.results.update(verification)
            self.results["status"] = "success"
            
            log("SIMPLE_SUNGROW_LOADER", f"Successfully loaded SUNGROW data: {verification['row_count']} rows", "INFO")
            
        except Exception as e:
            self.results["error"] = str(e)
            log("SIMPLE_SUNGROW_LOADER", f"Failed to load SUNGROW data: {str(e)}", "ERROR")
        
        finally:
            cursor.close()
            conn.close()
        
        # Log final results
        self.results["end_time"] = pd.Timestamp.now().isoformat()
        
        # Log results to JSON
        pipeline_logger.log_json("SIMPLE_SUNGROW_LOADER", self.results)
        
        return self.results

def main():
    """Main entry point for the simple SUNGROW data loader."""
    try:
        loader = SimpleSungrowLoader()
        results = loader.run_loader()
        
        # Print summary
        print("\n" + "="*60)
        print("SIMPLE SUNGROW DATA LOADER SUMMARY")
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
        log("SIMPLE_SUNGROW_LOADER", f"Critical loader error: {str(e)}", "CRITICAL")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 