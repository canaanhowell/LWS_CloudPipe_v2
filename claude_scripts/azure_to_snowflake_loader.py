#!/usr/bin/env python3
"""
Azure to Snowflake CSV Loader

This script loads CSV files from Azure Blob Storage container 'pbi25' into their
corresponding Snowflake tables based on the table_mapping.json configuration.

Features:
- Automatic blob discovery and matching
- Column name cleaning for Snowflake compatibility
- Data type handling and validation
- Comprehensive error handling and logging
- Progress tracking and verification
"""

import os
import sys
import json
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from azure.storage.blob import BlobServiceClient
from datetime import datetime
import logging
import re
from typing import Dict, List, Tuple, Optional, Any

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('azure_snowflake_loader.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class AzureToSnowflakeLoader:
    """Main class for loading CSV files from Azure Blob Storage to Snowflake"""
    
    def __init__(self, config_dir: str = "LWS_CloudPipe_v2/config_files"):
        """
        Initialize the loader with configuration directory
        
        Args:
            config_dir: Directory containing configuration files
        """
        self.config_dir = config_dir
        self.settings = None
        self.table_mapping = None
        self.blob_service_client = None
        self.snowflake_conn = None
        
    def load_configuration(self) -> None:
        """Load settings and table mapping from JSON files"""
        try:
            # Load settings
            settings_path = os.path.join(self.config_dir, 'settings.json')
            with open(settings_path, 'r') as f:
                self.settings = json.load(f)
            logger.info("✅ Loaded settings configuration")
            
            # Load table mapping
            mapping_path = os.path.join(self.config_dir, 'table_mapping.json')
            with open(mapping_path, 'r') as f:
                self.table_mapping = json.load(f)
            logger.info(f"✅ Loaded table mapping for {len(self.table_mapping)} tables")
            
        except Exception as e:
            logger.error(f"❌ Failed to load configuration: {str(e)}")
            raise
    
    def setup_azure_connection(self) -> None:
        """Initialize Azure Blob Storage connection"""
        try:
            connection_string = self.settings['AZURE_STORAGE_CONNECTION_STRING']
            self.blob_service_client = BlobServiceClient.from_connection_string(connection_string)
            logger.info("✅ Connected to Azure Blob Storage")
            
        except Exception as e:
            logger.error(f"❌ Failed to connect to Azure: {str(e)}")
            raise
    
    def setup_snowflake_connection(self) -> None:
        """Initialize Snowflake connection using private key authentication"""
        try:
            # Load private key
            private_key_path = os.path.join(self.config_dir, 'snowflake_private_key.txt')
            
            from cryptography.hazmat.primitives import serialization
            from cryptography.hazmat.backends import default_backend
            
            with open(private_key_path, 'r') as f:
                p_key = serialization.load_pem_private_key(
                    f.read().encode('utf-8'),
                    password=None,
                    backend=default_backend()
                )
            
            # Convert to bytes format for Snowflake
            pkb = p_key.private_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption()
            )
            
            snowflake_config = {
                'user': self.settings['SNOWFLAKE_USER'],
                'account': self.settings['SNOWFLAKE_ACCOUNT'],
                'private_key': pkb,
                'warehouse': self.settings['SNOWFLAKE_WAREHOUSE'],
                'database': self.settings['SNOWFLAKE_DATABASE'],
                'schema': 'PUBLIC'
            }
            
            self.snowflake_conn = snowflake.connector.connect(**snowflake_config)
            logger.info("✅ Connected to Snowflake")
            
        except Exception as e:
            logger.error(f"❌ Failed to connect to Snowflake: {str(e)}")
            raise
    
    def list_azure_blobs(self, container_name: str = "pbi25") -> List[str]:
        """
        List all blobs in the specified Azure container
        
        Args:
            container_name: Name of the Azure container (default: pbi25)
            
        Returns:
            List of blob names
        """
        try:
            container_client = self.blob_service_client.get_container_client(container_name)
            blobs = [blob.name for blob in container_client.list_blobs()]
            logger.info(f"📋 Found {len(blobs)} blobs in container '{container_name}'")
            return blobs
            
        except Exception as e:
            logger.error(f"❌ Failed to list blobs in container '{container_name}': {str(e)}")
            raise
    
    def find_matching_blob(self, target_name: str, available_blobs: List[str]) -> Optional[str]:
        """
        Find matching blob name with fuzzy matching
        
        Args:
            target_name: Target blob name to find
            available_blobs: List of available blob names
            
        Returns:
            Matching blob name or None if not found
        """
        # Try exact match first
        if target_name in available_blobs:
            return target_name
        
        # Try with .csv extension
        csv_name = f"{target_name}.csv"
        if csv_name in available_blobs:
            return csv_name
        
        # Try case-insensitive match
        target_lower = target_name.lower()
        for blob in available_blobs:
            if blob.lower() == target_lower or blob.lower() == f"{target_lower}.csv":
                return blob
        
        # Try partial matching (contains)
        for blob in available_blobs:
            if target_lower in blob.lower() or blob.lower() in target_lower:
                return blob
        
        return None
    
    def clean_column_name(self, column_name: str) -> str:
        """
        Clean column names for Snowflake compatibility
        
        Args:
            column_name: Original column name
            
        Returns:
            Cleaned column name safe for SQL without quotes
        """
        # Remove quotes and strip whitespace
        cleaned = str(column_name).strip().strip('"\'')
        
        # Replace spaces with underscores
        cleaned = cleaned.replace(' ', '_')
        
        # Replace problematic characters with underscores
        cleaned = re.sub(r'[/\(\)\?\-\.,\:\;\!\@\#\$\%\^\&\*\+\=\[\]\{\}\|\\\`\~\<\>\"\']', '_', cleaned)
        
        # Replace emojis and unicode characters
        cleaned = re.sub(r'[^\w\s_]', '_', cleaned)
        
        # Remove multiple consecutive underscores
        cleaned = re.sub(r'_+', '_', cleaned)
        
        # Remove leading/trailing underscores
        cleaned = cleaned.strip('_')
        
        # Handle empty names
        if not cleaned:
            cleaned = 'UNNAMED_COLUMN'
        
        # Ensure it starts with a letter or underscore (SQL requirement)
        if cleaned and not cleaned[0].isalpha() and cleaned[0] != '_':
            cleaned = f'COL_{cleaned}'
        
        # Handle reserved words
        reserved_words = ['SELECT', 'FROM', 'WHERE', 'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'DROP', 'TABLE', 'COLUMN']
        if cleaned.upper() in reserved_words:
            cleaned = f'COL_{cleaned}'
        
        return cleaned
    
    def load_csv_from_azure(self, container_name: str, blob_name: str) -> pd.DataFrame:
        """
        Download and load CSV from Azure Blob Storage
        
        Args:
            container_name: Azure container name
            blob_name: Blob name to download
            
        Returns:
            Pandas DataFrame with CSV data
        """
        try:
            container_client = self.blob_service_client.get_container_client(container_name)
            blob_client = container_client.get_blob_client(blob_name)
            
            # Download and read CSV
            download_stream = blob_client.download_blob()
            df = pd.read_csv(download_stream)
            
            logger.info(f"📊 Downloaded CSV '{blob_name}' with shape {df.shape}")
            return df
            
        except Exception as e:
            logger.error(f"❌ Failed to download CSV '{blob_name}': {str(e)}")
            raise
    
    def prepare_dataframe(self, df: pd.DataFrame, table_name: str) -> pd.DataFrame:
        """
        Prepare DataFrame for Snowflake loading
        
        Args:
            df: Original DataFrame
            table_name: Target table name for logging
            
        Returns:
            Prepared DataFrame
        """
        # Note: CSV files are pre-cleaned by pre_load_cleaner.py
        # Column names should already match Snowflake table schemas exactly
        # Skip cleaning to preserve exact Snowflake column names
        original_columns = df.columns.tolist()
        cleaned_columns = original_columns  # No cleaning needed
        column_mapping = {col: col for col in original_columns}  # Identity mapping
        
        # Convert all columns to string to avoid data type issues
        for col in df.columns:
            df[col] = df[col].astype(str)
        
        # Replace 'nan' strings with None
        df = df.replace('nan', None)
        
        # Truncate long strings to avoid Snowflake limits
        for col in df.columns:
            df[col] = df[col].apply(lambda x: str(x)[:1000] if x and len(str(x)) > 1000 else x)
        
        # Save column mapping for reference
        os.makedirs('logs', exist_ok=True)
        mapping_file = f"logs/column_mapping_{table_name.replace('.', '_')}.json"
        with open(mapping_file, 'w') as f:
            json.dump(column_mapping, f, indent=2)
        
        logger.info(f"🧹 Prepared DataFrame for '{table_name}': {len(cleaned_columns)} columns")
        return df
    
    def load_to_snowflake(self, df: pd.DataFrame, table_mapping: Dict[str, Any]) -> Dict[str, Any]:
        """
        Load DataFrame to Snowflake table
        
        Args:
            df: Prepared DataFrame
            table_mapping: Table mapping configuration
            
        Returns:
            Dictionary with loading results
        """
        table_name = table_mapping['snowflake_table']
        database_schema = table_mapping['snowflake_database']
        
        try:
            cursor = self.snowflake_conn.cursor()
            
            # Set database and schema context
            if '.' in database_schema:
                db_parts = database_schema.split('.')
                database = db_parts[0]
                schema = db_parts[1]
                cursor.execute(f"USE DATABASE {database}")
                cursor.execute(f"USE SCHEMA {schema}")
            
            # Get simple table name (without schema prefix)
            simple_table_name = table_name.split('.')[-1]
            
            # Check if table exists
            cursor.execute(f"SHOW TABLES LIKE '{simple_table_name}'")
            if not cursor.fetchone():
                raise Exception(f"Table {table_name} does not exist in Snowflake")
            
            # Truncate table before loading
            logger.info(f"🗑️ Truncating table {table_name}")
            cursor.execute(f"TRUNCATE TABLE {simple_table_name}")
            
            # Load data using write_pandas with quoted identifiers for column names
            success, nchunks, nrows, output = write_pandas(
                self.snowflake_conn,
                df,
                simple_table_name,
                auto_create_table=False,
                overwrite=True,
                quote_identifiers=True  # Required for column names with spaces/special chars
            )
            
            if success:
                # Verify row count
                cursor.execute(f"SELECT COUNT(*) FROM {simple_table_name}")
                actual_count = cursor.fetchone()[0]
                
                result = {
                    'status': 'success',
                    'rows_loaded': nrows,
                    'actual_count': actual_count,
                    'expected_count': int(table_mapping['estimated_row_count']),
                    'match': actual_count == int(table_mapping['estimated_row_count'])
                }
                
                logger.info(f"✅ Successfully loaded {table_name}: {nrows} rows")
                return result
            else:
                raise Exception(f"write_pandas failed: {output}")
                
        except Exception as e:
            logger.error(f"❌ Failed to load data to {table_name}: {str(e)}")
            return {
                'status': 'failed',
                'error': str(e),
                'rows_loaded': 0,
                'actual_count': 0,
                'expected_count': int(table_mapping['estimated_row_count']),
                'match': False
            }
    
    def run_pipeline(self, container_name: str = "pbi25") -> Dict[str, Any]:
        """
        Execute the complete pipeline
        
        Args:
            container_name: Azure container name (default: pbi25)
            
        Returns:
            Dictionary with pipeline results
        """
        logger.info("🚀 Starting Azure to Snowflake CSV loading pipeline")
        
        try:
            # Setup connections
            self.load_configuration()
            self.setup_azure_connection()
            self.setup_snowflake_connection()
            
            # List available blobs
            available_blobs = self.list_azure_blobs(container_name)
            
            # Process each table mapping
            results = []
            total_tables = len(self.table_mapping)
            
            for i, mapping in enumerate(self.table_mapping, 1):
                table_name = mapping['snowflake_table']
                azure_csv_name = mapping['azure_csv_name']
                
                logger.info(f"📊 Processing table {i}/{total_tables}: {table_name}")
                
                result = {
                    'table_name': table_name,
                    'azure_csv_name': azure_csv_name,
                    'status': 'pending'
                }
                
                try:
                    # Find matching blob
                    matching_blob = self.find_matching_blob(azure_csv_name, available_blobs)
                    
                    if not matching_blob:
                        result['status'] = 'failed'
                        result['error'] = f"No matching blob found for {azure_csv_name}"
                        logger.warning(f"⚠️ No matching blob found for {azure_csv_name}")
                        results.append(result)
                        continue
                    
                    logger.info(f"✅ Found matching blob: {matching_blob}")
                    
                    # Load CSV from Azure
                    df = self.load_csv_from_azure(container_name, matching_blob)
                    
                    # Prepare DataFrame
                    df_prepared = self.prepare_dataframe(df, table_name)
                    
                    # Load to Snowflake
                    load_result = self.load_to_snowflake(df_prepared, mapping)
                    result.update(load_result)
                    
                except Exception as e:
                    result['status'] = 'failed'
                    result['error'] = str(e)
                    logger.error(f"❌ Error processing {table_name}: {str(e)}")
                
                results.append(result)
            
            # Generate summary
            successful = sum(1 for r in results if r['status'] == 'success')
            failed = sum(1 for r in results if r['status'] == 'failed')
            
            summary = {
                'timestamp': datetime.now().isoformat(),
                'total_tables': total_tables,
                'successful': successful,
                'failed': failed,
                'success_rate': f"{(successful/total_tables)*100:.1f}%",
                'results': results
            }
            
            # Save results
            os.makedirs('logs', exist_ok=True)
            with open('logs/azure_snowflake_load_results.json', 'w') as f:
                json.dump(summary, f, indent=2)
            
            logger.info(f"🎉 Pipeline completed! Success: {successful}, Failed: {failed}")
            logger.info(f"📄 Results saved to: logs/azure_snowflake_load_results.json")
            
            return summary
            
        except Exception as e:
            logger.error(f"❌ Critical pipeline error: {str(e)}")
            raise
        
        finally:
            # Close connections
            if self.snowflake_conn:
                self.snowflake_conn.close()
                logger.info("🔒 Closed Snowflake connection")

def main():
    """Main execution function"""
    try:
        loader = AzureToSnowflakeLoader()
        results = loader.run_pipeline()
        
        # Print summary
        print("\n" + "="*50)
        print("PIPELINE SUMMARY")
        print("="*50)
        print(f"Total Tables: {results['total_tables']}")
        print(f"Successful: {results['successful']}")
        print(f"Failed: {results['failed']}")
        print(f"Success Rate: {results['success_rate']}")
        print("="*50)
        
        # Print failed tables if any
        failed_tables = [r for r in results['results'] if r['status'] == 'failed']
        if failed_tables:
            print("\nFAILED TABLES:")
            for table in failed_tables:
                print(f"- {table['table_name']}: {table.get('error', 'Unknown error')}")
        
    except Exception as e:
        logger.error(f"❌ Pipeline execution failed: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()