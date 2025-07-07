#!/usr/bin/env python3
"""
Pre-Load Cleaner - Reformat CSV files for Snowflake compatibility

This script downloads CSV files from Azure Blob Storage, cleans and reformats
column names to match Snowflake table schemas, and uploads the cleaned versions
back to Azure, replacing the originals.

Features:
- Converts CSV column names to match Snowflake table column formats
- Creates backups of original files before modification
- Handles different naming conventions (underscores vs spaces)
- Provides detailed logging and verification
"""

import os
import sys
import json
import pandas as pd
import snowflake.connector
from azure.storage.blob import BlobServiceClient
from datetime import datetime
import logging
import re
from typing import Dict, List, Tuple, Optional, Any
import tempfile
from io import StringIO

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('pre_load_cleaner.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class PreLoadCleaner:
    """Clean and reformat CSV files for Snowflake compatibility"""
    
    def __init__(self, config_dir: str = "LWS_CloudPipe_v2/config_files"):
        """Initialize the cleaner with configuration directory"""
        self.config_dir = config_dir
        self.settings = None
        self.table_mapping = None
        self.blob_service_client = None
        self.snowflake_conn = None
        self.column_mappings = {}
        
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
    
    def setup_connections(self) -> None:
        """Initialize Azure and Snowflake connections"""
        try:
            # Azure connection
            connection_string = self.settings['AZURE_STORAGE_CONNECTION_STRING']
            self.blob_service_client = BlobServiceClient.from_connection_string(connection_string)
            logger.info("✅ Connected to Azure Blob Storage")
            
            # Snowflake connection
            private_key_path = os.path.join(self.config_dir, 'snowflake_private_key.txt')
            
            from cryptography.hazmat.primitives import serialization
            from cryptography.hazmat.backends import default_backend
            
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
            logger.error(f"❌ Failed to setup connections: {str(e)}")
            raise
    
    def get_snowflake_columns(self, table_name: str, database_schema: str) -> Dict[str, str]:
        """Get column names from Snowflake table and create mapping"""
        try:
            cursor = self.snowflake_conn.cursor()
            
            # Set database and schema context
            if '.' in database_schema:
                db_parts = database_schema.split('.')
                database = db_parts[0]
                schema = db_parts[1]
                cursor.execute(f"USE DATABASE {database}")
                cursor.execute(f"USE SCHEMA {schema}")
            
            # Get simple table name
            simple_table_name = table_name.split('.')[-1]
            
            # Get column information
            cursor.execute(f"DESCRIBE TABLE {simple_table_name}")
            columns_info = cursor.fetchall()
            
            # Create mapping from cleaned CSV format to actual Snowflake format
            column_mapping = {}
            for row in columns_info:
                sf_column = row[0]  # Actual Snowflake column name
                # Create cleaned version (what CSV would have)
                cleaned_version = self.csv_to_snowflake_format(sf_column)
                column_mapping[cleaned_version] = sf_column
            
            logger.info(f"📋 Retrieved {len(column_mapping)} column mappings from {table_name}")
            return column_mapping
            
        except Exception as e:
            logger.error(f"❌ Failed to get columns from {table_name}: {str(e)}")
            return {}
    
    def csv_to_snowflake_format(self, snowflake_column: str) -> str:
        """Convert Snowflake column name to CSV format (reverse engineering)"""
        # This mimics what our CSV cleaning would do to a Snowflake column
        cleaned = str(snowflake_column).strip().strip('"\'')
        cleaned = cleaned.replace(' ', '_')
        cleaned = re.sub(r'[/\(\)\?\-\.,\:\;\!\@\#\$\%\^\&\*\+\=\[\]\{\}\|\\\`\~\<\>\"\']', '_', cleaned)
        cleaned = re.sub(r'[^\w\s_]', '_', cleaned)
        cleaned = re.sub(r'_+', '_', cleaned)
        cleaned = cleaned.strip('_')
        
        if not cleaned:
            cleaned = 'UNNAMED_COLUMN'
        
        if cleaned and not cleaned[0].isalpha() and cleaned[0] != '_':
            cleaned = f'COL_{cleaned}'
            
        return cleaned.upper()
    
    def find_matching_blob(self, target_name: str, available_blobs: List[str]) -> Optional[str]:
        """Find matching blob name with fuzzy matching"""
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
        
        # Try partial matching
        for blob in available_blobs:
            if target_lower in blob.lower() or blob.lower() in target_lower:
                return blob
        
        return None
    
    def download_csv_from_azure(self, container_name: str, blob_name: str) -> pd.DataFrame:
        """Download CSV from Azure Blob Storage"""
        try:
            container_client = self.blob_service_client.get_container_client(container_name)
            blob_client = container_client.get_blob_client(blob_name)
            
            # Download CSV
            download_stream = blob_client.download_blob()
            df = pd.read_csv(download_stream)
            
            logger.info(f"📥 Downloaded CSV '{blob_name}' with shape {df.shape}")
            return df
            
        except Exception as e:
            logger.error(f"❌ Failed to download CSV '{blob_name}': {str(e)}")
            raise
    
    def create_backup(self, container_name: str, blob_name: str) -> str:
        """Create a backup of the original file"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_name = f"backup_{timestamp}_{blob_name}"
            
            container_client = self.blob_service_client.get_container_client(container_name)
            
            # Copy original to backup
            source_blob = container_client.get_blob_client(blob_name)
            backup_blob = container_client.get_blob_client(backup_name)
            
            # Start copy operation
            copy_source = source_blob.url
            backup_blob.start_copy_from_url(copy_source)
            
            logger.info(f"💾 Created backup: {backup_name}")
            return backup_name
            
        except Exception as e:
            logger.error(f"❌ Failed to create backup for {blob_name}: {str(e)}")
            raise
    
    def clean_csv_columns(self, df: pd.DataFrame, table_name: str) -> pd.DataFrame:
        """Clean CSV columns to match Snowflake table schema"""
        try:
            if table_name not in self.column_mappings:
                logger.warning(f"⚠️ No column mapping found for {table_name}, skipping cleaning")
                return df
            
            column_mapping = self.column_mappings[table_name]
            
            # Clean the current CSV column names
            current_columns = []
            for col in df.columns:
                cleaned_col = self.csv_to_snowflake_format(col)
                current_columns.append(cleaned_col)
            
            # Create rename mapping
            rename_mapping = {}
            unmatched_columns = []
            
            for i, (original_col, cleaned_col) in enumerate(zip(df.columns, current_columns)):
                if cleaned_col in column_mapping:
                    # Map to actual Snowflake column name
                    snowflake_col = column_mapping[cleaned_col]
                    rename_mapping[original_col] = snowflake_col
                else:
                    # Column not found in Snowflake table
                    unmatched_columns.append(original_col)
            
            # Apply column renaming
            df_cleaned = df.rename(columns=rename_mapping)
            
            # Log results
            matched_count = len(rename_mapping)
            total_count = len(df.columns)
            
            logger.info(f"🧹 Cleaned {table_name}: {matched_count}/{total_count} columns matched")
            
            if unmatched_columns:
                logger.warning(f"⚠️ Unmatched columns in {table_name}: {unmatched_columns[:5]}{'...' if len(unmatched_columns) > 5 else ''}")
            
            # Save column mapping for reference
            mapping_info = {
                'original_columns': list(df.columns),
                'cleaned_columns': current_columns,
                'snowflake_columns': list(rename_mapping.values()),
                'rename_mapping': rename_mapping,
                'unmatched_columns': unmatched_columns,
                'match_rate': f"{(matched_count/total_count)*100:.1f}%"
            }
            
            os.makedirs('logs', exist_ok=True)
            mapping_file = f"logs/cleaning_mapping_{table_name.replace('.', '_')}.json"
            with open(mapping_file, 'w') as f:
                json.dump(mapping_info, f, indent=2)
            
            return df_cleaned
            
        except Exception as e:
            logger.error(f"❌ Failed to clean columns for {table_name}: {str(e)}")
            return df
    
    def upload_csv_to_azure(self, df: pd.DataFrame, container_name: str, blob_name: str) -> None:
        """Upload cleaned CSV back to Azure Blob Storage"""
        try:
            # Convert DataFrame to CSV string
            csv_buffer = StringIO()
            df.to_csv(csv_buffer, index=False)
            csv_content = csv_buffer.getvalue()
            
            # Upload to Azure
            container_client = self.blob_service_client.get_container_client(container_name)
            blob_client = container_client.get_blob_client(blob_name)
            
            blob_client.upload_blob(csv_content, overwrite=True)
            
            logger.info(f"📤 Uploaded cleaned CSV '{blob_name}' with shape {df.shape}")
            
        except Exception as e:
            logger.error(f"❌ Failed to upload CSV '{blob_name}': {str(e)}")
            raise
    
    def verify_upload(self, container_name: str, blob_name: str, expected_shape: Tuple[int, int]) -> bool:
        """Verify the uploaded file is correct"""
        try:
            # Download and check the uploaded file
            df_verify = self.download_csv_from_azure(container_name, blob_name)
            
            if df_verify.shape == expected_shape:
                logger.info(f"✅ Verification passed for {blob_name}: shape {df_verify.shape}")
                return True
            else:
                logger.error(f"❌ Verification failed for {blob_name}: expected {expected_shape}, got {df_verify.shape}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Verification error for {blob_name}: {str(e)}")
            return False
    
    def process_table(self, mapping: Dict[str, Any], container_name: str, available_blobs: List[str]) -> Dict[str, Any]:
        """Process a single table mapping"""
        table_name = mapping['snowflake_table']
        azure_csv_name = mapping['azure_csv_name']
        database_schema = mapping['snowflake_database']
        
        result = {
            'table_name': table_name,
            'azure_csv_name': azure_csv_name,
            'status': 'pending',
            'backup_created': None,
            'columns_cleaned': 0,
            'original_shape': None,
            'final_shape': None
        }
        
        try:
            # Find matching blob
            matching_blob = self.find_matching_blob(azure_csv_name, available_blobs)
            
            if not matching_blob:
                result['status'] = 'blob_not_found'
                result['error'] = f"No matching blob found for {azure_csv_name}"
                return result
            
            result['blob_name'] = matching_blob
            
            # Get Snowflake column mapping
            if table_name not in self.column_mappings:
                column_mapping = self.get_snowflake_columns(table_name, database_schema)
                if not column_mapping:
                    result['status'] = 'snowflake_schema_failed'
                    result['error'] = "Failed to get Snowflake table schema"
                    return result
                self.column_mappings[table_name] = column_mapping
            
            # Download original CSV
            df_original = self.download_csv_from_azure(container_name, matching_blob)
            result['original_shape'] = df_original.shape
            
            # Create backup
            backup_name = self.create_backup(container_name, matching_blob)
            result['backup_created'] = backup_name
            
            # Clean columns
            df_cleaned = self.clean_csv_columns(df_original, table_name)
            result['final_shape'] = df_cleaned.shape
            result['columns_cleaned'] = len([col for col in df_original.columns if col != df_cleaned.columns[list(df_original.columns).index(col)] if col in df_original.columns])
            
            # Upload cleaned CSV
            self.upload_csv_to_azure(df_cleaned, container_name, matching_blob)
            
            # Verify upload
            if self.verify_upload(container_name, matching_blob, df_cleaned.shape):
                result['status'] = 'success'
                logger.info(f"✅ Successfully processed {table_name}")
            else:
                result['status'] = 'verification_failed'
                result['error'] = "Upload verification failed"
            
        except Exception as e:
            result['status'] = 'error'
            result['error'] = str(e)
            logger.error(f"❌ Error processing {table_name}: {str(e)}")
        
        return result
    
    def run_cleaning(self, container_name: str = "pbi25") -> Dict[str, Any]:
        """Run the complete cleaning process"""
        logger.info("🚀 Starting pre-load CSV cleaning pipeline")
        
        try:
            # Setup
            self.load_configuration()
            self.setup_connections()
            
            # List available blobs
            container_client = self.blob_service_client.get_container_client(container_name)
            available_blobs = [blob.name for blob in container_client.list_blobs()]
            logger.info(f"📦 Found {len(available_blobs)} blobs in container")
            
            # Process each table
            results = []
            total_tables = len(self.table_mapping)
            
            for i, mapping in enumerate(self.table_mapping, 1):
                table_name = mapping['snowflake_table']
                logger.info(f"🔄 Processing table {i}/{total_tables}: {table_name}")
                
                result = self.process_table(mapping, container_name, available_blobs)
                results.append(result)
            
            # Generate summary
            successful = sum(1 for r in results if r['status'] == 'success')
            failed = sum(1 for r in results if r['status'] != 'success')
            
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
            results_file = f"logs/pre_load_cleaning_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            with open(results_file, 'w') as f:
                json.dump(summary, f, indent=2)
            
            logger.info(f"🎉 Cleaning completed! Success: {successful}, Failed: {failed}")
            logger.info(f"📄 Results saved to: {results_file}")
            
            return summary
            
        except Exception as e:
            logger.error(f"❌ Cleaning pipeline failed: {str(e)}")
            raise
        
        finally:
            if self.snowflake_conn:
                self.snowflake_conn.close()
                logger.info("🔒 Closed Snowflake connection")

def main():
    """Main execution function"""
    try:
        cleaner = PreLoadCleaner()
        results = cleaner.run_cleaning()
        
        # Print summary
        print("\n" + "="*60)
        print("PRE-LOAD CLEANING SUMMARY")
        print("="*60)
        print(f"Total Tables: {results['total_tables']}")
        print(f"Successfully Cleaned: {results['successful']}")
        print(f"Failed: {results['failed']}")
        print(f"Success Rate: {results['success_rate']}")
        print("="*60)
        
        # Print details for failed tables
        failed_tables = [r for r in results['results'] if r['status'] != 'success']
        if failed_tables:
            print("\nFAILED TABLES:")
            for table in failed_tables:
                print(f"- {table['table_name']}: {table.get('error', 'Unknown error')}")
        
        # Print successful cleanings
        successful_tables = [r for r in results['results'] if r['status'] == 'success']
        if successful_tables:
            print(f"\nSUCCESSFULLY CLEANED TABLES ({len(successful_tables)}):")
            for table in successful_tables:
                original_shape = table.get('original_shape', 'Unknown')
                final_shape = table.get('final_shape', 'Unknown')
                print(f"- {table['table_name']}: {original_shape} -> {final_shape}")
        
    except Exception as e:
        logger.error(f"❌ Pre-load cleaning failed: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()