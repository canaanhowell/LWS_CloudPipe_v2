#!/usr/bin/env python3
"""
Schema Analyzer - Compare Azure CSV headers with Snowflake table schemas

This script downloads CSV files from Azure Blob Storage, analyzes their headers,
compares them with corresponding Snowflake table schemas, and generates mapping
configurations for the data loader.
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

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('schema_analysis.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class SchemaAnalyzer:
    """Analyze and compare CSV and Snowflake schemas"""
    
    def __init__(self, config_dir: str = "LWS_CloudPipe_v2/config_files"):
        """Initialize the analyzer with configuration directory"""
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
    
    def clean_column_name(self, column_name: str) -> str:
        """Clean column names for comparison"""
        cleaned = str(column_name).strip().strip('"\'')
        cleaned = cleaned.replace(' ', '_')
        cleaned = re.sub(r'[/\(\)\?\-\.,\:\;\!\@\#\$\%\^\&\*\+\=\[\]\{\}\|\\\`\~\<\>\"\']', '_', cleaned)
        cleaned = re.sub(r'[^\w\s_]', '_', cleaned)
        cleaned = re.sub(r'_+', '_', cleaned)
        cleaned = cleaned.strip('_')
        
        if not cleaned:
            cleaned = 'UNNAMED_COLUMN'
        
        if cleaned and not cleaned[0].isalpha() and cleaned[0] != '_':
            cleaned = f'COL_{cleaned}'
            
        return cleaned.upper()  # Snowflake is case-insensitive, use uppercase for comparison
    
    def get_csv_headers(self, container_name: str, blob_name: str) -> List[str]:
        """Download CSV and extract headers"""
        try:
            container_client = self.blob_service_client.get_container_client(container_name)
            blob_client = container_client.get_blob_client(blob_name)
            
            # Download and read only the header
            download_stream = blob_client.download_blob()
            df = pd.read_csv(download_stream, nrows=0)  # Only read headers
            
            # Clean column names
            cleaned_headers = [self.clean_column_name(col) for col in df.columns]
            
            logger.info(f"📊 Retrieved {len(cleaned_headers)} headers from {blob_name}")
            return cleaned_headers
            
        except Exception as e:
            logger.error(f"❌ Failed to get headers from {blob_name}: {str(e)}")
            return []
    
    def get_snowflake_columns(self, table_name: str, database_schema: str) -> List[str]:
        """Get column names from Snowflake table"""
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
            
            # Extract column names (first element in each row)
            column_names = [row[0].upper() for row in columns_info]
            
            logger.info(f"📋 Retrieved {len(column_names)} columns from {table_name}")
            return column_names
            
        except Exception as e:
            logger.error(f"❌ Failed to get columns from {table_name}: {str(e)}")
            return []
    
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
    
    def compare_schemas(self, csv_headers: List[str], snowflake_columns: List[str]) -> Dict[str, Any]:
        """Compare CSV headers with Snowflake columns"""
        
        # Convert to sets for comparison
        csv_set = set(csv_headers)
        snowflake_set = set(snowflake_columns)
        
        # Find matches and differences
        exact_matches = csv_set.intersection(snowflake_set)
        csv_only = csv_set - snowflake_set
        snowflake_only = snowflake_set - csv_set
        
        # Try to find potential matches for unmatched columns
        potential_matches = {}
        for csv_col in csv_only:
            for sf_col in snowflake_only:
                # Simple similarity check
                if csv_col in sf_col or sf_col in csv_col:
                    potential_matches[csv_col] = sf_col
                elif len(csv_col) > 3 and len(sf_col) > 3:
                    # Check if they share significant substring
                    csv_words = set(csv_col.split('_'))
                    sf_words = set(sf_col.split('_'))
                    if len(csv_words.intersection(sf_words)) >= 2:
                        potential_matches[csv_col] = sf_col
        
        return {
            'csv_column_count': len(csv_headers),
            'snowflake_column_count': len(snowflake_columns),
            'exact_matches': list(exact_matches),
            'exact_match_count': len(exact_matches),
            'csv_only_columns': list(csv_only),
            'snowflake_only_columns': list(snowflake_only),
            'potential_matches': potential_matches,
            'match_percentage': (len(exact_matches) / max(len(csv_headers), len(snowflake_columns))) * 100,
            'csv_headers': csv_headers,
            'snowflake_columns': snowflake_columns
        }
    
    def analyze_all_schemas(self, container_name: str = "pbi25") -> Dict[str, Any]:
        """Analyze schemas for all tables in the mapping"""
        logger.info("🔍 Starting comprehensive schema analysis")
        
        # List available blobs
        container_client = self.blob_service_client.get_container_client(container_name)
        available_blobs = [blob.name for blob in container_client.list_blobs()]
        logger.info(f"📦 Found {len(available_blobs)} blobs in container")
        
        analysis_results = {}
        
        for i, mapping in enumerate(self.table_mapping, 1):
            table_name = mapping['snowflake_table']
            azure_csv_name = mapping['azure_csv_name']
            database_schema = mapping['snowflake_database']
            
            logger.info(f"🔍 Analyzing table {i}/{len(self.table_mapping)}: {table_name}")
            
            result = {
                'table_name': table_name,
                'azure_csv_name': azure_csv_name,
                'status': 'pending'
            }
            
            try:
                # Find matching blob
                matching_blob = self.find_matching_blob(azure_csv_name, available_blobs)
                
                if not matching_blob:
                    result['status'] = 'blob_not_found'
                    result['error'] = f"No matching blob found for {azure_csv_name}"
                    analysis_results[table_name] = result
                    continue
                
                result['blob_name'] = matching_blob
                
                # Get CSV headers
                csv_headers = self.get_csv_headers(container_name, matching_blob)
                
                if not csv_headers:
                    result['status'] = 'csv_read_failed'
                    result['error'] = "Failed to read CSV headers"
                    analysis_results[table_name] = result
                    continue
                
                # Get Snowflake columns
                snowflake_columns = self.get_snowflake_columns(table_name, database_schema)
                
                if not snowflake_columns:
                    result['status'] = 'snowflake_read_failed'
                    result['error'] = "Failed to read Snowflake table schema"
                    analysis_results[table_name] = result
                    continue
                
                # Compare schemas
                comparison = self.compare_schemas(csv_headers, snowflake_columns)
                result.update(comparison)
                result['status'] = 'analyzed'
                
                logger.info(f"✅ {table_name}: {comparison['exact_match_count']}/{comparison['csv_column_count']} columns match ({comparison['match_percentage']:.1f}%)")
                
            except Exception as e:
                result['status'] = 'error'
                result['error'] = str(e)
                logger.error(f"❌ Error analyzing {table_name}: {str(e)}")
            
            analysis_results[table_name] = result
        
        return analysis_results
    
    def generate_column_mappings(self, analysis_results: Dict[str, Any]) -> Dict[str, Dict[str, str]]:
        """Generate column mappings for successful schema matches"""
        mappings = {}
        
        for table_name, result in analysis_results.items():
            if result['status'] != 'analyzed':
                continue
            
            # Create mapping from CSV to Snowflake columns
            column_mapping = {}
            
            # Add exact matches
            for col in result['exact_matches']:
                column_mapping[col] = col
            
            # Add potential matches
            for csv_col, sf_col in result['potential_matches'].items():
                column_mapping[csv_col] = sf_col
            
            # For unmatched CSV columns, map to themselves (will need manual review)
            unmatched_csv = set(result['csv_only_columns']) - set(result['potential_matches'].keys())
            for col in unmatched_csv:
                column_mapping[col] = col  # Will need manual review
            
            mappings[table_name] = column_mapping
        
        return mappings
    
    def save_analysis_results(self, analysis_results: Dict[str, Any], column_mappings: Dict[str, Dict[str, str]]) -> None:
        """Save analysis results to files"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Save detailed analysis
        analysis_file = f"schema_analysis_{timestamp}.json"
        with open(analysis_file, 'w') as f:
            json.dump(analysis_results, f, indent=2)
        
        # Save column mappings
        mappings_file = f"column_mappings_{timestamp}.json"
        with open(mappings_file, 'w') as f:
            json.dump(column_mappings, f, indent=2)
        
        # Generate summary report
        report_file = f"schema_analysis_report_{timestamp}.txt"
        with open(report_file, 'w') as f:
            f.write("SCHEMA ANALYSIS SUMMARY REPORT\n")
            f.write("=" * 50 + "\n\n")
            
            total_tables = len(analysis_results)
            analyzed_count = sum(1 for r in analysis_results.values() if r['status'] == 'analyzed')
            
            f.write(f"Total Tables: {total_tables}\n")
            f.write(f"Successfully Analyzed: {analyzed_count}\n")
            f.write(f"Analysis Success Rate: {(analyzed_count/total_tables)*100:.1f}%\n\n")
            
            for table_name, result in analysis_results.items():
                f.write(f"\nTABLE: {table_name}\n")
                f.write("-" * 40 + "\n")
                f.write(f"Status: {result['status']}\n")
                
                if result['status'] == 'analyzed':
                    f.write(f"CSV Columns: {result['csv_column_count']}\n")
                    f.write(f"Snowflake Columns: {result['snowflake_column_count']}\n")
                    f.write(f"Exact Matches: {result['exact_match_count']}\n")
                    f.write(f"Match Percentage: {result['match_percentage']:.1f}%\n")
                    
                    if result['csv_only_columns']:
                        f.write(f"\nCSV-Only Columns ({len(result['csv_only_columns'])}):\n")
                        for col in result['csv_only_columns'][:10]:  # Show first 10
                            f.write(f"  - {col}\n")
                    
                    if result['snowflake_only_columns']:
                        f.write(f"\nSnowflake-Only Columns ({len(result['snowflake_only_columns'])}):\n")
                        for col in result['snowflake_only_columns'][:10]:  # Show first 10
                            f.write(f"  - {col}\n")
                    
                    if result['potential_matches']:
                        f.write(f"\nPotential Matches ({len(result['potential_matches'])}):\n")
                        for csv_col, sf_col in result['potential_matches'].items():
                            f.write(f"  {csv_col} -> {sf_col}\n")
                else:
                    f.write(f"Error: {result.get('error', 'Unknown error')}\n")
        
        logger.info(f"📄 Analysis results saved:")
        logger.info(f"  - Detailed: {analysis_file}")
        logger.info(f"  - Mappings: {mappings_file}")
        logger.info(f"  - Report: {report_file}")
    
    def run_analysis(self, container_name: str = "pbi25") -> None:
        """Run the complete schema analysis"""
        try:
            logger.info("🚀 Starting schema analysis pipeline")
            
            # Setup
            self.load_configuration()
            self.setup_connections()
            
            # Analyze schemas
            analysis_results = self.analyze_all_schemas(container_name)
            
            # Generate mappings
            column_mappings = self.generate_column_mappings(analysis_results)
            
            # Save results
            self.save_analysis_results(analysis_results, column_mappings)
            
            # Print summary
            analyzed_count = sum(1 for r in analysis_results.values() if r['status'] == 'analyzed')
            avg_match_rate = sum(r.get('match_percentage', 0) for r in analysis_results.values() if r['status'] == 'analyzed') / max(analyzed_count, 1)
            
            logger.info(f"🎉 Analysis completed!")
            logger.info(f"📊 Tables analyzed: {analyzed_count}/{len(analysis_results)}")
            logger.info(f"📈 Average match rate: {avg_match_rate:.1f}%")
            
        except Exception as e:
            logger.error(f"❌ Analysis failed: {str(e)}")
            raise
        
        finally:
            if self.snowflake_conn:
                self.snowflake_conn.close()

def main():
    """Main execution function"""
    try:
        analyzer = SchemaAnalyzer()
        analyzer.run_analysis()
        
    except Exception as e:
        logger.error(f"❌ Schema analysis failed: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()