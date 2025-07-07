#!/usr/bin/env python3
"""
Data Type Fixer for Azure to Snowflake CSV Loading
Fixes data type mismatches identified during the loading process.
"""

import pandas as pd
import json
import re
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from azure.storage.blob import BlobServiceClient
import numpy as np
from io import StringIO

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DataTypeFixer:
    def __init__(self, config_path: str = "../LWS_CloudPipe_v2/config_files/settings.json"):
        """Initialize the data type fixer with configuration."""
        with open(config_path, 'r') as f:
            self.config = json.load(f)
        
        # Parse connection string for Azure credentials
        conn_str = self.config['AZURE_STORAGE_CONNECTION_STRING']
        self.blob_service_client = BlobServiceClient.from_connection_string(conn_str)
        
        self.container_name = self.config['BLOB_CONTAINER']
        self.results = []
        
    def convert_number_to_date(self, value: Any) -> Optional[str]:
        """Convert numeric values to date format."""
        if pd.isna(value) or value == '' or value is None:
            return None
            
        try:
            # Handle numeric values that might be Excel serial dates
            if isinstance(value, (int, float)):
                # Excel serial date (days since 1900-01-01, accounting for leap year bug)
                if value > 0 and value < 100000:  # Reasonable date range
                    excel_epoch = datetime(1900, 1, 1)
                    # Excel incorrectly treats 1900 as a leap year
                    if value >= 61:  # After Feb 28, 1900
                        value -= 1
                    converted_date = excel_epoch + timedelta(days=value - 2)
                    return converted_date.strftime('%Y-%m-%d')
                # Unix timestamp
                elif value > 1000000000:  # After 2001
                    converted_date = datetime.fromtimestamp(value)
                    return converted_date.strftime('%Y-%m-%d')
                    
            # Handle string values
            str_value = str(value).strip()
            if str_value.replace('.', '').replace('-', '').isdigit():
                num_value = float(str_value)
                return self.convert_number_to_date(num_value)
                
        except Exception as e:
            logger.warning(f"Could not convert {value} to date: {e}")
            
        return None
    
    def fix_date_columns(self, df: pd.DataFrame, table_name: str) -> pd.DataFrame:
        """Fix DATE columns that contain numeric values."""
        date_patterns = [
            r'.*date.*', r'.*Date.*', r'.*DATE.*',
            r'.*signed.*', r'.*Signed.*', r'.*SIGNED.*'
        ]
        
        fixed_columns = []
        
        for col in df.columns:
            # Check if column name suggests it's a date
            is_date_column = any(re.match(pattern, col, re.IGNORECASE) for pattern in date_patterns)
            
            if is_date_column:
                logger.info(f"Processing date column: {col}")
                original_nulls = df[col].isna().sum()
                
                # Apply date conversion
                df[col] = df[col].apply(self.convert_number_to_date)
                
                new_nulls = df[col].isna().sum()
                converted_count = len(df) - new_nulls
                
                logger.info(f"  - Original nulls: {original_nulls}, New nulls: {new_nulls}")
                logger.info(f"  - Successfully converted: {converted_count} values")
                
                fixed_columns.append(col)
        
        return df, fixed_columns
    
    def fix_numeric_columns(self, df: pd.DataFrame, table_name: str) -> pd.DataFrame:
        """Fix numeric columns with invalid values."""
        fixed_columns = []
        
        for col in df.columns:
            if df[col].dtype == 'object':  # String columns that might contain numbers
                # Check if column has mixed alphanumeric values that should be numeric
                non_null_values = df[col].dropna().astype(str)
                
                if len(non_null_values) > 0:
                    # Count how many values look like they should be numeric
                    numeric_pattern = r'^[\d\.\-\+]+$'
                    alphanumeric_pattern = r'^[A-Za-z0-9]+$'
                    
                    numeric_count = sum(1 for val in non_null_values if re.match(numeric_pattern, val))
                    alphanumeric_count = sum(1 for val in non_null_values if re.match(alphanumeric_pattern, val))
                    
                    # If we have mixed values, try to clean them
                    if numeric_count > 0 and alphanumeric_count > 0:
                        logger.info(f"Fixing mixed numeric column: {col}")
                        
                        def clean_numeric_value(val):
                            if pd.isna(val) or val == '':
                                return None
                            
                            str_val = str(val).strip()
                            
                            # Try to extract numeric part
                            numeric_match = re.search(r'[\d\.\-\+]+', str_val)
                            if numeric_match:
                                try:
                                    return float(numeric_match.group())
                                except:
                                    pass
                            
                            # If it's purely alphabetic, convert to null
                            if re.match(r'^[A-Za-z]+$', str_val):
                                return None
                                
                            return val
                        
                        df[col] = df[col].apply(clean_numeric_value)
                        fixed_columns.append(col)
        
        return df, fixed_columns
    
    def fix_identifier_issues(self, df: pd.DataFrame, table_name: str) -> pd.DataFrame:
        """Fix issues with column identifiers that have special characters."""
        fixed_columns = []
        
        # Check for problematic column names
        for col in df.columns:
            if any(char in col for char in ['(', ')', '?', '-']):
                logger.info(f"Found problematic column identifier: {col}")
                
                # For SERVICE table, we know there are issues with certain columns
                if 'SERVICE' in table_name:
                    # Try to clean the data in problematic columns
                    df[col] = df[col].astype(str).replace('nan', '')
                    fixed_columns.append(col)
        
        return df, fixed_columns
    
    def fix_csv_data_types(self, table_name: str, csv_name: str) -> Dict[str, Any]:
        """Fix data type issues in a specific CSV file."""
        logger.info(f"Starting data type fixing for table: {table_name}")
        
        try:
            # Download CSV from Azure
            blob_client = self.blob_service_client.get_blob_client(
                container=self.container_name, 
                blob=csv_name
            )
            
            csv_content = blob_client.download_blob().content_as_text()
            df = pd.read_csv(StringIO(csv_content))
            
            logger.info(f"Loaded CSV with shape: {df.shape}")
            
            # Create backup before modification
            backup_name = f"backup_before_type_fix_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{csv_name}"
            backup_blob_client = self.blob_service_client.get_blob_client(
                container=self.container_name,
                blob=backup_name
            )
            backup_blob_client.upload_blob(csv_content, overwrite=True)
            
            # Apply fixes based on table type
            all_fixed_columns = []
            
            # Fix date columns
            df, date_fixed = self.fix_date_columns(df, table_name)
            all_fixed_columns.extend(date_fixed)
            
            # Fix numeric columns
            df, numeric_fixed = self.fix_numeric_columns(df, table_name)
            all_fixed_columns.extend(numeric_fixed)
            
            # Fix identifier issues
            df, identifier_fixed = self.fix_identifier_issues(df, table_name)
            all_fixed_columns.extend(identifier_fixed)
            
            # Upload fixed CSV back to Azure
            fixed_csv_content = df.to_csv(index=False)
            blob_client.upload_blob(fixed_csv_content, overwrite=True)
            
            return {
                'table_name': table_name,
                'azure_csv_name': csv_name,
                'status': 'success',
                'backup_created': backup_name,
                'columns_fixed': len(all_fixed_columns),
                'fixed_columns': all_fixed_columns,
                'original_shape': list(pd.read_csv(StringIO(csv_content)).shape),
                'final_shape': list(df.shape)
            }
            
        except Exception as e:
            logger.error(f"Error fixing data types for {table_name}: {str(e)}")
            return {
                'table_name': table_name,
                'azure_csv_name': csv_name,
                'status': 'failed',
                'error': str(e)
            }
    
    def fix_all_tables(self) -> None:
        """Fix data types for all tables in the mapping."""
        with open('../LWS_CloudPipe_v2/config_files/table_mapping.json', 'r') as f:
            table_mapping = json.load(f)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        for config in table_mapping:
            table_name = config['snowflake_table']
            azure_csv_name = config['azure_csv_name'] + '.csv'
            result = self.fix_csv_data_types(table_name, azure_csv_name)
            self.results.append(result)
        
        # Save results
        results_summary = {
            'timestamp': datetime.now().isoformat(),
            'total_tables': len(self.results),
            'successful': sum(1 for r in self.results if r['status'] == 'success'),
            'failed': sum(1 for r in self.results if r['status'] == 'failed'),
            'results': self.results
        }
        
        results_summary['success_rate'] = f"{results_summary['successful'] / results_summary['total_tables'] * 100:.1f}%"
        
        # Save results to file
        results_file = f"../logs/data_type_fixing_results_{timestamp}.json"
        with open(results_file, 'w') as f:
            json.dump(results_summary, f, indent=2)
        
        logger.info(f"Data type fixing completed. Results saved to: {results_file}")
        logger.info(f"Success rate: {results_summary['success_rate']}")

if __name__ == "__main__":
    fixer = DataTypeFixer()
    fixer.fix_all_tables()