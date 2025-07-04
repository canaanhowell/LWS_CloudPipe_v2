#!/usr/bin/env python3
"""
Cloud-Native CSV Cleaning Script

- Reads table_mapping.json for mapping info
- Downloads raw CSVs directly from Azure Blob Storage
- Cleans/processes each CSV in memory (pandas)
- Uploads cleaned CSVs back to Azure Blob Storage with correct naming
- No local file dependencies (except optional debug logging)
"""

import pandas as pd
import json
import io
import sys
from pathlib import Path

# Add helper_scripts to path
sys.path.append(str(Path(__file__).parent / "helper_scripts" / "Utils"))

# Load credentials from settings.json
def load_credentials():
    # Always look for settings.json in the project root
    cred_file = Path(__file__).parent.parent / "settings.json"
    if not cred_file.exists():
        print("Missing settings.json file!")
        return {}
    with open(cred_file, "r") as f:
        return json.load(f)

config = load_credentials()

try:
    from azure.storage.blob import BlobServiceClient
except ImportError:
    print("azure-storage-blob SDK is not installed. Please install it with 'pip install azure-storage-blob'.")
    sys.exit(1)

# --- Utility Functions ---
def clean_string_value(value):
    if pd.isna(value):
        return None
    cleaned = str(value).strip()
    if not cleaned or cleaned.isspace():
        return None
    hidden_chars = [chr(i) for i in range(0, 32)]
    for char in hidden_chars:
        cleaned = cleaned.replace(char, '')
    if not cleaned or cleaned.isspace():
        return None
    return cleaned

def rename_google_analytics_headers(df):
    header_map = {
        'dimension_1': 'date',
        'dimension_2': 'pageTitle',
        'dimension_3': 'pagePath',
        'metric_1': 'totalUsers',
        'metric_2': 'sessions',
        'metric_3': 'newUsers',
        'metric_4': 'averageSessionDuration',
        'metric_5': 'bounceRate',
        'metric_6': 'engagementRate',
        'metric_7': 'conversions',
        'metric_8': 'userEngagementDuration',
        'metric_9': 'eventCount',
        'metric_10': 'screenPageViews',
    }
    return df.rename(columns=header_map)

def load_table_mapping():
    mapping_path = Path("config_files/table_mapping.json")
    with open(mapping_path, "r", encoding="utf-8") as f:
        return json.load(f)

def get_blob_service_client():
    connection_string = config.get('AZURE_STORAGE_CONNECTION_STRING')
    if not connection_string:
        print("AZURE_STORAGE_CONNECTION_STRING not found in config.")
        sys.exit(1)
    return BlobServiceClient.from_connection_string(connection_string)

def download_blob_to_df(blob_service_client, container, blob_name):
    blob_client = blob_service_client.get_blob_client(container=container, blob=blob_name)
    stream = blob_client.download_blob()
    return pd.read_csv(io.BytesIO(stream.readall()))

def upload_df_to_blob(blob_service_client, container, df, blob_name):
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    blob_client = blob_service_client.get_blob_client(container=container, blob=blob_name)
    blob_client.upload_blob(csv_buffer.getvalue(), overwrite=True)
    print(f"Uploaded cleaned file to Azure: {blob_name}")

def clean_csv(df, mapping_entry):
    print(f"Checking mapping entry: {mapping_entry['cleaned_csv_name']}")
    
    # Step 1: Remove completely blank rows
    initial_rows = len(df)
    df = df.dropna(how='all')
    if len(df) < initial_rows:
        print(f"Removed {initial_rows - len(df)} completely blank rows")
    
    # Step 2: Clean all string values to remove whitespace and hidden characters
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].apply(clean_string_value)
    
    # Step 3: Remove duplicates based on primary key
    pk = mapping_entry.get("primary_key")
    is_composite = mapping_entry.get("composite", False)
    
    if pk:
        if is_composite:
            # Handle composite primary key (e.g., "Scoop ID + Project name")
            pk_parts = [part.strip() for part in pk.split('+')]
            print(f"Composite primary key parts: {pk_parts}")
            
            # Check if all parts exist in the dataframe
            missing_parts = [part for part in pk_parts if part not in df.columns]
            if missing_parts:
                print(f"Warning: Missing composite key parts: {missing_parts}")
            else:
                # Remove rows where any part of the composite key is null
                df = df.dropna(subset=pk_parts)
                print(f"Removed rows with null composite key values, now has {len(df)} rows")
                
                # Remove duplicates based on composite key
                before_dedup = len(df)
                df = df.drop_duplicates(subset=pk_parts)
                if len(df) < before_dedup:
                    print(f"Removed {before_dedup - len(df)} duplicate rows based on composite key: {pk_parts}")
        else:
            # Handle single primary key
            if pk in df.columns:
                print(f"Single primary key: {pk}")
                # Remove rows where primary key is null
                df = df.dropna(subset=[pk])
                print(f"Removed rows with null primary key, now has {len(df)} rows")
                
                # Remove duplicates based on primary key
                before_dedup = len(df)
                df = df.drop_duplicates(subset=[pk])
                if len(df) < before_dedup:
                    print(f"Removed {before_dedup - len(df)} duplicate rows based on primary key: {pk}")
            else:
                print(f"Warning: Primary key '{pk}' not found in columns")
    
    # Step 4: Special case for Google Analytics
    if mapping_entry["raw_csv_name"] == "lws.public.google_analytics_raw":
        df = rename_google_analytics_headers(df)
    
    print(f"Final cleaned DataFrame has {len(df)} rows")
    return df

def main():
    print("Starting cloud-native CSV cleaning...")
    mapping = load_table_mapping()
    blob_service_client = get_blob_service_client()
    container = config.get('BLOB_CONTAINER', 'pbi25')

    # List blobs in container
    container_client = blob_service_client.get_container_client(container)
    blob_list = [b.name for b in container_client.list_blobs() if b.name.endswith('_raw.csv')]
    print(f"Found {len(blob_list)} raw CSVs in Azure container '{container}'")

    for entry in mapping:
        raw_blob = entry["raw_csv_name"] + ".csv"
        cleaned_blob = entry["cleaned_csv_name"] + ".csv"
        if raw_blob not in blob_list:
            print(f"Raw file not found in Azure: {raw_blob}")
            continue
        print(f"Cleaning {raw_blob} -> {cleaned_blob}")
        try:
            df = download_blob_to_df(blob_service_client, container, raw_blob)
            cleaned_df = clean_csv(df, entry)
            upload_df_to_blob(blob_service_client, container, cleaned_df, cleaned_blob)
        except Exception as e:
            print(f"Error processing {raw_blob}: {e}")

    print("Cloud-native CSV cleaning complete!")

if __name__ == "__main__":
    main() 