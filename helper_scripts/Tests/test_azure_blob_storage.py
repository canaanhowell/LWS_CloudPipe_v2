#!/usr/bin/env python3
"""
Test Azure Blob Storage connection, list blobs, and download a sample file.
"""
import os
from pathlib import Path
import sys
import json
sys.path.append(str(Path(__file__).parent.parent / "Utils"))
from azure.storage.blob import BlobServiceClient
from logger import log

# Load credentials at the top
base_dir = Path(__file__).parent.parent.parent
    cred_file = base_dir / "settings.json"
if cred_file.exists():
    with open(cred_file, "r") as f:
        credentials = json.load(f)
else:
    credentials = {}

def test_azure_blob_storage():
    log("AZURE_BLOB_TEST", "Starting Azure Blob Storage connection test...", "INFO")
    connection_string = credentials.get("AZURE_STORAGE_CONNECTION_STRING")
    container_name = credentials.get("BLOB_CONTAINER", "pbi25")
    if not connection_string:
        log("AZURE_BLOB_TEST", "Missing AZURE_STORAGE_CONNECTION_STRING in environment.", "ERROR")
        return False
    try:
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        container_client = blob_service_client.get_container_client(container_name)
        log("AZURE_BLOB_TEST", f"Connected to container: {container_name}", "INFO")
        blobs = list(container_client.list_blobs())
        log("AZURE_BLOB_TEST", f"Found {len(blobs)} blobs in container.", "INFO")
        csv_blobs = [b for b in blobs if b.name.endswith('.csv')]
        if not csv_blobs:
            log("AZURE_BLOB_TEST", "No CSV files found in container.", "WARNING")
            return True
        # Download the first CSV file
        blob = csv_blobs[0]
        log("AZURE_BLOB_TEST", f"Attempting to download blob: {blob.name}", "INFO")
        blob_client = container_client.get_blob_client(blob.name)
        content = blob_client.download_blob().readall()
        # Save to data/csv/test_downloaded_blob.csv
        output_dir = Path(__file__).parent.parent.parent / "data" / "csv"
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / "test_downloaded_blob.csv"
        with open(output_path, "wb") as f:
            f.write(content)
        log("AZURE_BLOB_TEST", f"Successfully downloaded {blob.name} to {output_path}", "INFO")
        return True
    except Exception as e:
        log("AZURE_BLOB_TEST", f"Azure Blob Storage test failed: {str(e)}", "ERROR")
        return False

if __name__ == "__main__":
    result = test_azure_blob_storage()
    print(f"Azure Blob Storage test {'PASSED' if result else 'FAILED'}.") 