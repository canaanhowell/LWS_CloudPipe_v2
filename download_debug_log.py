#!/usr/bin/env python3
"""
Script to download debug log file from Azure Blob Storage
"""

import os
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential

def download_debug_log():
    """Download the debug log file from Azure Blob Storage"""
    
    # Azure Storage configuration
    account_name = "lwsdatapipeline"
    container_name = "lws-data"
    
    try:
        # Create the BlobServiceClient using DefaultAzureCredential
        credential = DefaultAzureCredential()
        blob_service_client = BlobServiceClient(
            account_url=f"https://{account_name}.blob.core.windows.net",
            credential=credential
        )
        
        # Get container client
        container_client = blob_service_client.get_container_client(container_name)
        
        # List all blobs to find debug files
        print("Searching for debug log files...")
        debug_files = []
        
        for blob in container_client.list_blobs():
            if 'debug' in blob.name.lower() or 'log' in blob.name.lower():
                debug_files.append({
                    'name': blob.name,
                    'size': blob.size,
                    'last_modified': blob.last_modified
                })
                print(f"Found: {blob.name} (Size: {bl.size} bytes, Modified: {bl.last_modified})")
        
        if not debug_files:
            print("No debug log files found!")
            return
        
        # Try to download the most likely debug file
        target_files = ['pipeline_debug_log.txt', 'debug_log.txt', 'pipeline.log']
        
        for target in target_files:
            try:
                blob_client = container_client.get_blob_client(target)
                if blob_client.exists():
                    print(f"\nDownloading {target}...")
                    with open(target, "wb") as download_file:
                        download_stream = blob_client.download_blob()
                        download_file.write(download_stream.readall())
                    print(f"Successfully downloaded {target}")
                    
                    # Read and display the content
                    print(f"\n=== Content of {target} ===")
                    with open(target, "r", encoding='utf-8') as f:
                        content = f.read()
                        print(content)
                    return
            except Exception as e:
                print(f"Could not download {target}: {e}")
        
        # If no specific files found, try the first debug file
        if debug_files:
            first_debug = debug_files[0]['name']
            print(f"\nTrying to download {first_debug}...")
            try:
                blob_client = container_client.get_blob_client(first_debug)
                with open(first_debug, "wb") as download_file:
                    download_stream = blob_client.download_blob()
                    download_file.write(download_stream.readall())
                print(f"Successfully downloaded {first_debug}")
                
                # Read and display the content
                print(f"\n=== Content of {first_debug} ===")
                with open(first_debug, "r", encoding='utf-8') as f:
                    content = f.read()
                    print(content)
            except Exception as e:
                print(f"Could not download {first_debug}: {e}")
                
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    download_debug_log() 