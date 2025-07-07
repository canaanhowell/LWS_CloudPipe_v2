import json
from azure.storage.blob import BlobServiceClient
from pathlib import Path

# Load settings
with open('settings.json', 'r') as f:
    settings = json.load(f)

connection_string = settings['AZURE_STORAGE_CONNECTION_STRING']
container_name = settings['BLOB_CONTAINER']

blob_service_client = BlobServiceClient.from_connection_string(connection_string)
container_client = blob_service_client.get_container_client(container_name)

print(f"Listing blobs in container: {container_name}\n")
for blob in container_client.list_blobs():
    print(blob.name) 