#!/usr/bin/env python3
"""
Test script to verify SharePoint file creation
"""

import os
import sys
import json
import pandas as pd
import requests
from pathlib import Path
from azure.identity import ClientSecretCredential

# Add helper_scripts to path
sys.path.append(str(Path(__file__).parent / "helper_scripts" / "Utils"))
from logger import log

def test_sharepoint_file_creation():
    """Test SharePoint file creation process."""
    try:
        base_dir = Path(__file__).parent
        csv_dir = base_dir / "data" / "csv"
        config_file = base_dir / "settings.json"
        
        print(f"Base directory: {base_dir}")
        print(f"CSV directory: {csv_dir}")
        print(f"Config file exists: {config_file.exists()}")
        
        # Load credentials
        with open(config_file, "r") as f:
            credentials = json.load(f)
        
        # Get SharePoint credentials
        client_id = credentials.get("SHAREPOINT_AZURE_CLIENT_ID")
        client_secret = credentials.get("SHAREPOINT_AZURE_CLIENT_SECRET")
        tenant_id = credentials.get("AZURE_TENANT_ID")
        site_id = credentials.get("SHAREPOINT_SITE_ID")
        
        print(f"Client ID: {client_id[:10]}..." if client_id else "Missing")
        print(f"Client Secret: {'Set' if client_secret else 'Missing'}")
        print(f"Tenant ID: {tenant_id[:10]}..." if tenant_id else "Missing")
        print(f"Site ID: {site_id[:10]}..." if site_id else "Missing")
        
        if not all([client_id, client_secret, tenant_id, site_id]):
            print("❌ Missing SharePoint credentials")
            return False
        
        # Get access token
        credential = ClientSecretCredential(tenant_id, client_id, client_secret)
        token = credential.get_token("https://graph.microsoft.com/.default")
        
        # Microsoft Graph API headers
        headers = {
            "Authorization": f"Bearer {token.token}",
            "Content-Type": "application/json"
        }
        
        # Get the Excel file from SharePoint
        file_path = "General/CRM TaskForce/SCOOP to Excel/Excel DATA from SCOOP.xlsx"
        graph_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/root:/{file_path}:/content"
        
        print(f"Downloading file from: {file_path}")
        response = requests.get(graph_url, headers=headers)
        
        if response.status_code != 200:
            print(f"❌ Failed to download file: {response.status_code}")
            return False
        
        print(f"✅ File downloaded successfully ({len(response.content)} bytes)")
        
        # Save Excel file temporarily
        temp_excel = csv_dir / "test_sharepoint_temp.xlsx"
        with open(temp_excel, 'wb') as f:
            f.write(response.content)
        
        print(f"✅ Temporary Excel file saved: {temp_excel}")
        print(f"File exists: {temp_excel.exists()}")
        print(f"File size: {temp_excel.stat().st_size} bytes")
        
        # Convert Excel to CSV
        excel_data = pd.read_excel(temp_excel, sheet_name=None)
        print(f"✅ Excel file read successfully. Sheets: {list(excel_data.keys())}")
        
        # Save each sheet as separate CSV
        created_files = []
        for sheet_name, df in excel_data.items():
            csv_filename = f"test_sharepoint_{sheet_name.lower().replace(' ', '_')}.csv"
            csv_path = csv_dir / csv_filename
            df.to_csv(csv_path, index=False)
            created_files.append(csv_path)
            print(f"✅ Saved sheet '{sheet_name}' to {csv_filename}")
            print(f"  - File exists: {csv_path.exists()}")
            print(f"  - File size: {csv_path.stat().st_size} bytes")
            print(f"  - Rows: {len(df)}")
        
        # List all files in CSV directory
        print("\n📁 All files in CSV directory:")
        for file in csv_dir.glob("*"):
            print(f"  - {file.name} ({file.stat().st_size} bytes)")
        
        # Clean up temp file
        temp_excel.unlink()
        print(f"✅ Temporary file cleaned up")
        
        return True
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("🧪 Testing SharePoint file creation...")
    success = test_sharepoint_file_creation()
    print(f"\n{'✅ Test passed' if success else '❌ Test failed'}") 