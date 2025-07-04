#!/usr/bin/env python3
"""
LWS CloudPipe v2 - Data Pipeline
Connects to all endpoints and downloads CSV data for each source
"""

import os
import sys
import json
import pandas as pd
import requests
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any
import logging
import io

# Add helper_scripts to path
sys.path.append(str(Path(__file__).parent.parent / "helper_scripts" / "Utils"))
from logger import pipeline_logger, log

# Add required imports for each data source
try:
    import snowflake.connector
    from snowflake.connector.pandas_tools import write_pandas
except ImportError:
    log("PIPELINE", "Snowflake connector not installed. Run: pip install snowflake-connector-python", "WARNING")

try:
    from azure.storage.blob import BlobServiceClient
    from azure.identity import ClientSecretCredential
except ImportError:
    log("PIPELINE", "Azure SDK not installed. Run: pip install azure-storage-blob azure-identity", "WARNING")

try:
    from google.analytics.data_v1beta import BetaAnalyticsDataClient
    from google.analytics.data_v1beta.types import RunReportRequest, DateRange, Metric, Dimension
    from google.oauth2 import service_account
    GOOGLE_ANALYTICS_AVAILABLE = True
except ImportError as e:
    log("PIPELINE", f"Google Analytics SDK import failed: {e}", "ERROR")
    GOOGLE_ANALYTICS_AVAILABLE = False

class DataPipeline:
    def __init__(self):
        """Initialize the data pipeline with configuration and logging."""
        self.base_dir = Path(__file__).parent.parent
        self.csv_dir = self.base_dir / "data" / "csv"
        self.config_dir = self.base_dir / "config_files"
        self.credentials = self.load_credentials()
        
        # Ensure CSV directory exists
        self.csv_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize results tracking
        self.results = {
            "start_time": datetime.now().isoformat(),
            "endpoints": {},
            "success_count": 0,
            "failure_count": 0,
            "total_endpoints": 5
        }
        
        log("PIPELINE", "Data pipeline initialized", "INFO")
        
        # Azure Blob Storage setup
        self.azure_blob_service_client = None
        self.azure_container_client = None
        self.azure_container_name = self.credentials.get("BLOB_CONTAINER", "pbi25")
        connection_string = self.credentials.get("AZURE_STORAGE_CONNECTION_STRING")
        if connection_string:
            try:
                self.azure_blob_service_client = BlobServiceClient.from_connection_string(connection_string)
                self.azure_container_client = self.azure_blob_service_client.get_container_client(self.azure_container_name)
            except Exception as e:
                log("PIPELINE", f"Failed to initialize Azure Blob Storage client: {e}", "ERROR")
        else:
            log("PIPELINE", "No Azure Storage connection string found in credentials", "WARNING")
    
    def load_credentials(self):
        cred_file = self.base_dir / "settings.json"
        if not cred_file.exists():
            log("PIPELINE", "Missing settings.json file!", "ERROR")
            return {}
        with open(cred_file, "r") as f:
            return json.load(f)
    
    def log_json(self, data: Dict[str, Any]):
        """Log data to JSON log file."""
        pipeline_logger.log_json("PIPELINE", data)
    
    def upload_df_to_azure(self, df: pd.DataFrame, blob_name: str) -> bool:
        """Upload a DataFrame as CSV to Azure Blob Storage."""
        if not self.azure_container_client:
            log("AZURE_BLOB", "Azure container client not initialized", "ERROR")
            return False
        try:
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False)
            csv_bytes = csv_buffer.getvalue().encode("utf-8")
            self.azure_container_client.upload_blob(
                name=blob_name,
                data=csv_bytes,
                overwrite=True
            )
            log("AZURE_BLOB", f"Uploaded {blob_name} to Azure container {self.azure_container_name}", "INFO")
            return True
        except Exception as e:
            log("AZURE_BLOB", f"Failed to upload {blob_name} to Azure: {e}", "ERROR")
            return False
    
    def download_sharepoint_data(self) -> bool:
        """Download SharePoint Excel data and convert to CSV."""
        try:
            log("SHAREPOINT", "Starting SharePoint data download", "INFO")
            
            # Get SharePoint credentials from environment
            client_id = self.credentials.get("SHAREPOINT_AZURE_CLIENT_ID")
            client_secret = self.credentials.get("SHAREPOINT_AZURE_CLIENT_SECRET")
            tenant_id = self.credentials.get("AZURE_TENANT_ID")
            site_id = self.credentials.get("SHAREPOINT_SITE_ID")
            
            if not all([client_id, client_secret, tenant_id, site_id]):
                log("SHAREPOINT", "Missing SharePoint credentials in environment", "ERROR")
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
            
            log("SHAREPOINT", f"Downloading file from: {file_path}", "INFO")
            response = requests.get(graph_url, headers=headers)
            
            if response.status_code != 200:
                log("SHAREPOINT", f"Failed to download file: {response.status_code}", "ERROR")
                return False
            
            # Save Excel file temporarily
            temp_excel = self.csv_dir / "sharepoint_data_temp.xlsx"
            with open(temp_excel, 'wb') as f:
                f.write(response.content)
            
            # Convert Excel to CSV
            excel_data = pd.read_excel(temp_excel, sheet_name=None)
            
            # Save each sheet as separate CSV with new naming convention
            sheet_mapping = {
                "Service": "lws.public.service_raw.csv",
                "Projects": "lws.public.projects_raw.csv", 
                "Sungrow": "lws.public.sharepoint_sungrow_raw.csv"
            }
            
            for sheet_name, df in excel_data.items():
                csv_filename = sheet_mapping.get(sheet_name, f"sharepoint_{sheet_name.lower().replace(' ', '_')}.csv")
                csv_path = self.csv_dir / csv_filename
                df.to_csv(csv_path, index=False)
                self.upload_df_to_azure(df, csv_filename)
                log("SHAREPOINT", f"Saved sheet '{sheet_name}' to {csv_filename}", "INFO")
            
            # Clean up temp file
            temp_excel.unlink()
            
            log("SHAREPOINT", "SharePoint data download completed successfully", "INFO")
            return True
            
        except Exception as e:
            log("SHAREPOINT", f"Error downloading SharePoint data: {str(e)}", "ERROR")
            return False
    
    def download_monday_data(self) -> bool:
        """Download Monday.com data and convert to CSV."""
        try:
            log("MONDAY", "Starting Monday.com data download", "INFO")
            
            api_key = self.credentials.get("MONDAY_API_KEY")
            if not api_key:
                log("MONDAY", "Missing Monday.com API key", "ERROR")
                return False
            
            headers = {
                "Authorization": api_key,
                "Content-Type": "application/json"
            }
            
            # Get board IDs from environment
            board_ids = [
                self.credentials.get("SEAL_RESI_BOARD_ID"),
                self.credentials.get("SEAL_COMM_SALES_BOARD_ID"),
                self.credentials.get("SEAL_COMM_PM_BOARD_ID")
            ]
            
            # New query structure with pagination
            query = """
            query ($id: [ID!], $cursor: String) {
                boards(ids: $id) {
                    name
                    items_page(limit: 100, cursor: $cursor) {
                        cursor
                        items {
                            id
                            name
                            column_values {
                                id
                                text
                                value
                                column {
                                    title
                                }
                            }
                        }
                    }
                }
            }
            """
            
            board_names = {
                "1354724086": "SEAL_RESI",
                "1570431705": "SEAL_COMM_SALES", 
                "4328210594": "SEAL_COMM_PM"
            }
            
            # File naming mapping for Monday.com boards
            file_mapping = {
                "SEAL_RESI": "seal.public.seal_resi_raw.csv",
                "SEAL_COMM_SALES": "seal.public.seal_comm_sales_raw.csv",
                "SEAL_COMM_PM": "seal.public.seal_comm_pm_raw.csv"
            }
            
            total_items = 0
            boards_processed = 0
            
            for board_id in board_ids:
                if not board_id:
                    continue
                    
                board_name = board_names.get(board_id, f"BOARD_{board_id}")
                log("MONDAY", f"Downloading data from board: {board_id} ({board_name})", "INFO")
                
                board_items = []
                cursor = None
                
                while True:
                    variables = {
                        "id": [board_id],
                        "cursor": cursor
                    }
                    response = requests.post(
                        "https://api.monday.com/v2",
                        json={"query": query, "variables": variables},
                        headers=headers
                    )
                    if response.status_code != 200:
                        log("MONDAY", f"Failed to get board {board_id}: {response.status_code}", "ERROR")
                        break
                    data = response.json()
                    boards = data.get("data", {}).get("boards", [])
                    if not boards:
                        log("MONDAY", f"No boards returned for board_id {board_id}", "WARNING")
                        break
                    board = boards[0]
                    items_page = board.get("items_page", {})
                    items = items_page.get("items", [])
                    for item in items:
                        item_data = {
                            "board_id": board_id,
                            "item_id": item["id"],
                            "item_name": item["name"]
                        }
                        # Extract column values
                        for col in item.get("column_values", []):
                            col_title = col.get("column", {}).get("title", "").lower().replace(" ", "_")
                            item_data[col_title] = col.get("text", col.get("value", ""))
                        board_items.append(item_data)
                    cursor = items_page.get("cursor")
                    if not cursor:
                        break
                
                # Save individual CSV for this board with new naming convention
                if board_items:
                    df = pd.DataFrame(board_items)
                    csv_filename = file_mapping.get(board_name, f"monday_{board_name.lower()}.csv")
                    csv_path = self.csv_dir / csv_filename
                    df.to_csv(csv_path, index=False)
                    self.upload_df_to_azure(df, csv_filename)
                    log("MONDAY", f"Saved {len(board_items)} items to {csv_filename}", "INFO")
                    total_items += len(board_items)
                    boards_processed += 1
                else:
                    log("MONDAY", f"No items found for board {board_id}", "WARNING")
            
            if total_items > 0:
                log("MONDAY", f"Successfully processed {boards_processed} boards with {total_items} total items", "INFO")
                return True
            else:
                log("MONDAY", "No data retrieved from any Monday.com boards", "WARNING")
                return False
        except Exception as e:
            log("MONDAY", f"Error downloading Monday.com data: {str(e)}", "ERROR")
            return False
    
    def download_google_analytics_data(self) -> bool:
        """Download Google Analytics data and convert to CSV."""
        try:
            log("GOOGLE_ANALYTICS", "Starting Google Analytics data download", "INFO")
            
            if not GOOGLE_ANALYTICS_AVAILABLE:
                log("GOOGLE_ANALYTICS", "Google Analytics SDK not available", "ERROR")
                return False
            
            property_id = self.credentials.get("GOOGLE_ANALYTICS_PROPERTY_ID")
            service_account_path = self.config_dir / "google_analytics_service_account.json"
            
            if not property_id or not service_account_path.exists():
                log("GOOGLE_ANALYTICS", "Missing Google Analytics configuration", "ERROR")
                return False
            
            # Initialize Google Analytics client
            credentials = service_account.Credentials.from_service_account_file(
                str(service_account_path),
                scopes=["https://www.googleapis.com/auth/analytics.readonly"]
            )
            
            client = BetaAnalyticsDataClient(credentials=credentials)
            
            # Define date range (last 12 months for comprehensive data)
            end_date = datetime.now().date()
            start_date = end_date - timedelta(days=365)
            
            # Create report request
            request = RunReportRequest(
                property=f"properties/{property_id}",
                date_ranges=[DateRange(start_date=start_date.isoformat(), end_date=end_date.isoformat())],
                metrics=[
                    Metric(name="totalUsers"),
                    Metric(name="sessions"),
                    Metric(name="newUsers"),
                    Metric(name="averageSessionDuration"),
                    Metric(name="bounceRate"),
                    Metric(name="engagementRate"),
                    Metric(name="conversions"),
                    Metric(name="userEngagementDuration"),
                    Metric(name="eventCount"),
                    Metric(name="screenPageViews")
                ],
                dimensions=[
                    Dimension(name="date"),
                    Dimension(name="pageTitle"),
                    Dimension(name="pagePath")
                ]
            )
            
            log("GOOGLE_ANALYTICS", "Running analytics report", "INFO")
            response = client.run_report(request)
            
            # Convert response to DataFrame
            data = []
            for row in response.rows:
                row_data = {}
                for i, dimension in enumerate(row.dimension_values):
                    row_data[f"dimension_{i+1}"] = dimension.value
                for i, metric in enumerate(row.metric_values):
                    row_data[f"metric_{i+1}"] = metric.value
                data.append(row_data)
            
            if data:
                df = pd.DataFrame(data)
                csv_path = self.csv_dir / "lws.public.google_analytics_raw.csv"
                df.to_csv(csv_path, index=False)
                self.upload_df_to_azure(df, "lws.public.google_analytics_raw.csv")
                log("GOOGLE_ANALYTICS", f"Saved {len(data)} rows to lws.public.google_analytics_raw.csv", "INFO")
                return True
            else:
                log("GOOGLE_ANALYTICS", "No data retrieved from Google Analytics", "WARNING")
                return False
                
        except Exception as e:
            log("GOOGLE_ANALYTICS", f"Error downloading Google Analytics data: {str(e)}", "ERROR")
            return False
    
    def download_snowflake_data(self) -> bool:
        """Download Snowflake data and convert to CSV."""
        try:
            log("SNOWFLAKE", "Starting Snowflake data download", "INFO")
            
            # Get Snowflake credentials
            account = self.credentials.get("SNOWFLAKE_ACCOUNT")
            user = self.credentials.get("SNOWFLAKE_USER")
            warehouse = self.credentials.get("SNOWFLAKE_WAREHOUSE")
            database = self.credentials.get("SNOWFLAKE_DATABASE")
            private_key_path = self.config_dir / "snowflake_private_key.txt"
            
            if not all([account, user, warehouse, database]) or not private_key_path.exists():
                log("SNOWFLAKE", "Missing Snowflake configuration", "ERROR")
                return False
            
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
            
            # Get list of tables
            cursor = conn.cursor()
            cursor.execute("SHOW TABLES")
            tables = cursor.fetchall()
            
            log("SNOWFLAKE", f"Found {len(tables)} tables in database", "INFO")
            
            # Download data from each table
            for table_info in tables:
                table_name = table_info[1]  # Table name is in second column
                schema_name = table_info[2]  # Schema name is in third column
                
                try:
                    # Query table data - use just the table name since we're already in the LWS database
                    query = f"SELECT * FROM {table_name} LIMIT 10000"
                    df = pd.read_sql(query, conn)
                    
                    if not df.empty:
                        # Upload to Azure only (no local file saving)
                        csv_filename = f"snowflake_{schema_name}_{table_name}.csv"
                        self.upload_df_to_azure(df, csv_filename)
                        log("SNOWFLAKE", f"Saved {len(df)} rows from {schema_name}.{table_name}", "INFO")
                    
                except Exception as e:
                    log("SNOWFLAKE", f"Error querying table {schema_name}.{table_name}: {str(e)}", "WARNING")
                    continue
            
            conn.close()
            log("SNOWFLAKE", "Snowflake data download completed", "INFO")
            return True
            
        except Exception as e:
            log("SNOWFLAKE", f"Error downloading Snowflake data: {str(e)}", "ERROR")
            return False
    
    def download_azure_blob_data(self) -> bool:
        """Download Azure Blob Storage data and convert to CSV."""
        try:
            log("AZURE_BLOB", "Starting Azure Blob Storage data download", "INFO")
            
            connection_string = self.credentials.get("AZURE_STORAGE_CONNECTION_STRING")
            container_name = self.credentials.get("BLOB_CONTAINER", "pbi25")
            
            if not connection_string:
                log("AZURE_BLOB", "Missing Azure Storage connection string", "ERROR")
                return False
            
            # Create blob service client
            blob_service_client = BlobServiceClient.from_connection_string(connection_string)
            container_client = blob_service_client.get_container_client(container_name)
            
            # List all blobs in container
            blobs = container_client.list_blobs()
            csv_files = [blob for blob in blobs if blob.name.endswith('.csv')]
            
            log("AZURE_BLOB", f"Found {len(csv_files)} CSV files in container", "INFO")
            
            # List CSV files in container (no local downloading)
            for blob in csv_files:
                try:
                    log("AZURE_BLOB", f"Found CSV file: {blob.name}", "INFO")
                    
                except Exception as e:
                    log("AZURE_BLOB", f"Error processing blob {blob.name}: {str(e)}", "WARNING")
                    continue
            
            log("AZURE_BLOB", "Azure Blob Storage data download completed", "INFO")
            return True
            
        except Exception as e:
            log("AZURE_BLOB", f"Error downloading Azure Blob data: {str(e)}", "ERROR")
            return False
    
    def run_pipeline(self) -> Dict[str, Any]:
        """Run the complete data pipeline."""
        log("PIPELINE", "Starting data pipeline execution", "INFO")
        
        # Track progress
        endpoints = [
            ("SharePoint", self.download_sharepoint_data),
            ("Monday.com", self.download_monday_data),
            ("Google Analytics", self.download_google_analytics_data),
            ("Snowflake", self.download_snowflake_data),
            ("Azure Blob Storage", self.download_azure_blob_data)
        ]
        
        for i, (endpoint_name, download_func) in enumerate(endpoints, 1):
            log("PIPELINE", f"Processing endpoint {i}/{len(endpoints)}: {endpoint_name}", "INFO")
            
            try:
                success = download_func()
                self.results["endpoints"][endpoint_name] = {
                    "status": "success" if success else "failed",
                    "timestamp": datetime.now().isoformat()
                }
                
                if success:
                    self.results["success_count"] += 1
                    log("PIPELINE", f"{endpoint_name} completed successfully", "INFO")
                else:
                    self.results["failure_count"] += 1
                    log("PIPELINE", f"{endpoint_name} failed", "ERROR")
                    
            except Exception as e:
                self.results["endpoints"][endpoint_name] = {
                    "status": "error",
                    "error": str(e),
                    "timestamp": datetime.now().isoformat()
                }
                self.results["failure_count"] += 1
                log("PIPELINE", f"{endpoint_name} error: {str(e)}", "ERROR")
        
        # Finalize results
        self.results["end_time"] = datetime.now().isoformat()
        self.results["completion_percentage"] = (self.results["success_count"] / self.results["total_endpoints"]) * 100
        
        # Log final results
        self.log_json(self.results)
        
        # Log summary
        log("PIPELINE", f"Pipeline completed: {self.results['success_count']}/{self.results['total_endpoints']} endpoints successful", "INFO")
        log("PIPELINE", f"Completion percentage: {self.results['completion_percentage']:.1f}%", "INFO")
        
        return self.results

def main():
    """Main entry point for the data pipeline."""
    try:
        pipeline = DataPipeline()
        results = pipeline.run_pipeline()
        
        # Print summary
        print("\n" + "="*50)
        print("DATA PIPELINE EXECUTION SUMMARY")
        print("="*50)
        print(f"Start Time: {results['start_time']}")
        print(f"End Time: {results['end_time']}")
        print(f"Successful Endpoints: {results['success_count']}/{results['total_endpoints']}")
        print(f"Completion: {results['completion_percentage']:.1f}%")
        print("\nEndpoint Results:")
        
        for endpoint, details in results['endpoints'].items():
            status_symbol = "SUCCESS" if details['status'] == 'success' else "FAILED"
            print(f"  {endpoint}: {details['status']} ({status_symbol})")
        
        print("="*50)
        
        # Check if CSV files were created
        csv_files = list(pipeline.csv_dir.glob("*.csv"))
        print(f"\nCSV Files Created: {len(csv_files)}")
        for csv_file in csv_files:
            print(f"  {csv_file.name}")
        
        return 0 if results['success_count'] > 0 else 1
        
    except Exception as e:
        log("PIPELINE", f"Critical pipeline error: {str(e)}", "CRITICAL")
        return 1

if __name__ == "__main__":
    import sys
    pipeline = DataPipeline()
    if len(sys.argv) > 1 and sys.argv[1] == "--google-analytics":
        result = pipeline.download_google_analytics_data()
        print(f"Google Analytics extraction {'succeeded' if result else 'failed'}.")
        sys.exit(0 if result else 1)
    else:
        sys.exit(main()) 