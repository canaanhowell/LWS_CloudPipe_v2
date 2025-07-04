# Configuration Guide for LWS CloudPipe v2

This guide explains how to configure the environment variables needed for the data pipeline to connect to all endpoints.

## 📋 Required Environment Variables

Create a `.env` file in the root directory with the following variables:

### SharePoint Configuration
```bash
SHAREPOINT_CLIENT_ID=your_sharepoint_client_id
SHAREPOINT_CLIENT_SECRET=your_sharepoint_client_secret
AZURE_TENANT_ID=your_azure_tenant_id
SHAREPOINT_SITE_ID=your_sharepoint_site_id
```

### Monday.com Configuration
```bash
MONDAY_API_KEY=your_monday_api_key
SEAL_RESI_BOARD_ID=your_resi_board_id
SEAL_COMM_SALES_BOARD_ID=your_comm_sales_board_id
SEAL_COMM_PM_BOARD_ID=your_comm_pm_board_id
```

### Google Analytics Configuration
```bash
GOOGLE_ANALYTICS_PROPERTY_ID=your_ga_property_id
```

### Snowflake Configuration
```bash
SNOWFLAKE_ACCOUNT=your_snowflake_account
SNOWFLAKE_USER=your_snowflake_user
SNOWFLAKE_WAREHOUSE=your_snowflake_warehouse
SNOWFLAKE_DATABASE=your_snowflake_database
```

### Azure Blob Storage Configuration
```bash
AZURE_STORAGE_CONNECTION_STRING=your_azure_storage_connection_string
BLOB_CONTAINER=pbi25
```

## 🔧 Setup Instructions

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Configure Environment Variables
- Copy the variables above into a `.env` file
- Replace the placeholder values with your actual credentials
- Ensure the `.env` file is in the root directory of the project

### 3. Verify Configuration Files
Ensure these files exist in the `config_files/` directory:
- `google_analytics_service_account.json` - Google Analytics service account key
- `snowflake_private_key.txt` - Snowflake private key (base64 DER format)

### 4. Run the Pipeline
```bash
python pipeline_scripts/data_pipeline.py
```

## 🔍 Troubleshooting

### Missing Dependencies
If you get import errors, install the missing packages:
```bash
pip install pandas requests openpyxl azure-storage-blob azure-identity snowflake-connector-python google-analytics-data google-auth python-dotenv
```

### Authentication Issues
- Verify all environment variables are set correctly
- Check that credential files exist and are readable
- Ensure API keys and tokens are valid and not expired

### File Permissions
- Ensure the script has read access to configuration files
- Verify write permissions for the `data/csv/` directory

## 📊 Expected Output

After successful execution, you should see CSV files in the `data/csv/` directory:
- `sharepoint_*.csv` - SharePoint Excel data (one file per sheet)
- `monday_data.csv` - Monday.com board data
- `google_analytics_data.csv` - Google Analytics report data
- `snowflake_*.csv` - Snowflake table data (one file per table)
- `azure_blob_*.csv` - Azure Blob Storage CSV files

## 🚨 Security Notes

- Never commit the `.env` file to version control
- Keep credential files secure and restrict access
- Use environment variables for sensitive data in production
- Regularly rotate API keys and tokens 