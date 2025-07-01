# Power BI to Snowflake Connection Guide

## Overview
This guide shows you how to create an automated connection from Power BI to Snowflake for real-time data visualization.

## Method 1: Direct Connection (Recommended)

### Step 1: Set up Snowflake for Power BI
```powershell
# Run the setup script
.\setup_powerbi_connection.ps1 -Setup

# Test the connection
.\setup_powerbi_connection.ps1 -Test

# Get connection information
.\setup_powerbi_connection.ps1 -ShowConnectionInfo
```

### Step 2: Connect from Power BI Desktop

1. **Open Power BI Desktop**
2. **Get Data** → **Database** → **Snowflake**
3. **Enter Connection Details:**
   - Server: `VYFUVVV-LU67053.snowflakecomputing.com`
   - Database: `lws`
   - Schema: `public`
   - Warehouse: `lws_warehouse`
   - Username: `powerbi_user`
   - Password: `[YourPassword]`

4. **Select Tables/Views:**
   - `monday_data`
   - `ga4_data`
   - `powerbi_dashboard_view` (recommended - combined view)

### Step 3: Configure Refresh Settings

1. **In Power BI Desktop:**
   - File → Options and settings → Data source settings
   - Select your Snowflake connection
   - Click "Edit Permissions"
   - Set refresh frequency (e.g., every 15 minutes)

2. **For Power BI Service:**
   - Publish to Power BI Service
   - Go to Workspace → Datasets → Settings
   - Configure scheduled refresh

## Method 2: Data Gateway (For On-Premises)

### Step 1: Install Data Gateway
1. Download from Microsoft
2. Install on your server/machine
3. Configure with your Microsoft account

### Step 2: Configure Gateway
1. Add Snowflake data source
2. Use same connection details as above
3. Test connection

## Method 3: Automated Data Pipeline

### Option A: Azure Data Factory
```yaml
# Sample ADF pipeline
pipeline:
  name: "Snowflake to Power BI"
  activities:
    - name: "Copy Data"
      type: "Copy"
      source:
        type: "SnowflakeSource"
        query: "SELECT * FROM powerbi_dashboard_view"
      sink:
        type: "AzureSqlSink"
        tableName: "powerbi_data"
```

### Option B: Azure Functions (Your Current Setup)
```python
# Add to your existing Azure Functions
import snowflake.connector
import pandas as pd

def update_powerbi_data():
    # Connect to Snowflake
    conn = snowflake.connector.connect(
        user='powerbi_user',
        password='YourPassword',
        account='VYFUVVV-LU67053',
        database='lws',
        schema='public',
        warehouse='lws_warehouse'
    )
    
    # Query data
    query = "SELECT * FROM powerbi_dashboard_view"
    df = pd.read_sql(query, conn)
    
    # Export to Power BI compatible format
    df.to_csv('powerbi_data.csv', index=False)
    
    # Upload to Azure Blob Storage for Power BI
    # (Your existing upload code)
```

## Connection Details

### Snowflake Configuration
- **Account:** `VYFUVVV-LU67053`
- **User:** `powerbi_user`
- **Database:** `lws`
- **Schema:** `public`
- **Warehouse:** `lws_warehouse`
- **Role:** `powerbi_role`

### Available Data Sources
1. **monday_data** - Monday.com project data
2. **ga4_data** - Google Analytics 4 data
3. **powerbi_dashboard_view** - Combined view for dashboards

## Best Practices

### 1. Performance Optimization
```sql
-- Create materialized views for better performance
CREATE OR REPLACE MATERIALIZED VIEW monday_summary AS
SELECT 
    status,
    COUNT(*) as count,
    DATE(created_date) as date
FROM monday_data
GROUP BY status, DATE(created_date);

-- Grant access to Power BI role
GRANT SELECT ON MATERIALIZED VIEW monday_summary TO ROLE powerbi_role;
```

### 2. Data Refresh Strategy
- **Real-time:** Direct connection (recommended)
- **Near real-time:** 15-minute refresh intervals
- **Daily:** Scheduled refresh at off-peak hours

### 3. Security
- Use dedicated Power BI user (already created)
- Read-only permissions
- Network restrictions if needed

## Troubleshooting

### Common Issues

1. **Connection Timeout**
   - Increase connection timeout in Power BI
   - Check warehouse size and auto-suspend settings

2. **Authentication Errors**
   - Verify username/password
   - Check user role permissions
   - Ensure warehouse is active

3. **Performance Issues**
   - Use materialized views
   - Optimize queries
   - Increase warehouse size temporarily

### Testing Connection
```powershell
# Test the connection
.\snowflake_simple.ps1 -Batch -Commands @(
    "SELECT CURRENT_USER();",
    "SELECT COUNT(*) FROM powerbi_dashboard_view;"
)
```

## Next Steps

1. **Run the setup script** to create Power BI user and permissions
2. **Test the connection** to ensure everything works
3. **Connect from Power BI Desktop** using the provided details
4. **Create your first dashboard** using the available data
5. **Configure refresh settings** for automated updates
6. **Publish to Power BI Service** for sharing and collaboration

## Files Created

- `powerbi_connection_setup.sql` - SQL setup script
- `setup_powerbi_connection.ps1` - PowerShell automation script
- `POWERBI_CONNECTION_GUIDE.md` - This guide

## Support

If you encounter issues:
1. Check the troubleshooting section
2. Verify connection details
3. Test with the provided scripts
4. Check Snowflake logs for errors 