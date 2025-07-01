# Power BI Automated Connection to Snowflake Setup Guide

## Overview
This guide sets up an automated connection between Power BI and Snowflake that supports scheduled refresh without requiring a gateway.

## Prerequisites
- ✅ Snowflake ACCOUNTADMIN access
- ✅ Power BI Desktop installed
- ✅ Power BI Service (Pro/Premium) access

## Step 1: Run the Automated Setup Script

```powershell
.\setup_powerbi_automated.ps1
```

This script will:
- Create a dedicated Power BI user (`powerbi_automated`)
- Create a dedicated role (`powerbi_automated_role`)
- Grant necessary permissions
- Set up default settings

## Step 2: Power BI Desktop Connection

### 2.1 Get Data from Snowflake
1. Open Power BI Desktop
2. Click **Get Data** > **Database** > **Snowflake**
3. Enter connection details:
   - **Server**: `VYFUVVV-LU67053.snowflakecomputing.com`
   - **Database**: `lws`
   - **Schema**: `public`
   - **Warehouse**: `lws_warehouse`
   - **Username**: `powerbi_automated`
   - **Password**: `PowerBI2024!Secure`
   - **Role**: `powerbi_automated_role`

### 2.2 Connection Options
- **Data Connectivity mode**: Choose **DirectQuery** for real-time data
- **Import**: For smaller datasets that can be refreshed
- **DirectQuery**: For large datasets and real-time access

### 2.3 Test Connection
Run this query to test the connection:
```sql
SELECT CURRENT_TIMESTAMP() as test_connection
```

## Step 3: Power BI Service Setup

### 3.1 Publish to Power BI Service
1. In Power BI Desktop, click **Publish**
2. Select your workspace
3. Click **Publish**

### 3.2 Configure Data Source Credentials
1. Go to **Settings** > **Data source credentials**
2. Click **Edit credentials**
3. Enter the same connection details as Desktop
4. Click **Sign in**

### 3.3 Set Up Scheduled Refresh
1. Go to **Settings** > **Scheduled refresh**
2. Enable **Keep your data up to date**
3. Configure refresh schedule:
   - **Frequency**: Daily
   - **Time**: 06:00 AM (or your preferred time)
   - **Time zone**: Pacific Standard Time
4. Click **Apply**

## Step 4: Advanced Configuration

### 4.1 Row-Level Security (Optional)
If you need row-level security, create RLS policies in Snowflake:

```sql
-- Example RLS policy
CREATE ROW ACCESS POLICY powerbi_rls_policy AS (user_role VARCHAR)
RETURNS BOOLEAN ->
  CASE 
    WHEN user_role = 'admin' THEN TRUE
    WHEN user_role = 'user' THEN user_role = CURRENT_USER()
    ELSE FALSE
  END;

-- Apply to tables
ALTER TABLE your_table ADD ROW ACCESS POLICY powerbi_rls_policy ON (user_role);
```

### 4.2 Performance Optimization
1. **Use DirectQuery** for large datasets
2. **Create materialized views** for complex queries
3. **Optimize warehouse size** based on query complexity
4. **Use query folding** when possible

## Step 5: Monitoring and Troubleshooting

### 5.1 Connection Monitoring
- Monitor connection status in Power BI Service
- Check Snowflake query history for performance
- Review error logs in both systems

### 5.2 Common Issues and Solutions

#### Issue: Connection Timeout
**Solution**: Increase warehouse size or optimize queries

#### Issue: Authentication Failed
**Solution**: Verify credentials and user permissions

#### Issue: Scheduled Refresh Fails
**Solution**: Check Power BI Service logs and Snowflake user status

### 5.3 Performance Monitoring Queries
```sql
-- Check Power BI user activity
SELECT 
    query_text,
    start_time,
    end_time,
    total_elapsed_time,
    bytes_scanned
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())
WHERE user_name = 'POWERBI_AUTOMATED'
ORDER BY start_time DESC;

-- Monitor warehouse usage
SELECT 
    warehouse_name,
    credits_used,
    bytes_scanned,
    percentage_scanned_from_cache
FROM TABLE(INFORMATION_SCHEMA.WAREHOUSE_METERING_HISTORY(
    DATE_RANGE_START=>DATEADD('day', -7, CURRENT_DATE()),
    DATE_RANGE_END=>CURRENT_DATE()
))
WHERE warehouse_name = 'LWS_WAREHOUSE';
```

## Security Considerations

### 6.1 User Permissions
- Power BI user has minimal required permissions
- Only SELECT access to data
- No ability to modify data or structure

### 6.2 Network Security
- All connections use TLS 1.2+ encryption
- No gateway required (cloud-to-cloud connection)
- IP restrictions can be applied if needed

### 6.3 Password Management
- Consider using Snowflake key pair authentication
- Rotate passwords regularly
- Monitor for suspicious activity

## Connection Details Summary

| Setting | Value |
|---------|-------|
| Server | VYFUVVV-LU67053.snowflakecomputing.com |
| Database | lws |
| Schema | public |
| Warehouse | lws_warehouse |
| Username | powerbi_automated |
| Password | PowerBI2024!Secure |
| Role | powerbi_automated_role |

## Next Steps

1. **Test the connection** in Power BI Desktop
2. **Publish your report** to Power BI Service
3. **Configure scheduled refresh**
4. **Monitor performance** and adjust as needed
5. **Set up alerts** for connection issues

## Support

For issues with:
- **Power BI**: Check Power BI Service status and logs
- **Snowflake**: Check query history and user permissions
- **Connection**: Verify network connectivity and credentials 