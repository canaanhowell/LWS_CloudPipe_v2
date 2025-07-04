# Connection Testing Guide

This guide documents the comprehensive connection testing framework created for the LWS CloudPipe v2 project.

## 🎯 Objective

Create connection test scripts for every endpoint described in `connections.md` to ensure all data pipeline connections are working correctly.

## 📋 Test Scripts Overview

### 1. **Master Test Runner**
- **File:** `helper_scripts/Tests/run_all_connection_tests.py`
- **Purpose:** Runs all individual connection tests and generates comprehensive reports
- **Output:** JSON results and markdown report in `logs/` directory

### 2. **Comprehensive Connection Tester**
- **File:** `helper_scripts/Tests/test_connections.py`
- **Purpose:** Tests all endpoints in a single script
- **Tests:** Environment variables, SharePoint, Monday.com, Google Analytics, Snowflake, Azure Blob Storage, Flask

### 3. **Individual Endpoint Testers**

#### SharePoint Connection Tester
- **File:** `helper_scripts/Tests/test_sharepoint_connection.py`
- **Tests:**
  - Environment variables (Azure credentials)
  - Azure token retrieval
  - Microsoft Graph API access
  - SharePoint site access

#### Monday.com Connection Tester
- **File:** `helper_scripts/Tests/test_monday_connection.py`
- **Tests:**
  - Environment variables (API key, board IDs)
  - API key format validation
  - API endpoint access
  - Board access verification
  - GraphQL query functionality

#### Snowflake Connection Tester
- **File:** `helper_scripts/Tests/test_snowflake_connection.py`
- **Tests:**
  - Environment variables (account, user, warehouse, database)
  - Private key file existence and format
  - Connection parameters validation
  - Snowflake connector import
  - Database connection (if connector available)
  - Database structure access

#### Flask Endpoints Tester
- **File:** `helper_scripts/Tests/test_flask_endpoints.py`
- **Tests:**
  - Flask application running status
  - Health check endpoint (`/health`)
  - Pipeline status endpoint (`/api/pipeline/status`)
  - Pipeline run endpoint (`/api/pipeline/run`)
  - CSV upload endpoint (`/api/upload/csv`)
  - Download endpoint (`/api/download/{filename}`)
  - Environment variables
  - Application file structure

## 🚀 Usage

### Run All Tests
```bash
python helper_scripts/Tests/run_all_connection_tests.py
```

### Run Individual Tests
```bash
# Comprehensive test
python helper_scripts/Tests/test_connections.py

# Individual endpoint tests
python helper_scripts/Tests/test_sharepoint_connection.py
python helper_scripts/Tests/test_monday_connection.py
python helper_scripts/Tests/test_snowflake_connection.py
python helper_scripts/Tests/test_flask_endpoints.py
```

## 📊 Test Results

### Output Files
- `logs/connection_tests.log` - Main test log
- `logs/connection_test_results.json` - JSON test results
- `logs/connection_test_report.md` - Human-readable report
- `logs/master_connection_test_results.json` - Master test results
- `logs/master_connection_tests.log` - Master test log

### Individual Test Logs
- `logs/sharepoint_connection_test.log`
- `logs/monday_connection_test.log`
- `logs/snowflake_connection_test.log`
- `logs/flask_endpoints_test.log`

## 🔧 Test Features

### Error Handling
- Comprehensive exception handling for all tests
- Graceful degradation when services are unavailable
- Detailed error messages and logging

### Logging Integration
- Uses the improved `logger.py` with structured logging
- JSON and human-readable log formats
- Progress tracking and status updates

### Environment Validation
- Checks for required environment variables
- Validates configuration file existence
- Tests credential formats and accessibility

### Connection Testing
- Tests actual API endpoints where possible
- Validates authentication mechanisms
- Checks service availability and response formats

## 📈 Test Coverage

### Environment Variables Tested
- `AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET`
- `MONDAY_API_KEY`, `SEAL_RESI_BOARD_ID`, `SEAL_COMM_SALES_BOARD_ID`, `SEAL_COMM_PM_BOARD_ID`
- `GOOGLE_ANALYTICS_PROPERTY_ID`
- `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_WAREHOUSE`, `SNOWFLAKE_DATABASE`
- `AZURE_STORAGE_CONNECTION_STRING`
- `FLASK_APP`, `FLASK_ENV`, `SECRET_KEY`

### Configuration Files Tested
- `config_files/snowflake_private_key.txt`
- `config_files/google_analytics_service_account.json`
- Various Docker and Flask configuration files

### API Endpoints Tested
- Microsoft Graph API (`https://graph.microsoft.com/v1.0/`)
- Monday.com API (`https://api.monday.com/v2`)
- Google Analytics Data API
- Snowflake database connection
- Azure Blob Storage connection
- Flask application endpoints

## 🛠️ Troubleshooting

### Common Issues

1. **Missing Environment Variables**
   - Check `.env` file exists and contains required variables
   - Verify variable names match exactly

2. **Authentication Failures**
   - Verify API keys and credentials are valid
   - Check token expiration for Azure services
   - Ensure proper permissions are granted

3. **File Not Found Errors**
   - Verify configuration files exist in correct locations
   - Check file permissions and accessibility

4. **Connection Timeouts**
   - Check network connectivity
   - Verify firewall settings
   - Test with minimal requests first

### Debugging Tips

1. **Check Individual Test Logs**
   - Each test creates its own log file with detailed information
   - Look for specific error messages and stack traces

2. **Run Tests Individually**
   - Use individual test scripts to isolate issues
   - Focus on one endpoint at a time

3. **Verify Environment Setup**
   - Ensure all required packages are installed
   - Check Python environment and dependencies

## 📝 Maintenance

### Adding New Tests
1. Create new test script in `helper_scripts/Tests/`
2. Follow the existing pattern with proper logging
3. Add to the master test runner if needed
4. Update this documentation

### Updating Existing Tests
1. Modify test logic as needed
2. Update error handling and logging
3. Test thoroughly before committing
4. Update documentation

## 🎉 Success Criteria

The connection testing framework is considered successful when:
- ✅ All test scripts run without errors
- ✅ All endpoints are accessible and responding
- ✅ Environment variables are properly configured
- ✅ Configuration files are present and valid
- ✅ Authentication mechanisms are working
- ✅ Detailed reports are generated for monitoring

---

**Last Updated:** 2025-07-01  
**Version:** 1.0  
**Status:** Complete ✅ 