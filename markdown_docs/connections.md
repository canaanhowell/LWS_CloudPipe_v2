# Data Source Connection Methods

This document outlines the connection methods, authentication, and configuration for each data source in the Azure to Snowflake pipeline.

## 📊 Data Sources Overview

| Data Source | Type | Connection Method | Authentication | Status |
|-------------|------|------------------|----------------|---------|
| **SharePoint Excel** | Excel Files | Microsoft Graph API | Azure AD App | ✅ Active |
| **Monday.com** | API | Monday.com API | API Key | ✅ Active |
| **Google Analytics** | API | Google Analytics 4 API | Service Account | ✅ Active |
| **Snowflake** | Database | Snowflake Connector | Key Pair Auth | ✅ Active |
| **Azure Blob Storage** | Cloud Storage | Azure Storage SDK | Connection String | ✅ Active |

---

## 🔗 SharePoint Excel Connection

### Connection Method
Microsoft Graph API

### Authentication
Azure AD App Registration

### Configuration Files
- `.env` - Azure credentials, SharePoint Registered App Credentials

### SharePoint Details
- **Site:** Sales Team LWS
- **URL:** `https://lightwavesolar.sharepoint.com/sites/SalesTeamLWS`
- **Folder:** `General/CRM TaskForce/SCOOP to Excel`
- **Target File:** `Excel DATA from SCOOP.xlsx`

### API Endpoints Used
- **Sites:** `https://graph.microsoft.com/v1.0/sites`
- **Drive Items:** `https://graph.microsoft.com/v1.0/sites/{site_id}/drive/root:/{path}:/children`
- **File Download:** `https://graph.microsoft.com/v1.0/sites/{site_id}/drive/items/{file_id}/content`

### Permissions Required
- `Sites.Read.All`
- `Files.Read.All`

### Environment Variables
```json
{
  "SHAREPOINT_SITE_ID": "lightwavesolar.sharepoint.com,9018e071-016a-4b74-93dc-cc82409015f3,3a46259c-0b21-44ad-a49e-cff3f4066bba",
  "SHAREPOINT_SCOOP_DRIVE_ID": "b!ceAYkGoBdEuT3MyCQJAV85wlRjohC61EpJ7P8_QGa7oMoVAAw-14T4fYELI8kVQK",
  "SHAREPOINT_SCOOP_FOLDER_ID": "01PMFOLQ2SMWN6MYMM5ZHLCLT2CUQ2JJHK",
  "SHAREPOINT_CLIENT_ID": "ab4b3724-b6a3-4b9b-ace2-861e6a99d478",
  "SHAREPOINT_CLIENT_SECRET": "[REDACTED]",
  "AZURE_TENANT_ID": "d1b8191a-f4ec-49f8-9bff-15c58f9316a2"
}
```

---

## 📅 Monday.com Connection

### Connection Method
Monday.com REST API

### Authentication
API Key

### Configuration File
- `.env` - Board IDs, API Key

### API Details
- **Base URL:** `https://api.monday.com/v2`
- **Authentication:** Bearer token in Authorization header
- **GraphQL Endpoint:** `/graphql`

### API Endpoints Used
- **Items Query:** GraphQL query for board items
- **Column Values:** GraphQL query for column data

### Environment Variables
```json
{
  "MONDAY_API_KEY": "[REDACTED]",
  "SEAL_RESI_BOARD_ID": "1354724086",
  "SEAL_COMM_SALES_BOARD_ID": "1570431705",
  "SEAL_COMM_PM_BOARD_ID": "4328210594"
}
```

---

## 📈 Google Analytics Connection

### Connection Method
Google Analytics 4 API

### Authentication
Service Account

### Configuration File
- `.env` - Service account key

### API Details
- **Service:** Google Analytics Data API v1
- **Authentication:** Service account with JSON key file
- **Scopes:** `https://www.googleapis.com/auth/analytics.readonly`

### API Endpoints Used
- **Run Report:** `https://analyticsdata.googleapis.com/v1beta/properties/{property_id}:runReport`

### Environment Variables
```json
{
  "GOOGLE_ANALYTICS_PROPERTY_ID": "298401772"
}
```

---

## ❄️ Snowflake Connection

### Connection Method
Snowflake Connector for Python

### Authentication
Key Pair Authentication (JWT)

### Configuration Files
- `snowflake_private_key.txt` - Private key (base64 DER format)
- `.env` - Connection parameters

### Connection Details
- **Authentication Method:** Key pair (private key)
- **Key Format:** Base64 DER encoded
- **Key Location:** `snowflake_private_key.txt`

### Databases
- **LWS Database:** `lws.public` - Lightwave Solar data
- **SEAL Database:** `seal.public` - SEAL data
- **Shared Database:** `shared_dimensions.public` - External stage, common date table

### Environment Variables
```json
{
  "SNOWFLAKE_ACCOUNT": "VYFUVVV-LU67053",
  "SNOWFLAKE_USER": "CHOWELL@LIGHTWAVESOLAR.COM",
  "SNOWFLAKE_WAREHOUSE": "compute_wh",
  "SNOWFLAKE_DATABASE": "LWS",
  "SNOWFLAKE_PRIVATE_KEY_PATH": "snowflake_private_key.txt"
}
```

---

## ☁️ Azure Blob Storage Connection

### Connection Method
Azure Storage SDK for Python

### Authentication
Connection String

### Configuration Files
- `.env` - Storage connection string

### Blob URLs
- **Base URL:** `https://pbi25.blob.core.windows.net/pbi25/`
- **Files:** CSV files uploaded with original names

### Environment Variables
```json
{
  "AZURE_STORAGE_CONNECTION_STRING": "[REDACTED]",
  "STORAGE_URL": "https://pbi25.blob.core.windows.net",
  "STORAGE_CONTAINER": "pbi25",
  "BLOB_CONTAINER": "pbi25"
}
```

---

## 🐳 Docker Containerization

### Container Types
- **Flask Web App** - REST API endpoints for data pipeline
- **Azure Functions** - Serverless data processing
- **Background Workers** - Scheduled data extraction and processing

### Docker Configuration Files
- `Dockerfile` - Main application container
- `Dockerfile.flask` - Flask web application
- `Dockerfile.simple` - Minimal container for testing
- `docker-compose.yml` - Local development environment
- `docker-compose.azure.yml` - Azure production environment

### Container Images
- **Base Image:** `python:3.11-slim`
- **Runtime:** Python 3.11 with Azure Functions runtime
- **Port:** 80 (HTTP), 443 (HTTPS)

### Environment Variables for Containers
```json
{
  "FUNCTIONS_WORKER_RUNTIME": "python",
  "AzureWebJobsStorage": "UseDevelopmentStorage=true",
  "WEBSITE_HOSTNAME": "azure-data-pipeline.azurewebsites.net",
  "WEBSITE_SITE_NAME": "azure-data-pipeline",
  "DOCKER_ENABLE_CI": "true"
}
```

---

## ☁️ Azure Container Services

### Azure Container Registry (ACR)
- **Registry Name:** `lightwavesolaracr`
- **Login Server:** `lightwavesolaracr.azurecr.io`
- **Authentication:** Service Principal

### Azure Container Instances (ACI)
- **Resource Group:** `lws-data-rg`
- **Container Name:** `azure-data-pipeline`
- **CPU:** 1 vCPU
- **Memory:** 2 GB
- **OS Type:** Linux

### Azure App Service (Web App)
- **Service Plan:** `lws-data-plan`
- **App Name:** `azure-data-pipeline`
- **Runtime Stack:** Python 3.11
- **Deployment Method:** Container Registry

### Azure Functions
- **Function App:** `lws-data-functions`
- **Runtime:** Python 3.11
- **Hosting Plan:** Consumption
- **Triggers:** Timer, HTTP, Blob Storage

### Environment Variables for Azure Services
```json
{
  "AZURE_TENANT_ID": "d1b8191a-f4ec-49f8-9bff-15c58f9316a2",
  "AZURE_CLIENT_ID": "ab4b3724-b6a3-4b9b-ace2-861e6a99d478",
  "AZURE_CLIENT_SECRET": "[REDACTED]",
  "AZURE_SUBSCRIPTION_ID": "[REDACTED]",
  "AZURE_RESOURCE_GROUP": "lws-data-rg",
  "AZURE_LOCATION": "East US"
}
```

---

## 🌐 Flask Web Application

### Application Structure
- **Main App:** Flask application entry point
- **API Routes:** REST endpoints for data pipeline operations
- **Background Tasks:** Celery workers for async processing
- **Health Checks:** `/health` endpoint for monitoring

### Flask Configuration
- **Framework:** Flask 2.3+
- **WSGI Server:** Gunicorn (production)
- **Development Server:** Flask built-in (development)
- **Port:** 5000 (development), 80 (production)

### API Endpoints
- **Health Check:** `GET /health`
- **Pipeline Status:** `GET /api/pipeline/status`
- **Run Pipeline:** `POST /api/pipeline/run`
- **Upload CSV:** `POST /api/upload/csv`
- **Download Results:** `GET /api/download/{filename}`

### Environment Variables for Flask
```json
{
  "FLASK_APP": "app.py",
  "FLASK_ENV": "production",
  "FLASK_DEBUG": "false",
  "SECRET_KEY": "[REDACTED]",
  "CELERY_BROKER_URL": "redis://localhost:6379/0",
  "CELERY_RESULT_BACKEND": "redis://localhost:6379/0"
}
```

---

## 🚀 Deployment Configuration

### Local Development
```bash
# Run with Docker Compose
docker-compose up -d

# Run Flask app directly
python app.py

# Run Azure Functions locally
func start
```

### Azure Deployment
```bash
# Deploy to Azure Container Registry
docker build -t lightwavesolaracr.azurecr.io/azure-data-pipeline:latest .
docker push lightwavesolaracr.azurecr.io/azure-data-pipeline:latest

# Deploy to Azure App Service
az webapp config container set --name azure-data-pipeline --resource-group lws-data-rg --docker-custom-image-name lightwavesolaracr.azurecr.io/azure-data-pipeline:latest
```

### Environment Setup for Cloud
- **Azure CLI:** Authenticated with service principal
- **Docker:** Logged into Azure Container Registry
- **Environment Variables:** Set in Azure App Service Configuration
- **Secrets:** Stored in Azure Key Vault

---

## 🚨 Troubleshooting

### Common Issues

#### 1. Authentication Failures
- Check environment variables in `.env`
- Verify credential files exist and are readable
- Test individual connections with test scripts

#### 2. Permission Errors
- Verify API permissions for Azure AD app
- Check Monday.com API key permissions
- Validate Snowflake user roles and privileges

#### 3. Connection Timeouts
- Check network connectivity
- Verify firewall settings
- Test with minimal requests first

#### 4. File Not Found
- Verify SharePoint file paths
- Check Azure blob container access
- Validate local file paths

#### 5. Container Issues
- Check Docker daemon is running
- Verify container registry authentication
- Check Azure service quotas and limits

#### 6. Flask Application Issues
- Verify all environment variables are set
- Check port availability
- Review application logs for errors

---

## 📋 Environment Setup Checklist

- [ ] Azure AD app registered with correct permissions
- [ ] Monday.com API key generated and configured
- [ ] Google Analytics service account created
- [ ] Snowflake key pair authentication set up
- [ ] Azure Storage connection string configured
- [ ] All environment variables set in `.env`
- [ ] Required credential files present
- [ ] Test connections successful for all data sources
- [ ] Docker installed and configured
- [ ] Azure CLI installed and authenticated
- [ ] Azure Container Registry created
- [ ] Flask application tested locally
- [ ] Azure App Service plan created
- [ ] Container deployment tested

---

<!-- Last updated: 2025-07-01 -->
<!-- This document is used by AI assistants to understand the data pipeline connections and cloud deployment -->