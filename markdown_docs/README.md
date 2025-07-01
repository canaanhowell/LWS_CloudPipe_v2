# Monday.com to Snowflake Data Pipeline

A comprehensive data pipeline that automates the collection, processing, and storage of data from multiple sources into Snowflake for analytics and reporting.

## 🚀 Overview

This project implements a complete ETL (Extract, Transform, Load) pipeline that:

- **Extracts** data from Monday.com boards (Residential, Commercial Sales, Commercial PM)
- **Retrieves** Google Analytics GA4 data with comprehensive metrics
- **Processes** Excel data from SCOOP
- **Transforms** and cleans data using Python-based CSV processing
- **Loads** all data into Snowflake for centralized analytics
- **Connects** to Power BI for visualization

## 📁 Project Structure

```
Azure/
├── main/                          # Core pipeline scripts
│   ├── seal_resi.py              # Monday.com Residential data
│   ├── seal_comm_sales.py        # Monday.com Commercial Sales data
│   ├── seal_comm_pm.py           # Monday.com Commercial PM data
│   ├── merge_seal_comm.py        # Merge Commercial datasets
│   ├── ga4_analytics.py          # Google Analytics GA4 data
│   ├── lws_excel_enhanced.py     # Excel data processing
│   └── csv_cleaner.py            # CSV cleaning and schema enforcement
├── sql_scripts/                   # Snowflake database scripts
├── powershell_scripts/           # PowerShell automation scripts
├── python_scripts/               # Additional Python utilities
├── config_files/                 # Configuration files
├── csv_files/                    # Data exports (gitignored)
├── excel_files/                  # Excel data (gitignored)
├── markdown_docs/                # Documentation
├── requirements.txt              # Python dependencies
└── test_full_pipeline.py         # End-to-end pipeline testing
```

## 🛠️ Prerequisites

- Python 3.10+
- Snowflake CLI with SAML authentication
- Azure CLI for Azure Storage access
- Azure Blob Storage account
- Monday.com API access
- Google Analytics GA4 property
- Power BI (for visualization)

## 🔧 Setup Instructions

### 1. Environment Setup

```bash
# Create virtual environment
python -m venv venv
venv\Scripts\activate  # Windows
source venv/bin/activate  # Linux/Mac

# Install dependencies
pip install -r requirements.txt
```

### 2. Configuration

1. **Environment Variables**: Create `local.settings.json` with:
   - Monday.com API token
   - Azure Storage connection string
   - Google Analytics credentials
   - Snowflake connection details

2. **Google Analytics Setup**: 
   - Download `client_secrets.json` from Google Cloud Console
   - Set up OAuth 2.0 credentials
   - Configure GA4 property ID

3. **Snowflake Setup**:
   - Configure Snowflake CLI with SSO authentication
   - Create database, warehouse, and tables
   - Set up Azure Blob Storage integration

### 3. Database Setup

Run the SQL scripts in `sql_scripts/` to create:
- Monday.com data tables
- Google Analytics GA4 table
- Excel data tables
- Views and permissions

## 🚀 Usage

### Individual Scripts

```bash
# Monday.com data extraction
python main/seal_resi.py
python main/seal_comm_sales.py
python main/seal_comm_pm.py

# Google Analytics data
python main/ga4_analytics.py

# Excel data processing
python main/lws_excel_enhanced.py

# Data merging
python main/merge_seal_comm.py
```

### Full Pipeline Test

```bash
# Test entire pipeline
python test_full_pipeline.py
```

### PowerShell Automation

```powershell
# Run PowerShell scripts for setup and automation
.\powershell_scripts\setup_azure_blob_integration.ps1
.\powershell_scripts\upload_to_azure.ps1
```

## 📊 Data Flow

1. **Extract**: API calls to Monday.com and Google Analytics, Excel file processing
2. **Transform**: Data cleaning, merging, and CSV processing using Python
3. **Clean**: CSV cleaning and schema enforcement using `csv_cleaner.py`
4. **Load**: Upload cleaned CSVs to Azure Blob Storage
5. **Ingest**: Load into Snowflake tables using static schemas
6. **Analyze**: Connect to Power BI for visualization

## 🔧 Architecture Decisions

### Python-Based Data Processing
- **CSV Cleaner**: All data cleaning and schema enforcement is handled by `main/csv_cleaner.py`
- **Schema Management**: Table structures are managed via static SQL scripts, not dynamic DDL
- **Data Validation**: Primary key logic and duplicate removal happen in Python before Snowflake ingestion

### Why Not Snowflake-Only Automation?
After extensive testing, we abandoned the approach of using Snowflake stored procedures for dynamic table sync because:
- **Reliability Issues**: Dynamic DDL operations in Snowflake were prone to errors
- **Maintenance Complexity**: Stored procedures for schema management were difficult to debug and maintain
- **Column Order Problems**: Snowflake's handling of column order and type inference was inconsistent
- **Performance**: Python processing is more efficient for data cleaning and validation

### Current Approach Benefits
- **Reliability**: Predictable, testable data processing pipeline
- **Maintainability**: Clear separation of concerns between data cleaning and database operations
- **Auditability**: All data transformations are logged and can be reviewed
- **Flexibility**: Easy to add new data sources or modify cleaning logic

## 🔐 Security

- Sensitive files are excluded via `.gitignore`
- Environment variables store credentials in `local.settings.json`
- OAuth 2.0 authentication for APIs
- SAML authentication for Snowflake
- Azure CLI for Azure Storage access

## 📈 Monitoring

- Pipeline test results in `test_full_pipeline.py`
- Data validation queries in `sql_scripts/`
- Log files for debugging and monitoring
- CSV cleaner logs for data quality issues

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test the pipeline
5. Submit a pull request

## 📝 License

This project is proprietary and confidential.

## 🆘 Support

For issues and questions:
1. Check the documentation in `markdown_docs/`
2. Review the test results
3. Check log files for errors
4. Contact the development team

---

**Last Updated**: January 2025  
**Pipeline Status**: ✅ Fully Operational  
**Data Sources**: Monday.com, Google Analytics GA4, Excel  
**Destination**: Snowflake + Power BI  
**Architecture**: Python-based ETL with static Snowflake schemas 