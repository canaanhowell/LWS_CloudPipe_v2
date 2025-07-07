# Core Pipeline Scripts

This directory contains the main pipeline scripts that handle the primary data flow from source systems to Snowflake. Supporting utility scripts have been moved to `helper_scripts/pipeline_utils/` to keep this directory focused on core operations.

## Core Pipeline Scripts

### 1. `data_query.py` (25KB, 586 lines)
- **Main Purpose**: Main data pipeline that connects to all endpoints and downloads CSV data
- **Key Features**:
  - Downloads data from SharePoint, Monday.com, Google Analytics, Snowflake, and Azure Blob
  - Converts data to CSV format
  - Uploads to Azure Blob Storage
  - Handles multiple data sources in one pipeline
  - Supports real-time data extraction and transformation

### 2. `load_from_azure.py` (18KB, 425 lines)
- **Main Purpose**: Loads all cleaned CSV files from Azure Blob Storage to their corresponding Snowflake tables
- **Key Features**:
  - Uses `config_files/table_mapping.json` to determine which files to load
  - Handles column name cleaning for Snowflake compatibility
  - Creates tables if they don't exist
  - Verifies data loads with row counts
  - Supports automatic table creation and data validation

### 3. `verify_load_from_azure.py` (9.7KB, 233 lines)
- **Main Purpose**: Verifies that all CSV files were loaded correctly to their corresponding Snowflake tables
- **Key Features**:
  - Checks table existence
  - Verifies row counts match expected values
  - Provides detailed verification reports
  - Calculates success statistics
  - Generates comprehensive validation reports

### 4. `csv_cleaner.py` (6.9KB, 184 lines)
- **Main Purpose**: Cleans and standardizes CSV data before loading to Snowflake
- **Key Features**:
  - Removes problematic characters and formatting
  - Standardizes data types and formats
  - Handles missing values and data quality issues
  - Prepares data for optimal Snowflake loading

## Pipeline Workflow

### Standard Data Pipeline Flow

1. **Data Extraction** (`data_query.py`)
   - Connect to all source systems (SharePoint, Monday.com, Google Analytics, etc.)
   - Download and transform data to CSV format
   - Upload cleaned CSVs to Azure Blob Storage

2. **Data Loading** (`load_from_azure.py`)
   - Read table mapping configuration
   - Load CSV files from Azure to Snowflake
   - Create tables with appropriate schemas
   - Handle data type conversions

3. **Data Verification** (`verify_load_from_azure.py`)
   - Verify all tables were created successfully
   - Check row counts match expected values
   - Validate data integrity
   - Generate verification reports

4. **Data Cleaning** (`csv_cleaner.py`)
   - Clean and standardize data as needed
   - Handle data quality issues
   - Prepare data for analysis

### Advanced Workflow (with Utilities)

For more complex scenarios, you can incorporate utility scripts from `helper_scripts/pipeline_utils/`:

1. **Inspection**: Use `inspect_raw_csv.py` to understand data structure
2. **Type Mapping**: Run `create_column_type_mapping.py` to generate optimal schemas
3. **Table Management**: Use `truncate_all_tables.py` to clear existing data
4. **Table Recreation**: Run `recreate_tables_final.py` to create optimized tables
5. **Core Pipeline**: Execute the main pipeline scripts above
6. **Verification**: Use verification scripts to ensure success

## Configuration

All scripts use the following configuration files:
- `../config_files/settings.json` - Connection settings and credentials
- `../config_files/table_mapping.json` - Table mappings and metadata

## Dependencies

Core dependencies:
- Snowflake connector (`snowflake-connector-python`)
- Azure Blob Storage SDK (`azure-storage-blob`)
- Pandas (`pandas`)
- Custom logger from `../helper_scripts/Utils/logger.py`

## Logging

All scripts use the centralized logging system:
- Logs are written to `../logs/` directory
- Structured logging with JSON output
- Progress tracking and error reporting
- Detailed execution logs for debugging

## Error Handling

- Comprehensive error handling and reporting
- Graceful failure recovery
- Detailed error messages and stack traces
- Automatic retry mechanisms where appropriate

## Performance

- Optimized for large datasets
- Parallel processing where possible
- Memory-efficient data handling
- Progress tracking for long-running operations 