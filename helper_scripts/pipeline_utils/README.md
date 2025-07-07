# Pipeline Utilities

This directory contains utility scripts that support the main pipeline operations. These scripts were moved from the main `pipeline_scripts` directory to keep the core pipeline focused on the primary data flow.

## Scripts Overview

### Table Management Scripts

#### `create_column_type_mapping.py`
- **Purpose**: Analyzes CSV files and creates intelligent column type mappings for Snowflake
- **Key Features**:
  - Analyzes column names, keywords, and data patterns
  - Maps columns to appropriate Snowflake data types (VARCHAR, NUMBER, DATE, etc.)
  - Handles 488+ columns across multiple tables
  - Generates optimized schema definitions

#### `recreate_tables_final.py`
- **Purpose**: Recreates Snowflake tables with maximum VARCHAR lengths to handle all data
- **Key Features**:
  - Drops existing tables
  - Recreates tables with maximum VARCHAR(16,777,216) for string columns
  - Handles all data edge cases
  - Reloads data from Azure CSV files
  - Verifies data integrity

#### `recreate_tables_with_correct_types.py`
- **Purpose**: Recreates tables with proper column types based on the mapping
- **Key Features**:
  - Uses column type mapping to create optimized schemas
  - Handles data type conversions
  - Reloads data with proper types

#### `recreate_tables_with_fixed_types.py`
- **Purpose**: Alternative table recreation with fixed type handling
- **Key Features**:
  - Handles specific data type issues
  - Provides fallback type assignments

#### `truncate_all_tables.py`
- **Purpose**: Truncates all tables that were loaded from Azure to prepare for reloading
- **Key Features**:
  - Safely removes all data from tables
  - Tracks truncation results
  - Prepares tables for fresh data loads

### Data Inspection Scripts

#### `inspect_raw_csv.py`
- **Purpose**: Inspects raw CSV files for data quality and structure
- **Key Features**:
  - Analyzes column names and data types
  - Checks for null values and empty strings
  - Identifies problematic column names
  - Generates inspection reports

### Legacy Sungrow Scripts

These scripts are kept for reference but are no longer the primary data loading method:

- `load_sungrow_from_azure.py` - Original Sungrow-specific loader
- `load_sungrow_from_azure_v1.py` - Version 1 of Sungrow loader
- `load_sungrow_clean.py` - Clean version of Sungrow loader
- `load_sungrow_simple.py` - Simplified Sungrow loader
- `load_sungrow_data.py` - General Sungrow data loader

### Schema Management

#### `schema_sync_pipeline.py`
- **Purpose**: Synchronizes database schemas
- **Key Features**:
  - Compares source and target schemas
  - Generates migration scripts
  - Handles schema evolution

## Usage

These utility scripts can be run independently or as part of the main pipeline workflow. They are designed to support the core pipeline operations in `pipeline_scripts/`.

### Typical Workflow

1. **Inspection**: Use `inspect_raw_csv.py` to understand data structure
2. **Type Mapping**: Run `create_column_type_mapping.py` to generate optimal schemas
3. **Table Management**: Use `truncate_all_tables.py` to clear existing data
4. **Recreation**: Run `recreate_tables_final.py` to create optimized tables
5. **Verification**: Use the main pipeline verification scripts

## Dependencies

All scripts require:
- Snowflake connector
- Azure Blob Storage SDK
- Pandas
- Custom logger from `../Utils/logger.py`

## Configuration

Scripts use the same configuration files as the main pipeline:
- `../../config_files/settings.json` - Connection settings
- `../../config_files/table_mapping.json` - Table mappings 