# Snowflake COPY INTO Reference Guide

This document provides comprehensive SQL commands and examples for loading data from Azure Blob Storage to Snowflake using COPY INTO commands.

## 📋 Table of Contents

1. [File Format Definitions](#file-format-definitions)
2. [Basic COPY INTO Commands](#basic-copy-into-commands)
3. [Pattern Matching](#pattern-matching)
4. [Error Handling and Validation](#error-handling-and-validation)
5. [Performance Optimization](#performance-optimization)
6. [Monitoring and History](#monitoring-and-history)
7. [Complete Examples](#complete-examples)

---

## 📄 File Format Definitions

### Standard CSV Format
```sql
CREATE OR REPLACE FILE FORMAT SHARED_DIMENSIONS.PUBLIC.CSV_STANDARD
TYPE = CSV
FIELD_DELIMITER = ','
RECORD_DELIMITER = '\n'
SKIP_HEADER = 1
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
TRIM_SPACE = TRUE
ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
EMPTY_FIELD_AS_NULL = TRUE
NULL_IF = ('NULL', 'null', '')
```

### GZIP Compressed CSV Format
```sql
CREATE OR REPLACE FILE FORMAT SHARED_DIMENSIONS.PUBLIC.CSV_GZIP
TYPE = CSV
FIELD_DELIMITER = ','
RECORD_DELIMITER = '\n'
SKIP_HEADER = 1
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
TRIM_SPACE = TRUE
ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
EMPTY_FIELD_AS_NULL = TRUE
NULL_IF = ('NULL', 'null', '')
COMPRESSION = GZIP
```

### JSON Format
```sql
CREATE OR REPLACE FILE FORMAT SHARED_DIMENSIONS.PUBLIC.JSON_STANDARD
TYPE = JSON
STRIP_OUTER_ARRAY = TRUE
STRIP_NULL_VALUES = TRUE
IGNORE_UTF8_ERRORS = FALSE
```

---

## 🔄 Basic COPY INTO Commands

### Single File Load
```sql
-- Basic COPY INTO for single CSV file
COPY INTO LWS.PUBLIC.PROJECTS
FROM @SHARED_DIMENSIONS.PUBLIC.LWS_PUBLIC_PROJECTS_STAGE
FILE_FORMAT = SHARED_DIMENSIONS.PUBLIC.CSV_STANDARD
ON_ERROR = CONTINUE
VALIDATION_MODE = RETURN_ERRORS
FORCE = TRUE;
```

### Multiple File Load with Pattern
```sql
-- Load multiple files matching pattern
COPY INTO LWS.PUBLIC.PROJECTS
FROM @SHARED_DIMENSIONS.PUBLIC.LWS_PUBLIC_PROJECTS_STAGE
PATTERN = '.*\.csv'
FILE_FORMAT = SHARED_DIMENSIONS.PUBLIC.CSV_STANDARD
ON_ERROR = CONTINUE
VALIDATION_MODE = RETURN_ERRORS
FORCE = TRUE;
```

### Compressed File Load
```sql
-- Load GZIP compressed CSV
COPY INTO LWS.PUBLIC.PROJECTS
FROM @SHARED_DIMENSIONS.PUBLIC.LWS_PUBLIC_PROJECTS_STAGE
FILE_FORMAT = SHARED_DIMENSIONS.PUBLIC.CSV_GZIP
ON_ERROR = CONTINUE
VALIDATION_MODE = RETURN_ERRORS
FORCE = TRUE;
```

---

## 🎯 Pattern Matching

### Date-Based Pattern Matching
```sql
-- Load files with date pattern (YYYY-MM-DD)
COPY INTO LWS.PUBLIC.PROJECTS
FROM @SHARED_DIMENSIONS.PUBLIC.LWS_PUBLIC_PROJECTS_STAGE
PATTERN = '.*[0-9]{4}-[0-9]{2}-[0-9]{2}\.csv'
FILE_FORMAT = SHARED_DIMENSIONS.PUBLIC.CSV_STANDARD
ON_ERROR = CONTINUE
VALIDATION_MODE = RETURN_ERRORS
FORCE = TRUE;
```

### Incremental Load Pattern
```sql
-- Load only new files (not previously loaded)
COPY INTO LWS.PUBLIC.PROJECTS
FROM @SHARED_DIMENSIONS.PUBLIC.LWS_PUBLIC_PROJECTS_STAGE
PATTERN = '.*\.csv'
FILE_FORMAT = SHARED_DIMENSIONS.PUBLIC.CSV_STANDARD
ON_ERROR = CONTINUE
VALIDATION_MODE = RETURN_ERRORS
FORCE = FALSE;  -- Don't reload existing files
```

### Multiple File Types
```sql
-- Load both CSV and JSON files
COPY INTO LWS.PUBLIC.PROJECTS
FROM @SHARED_DIMENSIONS.PUBLIC.LWS_PUBLIC_PROJECTS_STAGE
PATTERN = '.*\.(csv|json)'
FILE_FORMAT = SHARED_DIMENSIONS.PUBLIC.CSV_STANDARD
ON_ERROR = CONTINUE
VALIDATION_MODE = RETURN_ERRORS
FORCE = TRUE;
```

---

## ⚠️ Error Handling and Validation

### Validation Only (No Load)
```sql
-- Validate file format without loading
COPY INTO LWS.PUBLIC.PROJECTS
FROM @SHARED_DIMENSIONS.PUBLIC.LWS_PUBLIC_PROJECTS_STAGE
FILE_FORMAT = SHARED_DIMENSIONS.PUBLIC.CSV_STANDARD
VALIDATION_MODE = RETURN_ALL_ERRORS;
```

### Continue on Error
```sql
-- Continue loading even if some rows fail
COPY INTO LWS.PUBLIC.PROJECTS
FROM @SHARED_DIMENSIONS.PUBLIC.LWS_PUBLIC_PROJECTS_STAGE
FILE_FORMAT = SHARED_DIMENSIONS.PUBLIC.CSV_STANDARD
ON_ERROR = CONTINUE
VALIDATION_MODE = RETURN_ERRORS
FORCE = TRUE;
```

### Abort on Error
```sql
-- Stop loading if any errors occur
COPY INTO LWS.PUBLIC.PROJECTS
FROM @SHARED_DIMENSIONS.PUBLIC.LWS_PUBLIC_PROJECTS_STAGE
FILE_FORMAT = SHARED_DIMENSIONS.PUBLIC.CSV_STANDARD
ON_ERROR = ABORT_STATEMENT
VALIDATION_MODE = RETURN_ERRORS
FORCE = TRUE;
```

### Skip Files with Errors
```sql
-- Skip files that have errors
COPY INTO LWS.PUBLIC.PROJECTS
FROM @SHARED_DIMENSIONS.PUBLIC.LWS_PUBLIC_PROJECTS_STAGE
FILE_FORMAT = SHARED_DIMENSIONS.PUBLIC.CSV_STANDARD
ON_ERROR = SKIP_FILE
VALIDATION_MODE = RETURN_ERRORS
FORCE = TRUE;
```

---

## 🚀 Performance Optimization

### Column Transformations During Load
```sql
-- Transform data during load for better performance
COPY INTO LWS.PUBLIC.PROJECTS
FROM (
    SELECT 
        $1::VARCHAR AS project_name,
        $2::VARCHAR AS project_id,
        $3::DATE AS created_date,
        $4::NUMBER AS budget,
        UPPER($5::VARCHAR) AS status
    FROM @SHARED_DIMENSIONS.PUBLIC.LWS_PUBLIC_PROJECTS_STAGE
)
FILE_FORMAT = SHARED_DIMENSIONS.PUBLIC.CSV_STANDARD
ON_ERROR = CONTINUE
VALIDATION_MODE = RETURN_ERRORS
FORCE = TRUE;
```

### Data Type Casting
```sql
-- Cast columns to appropriate data types
COPY INTO LWS.PUBLIC.PROJECTS
FROM (
    SELECT 
        $1::VARCHAR(255) AS project_name,
        $2::VARCHAR(50) AS project_id,
        TRY_TO_DATE($3) AS created_date,
        TRY_TO_NUMBER($4) AS budget,
        $5::VARCHAR(100) AS status
    FROM @SHARED_DIMENSIONS.PUBLIC.LWS_PUBLIC_PROJECTS_STAGE
)
FILE_FORMAT = SHARED_DIMENSIONS.PUBLIC.CSV_STANDARD
ON_ERROR = CONTINUE
VALIDATION_MODE = RETURN_ERRORS
FORCE = TRUE;
```

### Filtering During Load
```sql
-- Filter data during load to reduce storage
COPY INTO LWS.PUBLIC.PROJECTS
FROM (
    SELECT 
        $1::VARCHAR AS project_name,
        $2::VARCHAR AS project_id,
        $3::DATE AS created_date,
        $4::NUMBER AS budget,
        $5::VARCHAR AS status
    FROM @SHARED_DIMENSIONS.PUBLIC.LWS_PUBLIC_PROJECTS_STAGE
    WHERE $5::VARCHAR != 'CANCELLED'  -- Filter out cancelled projects
)
FILE_FORMAT = SHARED_DIMENSIONS.PUBLIC.CSV_STANDARD
ON_ERROR = CONTINUE
VALIDATION_MODE = RETURN_ERRORS
FORCE = TRUE;
```

---

## 📊 Monitoring and History

### Check Load History for Specific Table
```sql
-- Get load history for the last 24 hours
SELECT 
    FILE_NAME,
    TABLE_NAME,
    STATUS,
    ROWS_LOADED,
    ROWS_PARSED,
    ROWS_REJECTED,
    ERROR_COUNT,
    FIRST_ERROR,
    LAST_LOAD_TIME
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'LWS.PUBLIC.PROJECTS',
    START_TIME => DATEADD(HOUR, -24, CURRENT_TIMESTAMP())
))
ORDER BY LAST_LOAD_TIME DESC;
```

### Check All Load History
```sql
-- Get load history for all tables
SELECT 
    FILE_NAME,
    TABLE_NAME,
    STATUS,
    ROWS_LOADED,
    ROWS_PARSED,
    ROWS_REJECTED,
    ERROR_COUNT,
    FIRST_ERROR,
    LAST_LOAD_TIME
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    START_TIME => DATEADD(HOUR, -24, CURRENT_TIMESTAMP())
))
ORDER BY LAST_LOAD_TIME DESC;
```

### List Files in Stage
```sql
-- List all files in the stage
LIST @SHARED_DIMENSIONS.PUBLIC.LWS_PUBLIC_PROJECTS_STAGE;
```

### Check Stage File Details
```sql
-- Get detailed information about files in stage
SELECT 
    METADATA$FILENAME,
    METADATA$FILE_ROW_NUMBER,
    METADATA$FILE_CONTENT_KEY,
    METADATA$FILE_LAST_MODIFIED,
    METADATA$START_SCAN_TIME
FROM @SHARED_DIMENSIONS.PUBLIC.LWS_PUBLIC_PROJECTS_STAGE
(FILE_FORMAT => SHARED_DIMENSIONS.PUBLIC.CSV_STANDARD)
LIMIT 10;
```

---

## 🎯 Complete Examples

### Example 1: Load Projects Table
```sql
-- Set context
USE DATABASE LWS;
USE SCHEMA PUBLIC;

-- Load projects data
COPY INTO LWS.PUBLIC.PROJECTS
FROM @SHARED_DIMENSIONS.PUBLIC.LWS_PUBLIC_PROJECTS_STAGE
FILE_FORMAT = SHARED_DIMENSIONS.PUBLIC.CSV_STANDARD
ON_ERROR = CONTINUE
VALIDATION_MODE = RETURN_ERRORS
FORCE = TRUE;

-- Verify load
SELECT COUNT(*) FROM LWS.PUBLIC.PROJECTS;
```

### Example 2: Load Service Table with Transformations
```sql
-- Set context
USE DATABASE LWS;
USE SCHEMA PUBLIC;

-- Load service data with transformations
COPY INTO LWS.PUBLIC.SERVICE
FROM (
    SELECT 
        $1::VARCHAR(255) AS scoop_id,
        $2::VARCHAR(255) AS project_name,
        TRY_TO_DATE($3) AS service_date,
        TRY_TO_NUMBER($4) AS service_hours,
        UPPER($5::VARCHAR(100)) AS service_type,
        $6::VARCHAR(500) AS description
    FROM @SHARED_DIMENSIONS.PUBLIC.LWS_PUBLIC_SERVICE_STAGE
)
FILE_FORMAT = SHARED_DIMENSIONS.PUBLIC.CSV_STANDARD
ON_ERROR = CONTINUE
VALIDATION_MODE = RETURN_ERRORS
FORCE = TRUE;

-- Verify load
SELECT COUNT(*) FROM LWS.PUBLIC.SERVICE;
```

### Example 3: Load SEAL Data with Pattern Matching
```sql
-- Set context
USE DATABASE SEAL;
USE SCHEMA PUBLIC;

-- Load SEAL residential data
COPY INTO SEAL.PUBLIC.RESI
FROM @SHARED_DIMENSIONS.PUBLIC.SEAL_PUBLIC_RESI_STAGE
PATTERN = '.*\.csv'
FILE_FORMAT = SHARED_DIMENSIONS.PUBLIC.CSV_STANDARD
ON_ERROR = CONTINUE
VALIDATION_MODE = RETURN_ERRORS
FORCE = TRUE;

-- Verify load
SELECT COUNT(*) FROM SEAL.PUBLIC.RESI;
```

### Example 4: Load Google Analytics Data
```sql
-- Set context
USE DATABASE LWS;
USE SCHEMA PUBLIC;

-- Load Google Analytics data
COPY INTO LWS.PUBLIC.GOOGLE_ANALYTICS
FROM @SHARED_DIMENSIONS.PUBLIC.LWS_PUBLIC_GOOGLE_ANALYTICS_STAGE
FILE_FORMAT = SHARED_DIMENSIONS.PUBLIC.CSV_STANDARD
ON_ERROR = CONTINUE
VALIDATION_MODE = RETURN_ERRORS
FORCE = TRUE;

-- Verify load
SELECT COUNT(*) FROM LWS.PUBLIC.GOOGLE_ANALYTICS;
```

---

## 🔧 Troubleshooting

### Common Error Solutions

#### 1. File Format Errors
```sql
-- Recreate file format if issues occur
DROP FILE FORMAT IF EXISTS SHARED_DIMENSIONS.PUBLIC.CSV_STANDARD;
CREATE OR REPLACE FILE FORMAT SHARED_DIMENSIONS.PUBLIC.CSV_STANDARD
TYPE = CSV
FIELD_DELIMITER = ','
RECORD_DELIMITER = '\n'
SKIP_HEADER = 1
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
TRIM_SPACE = TRUE
ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
EMPTY_FIELD_AS_NULL = TRUE
NULL_IF = ('NULL', 'null', '');
```

#### 2. Stage Issues
```sql
-- Refresh stage if files not found
ALTER STAGE SHARED_DIMENSIONS.PUBLIC.LWS_PUBLIC_PROJECTS_STAGE REFRESH;
```

#### 3. Permission Issues
```sql
-- Grant necessary permissions
GRANT USAGE ON DATABASE LWS TO ROLE YOUR_ROLE;
GRANT USAGE ON SCHEMA LWS.PUBLIC TO ROLE YOUR_ROLE;
GRANT INSERT ON TABLE LWS.PUBLIC.PROJECTS TO ROLE YOUR_ROLE;
```

#### 4. Validation Errors
```sql
-- Check for specific validation errors
SELECT 
    FILE_NAME,
    FIRST_ERROR,
    ERROR_COUNT
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'LWS.PUBLIC.PROJECTS',
    START_TIME => DATEADD(HOUR, -1, CURRENT_TIMESTAMP())
))
WHERE STATUS = 'LOAD_FAILED'
ORDER BY LAST_LOAD_TIME DESC;
```

---

## 📈 Best Practices

### 1. Always Use File Formats
- Create reusable file formats for consistency
- Use appropriate compression settings
- Set proper error handling parameters

### 2. Implement Error Handling
- Use `ON_ERROR = CONTINUE` for production loads
- Use `VALIDATION_MODE = RETURN_ERRORS` for debugging
- Monitor error counts and failed rows

### 3. Optimize Performance
- Use column transformations during load
- Implement proper data type casting
- Filter data during load when possible

### 4. Monitor Loads
- Check load history regularly
- Monitor row counts and error rates
- Set up alerts for failed loads

### 5. Use Pattern Matching
- Load multiple files efficiently
- Implement incremental loading
- Use date-based patterns for time-series data

---

## 🎯 Success Criteria

**Each Snowflake table matches its corresponding Azure storage CSV identically**

To verify this:
1. Compare row counts between Azure CSV and Snowflake table
2. Validate data types and formats
3. Check for any data corruption or missing records
4. Ensure all columns are properly loaded
5. Verify primary key constraints are maintained

---

*Last updated: 2025-01-27*
*This document is used by AI assistants to understand and execute Snowflake COPY INTO operations* 