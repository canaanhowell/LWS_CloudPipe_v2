# 📅 Date Dimension Table Guide

## Overview

The Date Dimension Table (`dim_date`) is a comprehensive date reference table that provides a standardized way to connect different date fields from various tables in your data warehouse. This table includes rich date attributes that enable powerful analytics and reporting capabilities.

## 🎯 Purpose

- **Standardize Date Handling**: Connect different date fields (contract signed date, customer signed date, created date, etc.)
- **Enable Rich Analytics**: Business day analysis, holiday impact, fiscal year reporting
- **Improve Query Performance**: Pre-calculated date attributes reduce runtime calculations
- **Support Time Intelligence**: Quarter-over-quarter, year-over-year comparisons

## 📊 Table Structure

### Core Fields

| Field | Type | Description | Example |
|-------|------|-------------|---------|
| `date_key` | INTEGER | Primary key in YYYYMMDD format | 20250128 |
| `full_date` | DATE | Actual date value | 2025-01-28 |
| `day_of_week` | INTEGER | 1-7 (Monday=1, Sunday=7) | 3 |
| `day_of_week_name` | VARCHAR(10) | Full day name | Tuesday |
| `day_of_week_short` | VARCHAR(3) | Short day name | Tue |
| `day_of_month` | INTEGER | Day of month (1-31) | 28 |
| `day_of_year` | INTEGER | Day of year (1-366) | 28 |
| `week_of_year` | INTEGER | Week number (1-53) | 5 |
| `month_number` | INTEGER | Month number (1-12) | 1 |
| `month_name` | VARCHAR(10) | Full month name | January |
| `month_name_short` | VARCHAR(3) | Short month name | Jan |
| `quarter` | INTEGER | Quarter (1-4) | 1 |
| `quarter_name` | VARCHAR(2) | Quarter name | Q1 |
| `year` | INTEGER | Full year | 2025 |
| `year_month` | VARCHAR(7) | YYYY-MM format | 2025-01 |
| `year_quarter` | VARCHAR(7) | YYYY-Q1 format | 2025-Q1 |

### Business Logic Fields

| Field | Type | Description |
|-------|------|-------------|
| `is_weekend` | BOOLEAN | TRUE if Saturday or Sunday |
| `is_holiday` | BOOLEAN | TRUE if US federal holiday |
| `is_business_day` | TRUE if not weekend and not holiday |
| `is_month_end` | BOOLEAN | TRUE if last day of month |
| `is_quarter_end` | BOOLEAN | TRUE if last day of quarter |
| `is_year_end` | BOOLEAN | TRUE if December 31st |

### Fiscal Year Fields

| Field | Type | Description |
|-------|------|-------------|
| `fiscal_year` | INTEGER | Fiscal year (July-June) |
| `fiscal_quarter` | INTEGER | Fiscal quarter |
| `fiscal_month` | INTEGER | Fiscal month |

### Metadata Fields

| Field | Type | Description |
|-------|------|-------------|
| `created_timestamp` | TIMESTAMP_NTZ | When record was created |
| `updated_timestamp` | TIMESTAMP_NTZ | When record was last updated |

## 🚀 Quick Start

### 1. Basic Date Join

```sql
-- Join any date field with the dimension table
SELECT 
    p.project_name,
    p.customer_signed_date,
    d.day_of_week_name,
    d.month_name,
    d.year,
    d.is_business_day
FROM projects p
JOIN dim_date d ON DATE(p.customer_signed_date) = d.full_date
WHERE p.customer_signed_date IS NOT NULL;
```

### 2. Get Date Key

```sql
-- Use helper function to get date key
SELECT get_date_key(DATE('2025-01-28')) as date_key;

-- Or get from timestamp
SELECT get_date_key_from_timestamp(CURRENT_TIMESTAMP()) as date_key;
```

### 3. Business Day Analysis

```sql
-- Analyze business vs non-business day patterns
SELECT 
    d.is_business_day,
    d.day_of_week_name,
    COUNT(*) as project_count
FROM projects p
JOIN dim_date d ON DATE(p.customer_signed_date) = d.full_date
GROUP BY d.is_business_day, d.day_of_week_name
ORDER BY d.is_business_day DESC, d.day_of_week;
```

## 📈 Common Use Cases

### 1. Monthly Performance Analysis

```sql
SELECT 
    d.year_month,
    d.month_name,
    COUNT(*) as total_projects,
    COUNT(CASE WHEN d.is_business_day THEN 1 END) as business_day_projects,
    COUNT(CASE WHEN d.is_holiday THEN 1 END) as holiday_projects
FROM projects p
JOIN dim_date d ON DATE(p.customer_signed_date) = d.full_date
GROUP BY d.year_month, d.month_name
ORDER BY d.year_month;
```

### 2. Quarterly Comparison

```sql
SELECT 
    d.quarter_name,
    d.year,
    COUNT(*) as project_count,
    AVG(CASE WHEN d.is_business_day THEN 1 ELSE 0 END) as business_day_ratio
FROM monday_seal_resi m
JOIN dim_date d ON DATE(m.created_date) = d.full_date
GROUP BY d.quarter_name, d.year
ORDER BY d.year, d.quarter;
```

### 3. Fiscal Year Reporting

```sql
SELECT 
    d.fiscal_year,
    d.fiscal_quarter,
    COUNT(*) as total_items,
    SUM(CASE WHEN d.is_business_day THEN 1 ELSE 0 END) as business_day_items
FROM monday_seal_comm_sales m
JOIN dim_date d ON DATE(m.created_date) = d.full_date
GROUP BY d.fiscal_year, d.fiscal_quarter
ORDER BY d.fiscal_year, d.fiscal_quarter;
```

### 4. Holiday Impact Analysis

```sql
SELECT 
    d.is_holiday,
    d.day_of_week_name,
    COUNT(*) as item_count,
    AVG(CASE WHEN d.is_business_day THEN 1 ELSE 0 END) as business_day_ratio
FROM monday_seal_resi m
JOIN dim_date d ON DATE(m.created_date) = d.full_date
GROUP BY d.is_holiday, d.day_of_week_name
ORDER BY d.is_holiday DESC, d.day_of_week;
```

## 🔧 Helper Functions

### `get_date_key(DATE)`
Returns the date key for a given date.

```sql
SELECT get_date_key(DATE('2025-01-28')) as date_key;
-- Returns: 20250128
```

### `get_date_key_from_timestamp(TIMESTAMP_NTZ)`
Returns the date key for a given timestamp.

```sql
SELECT get_date_key_from_timestamp(CURRENT_TIMESTAMP()) as date_key;
-- Returns: 20250128 (for today's date)
```

## 📋 Available Views

### `v_date_dimension`
Simplified view with most commonly used fields.

```sql
SELECT * FROM v_date_dimension 
WHERE full_date BETWEEN '2025-01-01' AND '2025-01-31';
```

### `v_monday_seal_resi_with_dates`
Pre-joined view with Monday.com residential data.

```sql
SELECT * FROM v_monday_seal_resi_with_dates 
WHERE created_year = 2025;
```

### `v_lws_projects_with_dates`
Pre-joined view with LWS projects data (when available).

```sql
SELECT * FROM v_lws_projects_with_dates 
WHERE customer_signed_year = 2025;
```

## 🛠️ Maintenance

### Populate Additional Dates

```sql
-- Add more dates to the dimension table
CALL populate_date_dimension(
    DATE('2020-01-01'),  -- Start date
    DATE('2030-12-31')   -- End date
);
```

### Update Holidays

The holiday logic is currently simplified. To add more holidays, modify the stored procedure:

```sql
-- Example: Add more US federal holidays
is_holiday_val := (
    current_date = DATE_FROM_PARTS(year_val, 1, 1) OR   -- New Year's Day
    current_date = DATE_FROM_PARTS(year_val, 7, 4) OR   -- Independence Day
    current_date = DATE_FROM_PARTS(year_val, 12, 25) OR -- Christmas Day
    current_date = DATE_FROM_PARTS(year_val, 11, 11) OR -- Veterans Day
    current_date = DATE_FROM_PARTS(year_val, 9, 5)      -- Labor Day
);
```

## 📊 Performance Tips

1. **Use Indexes**: The table has indexes on `full_date`, `year_month`, `year_quarter`, and `is_business_day`
2. **Date Key Joins**: Use `date_key` for integer-based joins when possible
3. **Partitioning**: Consider partitioning large fact tables by date
4. **Materialized Views**: Create materialized views for frequently used date-based aggregations

## 🔍 Troubleshooting

### Common Issues

1. **Date Format Mismatch**: Ensure your date fields are properly converted to DATE type
2. **Missing Dates**: Use the `populate_date_dimension` procedure to add missing date ranges
3. **Performance**: Use the indexed fields (`full_date`, `year_month`) for better performance

### Verification Queries

```sql
-- Check if table exists and has data
SELECT COUNT(*) as total_dates FROM dim_date;

-- Check date range
SELECT MIN(full_date) as earliest_date, MAX(full_date) as latest_date FROM dim_date;

-- Verify business day logic
SELECT 
    is_business_day,
    day_of_week_name,
    COUNT(*) as count
FROM dim_date 
WHERE full_date >= CURRENT_DATE() - 30
GROUP BY is_business_day, day_of_week_name
ORDER BY is_business_day DESC, day_of_week;
```

## 📚 Related Documentation

- [Data Warehouse Architecture Guide](../PROJECT_CURRENT_STATE_SUMMARY.md)
- [Snowflake Connection Guide](../SNOWFLAKE_AUTH_SOLUTIONS.md)
- [Azure Integration Guide](../AZURE_TO_SNOWFLAKE_LOADER_GUIDE.md)

## 🆘 Support

For issues or questions:
1. Check the log files: `date_dimension_creation.log`
2. Review the troubleshooting section above
3. Verify your Snowflake connection and permissions
4. Ensure your date fields are in the correct format

---

**Last Updated**: 2025-01-28  
**Version**: 1.0  
**Author**: Autonomous Cursor 