#!/usr/bin/env python3
"""
Export LWS.PUBLIC.PROJECTS table columns to CSV
"""

import snowflake.connector
import json
import base64
import pandas as pd
from pathlib import Path

def get_snowflake_connection():
    """Get Snowflake connection using settings.json"""
    config_path = Path(__file__).parent.parent.parent / "settings.json"
    
    with open(config_path, 'r') as f:
        config = json.load(f)
    
    conn = snowflake.connector.connect(
        account=config['SNOWFLAKE_ACCOUNT'],
        user=config['SNOWFLAKE_USER'],
        private_key=base64.b64decode(open(config['SNOWFLAKE_PRIVATE_KEY_PATH']).read().strip()),
        warehouse=config['SNOWFLAKE_WAREHOUSE'],
        database=config['SNOWFLAKE_DATABASE']
    )
    
    return conn

def get_table_columns(conn, database, schema, table):
    """Get column information for a specific table"""
    cursor = conn.cursor()
    
    # Query to get column information
    query = f"""
    SELECT 
        COLUMN_NAME,
        DATA_TYPE,
        IS_NULLABLE,
        COLUMN_DEFAULT,
        CHARACTER_MAXIMUM_LENGTH,
        NUMERIC_PRECISION,
        NUMERIC_SCALE,
        ORDINAL_POSITION
    FROM {database}.INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = '{schema}' 
    AND TABLE_NAME = '{table}'
    ORDER BY ORDINAL_POSITION
    """
    
    cursor.execute(query)
    columns = cursor.fetchall()
    cursor.close()
    
    return columns

def main():
    """Main function to export PROJECTS table columns"""
    print("Connecting to Snowflake...")
    conn = get_snowflake_connection()
    
    print("Getting LWS.PUBLIC.PROJECTS table columns...")
    columns = get_table_columns(conn, 'LWS', 'PUBLIC', 'PROJECTS')
    
    # Convert to DataFrame
    df = pd.DataFrame(columns, columns=[
        'COLUMN_NAME',
        'DATA_TYPE', 
        'IS_NULLABLE',
        'COLUMN_DEFAULT',
        'CHARACTER_MAXIMUM_LENGTH',
        'NUMERIC_PRECISION',
        'NUMERIC_SCALE',
        'ORDINAL_POSITION'
    ])
    
    # Save to CSV
    output_file = "LWS_PUBLIC_PROJECTS_columns.csv"
    df.to_csv(output_file, index=False)
    
    print(f"✅ Exported {len(df)} columns to {output_file}")
    print(f"📊 Column count: {len(df)}")
    
    # Display first few columns
    print("\nFirst 10 columns:")
    for i, row in df.head(10).iterrows():
        print(f"  {row['ORDINAL_POSITION']:2d}. {row['COLUMN_NAME']:<40} {row['DATA_TYPE']}")
    
    if len(df) > 10:
        print(f"  ... and {len(df) - 10} more columns")
    
    conn.close()
    print(f"\n📁 CSV file saved as: {output_file}")

if __name__ == "__main__":
    main() 