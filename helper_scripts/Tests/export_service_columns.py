#!/usr/bin/env python3
"""
Export LWS.PUBLIC.SERVICE table columns to CSV
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
    """Main function to export SERVICE table columns"""
    print("Connecting to Snowflake...")
    conn = get_snowflake_connection()
    
    print("Getting LWS.PUBLIC.SERVICE table columns...")
    columns = get_table_columns(conn, 'LWS', 'PUBLIC', 'SERVICE')
    
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
    output_file = "LWS_PUBLIC_SERVICE_columns.csv"
    df.to_csv(output_file, index=False)
    
    print(f"✅ Exported {len(df)} columns to {output_file}")
    print(f"📊 Column count: {len(df)}")
    
    # Check for api_test column
    api_test_rows = df[df['COLUMN_NAME'] == 'api_test']
    if not api_test_rows.empty:
        print(f"\n✅ Found api_test column:")
        for _, row in api_test_rows.iterrows():
            print(f"  Position {row['ORDINAL_POSITION']}: {row['COLUMN_NAME']} ({row['DATA_TYPE']})")
    else:
        print(f"\n❌ api_test column NOT found in SERVICE table")
    
    # Display last 10 columns (where api_test would likely be)
    print(f"\nLast 10 columns:")
    for i, row in df.tail(10).iterrows():
        print(f"  {row['ORDINAL_POSITION']:3d}. {row['COLUMN_NAME']:<40} {row['DATA_TYPE']}")
    
    conn.close()
    print(f"\n📁 CSV file saved as: {output_file}")

if __name__ == "__main__":
    main() 