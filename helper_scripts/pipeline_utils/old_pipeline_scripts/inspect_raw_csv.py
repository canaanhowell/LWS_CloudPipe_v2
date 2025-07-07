#!/usr/bin/env python3
"""
Script to inspect the raw CSV file from local storage
This will help us understand the data structure and debug Snowflake loading issues
"""

import os
import pandas as pd
import json
from datetime import datetime

def inspect_raw_csv():
    """Inspect the raw CSV file to understand its structure and content"""
    
    # Define the path to the raw CSV file
    csv_path = "data/csv/lws.public.sharepoint_sungrow_raw.csv"
    
    print(f"🔍 Inspecting raw CSV file: {csv_path}")
    print("=" * 60)
    
    # Check if file exists
    if not os.path.exists(csv_path):
        print(f"❌ File not found: {csv_path}")
        return
    
    try:
        # Read the CSV file
        print("📖 Reading CSV file...")
        df = pd.read_csv(csv_path)
        
        print(f"✅ Successfully loaded CSV file")
        print(f"📊 Shape: {df.shape}")
        print(f"📋 Columns: {len(df.columns)}")
        print()
        
        # Display column information
        print("📋 COLUMN INFORMATION:")
        print("-" * 40)
        for i, col in enumerate(df.columns):
            print(f"{i+1:2d}. '{col}' (dtype: {df[col].dtype})")
        print()
        
        # Display data types summary
        print("🔍 DATA TYPES SUMMARY:")
        print("-" * 40)
        dtype_counts = df.dtypes.value_counts()
        for dtype, count in dtype_counts.items():
            print(f"{dtype}: {count} columns")
        print()
        
        # Display first few rows
        print("📄 FIRST 5 ROWS:")
        print("-" * 40)
        print(df.head().to_string())
        print()
        
        # Display last few rows
        print("📄 LAST 5 ROWS:")
        print("-" * 40)
        print(df.tail().to_string())
        print()
        
        # Check for null values
        print("🔍 NULL VALUES CHECK:")
        print("-" * 40)
        null_counts = df.isnull().sum()
        if null_counts.sum() > 0:
            print("Columns with null values:")
            for col, count in null_counts[null_counts > 0].items():
                print(f"  '{col}': {count} nulls")
        else:
            print("✅ No null values found")
        print()
        
        # Check for empty strings
        print("🔍 EMPTY STRINGS CHECK:")
        print("-" * 40)
        empty_string_counts = {}
        for col in df.columns:
            if df[col].dtype == 'object':
                empty_count = (df[col] == '').sum()
                if empty_count > 0:
                    empty_string_counts[col] = empty_count
        
        if empty_string_counts:
            print("Columns with empty strings:")
            for col, count in empty_string_counts.items():
                print(f"  '{col}': {count} empty strings")
        else:
            print("✅ No empty strings found")
        print()
        
        # Sample data from each column
        print("🔍 SAMPLE DATA FROM EACH COLUMN:")
        print("-" * 40)
        for col in df.columns:
            sample_values = df[col].dropna().head(3).tolist()
            print(f"'{col}': {sample_values}")
        print()
        
        # Check for special characters in column names
        print("🔍 COLUMN NAME ANALYSIS:")
        print("-" * 40)
        problematic_columns = []
        for col in df.columns:
            if any(char in col for char in ['"', "'", ' ', '-', '.', '(', ')', '[', ']']):
                problematic_columns.append(col)
                print(f"⚠️  '{col}' contains special characters")
        
        if not problematic_columns:
            print("✅ All column names are clean")
        print()
        
        # Save inspection results to file
        inspection_results = {
            "timestamp": datetime.now().isoformat(),
            "file_path": csv_path,
            "shape": df.shape,
            "columns": df.columns.tolist(),
            "dtypes": {col: str(dtype) for col, dtype in df.dtypes.items()},
            "null_counts": null_counts.to_dict(),
            "empty_string_counts": empty_string_counts,
            "problematic_columns": problematic_columns,
            "sample_data": {col: df[col].dropna().head(3).tolist() for col in df.columns}
        }
        
        output_file = "logs/csv_inspection_results.json"
        os.makedirs("logs", exist_ok=True)
        with open(output_file, 'w') as f:
            json.dump(inspection_results, f, indent=2)
        
        print(f"💾 Inspection results saved to: {output_file}")
        
    except Exception as e:
        print(f"❌ Error reading CSV file: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    inspect_raw_csv() 