import json
import os
from pathlib import Path
import snowflake.connector

# Load credentials from settings.json in project root
settings_path = Path(__file__).parent.parent.parent / "settings.json"
with open(settings_path, "r") as f:
    config = json.load(f)

account = config["SNOWFLAKE_ACCOUNT"]
user = config["SNOWFLAKE_USER"]
warehouse = config["SNOWFLAKE_WAREHOUSE"]
database = "SHARED_DIMENSIONS"
role = config.get("SNOWFLAKE_ROLE", "ACCOUNTADMIN")
private_key_path = config.get("SNOWFLAKE_PRIVATE_KEY_PATH", "config_files/snowflake_private_key.txt")

# Read private key
with open(private_key_path, 'r') as f:
    private_key = f.read().strip()

# Connect to Snowflake
conn = snowflake.connector.connect(
    account=account,
    user=user,
    private_key=private_key,
    warehouse=warehouse,
    database=database,
    role=role
)

cursor = conn.cursor()

# List all stages in SHARED_DIMENSIONS.PUBLIC except AZURE_PBI25_STAGE
cursor.execute("""
    SELECT stage_name
    FROM INFORMATION_SCHEMA.STAGES
    WHERE stage_schema = 'PUBLIC'
      AND stage_catalog = 'SHARED_DIMENSIONS'
      AND stage_name != 'AZURE_PBI25_STAGE'
""")
stages = [row[0] for row in cursor.fetchall()]

if not stages:
    print("No stages to drop.")
else:
    print(f"Dropping {len(stages)} stages in SHARED_DIMENSIONS.PUBLIC (except AZURE_PBI25_STAGE):")
    for stage in stages:
        drop_sql = f"DROP STAGE IF EXISTS SHARED_DIMENSIONS.PUBLIC.{stage}"
        print(f"  - {drop_sql}")
        try:
            cursor.execute(drop_sql)
        except Exception as e:
            print(f"    Error dropping {stage}: {e}")
    print("Done.")

cursor.close()
conn.close() 