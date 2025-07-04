import json
from pathlib import Path
import snowflake.connector

# Load credentials from settings.json in project root
settings_path = Path(__file__).parent.parent.parent / "settings.json"
with open(settings_path, "r") as f:
    config = json.load(f)

account = config["SNOWFLAKE_ACCOUNT"]
user = config["SNOWFLAKE_USER"]
warehouse = config["SNOWFLAKE_WAREHOUSE"]
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
    role=role
)
cursor = conn.cursor()

# Only process LWS and SEAL databases
databases = ["LWS", "SEAL"]

total_dropped = 0
for db in databases:
    try:
        print(f"Processing database: {db}")
        cursor.execute(f"USE DATABASE {db}")
        cursor.execute("SHOW SCHEMAS")
        schemas = [row[1] for row in cursor.fetchall()]
        
        for schema in schemas:
            try:
                print(f"  Processing schema: {schema}")
                cursor.execute(f"USE SCHEMA {db}.{schema}")
                
                # Get procedures with their signatures
                cursor.execute("""
                    SELECT PROCEDURE_NAME, ARGUMENT_SIGNATURE 
                    FROM INFORMATION_SCHEMA.PROCEDURES 
                    WHERE PROCEDURE_SCHEMA = %s
                """, (schema,))
                procs = cursor.fetchall()
                
                if procs:
                    print(f"    Found {len(procs)} procedures in {db}.{schema}:")
                    
                for proc_name, arg_sig in procs:
                    print(f"      - Dropping procedure: {proc_name}")
                    
                    # Try different approaches to drop the procedure
                    success = False
                    
                    # Method 1: Try with full signature
                    if arg_sig and not success:
                        try:
                            drop_sql = f"DROP PROCEDURE IF EXISTS {db}.{schema}.{proc_name}{arg_sig}"
                            cursor.execute(drop_sql)
                            total_dropped += 1
                            print(f"        ✓ Successfully dropped {proc_name} (with signature)")
                            success = True
                        except Exception as e:
                            print(f"        ✗ Method 1 failed: {e}")
                    
                    # Method 2: Try with just procedure name (for procedures with no arguments)
                    if not success:
                        try:
                            drop_sql = f"DROP PROCEDURE IF EXISTS {db}.{schema}.{proc_name}"
                            cursor.execute(drop_sql)
                            total_dropped += 1
                            print(f"        ✓ Successfully dropped {proc_name} (without signature)")
                            success = True
                        except Exception as e:
                            print(f"        ✗ Method 2 failed: {e}")
                    
                    # Method 3: Try with empty parentheses
                    if not success:
                        try:
                            drop_sql = f"DROP PROCEDURE IF EXISTS {db}.{schema}.{proc_name}()"
                            cursor.execute(drop_sql)
                            total_dropped += 1
                            print(f"        ✓ Successfully dropped {proc_name} (with empty parentheses)")
                            success = True
                        except Exception as e:
                            print(f"        ✗ Method 3 failed: {e}")
                    
                    if not success:
                        print(f"        ✗ All methods failed for {proc_name}")
                            
            except Exception as e:
                print(f"    Error in schema {db}.{schema}: {e}")
                continue
                
    except Exception as e:
        print(f"Error in database {db}: {e}")
        continue

print(f"\nTotal procedures dropped: {total_dropped}")
cursor.close()
conn.close() 