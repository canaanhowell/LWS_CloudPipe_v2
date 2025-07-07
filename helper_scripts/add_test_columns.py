import json
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
import snowflake.connector

# Load settings
with open('settings.json', 'r') as f:
    s = json.load(f)

private_key_path = s['SNOWFLAKE_PRIVATE_KEY_PATH']
with open(private_key_path, 'rb') as key_file:
    pk = serialization.load_pem_private_key(
        key_file.read(),
        password=None,
        backend=default_backend()
    )
pkb = pk.private_bytes(
    encoding=serialization.Encoding.DER,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption()
)

conn = snowflake.connector.connect(
    account=s['SNOWFLAKE_ACCOUNT'],
    user=s['SNOWFLAKE_USER'],
    private_key=pkb,
    warehouse=s['SNOWFLAKE_WAREHOUSE'],
    database='LWS'
)
cur = conn.cursor()

try:
    cur.execute('ALTER TABLE PUBLIC.PROJECTS ADD COLUMN "c-test" VARCHAR(255)')
    print('Added column: c-test')
except Exception as e:
    print(f'Error adding c-test: {e}')

try:
    cur.execute('ALTER TABLE PUBLIC.PROJECTS ADD COLUMN "c-test2" VARCHAR(255)')
    print('Added column: c-test2')
except Exception as e:
    print(f'Error adding c-test2: {e}')

cur.close()
conn.close()
print('Done.') 