import snowflake.connector
import json
import base64

with open('settings.json') as f:
    config = json.load(f)

conn = snowflake.connector.connect(
    account=config['SNOWFLAKE_ACCOUNT'],
    user=config['SNOWFLAKE_USER'],
    private_key=base64.b64decode(open(config['SNOWFLAKE_PRIVATE_KEY_PATH']).read().strip()),
    warehouse=config['SNOWFLAKE_WAREHOUSE'],
    database=config['SNOWFLAKE_DATABASE']
)

cur = conn.cursor()

def try_describe(query, label):
    print(f'\n{label}')
    try:
        cur.execute(query)
        rows = cur.fetchall()
        if not rows:
            print('No rows returned.')
        for row in rows:
            print(row)
    except Exception as e:
        print('Error:', e)

try_describe('DESCRIBE TABLE LWS.PUBLIC.SERVICE', 'DESCRIBE TABLE LWS.PUBLIC.SERVICE (unquoted):')
try_describe('DESCRIBE TABLE LWS.PUBLIC."SERVICE"', 'DESCRIBE TABLE LWS.PUBLIC."SERVICE" (quoted, all caps):')
try_describe('DESCRIBE TABLE LWS.PUBLIC."service"', 'DESCRIBE TABLE LWS.PUBLIC."service" (quoted, all lowercase):')

cur.close()
conn.close() 