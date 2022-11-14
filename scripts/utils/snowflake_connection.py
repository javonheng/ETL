import snowflake.connector
from decouple import config

sf_user = config('SF_USER')
sf_pw = config('SF_PW')
sf_acc_name = config('SF_ACC_NAME')
sf_schema = config('SF_SCHEMA')
sf_db = config('SF_DATABASE')
sf_warehouse = config('SF_WAREHOUSE')
sf_region = config('SF_REGION')

def snowflake_conn():
    snowflake_conn = snowflake.connector.connect(
        user=sf_user,
        password=sf_pw,
        account=sf_acc_name,
        warehouse=sf_warehouse,
        database=sf_db,
        schema=sf_schema,
        # region=sf_region,
        # authenticator='externalbrowser',
        # role='ACCOUNTADMIN'
    )

    return snowflake_conn
