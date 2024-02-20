import snowflake.connector
from gam_activity.loggers import logger
import param
 
# Snowflake connectivity
def create_connection():
    try:
        conn = snowflake.connector.connect(
        user=param.user,
        password=param.password,
        account=param.account,
        database= param.database,
        warehouse=param.warehouse,
        schema= param.schema
        )
        return conn
    except snowflake.connector.errors.Error as e:
        logger.info(f"Error connecting to Snowflake: {e}")
        return None
 
snowflake_conn = create_connection()
 
if snowflake_conn:
    logger.info("Connected to Snowflake successfully.")