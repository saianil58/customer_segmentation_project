import logging
from utils import get_snowflake_conn_cursor, close_snowflake_conection

logging.basicConfig(level=logging.INFO)


def calculate_segements_in_snowflake():
    """
    This Function is to create segements based on the historical data
    The data will be read from snowflake and created as a new table 
    """

    logging.info("Segments Calculations Start")

    # get the snowflake connection and find cursor
    ret_val = get_snowflake_conn_cursor()
    cur = ret_val[0]

    # Open the SQL file and get the query to be executed
    fd = open("segments_query.sql", "r")
    sqlFile = fd.read()
    fd.close()

    # Execute the sql command
    cur.execute(sqlFile)

    # Close the snowflake connection
    close_snowflake_conection(ret_val)

    logging.info("Segments Calculations Completed")
