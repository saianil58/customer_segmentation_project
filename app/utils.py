import snowflake.connector
import logging

logging.basicConfig(level=logging.INFO)


def get_snowflake_conn_cursor():
    """
    This function is used to connect to snowflake
    This returns connection and cursor objects
    They can be used based on the need
    """
    logging.info("making connection")
    conn = snowflake.connector.connect(
        user="saianil58",
        password="<please use the shared password>",
        account="du59886.ap-south-1.aws",
        warehouse="SAI_WH",
        database="DH_DATABASE",
        schema="DH_RAW",
        role="ACCOUNTADMIN",
    )
    cur = conn.cursor()
    logging.info("conn established")
    return cur, conn


def close_snowflake_conection(cur_conn):
    """
    This function is used to close the cursor object
    and connection object created from get_snowflake_conn_cursor
    """
    cur = cur_conn[0]
    conn = cur_conn[1]
    cur.close
    conn.close
