import pandas as pd
import logging
from utils import get_snowflake_conn_cursor, close_snowflake_conection
from snowflake.connector.pandas_tools import write_pandas

logging.basicConfig(level=logging.INFO)


def load_raw_data_into_snowflake():

    logging.info("Data Load from File in Progress")

    # get the snowflake connection
    ret_val = get_snowflake_conn_cursor()
    conn = ret_val[1]

    # Read the file into a dataframe
    df = pd.read_parquet("data.parquet.gzip")

    # Snowflake needs all the columns in Upper case for loading
    df.columns = [x.upper() for x in df.columns]

    # Remove the index from the dataframe before loading
    df.reset_index(drop=True, inplace=True)

    # write_pandas is a function to load the data from df into snowflake
    # more details are here
    # https://docs.snowflake.com/en/user-guide/python-connector-api.html#write_pandas
    success, nchunks, nrows, _ = write_pandas(
        conn=conn,
        df=df,
        table_name="OLD_VOUCHER_DATA",
        database="DH_DATABASE",
        schema="DH_RAW",
    )

    # closing the snowflake connection
    close_snowflake_conection(ret_val)

    logging.info("Data Load from File is Completed")
