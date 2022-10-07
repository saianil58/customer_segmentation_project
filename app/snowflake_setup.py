import logging
from utils import get_snowflake_conn_cursor, close_snowflake_conection

logging.basicConfig(level=logging.INFO)


def executeScriptsFromFile(filename):
    """
    This function is wrapper to execute one sql file
    with multiple sql statements. 
    """

    logging.info("Snowflake Setup is Started")
    ret_val = get_snowflake_conn_cursor()

    cur = ret_val[0]

    # Open and read the file as a single buffer
    fd = open(filename, "r")
    sqlFile = fd.read()
    fd.close()

    # all SQL commands (split on ';')
    sqlCommands = sqlFile.split(";")

    # Execute every command from the input file
    for command in sqlCommands:
        try:
            cur.execute(command)
        except Exception as e:
            logging.info("Command skipped: ", e)

    close_snowflake_conection(ret_val)
    logging.info("Snowflake Setup is Completed !")