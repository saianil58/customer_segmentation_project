from calculate_segements import calculate_segements_in_snowflake
from file_downloader import download_file_from_url
from raw_data_load import load_raw_data_into_snowflake
from snowflake_setup import executeScriptsFromFile
import logging

logging.basicConfig(level=logging.INFO)


def run_pipeline():
    """
    This is a wrapper method for the pipeline execution.
    This is usually done via Airflow as orchestrator.
    as a POC, we are doing this as python
    """

    # step 0 setup snowflake
    logging.info("Step 0:")
    executeScriptsFromFile("snowflake_setup.sql")

    # step 1 download the file from google drive
    logging.info("Step 1: ")
    download_file_from_url(
        url_path="https://drive.google.com/file/d/1t_0Wt5Vbs44oZoTrG0Hwg9OZxNkoo5m4/view",
        file_name="data.parquet.gzip",
    )

    # step 2 setup the snowflake accounts
    logging.info("Step 2: ")
    load_raw_data_into_snowflake()

    # step 3 make segments based on the data
    logging.info("Step 3: ")
    calculate_segements_in_snowflake()

    logging.info('Pipeline is completed!! Data is ready.')
run_pipeline()