from turtle import clear
import gdown
import logging

logging.basicConfig(level=logging.INFO)


def download_file_from_url(url_path, file_name):
    """
    This method will allow us to download the files
    from google drive, provided we have public access
    This can also take authorizations and work, if the
    files are not public. 

    We shall use the library without authorizations.
    """

    url = url_path
    output = file_name

    logging.info(f"RETRIEVING FILE FROM URL: {url}")

    gdown.download(url, output, quiet=True, fuzzy=True)

    logging.info(f"{output} downloaded sucessfully")
