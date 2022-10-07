import pytest
from urllib.error import URLError
from unittest.mock import MagicMock
import os
import sys
import inspect

currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0, parentdir) 
from file_downloader import download_file_from_url
from utils import get_snowflake_conn_cursor, close_snowflake_conection

dummy_dir = '/'


@pytest.mark.parametrize("url,dst_path",
                         [
                             ("https://www.dummy.url.for.test", dummy_dir)
                         ]
                         )
def test_download(url: str, dst_path: str) -> None:
    with pytest.raises(URLError):
        download_file_from_url(url, dst_path)

def test_snowflake_connection_open():
    cur_conn=get_snowflake_conn_cursor()
    close_snowflake_conection(cur_conn)
    