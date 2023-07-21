"""
This module implements tests for the caching functionality of all major classes of drivers. 

i.e. we are testing
- DataFrameDriver via CsvDriver
- MapDriver via MessagepackDriver
- Driver via FileDriver
"""

import os

import fsspec
import pandas as pd
import pytest
from squirrel.constants import SQUIRREL_TMP_DATA_BUCKET
from squirrel.driver import CsvDriver

REMOTE_DIR = SQUIRREL_TMP_DATA_BUCKET + "/cache_test"


@pytest.fixture(scope="session")
def cache_dir(tmp_path_factory):
    return tmp_path_factory.mktemp("cache_test")


def test_caching_csv(cache_dir) -> None:
    """
    Tests caching functionality of DataFrameDriver via CsvDriver.
    """

    cache_dir = str(cache_dir)  # convert PosixPath to str

    # example data
    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})

    # upload example data to remote bucket
    remote_path = REMOTE_DIR + "/test.csv"
    df.to_csv(remote_path, index=False)
    driver = CsvDriver(
        remote_path,
        storage_options={
            "protocol": "simplecache",
            "target_protocol": "gs",
            "cache_storage": cache_dir,
        },
    )

    # check if remote data is same as local data
    assert all(driver.get_df() == df)

    # check if we have a file in our caching directory
    assert len(os.listdir(cache_dir)) == 1
    file_name = os.listdir(cache_dir)[0]

    # check if cached file contains our original data
    with open(os.path.join(cache_dir, file_name), "r") as f:
        assert f.read() == "a,b\n1,4\n2,5\n3,6\n"

    # remove example data from cache directory
    os.remove(os.path.join(cache_dir, file_name))

    # remove example data from bucket
    fs = fsspec.filesystem("gs")
    fs.rm(remote_path)