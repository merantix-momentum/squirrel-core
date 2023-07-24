import os

import fsspec
import numpy as np
import pandas as pd
import pytest
from squirrel.constants import SQUIRREL_TMP_DATA_BUCKET
from squirrel.driver import CsvDriver, MessagepackDriver

REMOTE_DIR = SQUIRREL_TMP_DATA_BUCKET + "/cache_test"


@pytest.fixture(scope="session")
def cache_dir(tmp_path_factory: pytest.TempdirFactory) -> str:
    """Returns a temporary directory for caching."""
    return str(tmp_path_factory.mktemp("cache_test"))  # convert PosixPath to str


def test_caching_csv(cache_dir: str) -> None:
    """Tests caching functionality of DataFrameDriver via CsvDriver."""

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
    cache_path = os.path.join(cache_dir, file_name)
    with open(cache_path) as f:
        assert f.read() == "a,b\n1,4\n2,5\n3,6\n"

    # remove example data from cache directory
    os.remove(cache_path)

    # remove example data from bucket
    fs = fsspec.filesystem("gs")
    fs.rm(remote_path)


def test_caching_messagepack(cache_dir: str) -> None:
    """Tests caching functionality of MapDriver via MessagepackDriver."""

    # example data
    data = {"data": np.random.rand(10)}

    # upload example data to remote bucket
    store = MessagepackDriver(REMOTE_DIR).store
    key = "0001"  # all data in one shard
    store.set(key=key, value=data)

    # download example data from remote bucket
    driver = MessagepackDriver(
        REMOTE_DIR,
        storage_options={
            "protocol": "simplecache",
            "target_protocol": "gs",
            "cache_storage": cache_dir,
        },
    )
    down_data = next(iter(driver.store.get(key=key)))

    # check if remote data is same as local data
    assert all(down_data["data"] == data["data"])

    # check if we have a file in our caching directory
    assert len(os.listdir(cache_dir)) == 1
    file_name = os.listdir(cache_dir)[0]

    # check if cached file contains our original data
    cache_path = os.path.join(cache_dir, file_name)
    # (note: we can't use store.get here, because SquirrelStore hardcodes the file extension to .gz,
    # which is not the case for fss
    # pec caching, which uses no file extension, hence using the
    # lower-level serializer API here)
    cached_data = next(iter(driver.store.serializer.deserialize_shard_from_file(cache_path)))
    assert all(cached_data["data"] == data["data"])

    # remove example data from cache directory
    os.remove(cache_path)

    # remove example data from bucket
    fs = fsspec.filesystem("gs")
    fs.rm(os.path.join(REMOTE_DIR, f"{key}.gz"))
