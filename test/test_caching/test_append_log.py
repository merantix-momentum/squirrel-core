import random
import time
from concurrent.futures import ThreadPoolExecutor
from time import sleep
from typing import List, Tuple

import numpy as np
import pytest
from pytest import FixtureRequest
from zarr.hierarchy import Group

from squirrel.caching.append_log import ALogColumns, ALogRow, AppendLog
from squirrel.constants import SQUIRREL_DIR, URL

NUM_OF_ROWS = 10 ** 3 + 50
SHAPE = Tuple[int, int, int]
MOCK_KEY_PREFIX = "some/positive/key"
MOCK_KEY_NEG_PREFIX = "some/negative/key"
# prefix different from MOCK_KEY_PREFIX, served as a negative comparison group.


def test_append_log_append_rows(create_append_log: AppendLog) -> None:
    """Test if the append logs saved on disk have the same columns and desired number of records."""
    alog = create_append_log
    df = alog.read()
    assert len(df.index) == NUM_OF_ROWS  # have the same num of rows
    assert (df.columns == ALogColumns).all()  # have the same columns


def test_append_log_reconstruct_all_keys(
    mock_mixed_keys: List[str], create_append_log_with_mixed_keys: AppendLog
) -> None:
    """Test append log reconstruct all keys from csv."""
    alog = create_append_log_with_mixed_keys
    keys = alog.reconstruct_all_keys()
    assert keys == set(mock_mixed_keys)


def test_append_log_reconstruct_key_cluster(
    mock_mixed_keys: List[str], create_append_log_with_mixed_keys: AppendLog
) -> None:
    """Test append log reconstruct a key cluster."""
    alog = create_append_log_with_mixed_keys
    keys = alog.reconstruct_key_cluster(prefix=MOCK_KEY_PREFIX)
    assert keys == set(mock_mixed_keys[: NUM_OF_ROWS // 2])  # The first half are positive keys


def test_append_log_integrate_with_squirrel_group(create_test_group: Group, array_shape: SHAPE) -> None:
    """Test append log works with squirrel group."""
    n_keys = 10
    group = create_test_group

    # test add keys
    for i in range(n_keys):
        group[str(i)] = np.random.randint(0, 255, size=array_shape, dtype="|i2")
    group.close()  # Call SquirrelGroup.__exit__ or SquirrelGroup.close() to make sure flushing the logs.
    df = group.append_log.read()
    assert len(df.index) == n_keys


@pytest.fixture
def mock_keys() -> List[str]:
    """A mock list of keys to append in append log."""
    return [f"{MOCK_KEY_PREFIX}/{i}" for i in range(NUM_OF_ROWS)]


@pytest.fixture
def mock_negative_keys() -> List[str]:
    """A mock list of keys with different prefix than that of `mock_keys`. Used as a control group."""
    return [f"{MOCK_KEY_NEG_PREFIX}/{i}" for i in range(NUM_OF_ROWS)]


@pytest.fixture
def mock_mixed_keys(mock_keys: List[str], mock_negative_keys: List[str]) -> List[str]:
    """Return a list of keys with two different prefixes."""
    # Notice that NUM_OF_ROWS is even.
    return mock_keys[: NUM_OF_ROWS // 2] + mock_negative_keys[NUM_OF_ROWS // 2 :]


@pytest.fixture
def mock_time() -> List[float]:
    """A mock list of time stamps to append in append log."""
    current_time = time.time()
    mock_time = [current_time + i for i in range(NUM_OF_ROWS)]
    return mock_time


@pytest.fixture
def mock_sizeof() -> List[int]:
    """A mock list of 'sizeof' for append log."""
    return [random.randrange(1, 10 ** 6) for _ in range(NUM_OF_ROWS)]


@pytest.fixture
def mock_random_operations() -> List[int]:
    """A mock list of operations for append log. Take values +1 and -1, representing 'upsert' and 'remove'."""
    op_samples = (+1, -1)
    return [random.choice(op_samples) for _ in range(NUM_OF_ROWS)]


@pytest.fixture
def mock_rows(
    mock_keys: List[str], mock_time: List[float], mock_sizeof: List[int], mock_random_operations: List[int]
) -> List[ALogRow]:
    """Mock rows for append log."""
    mock_rows = [
        ALogRow(key=k, operation=o, sizeof=s, timestamp=t)
        for k, o, s, t in zip(mock_keys, mock_random_operations, mock_sizeof, mock_time)
    ]
    return mock_rows


@pytest.fixture(params=[1, 4])  # parametrize number of threads
def create_append_log(mock_rows: List[Tuple[str, str]], test_path: URL, request: FixtureRequest) -> AppendLog:
    """Create an testing append log with very small threshold."""
    squirrel_dir = f"{test_path}/{SQUIRREL_DIR}"

    alog = AppendLog(squirrel_dir, writing_row_threshold=200)
    with ThreadPoolExecutor(request.param) as executor:
        executor.map(alog.append, mock_rows)
    alog.close()
    return alog


@pytest.fixture
def create_append_log_with_mixed_keys(
    mock_mixed_keys: List[str], mock_time: List[float], mock_sizeof: List[int], test_path: URL
) -> AppendLog:
    """Create an append log with a list of keys of mixed prefix. Used for testing `AppendLog.reconstruct_key_cluster`.
    It should filter out the keys with the prefix that we want.
    """
    squirrel_dir = f"{test_path}/{SQUIRREL_DIR}"
    mock_operations = [1 for _ in range(NUM_OF_ROWS)]
    mock_rows = [
        ALogRow(key=k, operation=o, sizeof=s, timestamp=t)
        for k, o, s, t in zip(mock_mixed_keys, mock_operations, mock_sizeof, mock_time)
    ]
    alog = AppendLog(squirrel_dir, writing_row_threshold=200)
    for row in mock_rows:
        alog.append(row)
    alog.close()
    return alog


def test_row_timestamps() -> None:
    """Test that two consecutively created appendlog rows have different timestamps."""
    row1 = ALogRow(key="key1", operation=1)
    # wait a bit to make sure timestamps should be different, just in case
    sleep(0.25)
    row2 = ALogRow(key="key2", operation=1)
    assert row2.timestamp > row1.timestamp


def test_row_operation() -> None:
    """Test that the operation field is validated."""
    for val in (1, -1):
        ALogRow(key="key", operation=val)

    for val in (2, 100, -3):
        with pytest.raises(ValueError):
            ALogRow(key="key", operation=val)
