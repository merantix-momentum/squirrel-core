"""This module defines shared fixtures that can be used inside a conftest.py like this

from squirrel.integration_test.shared_fixtures import *  # noqa: F401, F403

These fixtures will then be shared with the tests under the scope of the conftest.py.

This is useful for splitting unit and integration tests and defining separate conftest files.
Specific fixtures for integration or unit tests can then be defined in the respective conftest.
"""

import itertools
import os
import random
import time
from typing import List, Tuple
from unittest.mock import MagicMock

import pytest
import random_name
from pytest import FixtureRequest

from squirrel.constants import URL, SQUIRREL_TMP_DATA_BUCKET
from squirrel.fsspec.fs import get_fs_from_url
from squirrel.integration_test.helpers import create_torch_mock

SHAPE = Tuple[int, int, int]

__all__ = ["keys", "array_shape", "test_gcs_url", "test_path", "torch_worker", "torch_world", "torch_mock"]


@pytest.fixture
def keys() -> List[str]:
    """Define shards used in testing."""
    return [str(1), str(2), str(3), str(4)]


@pytest.fixture
def array_shape() -> SHAPE:
    """Set the testing array shape."""
    return 3, 5, 5


@pytest.fixture
def test_gcs_url() -> str:
    """Get the path for testing gcs bucket store. Change of gcs target location should be set here, instead of
    test_path.
    """
    random.seed(os.getpid() + time.time())
    # Use random name as the tmp dataset to avoid reading and writing the same dataset from two endpoints,
    # when there are two users run squirrel tests at the same time.
    tmp_gcs_path = f"{SQUIRREL_TMP_DATA_BUCKET}/test-data/tmp/{random_name.generate_name()}"
    yield tmp_gcs_path
    # teardown
    fs = get_fs_from_url(tmp_gcs_path)
    try:
        fs.rm(tmp_gcs_path, recursive=True)
    except FileNotFoundError:  # some testing functions remove store content, ignore if store no longer exists.
        pass


@pytest.fixture(params=["local", "gcs"])  # The names are only for parametrization, have no logical consequence here.
def test_path(request: FixtureRequest, tmp_path: URL, test_gcs_url: URL) -> URL:
    """
    Use parametrization in pytest to generate two paths consecutively. Any testing function and fixture
    follows this fixture will bifurcated into two, thus automatically test both local and gcs functionalities.
    To turn off the 'gcs' testing, simply remove 'gcs' from params without destroy the overall structure.

    Args:
        request (FixtureRequest): pytest function, to allow UDF accesses properties under Request.
        tmp_path (URL): local temporary path.
        test_gcs_url (URL): testing path on gcs. The content will be erased every time the testing is finished.
    """

    pid = os.getpid()
    local_path = str(tmp_path).rstrip("/") + "/" + str(pid)
    if request.param == "local":
        return local_path
    elif request.param == "gcs":
        return test_gcs_url
    else:  # capture leaky case.
        raise KeyError("Spelling error for fixture parameters.")


@pytest.fixture(params=list(itertools.chain.from_iterable([[(i, j) for i in range(0, j)] for j in range(1, 3)])))
def torch_worker(request: FixtureRequest) -> int:
    """Pytorch worker config (worker_idx, num_worker)."""
    return request.param


@pytest.fixture(params=list(itertools.chain.from_iterable([[(i, j) for i in range(0, j)] for j in range(1, 3)])))
def torch_world(request: FixtureRequest) -> int:
    """Pytorch world config (rank_idx, world_size)."""
    return request.param


@pytest.fixture
def torch_mock(torch_worker: Tuple[int, int], torch_world: Tuple[int, int]) -> MagicMock:
    """Returns a mock of the pytorch module with parallel and distributed worker."""
    return create_torch_mock(torch_worker, torch_world)
