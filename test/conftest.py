"""This module defines specific fixtures for unit tests. Shared fixtures are defined in shared_fixtures.py.

###################################
Please do not import from this file.
###################################

Not importing from conftest is a best practice described in the note here:
https://pytest.org/en/6.2.x/writing_plugins.html#conftest-py-local-per-directory-plugins
"""

import logging
import os
import subprocess
from concurrent.futures import ThreadPoolExecutor
from typing import List

import numpy as np
import pytest
from pytest import FixtureRequest
from zarr.hierarchy import Group

from squirrel.constants import URL, SHAPE
from squirrel.integration_test.shared_fixtures import *  # noqa: F401, F403
from squirrel.zarr.group import get_group

logger = logging.getLogger(__name__)


@pytest.fixture(params=[1, 2, 3])
def shards(request: FixtureRequest) -> int:
    """Number of shards."""
    return request.param


@pytest.fixture
def create_test_group(test_path: URL, array_shape: SHAPE, keys: List[str]) -> Group:
    """Construct one store for each location listed in test_path."""

    # clean-up previous existing group.
    _ = get_group(test_path, mode="a", overwrite=True)

    # write
    def write_shard(key: str, shape: SHAPE = array_shape) -> None:
        root = get_group(test_path, mode="a")
        z = root.zeros(key, shape=shape)
        z[:] = np.random.randint(0, 255, size=shape, dtype="|i2")
        z.attrs["dummy_meta"] = "a" * 10
        z.attrs["key"] = key

    # write items in parallel
    with ThreadPoolExecutor() as executor:
        futures = []
        for key in keys:
            futures.append(executor.submit(write_shard, key))
        for future in futures:
            future.result()

    return get_group(test_path, mode="a", overwrite=False)


@pytest.fixture
def num_samples() -> int:
    """Number of samples used for record store, msgpack store, and jsonl store"""
    return 4


@pytest.fixture
def toggle_wandb() -> None:
    """Turn wandb into offline mode in test. And restore its previous state if being set, otherwise, turn back to online
    mode again.
    """
    MODE = os.environ.get("WANDB_MODE")
    subprocess.run(["wandb", "offline"])
    yield None
    subprocess.run(["wandb", MODE if MODE is not None else "online"])
