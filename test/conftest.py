"""This module defines specific fixtures for unit tests. Shared fixtures are defined in shared_fixtures.py.

###################################
Please do not import from this file.
###################################

Not importing from conftest is a best practice described in the note here:
https://pytest.org/en/6.2.x/writing_plugins.html#conftest-py-local-per-directory-plugins
"""

import json
import logging
import os
import pathlib
import subprocess
import random
import tempfile
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING, List, Tuple
from uuid import uuid4

from faker import Faker
import fsspec
import numpy as np
import pytest
from pytest import FixtureRequest, TempPathFactory
from zarr.hierarchy import Group

from squirrel.catalog import Catalog, Source
from squirrel.constants import URL
from squirrel.driver import JsonlDriver, MessagepackDriver
from squirrel.integration_test.helpers import SHAPE, get_sample
from squirrel.integration_test.shared_fixtures import *  # noqa: F401, F403
from squirrel.iterstream import Composable, IterableSource
from squirrel.serialization import JsonSerializer, MessagepackSerializer
from squirrel.store import FilesystemStore
from squirrel.store.squirrel_store import SquirrelStore
from squirrel.zarr.group import get_group

if TYPE_CHECKING:
    from squirrel.constants import SampleType

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
def dummy_sq_store(test_path: URL, num_samples: int, array_shape: SHAPE) -> FilesystemStore:
    """Create a dummy SquirrelStore with MessagepackSerializer."""
    samples = [get_sample(array_shape) for _ in range(num_samples)]
    store = SquirrelStore(f"{test_path}/sq_store/", MessagepackSerializer())
    for sample in samples:
        store.set(value=sample, key=sample["key"])
    return store


@pytest.fixture
def dummy_msgpack_store(test_path: URL, num_samples: int, array_shape: SHAPE) -> SquirrelStore:
    """Create a SquirrelStore with msgpack serialization."""
    samples = [get_sample(array_shape) for _ in range(num_samples)]
    shards = [samples[: len(samples) // 2], samples[len(samples) // 2 :]]
    store = SquirrelStore(f"{test_path}/msgpack_store/", serializer=MessagepackSerializer())
    for shard_key, shard in enumerate(shards):
        store.set(value=shard, key=str(shard_key))
    return store


@pytest.fixture
def dummy_jsonl_store(test_path: URL, num_samples: int, array_shape: SHAPE) -> SquirrelStore:
    """Create a SquirrelStore with json serialization."""
    samples = [get_sample(array_shape) for _ in range(num_samples)]
    shards = [samples[: len(samples) // 2], samples[len(samples) // 2 :]]
    store = SquirrelStore(f"{test_path}/json_store/", serializer=JsonSerializer())
    for shard_key, shard in enumerate(shards):
        store.set(value=shard, key=str(shard_key))
    return store


@pytest.fixture
def custom_jsonl_store(test_path: URL, num_samples: int) -> SquirrelStore:
    """Create a SquirrelStore with json serialization."""

    def get_sample(shape: Tuple) -> SampleType:
        """Generate a single sample"""
        return {
            "image": np.random.randint(low=0, high=256, size=shape).tolist(),
            "label": np.random.choice([0, 1]),
            "meta": {"dtype": "uint8", "shape": shape},
        }

    samples = [get_sample((3, np.random.randint(32, 64), np.random.randint(32, 64))) for _ in range(num_samples)]
    shards = [samples[: len(samples) // 2], samples[len(samples) // 2 :]]
    store = SquirrelStore(test_path, JsonSerializer())
    for key, shard in enumerate(shards):
        store.set(value=shard, key=f"shard_{key}")
    return store


@pytest.fixture(params=["jsonl", "msgpack", "in-memory"])
def create_all_iterable_source(
    request: FixtureRequest, dummy_msgpack_store: SquirrelStore, dummy_jsonl_store: SquirrelStore, array_shape: SHAPE
) -> Composable:
    """Returns three different iterable sources in formats of `jsonl`, `msgpack` and in-memory `nd-arrays`."""
    if request.param == "msgpack":
        return MessagepackDriver(url=dummy_msgpack_store.url).get_iter()
    elif request.param == "jsonl":
        return JsonlDriver(url=dummy_jsonl_store.url).get_iter()
    else:
        return (
            IterableSource([get_sample(array_shape) for _ in range(1000)])
            .batched(10)
            .shuffle(10)
            .async_map(lambda x: x)
        )


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


@pytest.fixture
def local_msgpack_url(num_samples: int) -> str:
    """Create a temporary directory, write some dummy data to it and yield it"""
    with tempfile.TemporaryDirectory() as tmp_dir:
        store = SquirrelStore(url=tmp_dir, serializer=MessagepackSerializer())
        IterableSource([{f"_{i}": i} for i in range(num_samples)]).batched(2, drop_last_if_not_full=False).async_map(
            store.set
        ).join()
        yield tmp_dir


def _write_dummy_data(path: str, num_samples: int, compression: str = "gzip") -> None:
    fkr = Faker(["en_US", "de_DE", "fr_FR"])
    data_set = path.split("/")[-3]
    lines = [
        {
            "text": fkr.text(),
            "meta": {"src": path, "sha": uuid4().hex, "id": idx, "dataset": data_set},
        }
        for idx in range(num_samples)
    ]
    with fsspec.open(path, mode="wb", compression=compression) as fh:
        for line in lines:
            fh.write((json.dumps(line) + "\n").encode("utf-8"))


@pytest.fixture(scope="session")
def dummy_data_catalog(tmp_path_factory: TempPathFactory) -> Catalog:
    """Create a dummy catalog with test data and more."""
    td_0: pathlib.Path = tmp_path_factory.mktemp("data_")
    td_1: pathlib.Path = tmp_path_factory.mktemp("data_")
    td_2: pathlib.Path = tmp_path_factory.mktemp("data_")

    f_names_0 = [f"{td_0.resolve()}/train/{idx:05d}.jsonl.gz" for idx in range(5)]
    f_names_1 = [f"{td_1.resolve()}/train/{idx:05d}.jsonl.gz" for idx in range(3)]
    f_names_2 = [f"{td_2.resolve()}/train/{idx:05d}.jsonl.gz" for idx in range(10)]

    rng = random.Random(42)
    d0_counts = 0
    for path in f_names_0:
        f_cnt = rng.randint(32, 64)
        d0_counts += f_cnt
        _write_dummy_data(path, f_cnt, compression="gzip")

    d1_counts = 0
    for path in f_names_1:
        f_cnt = rng.randint(32, 64)
        d1_counts += f_cnt
        _write_dummy_data(path, f_cnt, compression="gzip")

    d2_counts = 0
    for path in f_names_2:
        f_cnt = rng.randint(32, 64)
        d2_counts += f_cnt
        _write_dummy_data(path, f_cnt, compression="gzip")

    cat = Catalog()

    cat["data_0"] = Source(
        "jsonl",
        driver_kwargs={
            "url": f"file://{td_0.resolve()}/train",
            "deser_hook": None,
            "storage_options": {},
        },
        metadata={"num_samples": d0_counts, "num_shards": len(f_names_0)},
    )
    cat["data_1"] = Source(
        "jsonl",
        driver_kwargs={
            "url": f"file://{td_1.resolve()}/train",
            "deser_hook": None,
            "storage_options": {},
        },
        metadata={"num_samples": d1_counts, "num_shards": len(f_names_1)},
    )

    cat["data_2"] = Source(
        "jsonl",
        driver_kwargs={
            "url": f"file://{td_2.resolve()}/train",
            "deser_hook": None,
            "storage_options": {},
        },
        metadata={"num_samples": d2_counts, "num_shards": len(f_names_2)},
    )

    return cat
