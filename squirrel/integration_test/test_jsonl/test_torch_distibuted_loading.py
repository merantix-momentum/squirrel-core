import concurrent.futures as futures
import json
import os
import random
import tempfile
from collections import Counter
from typing import Generator, Iterator, Tuple

import pytest
import torch.utils.data as tud

from squirrel.driver import JsonlDriver
from squirrel.iterstream.torch_composables import SplitByWorker, TorchIterable, skip_k
from squirrel.serialization import JsonSerializer
from squirrel.store import SquirrelStore

N_SHARDS = 20
MIN_SAMPLES_PER_SHARD = 50
MAX_SAMPLES_PER_SHARD = 100


@pytest.fixture(scope="module", autouse=True)
def test_data() -> Iterator[Tuple[str, int]]:
    """Fixture for this modules test data"""

    test_folder = tempfile.TemporaryDirectory()

    n_samples = create_data(test_folder=test_folder.name)
    yield test_folder.name, n_samples

    test_folder.cleanup()


def create_data(test_folder: str) -> int:
    """Helper function to create test data."""

    def create_shard(id_: int, min: int, max: int) -> Tuple[int, int]:
        num_samples = random.Random().randint(a=min, b=max)
        store = SquirrelStore(url=test_folder, serializer=JsonSerializer())
        shard = [{"shard_idx": id_, "sample_idx": idx} for idx in range(num_samples)]
        store.set(key=str(id_), value=shard)
        return id_, num_samples

    os.makedirs(test_folder, exist_ok=True)

    with futures.ThreadPoolExecutor(max_workers=4) as pool:
        futs = [pool.submit(create_shard, idx, MIN_SAMPLES_PER_SHARD, MAX_SAMPLES_PER_SHARD) for idx in range(N_SHARDS)]

    n_samples = 0
    for fut in futures.as_completed(futs):
        shard_id, _n_samps = fut.result()
        assert MIN_SAMPLES_PER_SHARD <= _n_samps <= MAX_SAMPLES_PER_SHARD
        n_samples += _n_samps

    return n_samples


def test_JSONSource(test_data: Generator) -> None:
    """Quick sanity test that the data can be loaded and a fixed number of samples taken."""
    test_data_folder, _ = test_data

    it = JsonlDriver(test_data_folder).get_iter().take(n=N_SHARDS * MIN_SAMPLES_PER_SHARD)

    cntr = sum(1 for _ in it)
    assert cntr == N_SHARDS * MIN_SAMPLES_PER_SHARD


@pytest.mark.parametrize("take_samples", [None, N_SHARDS * MIN_SAMPLES_PER_SHARD])
def test_multi_worker(test_data: Generator, take_samples: int) -> None:
    """Test that we can load the data across multiple PyTorch dataloaders workers in a single node setup"""
    test_data_folder, total_samples = test_data

    it = (
        JsonlDriver(test_data_folder)
        # In order to not have duplication of all kinds of buffers, we need to limit the buffer size
        # Increasing the buffer will lead to failing tests due to over-sampling.
        .get_iter(shuffle_key_buffer=0, prefetch_buffer=0, shuffle_item_buffer=0, max_workers=0)
        .compose(SplitByWorker)
        .shuffle(size=400)
        .take(n=N_SHARDS * MIN_SAMPLES_PER_SHARD)
        .compose(TorchIterable)
    )

    dl = tud.DataLoader(it, batch_size=None, num_workers=4)
    elems = [json.dumps(item) for item in dl]
    cntr = Counter(elems)

    assert min(cntr.values()) == 1
    assert max(cntr.values()) == 1
    assert len(cntr) == total_samples if take_samples is None else take_samples


@pytest.mark.parametrize("take_samples", [None, N_SHARDS * MIN_SAMPLES_PER_SHARD])
def test_multi_worker_multi_rank(test_data: Generator, take_samples: int) -> None:
    """Test we can load the data correctly in a multi-node and multi-worker setting."""
    test_data_folder, total_samples = test_data

    world_size = 2
    num_workers = 4
    _takeN = take_samples // (num_workers * world_size) if take_samples else None

    it_0 = (
        JsonlDriver(test_data_folder)
        # In order to not have duplication of all kinds of buffers, we need to limit the buffer size
        # Increasing the buffer will lead to failing tests due to over-sampling.
        .get_iter(
            shuffle_key_buffer=0,
            key_hooks=[skip_k(rank=0, world_size=world_size)],
            prefetch_buffer=0,
            shuffle_item_buffer=0,
            max_workers=0,
        )
        # Ensure SplitByWorker is after loading so the data within the shards can be nicely distributed across workers
        # If this is not the case, we distribute different shards to different workers, and then run into the problem
        # that some workers are not getting enough shards to fulfill the TakeN requirements.
        .compose(SplitByWorker)
        # Ensure Shuffling is after worker-split, so the same element does not get leaked to several workers
        .shuffle(size=400)
        # Ensure TakeN is after shuffle to get different shuffled samples each time.
        .take(n=_takeN).compose(TorchIterable)
    )

    it_1 = (
        JsonlDriver(test_data_folder)
        .get_iter(
            shuffle_key_buffer=0,
            key_hooks=[skip_k(rank=1, world_size=world_size)],
            prefetch_buffer=0,
            shuffle_item_buffer=0,
            max_workers=0,
        )
        .compose(SplitByWorker)
        .shuffle(size=400)
        .take(n=_takeN)
        .compose(TorchIterable)
    )

    dl_0 = tud.DataLoader(it_0, batch_size=None, num_workers=num_workers)
    dl_1 = tud.DataLoader(it_1, batch_size=None, num_workers=num_workers)
    elems_0 = [json.dumps(item) for item in dl_0]
    elems_1 = [json.dumps(item) for item in dl_1]

    assert set(elems_0).intersection(set(elems_1)) == set()
    cntr_0 = Counter(elems_0)
    cntr_1 = Counter(elems_1)

    assert min(cntr_0.values()) == 1
    assert max(cntr_0.values()) == 1

    assert min(cntr_1.values()) == 1
    assert max(cntr_1.values()) == 1

    if _takeN is not None:
        assert len(cntr_0) == num_workers * _takeN
        assert len(cntr_1) == num_workers * _takeN

    expected = total_samples if take_samples is None else take_samples
    assert len(cntr_0) + len(cntr_1) == expected
