import collections
import itertools
import multiprocessing
import pickle
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from typing import Any, List, Tuple
from unittest import mock

import numpy as np
import pytest
from zarr.hierarchy import Group

from squirrel.constants import URL
from squirrel.dataset.zarr_dataset import ZarrDataset, ZarrItemFetcher
from squirrel.integration_test.helpers import SHAPE, create_torch_mock
from squirrel.zarr.store import normalize_key


def assert_retrieve_zarr_fetcher_instance(val: Any) -> None:
    """
    Retrieve() for ZarrItemFetcher should return a list of Tuple[str, np.ndarray, dict] object. This convenience
    function is used to check for each retrieved val, if they conform with the typing.
    NB. Notice that this is not a testing function (not name.startswith('test'))
    """
    assert isinstance(val[0], str)
    assert isinstance(val[1], np.ndarray)
    assert isinstance(val[2], dict)


def assert_resultlist_fits_to_keylist(vals: List[Any], keys: List[str], ignore_order: bool = True) -> None:
    """
    Tests if items returned from a ZarrStream match to a list of keys.
    Assumes that the dataset was created by test.test_zarr_dataset.zarr_test_dataset or
    test.test_zarr_store.create_test_group. Moreover, it assumes that
    squirrel.dataset.zarr_dataset.ZarrItemFetcher was used for fetching.

    Args:
        vals: List of objects returned by a ZarrStream.
              Assumes that the dataset was created by test.test_zarr_dataset.zarr_test_dataset
              or test.test_zarr_store.create_test_group. Moreover, it assumes that
              squirrel.dataset.zarr_dataset.ZarrItemFetcher was used for fetching.
        keys: list of keys
        ignore_order: if True ignore ordering of lists
    """
    assert len(vals) == len(keys)

    keys = [normalize_key(k) for k in keys]
    keys_of_vals = [normalize_key(v[2]["key"]) for v in vals]

    if ignore_order:
        assert collections.Counter(keys_of_vals) == collections.Counter(keys)
    else:
        for idx, k in enumerate(keys):
            assert k == keys_of_vals[idx]


def assert_retrieve_zarr_fetcher_inistance(val: Any) -> None:
    """
    Retrieve() for ZarrItemFetcher should return a list of Tuple[str, np.ndarray, dict] object. This convenience
    function is used to check for each retrieved val, if they conform with the typing.

    NB. Notice that this is not a testing function (not name.startswith('test'))
    """
    assert isinstance(val[0], str)
    assert isinstance(val[1], np.ndarray)
    assert isinstance(val[2], dict)


def test_pickle_zarr_dataset(zarr_test_dataset: ZarrDataset, zarr_test_dataset_fetcher: ZarrItemFetcher) -> None:
    """Test if ZarrDataset is pickleable."""
    with zarr_test_dataset.get_stream(fetcher=zarr_test_dataset_fetcher) as _:
        pass
    _ = zarr_test_dataset.get_group()
    _ = pickle.dumps(zarr_test_dataset)


@pytest.mark.parametrize("batch_size", [2, 3])
def test_pytorch_key_cluster_fixlastbatch(
    zarr_test_dataset: ZarrDataset, torch_worker: Tuple[int, int], torch_world: Tuple[int, int], batch_size: int
) -> None:
    """Test if pytorch_key_cluster fixes last batch."""
    all_keys = list(zarr_test_dataset.get_group(mode="r"))
    for t_rank_idx in range(torch_world[1]):
        for t_worker_idx in range(torch_worker[1]):
            tm = create_torch_mock((t_worker_idx, torch_worker[1]), (t_rank_idx, torch_world[1]))
            with mock.patch.dict("sys.modules", torch=tm):
                a_keys = zarr_test_dataset.pytorch_key_cluster(
                    all_keys, split_by_worker=True, split_by_node=True, batch_size=batch_size
                )
                assert len(a_keys) % batch_size == 0


def _read_keys_like_pytorch_dataloader_worker(
    a_keys: List[str], ds: ZarrDataset, zarr_test_dataset_fetcher: ZarrItemFetcher
) -> List[Tuple[str, np.ndarray, dict]]:
    with ds.get_stream(fetcher=zarr_test_dataset_fetcher) as zds:
        for k in a_keys:
            zds.request(k)

        vals = []
        for _ in a_keys:
            val = zds.retrieve()
            vals.append(val)

    assert_resultlist_fits_to_keylist(vals, a_keys, ignore_order=False)
    return vals


def test_pytorch_dataloader_like_read_parallel_stream(
    zarr_test_dataset: ZarrDataset,
    torch_worker: Tuple[int, int],
    torch_world: Tuple[int, int],
    zarr_test_dataset_fetcher: ZarrItemFetcher,
) -> None:
    """Test parallel reading by shards from a stream."""
    # TODO broken test
    all_keys = list(zarr_test_dataset.get_group(mode="r"))
    # Tries to replicate how a pytorch dataloader uses a squirrel stream
    with ProcessPoolExecutor(mp_context=multiprocessing.get_context("forkserver")) as executor:
        futures = []
        for t_rank_idx in range(torch_world[1]):
            for t_worker_idx in range(torch_worker[1]):
                # create key list here since mock does not work in parallel
                tm = create_torch_mock((t_worker_idx, torch_worker[1]), (t_rank_idx, torch_world[1]))
                with mock.patch.dict("sys.modules", torch=tm):
                    a_keys = zarr_test_dataset.pytorch_key_cluster(all_keys, split_by_worker=True, split_by_node=True)
                futures.append(
                    executor.submit(
                        _read_keys_like_pytorch_dataloader_worker, a_keys, zarr_test_dataset, zarr_test_dataset_fetcher
                    )
                )

        result_vals = []
        for future in futures:
            vals = future.result()
            for val in vals:
                assert_retrieve_zarr_fetcher_inistance(val)
            result_vals.append(vals)

    # check if same number of keys were returned as asked for
    merged_result_vals = list(itertools.chain.from_iterable(result_vals))
    assert len(merged_result_vals) == len(all_keys)

    # check if returned keys fit requested keys
    assert_resultlist_fits_to_keylist(merged_result_vals, all_keys)


@pytest.mark.parametrize("key_repeats", list(range(2)))
def test_streaming_item_bulk_retrieve(
    zarr_test_dataset: ZarrDataset, zarr_test_dataset_fetcher: ZarrItemFetcher, key_repeats: int
) -> None:
    """Test iterating through a zarr group via a stream."""
    all_keys_original = list(zarr_test_dataset.get_group(mode="r"))

    # repeat keys to check if multiple keys can be queried and cache works correctly
    all_keys = []
    for k in all_keys_original:
        all_keys += [k for i in range(key_repeats + 1)]

    with zarr_test_dataset.get_stream(zarr_test_dataset_fetcher) as zds:
        for k in all_keys:
            zds.request(k)
        vals = []
        for _ in all_keys:
            vals.append(zds.retrieve())
        for val in vals:
            assert_retrieve_zarr_fetcher_instance(val)

    # check if returned keys fit requested keys
    assert_resultlist_fits_to_keylist(vals, all_keys, ignore_order=False)


def test_streaming_item_instant_retrieve(
    zarr_test_dataset: ZarrDataset, zarr_test_dataset_fetcher: ZarrItemFetcher
) -> None:
    """Test iterateing through a zarr group via a stream."""
    root = zarr_test_dataset.get_group(mode="r")
    with zarr_test_dataset.get_stream(fetcher=zarr_test_dataset_fetcher) as zds:
        for k in root.keys():
            zds.request(k)
            val = zds.retrieve()
            assert_retrieve_zarr_fetcher_instance(val)


def test_get_zarr_group(zarr_test_dataset: ZarrDataset) -> None:
    """Test getting a zarr group from a zarr dataset."""
    root = zarr_test_dataset.get_group(mode="a")
    assert isinstance(root, Group)


def test_optimize_for_reading_on_zarr_dataset(
    zarr_test_dataset: ZarrDataset, torch_worker: Tuple[int, int], torch_world: Tuple[int, int]
) -> None:
    """Call is_cached method on instance of ZarrDataset before and after calling optimize_for_reading"""
    assert zarr_test_dataset.is_cached() is False
    zarr_test_dataset.optimize_for_reading(shard=False, worker=torch_worker[1], nodes=torch_world[1], cache=True)
    assert zarr_test_dataset.is_cached() is True


@pytest.fixture
def zarr_test_dataset(test_path: URL, array_shape: SHAPE, keys: List[str]) -> ZarrDataset:
    """Construct a store at test_path."""
    # clean-up previous existing group.
    zd = ZarrDataset(test_path)
    root = zd.get_group(mode="a", overwrite=True)

    # write
    def write_shard(key: str, root: Group, shape: SHAPE = array_shape) -> None:
        z = root.zeros(key, shape=shape)
        z[:] = np.random.randint(0, 255, size=shape, dtype="|i2")
        z.attrs["dummy_meta"] = "a" * 5
        z.attrs["key"] = key

    with ThreadPoolExecutor() as executor:
        futures = []

        # fill shards
        for key in list(keys):  # flatten shards
            futures.append(executor.submit(write_shard, key, root))
        for future in futures:
            future.result()

    # test if store length is equal to items that were added to the store
    assert len(root) == len(keys)
    assert len(zd.get_group(mode="r")) == len(keys)

    return zd


@pytest.fixture
def zarr_test_dataset_fetcher(test_path: URL) -> ZarrItemFetcher:
    """Returns an instance of ZarrItemFetcher using test_path"""
    return ZarrItemFetcher(path=test_path)


@pytest.fixture
def create_write_parallel_stream(test_path: URL, array_shape: SHAPE, keys: List[str]) -> Group:
    """Construct a store at test_path using a parallel stream."""

    # clean-up previous existing group.
    zd = ZarrDataset(test_path)
    _ = zd.get_group(mode="a", overwrite=True)

    # write
    def write_shard(key: str, zds: ZarrDataset, shape: SHAPE = array_shape) -> None:
        val = (np.random.randint(0, 255, size=shape, dtype="|i2"), {"a" * 10})
        zds.write(key, val)

    with ThreadPoolExecutor() as executor:
        futures = []

        # fill shards
        with zarr_test_dataset.get_stream(zarr_test_dataset_fetcher) as zds:
            for key in keys:  # flatten shards
                futures.append(executor.submit(write_shard, key, zds))
            for future in futures:
                future.result()

    # wait until parallel write done
    while len(zd.get_group(mode="a")) != len(keys):
        time.sleep(0.1)

    return zd
