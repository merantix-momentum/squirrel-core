import itertools
from typing import List

import numpy as np
import pytest
import zarr
from pytest import FixtureRequest
from zarr.errors import ReadOnlyError
from zarr.hierarchy import Group

from squirrel.constants import URL
from squirrel.integration_test.helpers import SHAPE
from squirrel.zarr.convenience import get_group, optimize_for_reading
from squirrel.zarr.key import normalize_key
from squirrel.zarr.store import CachedStore, clean_cached_store, copy_store, is_cached, is_squirrel_key


def test_copy_store(create_test_group: Group, get_rw_group: Group) -> None:
    """Test copying a local or gcs zarr store into a local store."""
    copy_store(create_test_group.store, get_rw_group.store)
    assert len(create_test_group.store) == len(get_rw_group.store)


def test_normalize_key() -> None:
    """Test if normalization of a key is consistent"""
    testcases = [
        ("a", "a/"),
        ("a/", "a/"),
        ("/a/", "a/"),
        ("/a", "a/"),
        ("a/ab/", "a/ab/"),
        ("/a/ab/", "a/ab/"),
        ("a/ab/0.0", "a/ab/0.0"),
        ("a/ab/0.0/", "a/ab/0.0"),
        ("/a/ab/0.0/", "a/ab/0.0"),
        (".zarray", ".zarray"),
        ("a/.zarray", "a/.zarray"),
        (".zattrs", ".zattrs"),
        ("a/.zattrs", "a/.zattrs"),
        (".zgroup", ".zgroup"),
        ("a/.zgroup", "a/.zgroup"),
        ("sample_id/file.ext", "sample_id/file.ext/"),  # real use case where we set tile id to original filename
    ]

    for testcase in testcases:
        assert normalize_key(testcase[0]) == testcase[1]


def test_read_from_cached_store(get_cached_store_group: Group, array_shape: SHAPE) -> None:
    """Confirm the previously written array has the desired shape and value."""
    root = get_cached_store_group
    for i in root:
        # check shape
        assert root[i].shape == array_shape
        # test read
        assert np.any(root[i][:] != 0)
    pass


def test_clean_cached_store(get_cached_store_group: Group) -> None:
    """Test clean up a cached store."""
    store = get_cached_store_group.store
    assert is_cached(store)

    store = store.store
    keys = list(store.keys())
    clean_cached_store(store)
    with pytest.raises(KeyError):
        _ = [store[k] for k in keys if is_squirrel_key(k)]
    assert not is_cached(store)


def test_len_in_cached_store(get_r_group: Group, keys: List[str]) -> None:
    """Test len of a cached store. In our testing case, len = # of shard * 3 + 1, not true in general."""
    assert len(get_r_group.store) == len(keys) * 3 + 1


def test_keys_in_cached_store(get_r_group: Group) -> None:
    """Test keys() method in store, should yield a key (str) with each call."""
    keys = list(get_r_group.keys())
    assert len(keys) > 0
    for key in keys:
        assert isinstance(key, str)


def test_del_item_r_group(test_path: URL, create_test_group: Group) -> None:
    """Test if deleting an item to a read only store raises an read only exception."""
    _ = create_test_group
    root = get_group(test_path, "r")
    with pytest.raises(ReadOnlyError):
        del root[list(root)[0]]


def test_set_item_r_group(test_path: URL, create_test_group: Group) -> None:
    """Test if setting an item to a read only store raises an read only exception."""
    _ = create_test_group
    root = get_group(test_path, "r")
    with pytest.raises(ReadOnlyError):
        root["test"] = 1


def test_keys_consistent_for_cached_and_uncached_store(
    get_rw_group: Group,
    get_cached_store_group: Group,
) -> None:
    """Test if listdir works in a equal behavior for CachedStore and for FSStore."""
    root_c = get_cached_store_group
    root_uc = get_rw_group
    st_c = root_c.store
    st_uc = root_uc.store
    assert is_cached(st_c)
    assert not is_cached(st_uc)
    assert st_c.listdir() == st_uc.listdir()
    assert list(root_c) == list(root_uc)
    for key in list(root_uc.keys()):
        assert (root_uc[key][:] == root_c[key][:]).all()
    assert list(sorted(root_c.keys())) == list(sorted(root_uc.keys()))
    assert list(root_c.group_keys()) == list(root_uc.group_keys())


def test_keys_consistent_for_cityscape(
    cityscapes_dataset: Group,
    cityscapes_dataset_optimized: Group,
) -> None:
    """Test if listdir works in a equal behavior for CachedStore and for FSStore."""
    root_c = cityscapes_dataset_optimized
    root_uc = cityscapes_dataset
    st_c = root_c.store
    st_uc = root_uc.store
    assert is_cached(st_c)
    assert not is_cached(st_uc)
    assert st_c.listdir() == st_uc.listdir()
    assert list(root_c.keys()) == list(root_uc.keys())
    keys = [f"{k}/untiled_sample/data" for k in list(root_uc.keys())] + [
        f"{k}/untiled_sample/labels/label_0" for k in list(root_uc.keys())
    ]
    for key in keys:
        assert (root_uc[key][:] == root_c[key][:]).all()
    assert list(sorted(root_c.keys())) == list(sorted(root_uc.keys()))
    assert list(root_c.group_keys()) == list(root_uc.group_keys())


def test_optimize_on_a_read_only_group(get_r_group: Group) -> None:
    """Optimize reading will use cache_meta_store or cached_sharded_store to generate the squirrel cached
    metadata, and write it inside the root of zarr group.
    """
    store = get_r_group.store
    with pytest.raises(ReadOnlyError):
        optimize_for_reading(store)


def test_get_array_cluster(test_path: URL, create_test_group: Group) -> None:
    """Test if array clusters can be calculated."""
    _ = create_test_group
    root = get_group(test_path, "r")
    clusters = root.store.array_cluster()
    for k in list(itertools.chain.from_iterable(clusters)):
        assert k in root


def test_read_from_external_dataset(cityscapes_dataset: Group) -> None:
    """Read from a external test dataset generated from chameleon data transformer (cityscapes 1.2).
    There are 5 items inside. (Use print(root.tree()) to see more details.)
    """
    root = cityscapes_dataset
    assert len(root) == 5


def test_read_from_a_cached_external_dataset(cityscapes_dataset_optimized: Group) -> None:
    """Read from a external test dataset generated from chameleon data transformer (cityscapes 1.2) and cached
    by squirrel.
    """
    root = cityscapes_dataset_optimized
    keys = list(root.keys())
    assert len(keys) == 5


@pytest.fixture(params=["no_shard"])  # not using "shard" since sharding is not working rn
def get_cached_store_group(create_test_group: Group, request: FixtureRequest) -> Group:
    """Create a cached zarr store, and return the group after caching. Here we parametrize the sharding
    strategy into two parts. The first one use squirrel to auto-generate the guessed number of shards, the second one
    use the manually input number of shards instead.
    """
    store = create_test_group.store
    if request.param == "no_shard":
        optimize_for_reading(store, shard=False, cache=True)
    elif request.param == "shard":
        optimize_for_reading(store, shard=True, worker=4, nodes=1, cache=True)
    else:  # capture leaky case, make sure your test is faithful.
        raise KeyError("Spelling error for fixture parameters.")
    store = CachedStore(store)
    return zarr.group(store=store)


@pytest.fixture
def get_rw_group(test_path: URL) -> Group:
    """Test get general purpose store."""
    return get_group(test_path, mode="a")


@pytest.fixture
def get_r_group(test_path: URL, create_test_group: Group) -> Group:
    """Get a read-only group with content."""
    _ = list(create_test_group)  # get a store with actual content
    g = get_group(test_path, mode="r")
    return g
