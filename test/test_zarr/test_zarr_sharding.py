import collections
import itertools
import unittest.mock as mock
from typing import List, Tuple

import pytest
from zarr.hierarchy import Group

from squirrel.zarr.convenience import optimize_for_reading
from squirrel.zarr.sharding import (
    infer_num_splits,
    is_sharded,
    split_keys_into_key_clusters,
    suggest_shards_by_num_shards,
    suggest_shards_by_worker_config,
)
from squirrel.zarr.store import CachedStore


@pytest.mark.skip
def test_is_sharded(create_test_group: Group, shards: int) -> None:  # noqa F811
    """Test if group is sharded after sharding and not before sharding"""

    assert not is_sharded(create_test_group.store)

    optimize_for_reading(create_test_group.store, shard=True, cache=True)

    cs = CachedStore(create_test_group.store)
    assert is_sharded(cs)


@pytest.mark.skip
@pytest.mark.parametrize("split_by_worker", [True, False])
@pytest.mark.parametrize("split_by_node", [True, False])
@pytest.mark.parametrize("enable_mock_torch", [True, False])
def test_infer_num_splits(
    split_by_worker: bool,
    split_by_node: bool,
    torch_mock: mock.MagicMock,
    enable_mock_torch: bool,
    torch_worker: Tuple[int, int],
    torch_world: Tuple[int, int],
) -> None:
    """Tests sanity for returned values of infer_num_splits"""

    if enable_mock_torch:
        with mock.patch.dict("sys.modules", torch=torch_mock):
            idx, n_workers = infer_num_splits(split_by_worker=split_by_worker, split_by_node=split_by_node)

        # check if calculated number of workers is correct
        if split_by_node and split_by_worker:
            assert n_workers == torch_world[1] * torch_worker[1]
        elif split_by_node:
            assert n_workers == torch_world[1]
        elif split_by_worker:
            assert n_workers == torch_worker[1]
    else:
        idx, n_workers = infer_num_splits(split_by_worker=split_by_worker, split_by_node=split_by_node)
        # check if calculated number of workers is correct
        assert idx == 0
        assert n_workers == 1

    # check sanity for idx
    assert idx >= 0
    assert n_workers >= idx


@pytest.mark.skip
@pytest.mark.parametrize("keys", [["1" * i, "2" * i] for i in range(10)])
@pytest.mark.parametrize("n_splits", [1, 2])
@pytest.mark.parametrize("shuffle", [True, False])
def test_split_key_cluster(keys: List[str], n_splits: int, shuffle: bool) -> None:
    """Tests if requested splits match specifications"""

    c_splits = split_keys_into_key_clusters(keys=keys, n_splits=n_splits, shuffle=shuffle)

    # test if requested splits were returned
    assert len(c_splits) == n_splits

    m_c_splits = list(itertools.chain.from_iterable(c_splits))

    # test if number of elements in returned splits is equal to the number of requested elements
    assert len(keys) == len(m_c_splits)

    # test if the same elements are in requested and result lists
    assert collections.Counter(keys) == collections.Counter(m_c_splits)


@pytest.mark.skip
@pytest.mark.parametrize("shuffle_keys", [True, False])
def test_suggest_shards_by_worker_config(
    torch_worker: Tuple[int, int], torch_world: Tuple[int, int], shuffle_keys: bool, create_test_group: Group
) -> None:  # noqa F811
    """Test creation of a shard routing table"""

    routing = suggest_shards_by_worker_config(
        create_test_group.store, worker=torch_worker[1], nodes=torch_world[1], shuffle_keys=shuffle_keys
    )
    m_routing = list(itertools.chain.from_iterable(routing))

    # test if requested splits were returned
    assert len(routing) % (torch_worker[1] * torch_world[1]) == 0

    # test if number of elements in returned splits is equal to the number of requested elements
    assert collections.Counter(create_test_group.store.keys()) == collections.Counter(m_routing)


@pytest.mark.skip
@pytest.mark.parametrize("shuffle_keys", [True, False])
def test_suggest_shards_by_num_shards(shards: int, shuffle_keys: bool, create_test_group: Group) -> None:  # noqa F811
    """Test creation of a shard routing table"""

    routing = suggest_shards_by_num_shards(create_test_group.store, num_shards=shards, shuffle_keys=shuffle_keys)
    m_routing = list(itertools.chain.from_iterable(routing))

    # test if requested splits were returned
    assert len(routing) == shards

    # test if number of elements in returned splits is equal to the number of requested elements
    assert collections.Counter(create_test_group.store.keys()) == collections.Counter(m_routing)
