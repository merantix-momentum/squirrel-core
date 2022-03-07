import logging
import random
from typing import List, MutableMapping, Tuple

from zarr.storage import getsize

import squirrel.zarr.store as squirrel_zs

logger = logging.getLogger(__name__)


def is_sharded(store: MutableMapping) -> bool:
    """Returns true if store is sharded."""
    return (
        isinstance(store, squirrel_zs.CachedStore)
        and squirrel_zs.CachedStore.CPREFIX + squirrel_zs.CachedStore.CSHARD + "_0" + squirrel_zs.CachedStore.CROUTING
        in store.store
    )


def infer_num_splits(
    split_by_worker: bool = True, split_by_node: bool = True, torch_dist_group: str = None
) -> Tuple[int, int]:
    """Infer the current split and the total number of splits using pytorch if available, otherwise (0, 1).

    Args:
        split_by_worker (bool, optional): If True, split by torch worker. Defaults to True.
        split_by_node (bool, optional): If True, split by torch node. Defaults to True.
        torch_dist_group (str, optional): torch distributed group used for `split_by_node`. Defaults to
            torch.distributed.group.WORLD.

    Returns:
        Tuple[int, int]: Tuple of (current split index, total number of splits).
    """
    rank = (0, 1)  # (rank, world_size)
    worker = (0, 1)  # (id, num_workers)

    try:
        import torch

        if split_by_node and torch.distributed.is_available() and torch.distributed.is_initialized():
            group = torch_dist_group or torch.distributed.group.WORLD
            rank = torch.distributed.get_rank(group=group), torch.distributed.get_world_size(group=group)
            logger.debug(f"split keys to different nodes with rank {rank} in group {group}")

        if split_by_worker:
            worker_info = torch.utils.data.get_worker_info()
            if worker_info is not None:  # if multithreading
                worker = worker_info.id, worker_info.num_workers
            logger.debug(f"split keys to different workers with worker {worker}")
    except ModuleNotFoundError:
        pass
    return rank[0] * worker[1] + worker[0], worker[1] * rank[1]


def split_keys_into_key_clusters(
    keys: List[str], n_splits: int, shuffle: bool = False, seed: int = 0
) -> List[List[str]]:
    """Split a list of keys into key clusters (a list of list of keys).

    Args:
        keys (List[str]): A list of keys.
        n_splits (int): Number of output splits.
        shuffle (bool, optional): If True, shuffle keys without loss of access performance. To maintain efficient
            reading, first the shards are shuffled and then the keys within the shards are shuffled. Defaults to False.
        seed (int, optional): Seed used for shuffling. Useful for getting different splits per epoch. Defaults to 0.

    Returns:
        List[List[str]]: `n_splits` array clusters.
    """

    res = [[] for i in range(n_splits)]
    num_samples = len(keys)
    samples_per_split = num_samples // n_splits
    if samples_per_split <= 0:
        samples_per_split = 1

    if shuffle:
        rnd_gen = random.Random(seed)
        rnd_gen.shuffle(keys)

    split_i = 0
    for k in keys:
        res[split_i].append(k)
        if len(res[split_i]) == samples_per_split and split_i + 1 < n_splits:
            split_i += 1
    return res


def even_keys_by_batch_size(keys: List[str], batch_size: int) -> List[str]:
    """Make the number of keys in a list even to a multiple of a given batch_size.

    Args:
        keys (List[str]): A list of keys.
        batch_size (int): A given integer indicating the batch size of the training.

    Returns:
        List(int): Trim the tailing number of keys such that total number of the key list is a multiple of the given
            batch size.
    """
    assert batch_size >= 1
    n_even = (len(keys) // batch_size) * batch_size
    n_discard = len(keys) - n_even
    keys = keys[:n_even]
    logger.warning(
        f"Tail {n_discard} key clusters has been excluded to make the key cluster evenly distributable among nodes and "
        f"workers."
    )
    return keys


def suggest_shards_by_worker_config(
    store: MutableMapping, worker: int = 1, nodes: int = 1, max_size: int = 1000, shuffle_keys: bool = False
) -> List[List[str]]:
    """Get a grouping of keys to create shards based on the provided reader configuration.

    Args:
        store (MutableMapping): Store for which shards will be suggested.
        worker (int, optional): Number of workers to be used per node when reading data from the store. Defaults to 1.
        nodes (int, optional): Number of nodes to be used when reading data from the store. Defaults to 1.
        max_size (int, optional): Maximum total size of a shard in bytes. Defaults to 1000.
        shuffle_keys (bool, optional): Whether to shuffle keys in a shard. Defaults to False.

    Raises:
        ValueError: If the number of items in store is less than `worker * nodes`.

    Returns:
        List[List[str]]: List of keys for each shard.
    """
    # getsize just works on zarr groups
    k_groups = {k.rsplit("/", 1)[0] for k in store.keys()}
    k_size = {
        k: getsize(store, path=k)
        for k in k_groups
        if not squirrel_zs.is_zarr_key(k) and not squirrel_zs.is_squirrel_key(k)
    }
    total_size = sum([k_size[k] for k in k_size.keys()])

    # at least a shard per worker
    n_shards = worker * nodes

    if n_shards > len(k_groups):
        raise ValueError(f"Too many worker={worker} and nodes={nodes} for the length={len(k_groups)} of the store")

    # add a shard to each worker until approximately shards are smaller than max_size
    while total_size / n_shards > max_size:
        n_shards += worker * nodes

    # finetune n_shards by creating real sharding configuration
    while True:
        test_shards = suggest_shards_by_num_shards(store, n_shards, shuffle_keys=shuffle_keys)

        est_max_shard_size = 0
        for test_shard in test_shards:
            test_shard_k_groups = {k.rsplit("/", 1)[0] for k in test_shard}
            est_shard_size = sum(
                [
                    k_size[k]
                    for k in test_shard_k_groups
                    if not squirrel_zs.is_zarr_key(k) and not squirrel_zs.is_squirrel_key(k)
                ]
            )
            if est_shard_size > est_max_shard_size:
                est_max_shard_size = est_shard_size

        if est_max_shard_size <= max_size:
            logger.debug(f"selected {n_shards} shards for sharding config")
            return test_shards

        n_shards += worker * nodes


def suggest_shards_by_num_shards(store: MutableMapping, num_shards: int, shuffle_keys: bool = False) -> List[List[str]]:
    """Get a grouping of keys to create shards based on the target number of shards.

    Args:
        store (MutableMapping): Store for which shards will be suggested.
        num_shards (int): Number of shards to have.
        shuffle_keys (bool, optional): Whether to shuffle keys in a shard. Defaults to False.

    Returns:
        List[List[str]]: List of keys for each shard.
    """
    keys = list(store.keys())
    samples = list(dict.fromkeys([k.rsplit("/", 1)[0] for k in keys if not squirrel_zs.is_squirrel_key(k)]))
    # samples = keys without duplicate and the squirrel key.

    if shuffle_keys:
        random.shuffle(samples)

    routing = [[] for i in range(num_shards)]
    i = 0
    for s in samples:
        for k in [q for q in keys if q.rsplit("/", 1)[0] == s]:
            routing[i].append(k)
        if len(routing[i]) >= (len(keys) / num_shards):
            i += 1

    return routing
