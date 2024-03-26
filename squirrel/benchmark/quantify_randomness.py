from typing import Callable, Iterable

import numpy as np
from scipy.stats import kendalltau

from squirrel.constants import SeedType
from squirrel.driver import MapDriver
from squirrel.iterstream.base import Composable


class DummyShardedDriver(MapDriver):
    """Return integer elements in shards"""

    name = "dummy_sharded_driver"

    def __init__(self, num_shard: int, shard_size: int) -> None:
        """Init dummy sharded driver"""
        self.shard_size = shard_size
        self.key_it = range(num_shard)
        self.data = np.arange(num_shard * shard_size)

    def get(self, key: str) -> int:
        """Get item with key"""
        return self.data[int(key) * self.shard_size : (int(key) + 1) * self.shard_size]

    def keys(self) -> Iterable:
        """Get key iterator"""
        yield from map(str, self.key_it)

    def get_iter(self, flatten: bool = True, **kwargs) -> Composable:
        """Get iterator"""
        return super().get_iter(flatten=flatten, **kwargs)


def kendalltau_metric(result1: np.array, result2: np.array) -> float:
    """Compute the kendall tau randomness metric"""
    tau, _ = kendalltau(result1, result2)
    return tau


def quantify_randomness(
    num_shard: int,
    shard_size: int,
    buffer_size: int,
    initial: int,
    n_samples: int = 250,
    metric: Callable = kendalltau_metric,
    seed1: SeedType = None,
    seed2: SeedType = None,
) -> float:
    """Quantify the randomness of sampling from a driver with the given shuffle parameters.
    This function assumes that we always fully shuffle all keys and the parameters for the item buffer is what we
    are interested in.

    Args:
        num_shard (int): number of shards
        shard_size (int): size of each shard assuming that all shards are of equal size
        buffer_size (int): buffer size for item shuffle buffer
        initial (int): initial size of item shuffle buffer
        n_samples (int): influences the accuracy of the estimate by controlling the number of sampled trajectories
        metric (Callable): how to measure the distance
        seed1 (SeedType): seed for the first trajectory
        seed2 (SeedType): seed for the second trajectory

    Returns:
        float: randomness measure computed from the kendall tau coefficient. Values between 0 and 1 while 1 means
            completely deterministic and 0 means random.
    """
    driver = DummyShardedDriver(num_shard, shard_size)
    distances = []

    for _ in range(n_samples):
        # sample two random trajectories
        result1 = driver.get_iter(
            shuffle_key_buffer=num_shard,
            shuffle_item_buffer=buffer_size,
            item_shuffle_kwargs={"initial": initial, "seed": seed1},
            key_shuffle_kwargs={"seed": seed1},
        ).collect()
        result2 = driver.get_iter(
            shuffle_key_buffer=num_shard,
            shuffle_item_buffer=buffer_size,
            item_shuffle_kwargs={"initial": initial, "seed": seed2},
            key_shuffle_kwargs={"seed": seed2},
        ).collect()

        # and get their distance via the kendall tau function
        distances.append(metric(result1, result2))

    # return the median of distances
    return np.abs(np.median(np.array(distances)))
