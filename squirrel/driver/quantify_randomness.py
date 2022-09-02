import numpy as np
from scipy.stats import kendalltau

from squirrel.driver import MapDriver


class DummyShardedDriver(MapDriver):
    """Return integer elements in shards"""

    name = "dummy_sharded_driver"

    def __init__(self, num_shard: int, shard_size: int):
        self.shard_size = shard_size
        self.key_it = range(num_shard)
        self.data = np.arange(num_shard * shard_size)

    def get(self, key: str):
        return self.data[int(key) * self.shard_size : (int(key) + 1) * self.shard_size]

    def keys(self):
        yield from map(str, self.key_it)

    def get_iter(self, flatten: bool = True, **kwargs):
        return super().get_iter(flatten=flatten, **kwargs)


def quantify_randomness(num_shard: int, shard_size: int, buffer_size: int, initial: int, n_samples: int = 250) -> float:
    """Quantify the randomness of sampling from a driver with the given shuffle parameters.
       This function assumes that we always fully shuffle all keys and the parameters for the item buffer is what we
       are interested in.

    Args:
        num_shard (int): number of shards
        shard_size (int): size of each shard assuming that all shards are of equal size
        buffer_size (int): buffer size for item shuffle buffer
        initial (int): initial size of item shuffle buffer
        n_samples (int): influences the accuracy of the estimate by controlling the number of sampled trajectories

    Returns:
        float: randomness measure computed from the kendall tau coefficient. Values between 0 and 1 while 1 means
        completely deterministic and 0 means random.
    """
    driver = DummyShardedDriver(num_shard, shard_size)
    distances = []

    for _ in range(n_samples):
        # sample two random trajectories
        result1 = driver.get_iter(
            shuffle_key_buffer=num_shard, shuffle_item_buffer=buffer_size, item_shuffle_kwargs={"initial": initial}
        ).collect()
        result2 = driver.get_iter(
            shuffle_key_buffer=num_shard, shuffle_item_buffer=buffer_size, item_shuffle_kwargs={"initial": initial}
        ).collect()

        # and get their distance via the kendall tau function
        tau, _ = kendalltau(result1, result2)
        distances.append(tau)

    # return the median of distances
    return np.abs(np.median(np.array(distances)))
