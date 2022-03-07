import logging
import typing as t

import numpy as np
from zarr.errors import ReadOnlyError

from squirrel.constants import URL
from squirrel.dataset.dataset import AbstractDataset
from squirrel.dataset.stream_dataset import Fetcher, StreamDataset
from squirrel.zarr.convenience import get_group, optimize_for_reading
from squirrel.zarr.group import SquirrelGroup
from squirrel.zarr.sharding import even_keys_by_batch_size, infer_num_splits, split_keys_into_key_clusters
from squirrel.zarr.store import is_cached, normalize_key

logger = logging.getLogger(__name__)


__all__ = ["ZarrItemFetcher", "ZarrDataset"]


class ZarrItemFetcher(Fetcher):
    def __init__(self, path: URL, mode: str = "r", **kwargs) -> None:
        """Initialize the item fetcher with the recommended zarr store.

        Args:
            path (URL): Path to the zarr group.
            mode (str, optional): IO mode to use when getting the group. Defaults to "r".
            **kwargs: Keyword arguments that are passed to :py:meth:`squirrel.zarr.convenience.get_group` to get the
                group.
        """
        self.mode = mode
        self.group = get_group(path, mode=mode, **kwargs)
        # User app such as chameleon could use more advanced mode, such as 'w-' to enter a zarr group.
        # TODO: Loosen the restriction for such use cases.

    def fetch(self, key: str) -> t.Tuple[str, np.ndarray, t.Dict]:
        """Fetch an item from the zarr group by a key.

        Args:
            key (str): Identifier for the item in the store.

        Returns:
            Tuple[str, np.ndarray, Dict]: Tuple containing the key, data and metadata of the requested item
        """
        return key, self.group[key][:], self.group[key].attrs.asdict()

    def write(self, key: str, val: t.Tuple[t.Any, t.Dict]) -> None:
        """Write an item to the zarr group by a key.

        Args:
            key (str): Identifier for the item in the store.
            val (Any): Tuple containing the data and metadata that should be written to the store.

        Raises:
            ReadOnlyError: If the fetcher was initialized with a read-only mode.
        """
        if "r" in self.mode:
            raise ReadOnlyError
        self.group[key] = val[0]
        self.group[key].attrs = val[1]


class ZarrDataset(AbstractDataset):
    """Provides a single entry point to a zarr dataset.

    ZarrDataset wraps several squirrel functionalities into a single class so that the users are able to interact with
    a single object in order to work with datasets stored in zarr format. More specifically, it is possible to stream
    data from/to the dataset (:py:meth:`get_stream`), optimize a dataset for reading (:py:meth:`optimize_for_reading`),
    get read-optimized zarr groups (:py:meth:`get_group`), and obtain key clusters (:py:meth:`pytorch_key_cluster`).
    """

    def __init__(self, path: URL, **kwargs):
        """Initialize ZarrDataset.

        Args:
            path (URL): Path to the zarr store.
            **kwargs: Keyword arguments that are passed to :py:meth:`squirrel.zarr.convenience.get_group` to get the
                group.
        """
        self.path = path
        self.kwargs = kwargs
        self._r_group = None

    @staticmethod
    def get_stream(fetcher: ZarrItemFetcher, cache_size: int = 32, max_workers: int = None) -> StreamDataset:
        """Get a :py:class:`dataset stream <squirrel.dataset.stream_dataset.StreamDataset>` on top of the zarr group.

        Args:
            fetcher (ZarrItemFetcher): An instance of a class that inherits from
                :py:class:`~squirrel.dataset.zarr_dataset.ZarrItemFetcher`.
            cache_size (int, optional): Number of items to buffer. This should be set based on how big the items are.
                Should be greater than `max_workers`. Defaults to 32.
            max_workers (int, optional): Maximum number of workers in the process pool. If None, ThreadPoolExecutor
                determines a reasonable number. Defaults to None.

        Returns:
            StreamDataset: Dataset that can stream items from/to the zarr dataset.
        """
        return StreamDataset(
            fetcher=fetcher,
            cache_size=cache_size,
            max_workers=max_workers,
        )

    def get_group(self, mode: str = "a", overwrite: bool = False, **storage_options) -> SquirrelGroup:
        """Get the root zarr group.

        Returned root group is on a zarr store that is suggested by the given parameters. This method wraps the
        :py:meth:`squirrel.zarr.convenience.get_group` method.

        Args:
            mode (str, optional): File mode to use (e.g. "r", "w", "a"). Defaults to "a". `mode` affects the return
                type.
            overwrite (bool, optional): If True, deletes any pre-existing data in the store at path before creating the
                group. Defaults to False.
            **storage_options: Keyword arguments that are passed down to the init of the fsspec filesystem.

        Returns:
            SquirrelGroup: Root zarr group constructed with the given parameters, which has improved performance over
            a :py:class:`zarr.hierarchy.Group`. See :py:meth:`squirrel.zarr.convenience.get_group` for more details.
        """
        return get_group(self.path, mode=mode, overwrite=overwrite, **self.kwargs, **storage_options)

    def key_cluster(self, pattern: str = None) -> t.List[t.Set[str]]:
        """Returns cluster of groups in the hierarchy that can be efficiently read in bulk.

        Returns:
            keys of array cluster that can be fast iterated over with.

        Args:
            pattern (str, optional): Search pattern that needs to be excluded from the fetched array cluster. Defaults
                to None.

        Returns:
            List[Set[str]]: Keys of array clusters, with which the clusters can be iterated over fast.
        """
        if self._r_group is None:
            # save group to not read metadata multiple times
            self._r_group = self.get_group(mode="r")

        return self._r_group.store.array_cluster(pattern)

    @staticmethod
    def pytorch_key_cluster(
        keys: t.List[str],
        shuffle: bool = True,
        seed: int = 0,
        split_by_worker: bool = True,
        split_by_node: bool = True,
        torch_dist_group: str = None,
        batch_size: int = None,
    ) -> t.List[str]:
        """Returns the cluster of keys for the current PyTorch worker setup that can be efficiently read in bulk.

        Args:
            keys (List[str]): Keys that should be distributed to the current PyTorch worker setup. Can contain
                duplicates.
            shuffle (bool, optional): If True, keys are shuffled without loss of access performance. First, the shards
                are shuffled and then the keys within the shards are shuffled to maintain efficient reading. Defaults
                to True.
            seed (int, optional): Seed used for shuffling. Uses a random seed if None. Useful for getting different
                splits per epoch. Defaults to 0.
            split_by_worker (bool, optional): If True, split by PyTorch worker. Defaults to True.
            split_by_node (bool, optional): If True, split by PyTorch node. Defaults to True.
            torch_dist_group (str, optional): PyTorch distributed group used for `split_by_worker`. Defaults to
                `torch.distributed.group.WORLD`.
            batch_size (int, optional): If not None, samples are dropped so that the length of the returned cluster is
                divisible by `batch_size`. Defaults to None.

        Returns:
            List[str]: Key cluster for the current worker.
        """
        keys = [normalize_key(k) for k in keys]

        idx, n_splits = infer_num_splits(
            split_by_worker=split_by_worker, split_by_node=split_by_node, torch_dist_group=torch_dist_group
        )  # return the current split index and total number of splits under the current torch.distributed settings.

        keys = split_keys_into_key_clusters(keys, n_splits=n_splits, shuffle=shuffle, seed=seed)[idx]
        # return key_clusters and select the one corresponding to `idx`.

        if batch_size is not None:  # even the current list of keys by a given batch size.
            keys = even_keys_by_batch_size(keys, batch_size)

        return keys

    def optimize_for_reading(self, shard: bool = False, worker: int = 1, nodes: int = 1, cache: bool = True) -> None:
        """Optimize the dataset for reading.

        Wraps :py:meth:`squirrel.zarr.convenience.optimize_for_reading` to optimize the store, see that method for more
        details.

        Args:
            shard (bool, optional): **Not implemented yet, do not set to True**. If True, the store will be sharded.
                Defaults to False.
            worker (int, optional): Number of workers (per node) that will read the data. The store will be optimized
                for this target number of workers. Defaults to 1.
            nodes (int, optional): Number of nodes that will read the data. Each node is expected to have `worker`
                workers to read. Defaults to 1.
            cache (bool, optional): If True, the keys of the store will be cached for faster data access. Defaults to
                True.
        """
        logger.warning("Invoke optimize for reading, dataset opening mode flipped to 'a'.")
        optimize_for_reading(store=self.get_group(mode="a").store, shard=shard, worker=worker, nodes=nodes, cache=cache)

    def is_cached(self) -> bool:
        """Check if the store is cached."""
        return is_cached(self.get_group(mode="r").store)
