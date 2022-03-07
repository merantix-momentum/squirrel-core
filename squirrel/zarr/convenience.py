import logging
from typing import MutableMapping, Union

from zarr.errors import ReadOnlyError
from zarr.storage import contains_group, init_group

import squirrel.zarr.store as sq_zstore
from squirrel.constants import URL
from squirrel.zarr.group import SquirrelGroup
from squirrel.zarr.sync import SquirrelThreadSynchronizer

__all__ = ["get_group", "suggested_store", "optimize_for_reading"]


logger = logging.getLogger(__name__)


def get_group(path: URL, mode: str = "a", overwrite: bool = False, **storage_options) -> SquirrelGroup:
    """Constructs a zarr store with currently suggested parameters and opens it in a zarr group.

    Default `zarr.group` method, when passed the parameter `overwrite=False`, will always return a Group with
    `read_only=False`. Here we choose not to use this zarr function but construct one by our own settings, using
    :py:class:`~squirrel.zarr.group.SquirrelGroup`.

    Args:
        path (URL): fsspec path to store.
        mode (str, optional): IO mode (e.g. "r", "w", "a"). Defaults to "a". `mode` affects the store of the returned
                group.
        overwrite (bool, optional): If True, the store is cleaned before opening. Defaults to False.
        **storage_options: Keyword arguments passed to fsspec when obtaining a filesystem object corresponding to the
            given `path`.

    Raises:
        ReadOnlyError: If `mode == "r"` and `overwrite == True`.

    Returns:
        SquirrelGroup: Root zarr group constructed with the given parameters, which has improved performance over
        a :py:class:`zarr.hierarchy.Group`. If `mode != "r"` and either `overwrite == True` or if the group does not
        exist yet, first the group is initialized using :py:meth:`zarr.storage.init_group`. Then, the group is created
        on the store suggested by :py:meth:`suggested_store`. The store of the group is accessible as an attribute of
        the group, i.e. `group.store`.
    """
    store = suggested_store(path, mode=mode, **storage_options)
    if mode == "r":
        if overwrite:
            raise ReadOnlyError

        return SquirrelGroup(
            store=store,
            read_only=True,
            cache_attrs=True,
        )
    else:
        if overwrite or not contains_group(store):
            init_group(store, overwrite=overwrite)

        return SquirrelGroup(
            store=store,
            read_only=False,
            cache_attrs=True,
            synchronizer=SquirrelThreadSynchronizer(),
        )


def suggested_store(path: URL, mode: str = "a", **kwargs) -> Union[sq_zstore.SquirrelFSStore, sq_zstore.CachedStore]:
    """Suggest the appropriate squirrel store, given an IO mode.

    Args:
        path (URL): Path to a dataset or any subset of it one wishes to enter.
        mode (str, optional): Read "r", write "w", append "a" and other common modes for open() method. Defaults to
            "a".
        **kwargs: Other key word arguments that are passed to :py:class:`SquirrelFSStore`.

    Returns:
        Union[SquirrelFSStore, CachedStore]: If `mode != "r"`, a :py:class:`squirrel.zarr.store.SquirrelFSStore`
        is returned. Otherwise, a :py:class:`squirrel.zarr.store.CachedStore` wrapping a :py:class`SquirrelFSStore` is
        returned. Note that having a :py:class:`CachedStore` does not necessarily mean that the store is cached. If the
        store is not cached, then :py:meth:`squirrel.dataset.zarr_dataset.ZarrDataset.optimize_for_reading` should be
        called to cache the store first. Whether the store is cached can be checked by calling
        :py:meth:`squirrel.dataset.zarr_dataset.ZarrDataset.is_cached`.
    """
    store = sq_zstore.SquirrelFSStore(path, mode=mode, **kwargs)
    if mode == "r":
        store = sq_zstore.CachedStore(store)
    return store


def optimize_for_reading(
    store: MutableMapping,
    shard: bool = False,
    worker: int = 1,
    max_mem_size: int = 2 * 1024 ** 3,
    nodes: int = 1,
    cache: bool = True,
) -> None:
    """Optimize a zarr group so that reading from it is faster.

    The group can be cached and/or sharded. The store suggested by squirrel will be used to read the dataset content.

    Args:
        store (MutableMapping): Any store that is a subclass of MutableMapping.
        shard (bool, optional): **Not implemented yet, do not set to True**. If True, the store will be sharded.
            Defaults to False.
        worker (int, optional): Number of workers that will read the data after optimization. Only used if
            `shard == True`. Defaults to 1.
        max_mem_size (int, optional): Max size of a shard in bytes. Only used if `shard == True`. Defaults to 2*1024**3
            (i.e. 2 GB).
        nodes (int, optional): Number of nodes, each having `worker` workers, that will read the data after
            optimization. Only used if `shard == True`. Defaults to 1.
        cache (bool, optional): If True, the path keys will be cached for faster access to keys and corresponding data.
            Defaults to True.

    Raises:
        NotImplementedError: If `shard=True`.
    """
    logger.debug("clean_cached_store")
    sq_zstore.clean_cached_store(store)

    if cache:
        logger.debug("cache_meta_store")
        sq_zstore.cache_meta_store(store, cache_keys=True, cache_meta=False, compress=True, clean=False)

    if shard:
        raise NotImplementedError("Sharding is not implemented yet.")
