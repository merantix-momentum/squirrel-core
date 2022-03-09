"""
It is advised to use :py:class:`SquirrelGroup` over :py:class:`zarr.hierarchy.Group`, especially
when working with large datasets since the former provides performance boosts over the latter.
"""
from __future__ import annotations

import logging
import typing as t
from collections import MutableMapping

from zarr.errors import GroupNotFoundError, ReadOnlyError
from zarr.hierarchy import Group
from zarr.storage import contains_group, init_group
from zarr.sync import ThreadSynchronizer

from squirrel.constants import URL
from squirrel.zarr.key import normalize_key
from squirrel.zarr.store import SquirrelFSStore
from squirrel.zarr.sync import SquirrelThreadSynchronizer

__all__ = ["SquirrelGroup", "get_group"]

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
    store = SquirrelFSStore(path, mode=mode, **storage_options)
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


class SquirrelGroup(Group):
    """
    A modified :py:class:`zarr.hierarchy.Group` object with the following changes compared to the parent:

    * :py:meth:`keys` method uses the `ls` method of the file system instance of the store (i.e. self.store.fs.ls()) to
      request the list of keys. This results in ~100X speedup compared to calling the `keys()` method of the Group
      directly.

    * SquirrelGroup provides the :py:meth:`get_item` method, which takes as input a `kind` in addition to `key` and
      uses the kind information to bypass expensive `contains()` calls.
    """

    def __init__(
        self,
        store: MutableMapping,
        path: URL = None,
        read_only: bool = False,
        chunk_store: MutableMapping = None,
        cache_attrs: bool = True,
        synchronizer: ThreadSynchronizer = None,
    ):
        """Initialize SquirrelGroup.

        Args:
            store (MutableMapping): Store of the group.
            path (URL, optional): Path to the group. Defaults to None.
            read_only (bool, optional): True if group should be opened in read-only mode. Defaults to False.
            chunk_store (MutableMapping, optional): Separate storage for chunks. If not provided, `store` will be used
                for storage of both chunks and metadata. Defaults to None.
            cache_attrs (bool, optional): If True, user attributes will be cached for attribute read operations. If
                False, user attributes are reloaded from the store prior to all attribute read operations. Defaults to
                True.
            synchronizer (ThreadSynchronizer, optional): Array synchronizer to use. Defaults to None.

        Raises:
            Exception: If a group does not exist at the given `path`.
        """

        try:
            super().__init__(
                store=store,
                path=path,
                read_only=read_only,
                chunk_store=chunk_store,
                cache_attrs=cache_attrs,
                synchronizer=synchronizer,
            )
        except GroupNotFoundError:
            raise Exception(f"Group not found at path {self.store.url}")

        logger.debug(f"a new {self.__class__.__name__} class initialized; read_only: {self.read_only}")

    def keys(self, prefix: str = "") -> t.Generator[str, None, None]:
        """Returns a generator over keys one level below the provided prefix.

        Note that while the return type of this function is a generator, it still fetches all keys and saves them in a
        list. This is because of the way that `ls()` method works. As a result, calling this function where there are
        too many keys (e.g. at the root of a zarr group as big as ImageNet) may cause memory crashes.

        Args:
            prefix (str, optional): If provided, the keys under the given prefix are returned. Defaults to "".

        Returns:
            Generator[str, None, None]: A generator over keys.
        """
        store = self.store
        path = store.path
        if prefix:
            path += "/" + normalize_key(prefix)
        keys_full_path = self.store.fs.ls(path=path)
        for k in keys_full_path:
            key = k.split("/")[-1]
            if not key.startswith("."):
                yield key
