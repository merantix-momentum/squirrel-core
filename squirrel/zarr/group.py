import logging
import time
import typing as t
from collections import MutableMapping
from types import TracebackType

import numpy as np
from zarr.core import Array
from zarr.errors import GroupNotFoundError
from zarr.hierarchy import Group
from zarr.sync import ThreadSynchronizer

from squirrel.caching.append_log import ALogRow, AppendLog
from squirrel.caching.cache import getsize
from squirrel.constants import URL
from squirrel.zarr.key import normalize_key

__all__ = ["SquirrelGroup"]

logger = logging.getLogger(__name__)


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
        self.append_log = AppendLog(url=self.store.url)

    def get_item(self, key: str, kind: str) -> t.Union[Array, Group]:
        """Returns an array or a group given a key.

        If the type of the requested key is known (i.e. "array" or "group"), then we can avoid calling
        :py:func:`zarr.storage.contains_array` and :py:func:`zarr.storage.contains_group`, which results in substantial
        performance gain.

        Args:
            key (str): Key of the array or group to be returned.
            kind (str): "array" or "group", indicates if `key` points to an array or a group.

        Raises:
            ValueError: If `kind` is not one of "array" or "group".

        Returns:
            Union[zarr.core.Array, zarr.hierarchy.Group]: Array or group corresponding to `key`.
        """
        path = self._item_path(key)
        if kind == "array":
            return Array(
                self._store,
                read_only=self._read_only,
                path=path,
                chunk_store=self._chunk_store,
                synchronizer=self._synchronizer,
                cache_attrs=self.attrs.cache,
            )
        elif kind == "group":
            return Group(
                self._store,
                read_only=self._read_only,
                path=path,
                chunk_store=self._chunk_store,
                cache_attrs=self.attrs.cache,
                synchronizer=self._synchronizer,
            )
        else:
            raise ValueError(f'Unrecognized value {kind} for the kind argument, it must be either "array" or "group"')

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

    def __setitem__(self, item: str, value: np.ndarray) -> None:
        """Set value for an zarr array."""
        self._save_to_append_log(1, item, value)
        self.array(item, value, overwrite=True)

    def __delitem__(self, item: str) -> t.Any:
        """Delete an zarr array."""
        self._save_to_append_log(-1, item)
        return self._write_op(self._delitem_nosync, item)

    def __exit__(
        self, exc_type: t.Optional[BaseException], exc_val: t.Optional[BaseException], exc_tb: t.Optional[TracebackType]
    ) -> None:
        # for type hinting see
        # https://stackoverflow.com/questions/58808055/what-are-the-python-builtin-exit-argument-types
        """If the underlying Store has a ``close`` method, call it."""
        self.append_log.close()  # add `append_log.close()` to `Squirrel.Group.__exit__`.
        try:
            self.store.close()
        except AttributeError:
            pass

    def close(self) -> None:
        """Method to actively close the group."""
        self.append_log.close()
        try:
            self.store.close()
        except AttributeError:
            pass

    def _save_to_append_log(self, operation: int, item: str, value: np.ndarray = None) -> None:
        """Parse item and value into a row in append_log."""
        path_key = f"{self.store.url}/{item}"
        if value is not None:
            sizeof = getsize(value)
        else:
            sizeof = None  # do not calculate removed item size, save time
        row = ALogRow(timestamp=time.time(), operation=operation, key=path_key, sizeof=sizeof)
        self.append_log.append(row)
