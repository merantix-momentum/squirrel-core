from __future__ import annotations

import typing as t

from squirrel.store.filesystem import FilesystemStore, get_random_key

if t.TYPE_CHECKING:
    from squirrel.constants import SampleType, ShardType
    from squirrel.serialization import SquirrelSerializer


class SquirrelStore(FilesystemStore):
    """FilesystemStore that persist samples (Dict objects) or shards (i.e. list of samples)."""

    def __init__(self, url: str, serializer: SquirrelSerializer, clean: bool = False, **storage_options) -> None:
        """Initializes SquirrelStore.

        Args:
            url (str): Path to the root directory. If this path does not exist, it will be created.
            serializer (SquirrelSerializer): Serializer that is used to serialize data before persisting (see
                :py:meth:`set`) and to deserialize data after reading (see :py:meth:`get`). If not specified, data will
                not be (de)serialized. Defaults to None.
            clean (bool): If true, all files in the store will be removed recursively
            **storage_options: Keyword arguments passed to filesystem initializer.
        """
        super().__init__(url=url, serializer=serializer, clean=clean, **storage_options)

    def get(self, key: str, **kwargs) -> t.Iterator[SampleType]:
        """Yields the item with the given key.

        If the store has a serializer, data read from the file will be deserialized.

        Args:
            key (str): Key corresponding to the item to retrieve.
            **kwargs: Keyword arguments forwarded to :py:meth:`self.serializer.deserialize_shard_from_file`.

        Yields:
            (Any) Item with the given key.
        """
        fp = f"{self.url}/{key}.gz"
        yield from self.serializer.deserialize_shard_from_file(fp, fs=self.fs, **kwargs)

    def set(self, value: t.Union[SampleType, ShardType], key: t.Optional[str] = None, **kwargs) -> None:
        """Persists a shard or sample with the given key.

        Data item will be serialized before writing to a file.

        Args:
            value (Any): Shard or sample to be persisted. If `value` is a sample (i.e. not a list), it will be wrapped
                around with a list before persisting.
            key (Optional[str]): Optional key corresponding to the item to persist.
            **kwargs: Keyword arguments forwarded to :py:meth:`self.serializer.serialize_shard_to_file`.
        """
        if not isinstance(value, t.List):
            value = [value]

        if key is None:
            key = get_random_key()
        fp = f"{self.url}/{key}.gz"

        if not self._dir_exists:
            self.fs.makedirs(self.url, exist_ok=True)
            self._dir_exists = True

        self.serializer.serialize_shard_to_file(value, fp, fs=self.fs, **kwargs)

    def keys(self, nested: bool = False, **kwargs) -> t.Iterator[str]:
        """Yields all shard keys in the store."""
        for k in super().keys(nested=nested, **kwargs):
            # we only set .gz files, so we only read .gz files
            if k.endswith(".gz"):
                yield k.rsplit(".gz", 1)[0]


class CacheStore(SquirrelStore):
    def __init__(
        self,
        url: str,
        serializer: SquirrelSerializer,
        cache_url: str,
        clean: bool = False,
        cash_storage_options: dict[str, t.Any] | None = None,
        **storage_options,
    ):
        """
        Maintains a cache of the original data which is populated on the fly as the data is retrieved from the main
        store. The get() method will fetch the entire shard and stores it in the cache directory with the same key
        and serialization protocol, and then yields the samples within that shard. If the cache already exists, it
        streams the data from the cache.

        Note: the entire shard should fit in memory.
        Note: there is an overhead for caching the data in the first iteration which should be amortized over the
        multiple iterations.
        """
        super().__init__(url=url, serializer=serializer, clean=clean, **storage_options)
        cash_storage_options = cash_storage_options if cash_storage_options is not None else {}
        self._cache = SquirrelStore(cache_url, serializer, **cash_storage_options)

    def get(self, key: str, **kwargs) -> t.Iterator[SampleType]:
        """
        If the item is cached, read from cache, otherwise read from the original source, cache it and stream the items
        from the shard

        """
        if not self._is_cached(key):
            shard = list(super().get(key, **kwargs))
            self._cache.set(value=shard, key=key)
            yield from shard
        else:
            yield from self._cache.get(key, **kwargs)

    def _is_cached(self, key: str) -> bool:
        p = f"{self._cache.url}/{key}.gz"
        return self._cache.fs.exists(p)
