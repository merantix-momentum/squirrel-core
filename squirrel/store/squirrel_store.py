from __future__ import annotations

import typing as t
import fsspec

from squirrel.store.filesystem import FilesystemStore, get_random_key

if t.TYPE_CHECKING:
    from squirrel.constants import SampleType, ShardType
    from squirrel.serialization import SquirrelSerializer


class SquirrelStore(FilesystemStore):
    """FilesystemStore that persist samples (Dict objects) or shards (i.e. list of samples)."""

    def __init__(
        self,
        url: str,
        serializer: SquirrelSerializer,
        clean: bool = False,
        compression: t.Union[str, None] = "gzip",
        **storage_options,
    ) -> None:
        """Initializes SquirrelStore.

        Args:
            url (str): Path to the root directory. If this path does not exist, it will be created.
            serializer (SquirrelSerializer): Serializer that is used to serialize data before persisting (see
                :py:meth:`set`) and to deserialize data after reading (see :py:meth:`get`). If not specified, data will
                not be (de)serialized. Defaults to None.
            clean (bool): If true, all files in the store will be removed recursively
            compression (Union[str, None]): Compression method used during (de-)serialization.
                Use `None` to use no compression at all. The supported compression algorithms
                are given by :py:`fsspec.available_compressions`.
                Defaults to "gzip".
            **storage_options: Keyword arguments passed to filesystem initializer.
        """
        super().__init__(url=url, serializer=serializer, clean=clean, **storage_options)
        self.compression = compression

    def get(self, key: str, **kwargs) -> t.Iterator[SampleType]:
        """Yields the item with the given key.

        If the store has a serializer, data read from the file will be deserialized.

        Args:
            key (str): Key corresponding to the item to retrieve.
            **kwargs: Keyword arguments forwarded to :py:meth:`self.serializer.deserialize_shard_from_file`.

        Yields:
            (Any) Item with the given key.
        """
        fp = f"{self.url}/{key}"  # Key includes compression suffix
        yield from self.serializer.deserialize_shard_from_file(fp, fs=self.fs, **kwargs)

    def set(
        self,
        value: t.Union[SampleType, ShardType],
        key: t.Optional[str] = None,
        **kwargs,
    ) -> None:
        """Persists a shard or sample with the given key.

        Data item will be serialized and compressed before writing to a file.

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
        # mapping created lazily to ensure that fsspec compressions can be registered during execution
        compression_to_ext = {v: k for k, v in fsspec.utils.compressions.items()}
        full_key = f"{key}.{compression_to_ext[self.compression]}" if self.compression is not None else f"{key}"
        fp = f"{self.url}/{full_key}"
        self.serializer.serialize_shard_to_file(value, fp, fs=self.fs, **kwargs)

    def keys(self, nested: bool = False, **kwargs) -> t.Iterator[str]:
        """Yields all shard keys in the store."""
        for k in super().keys(nested=nested, **kwargs):
            # only return those that have valid and supported compression extension
            if fsspec.utils.infer_compression(k) == self.compression:
                yield k
