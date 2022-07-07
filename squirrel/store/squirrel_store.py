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
        **storage_options,
    ) -> None:
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
        fp = f"{self.url}/{key}"  # Key includes compression suffix
        yield from self.serializer.deserialize_shard_from_file(fp, fs=self.fs, **kwargs)

    def set(
        self,
        value: t.Union[SampleType, ShardType],
        key: t.Optional[str] = None,
        compression: t.Optional[str] = "gzip",
        **kwargs,
    ) -> str:
        """Persists a shard or sample with the given key.
        The provided key will be extended by the extension of the selected compression method.

        Data item will be serialized and compressed before writing to a file.

        Args:
            value (Any): Shard or sample to be persisted. If `value` is a sample (i.e. not a list), it will be wrapped
                around with a list before persisting.
            key (Optional[str]): Optional key corresponding to the item to persist.
                `key` may not contain `.` (dot) in the filename!
            compression (Union[str, None]): Default compression method.
                Use `None` to use no compression at all. The supported compression algorithms
                are given by :py:`fsspec.available_compressions`.
                Forwarded as keyword argument to :py:meth:`self.serializer.serialize_shard_to_file`.
                Defaults to "gzip".
            **kwargs: Keyword arguments forwarded to :py:meth:`self.serializer.serialize_shard_to_file`.

        Returns:
            (str) full key; key with the inferred compression extension
        """
        if not isinstance(value, t.List):
            value = [value]

        if key is None:
            key = get_random_key()

        if "." in key:
            # otherwise there is no way to guarantee that `compression=None` can be
            # retrieved from this key after setting it.
            raise ValueError("`key` contains the illegal character `.`")
        # mapping created lazily to ensure that fsspec compressions can be registered during execution
        compression_to_ext = {v: k for k, v in fsspec.utils.compressions.items()}
        full_key = f"{key}.{compression_to_ext[compression]}" if compression is not None else f"{key}"
        fp = f"{self.url}/{full_key}"
        self.serializer.serialize_shard_to_file(value, fp, fs=self.fs, compression=compression, **kwargs)
        # TODO: Discuss if we would like to return the full-key
        # As this would make it probably easier to test the functionality
        return full_key

    def keys(self, nested: bool = False, **kwargs) -> t.Iterator[str]:
        """Yields all shard keys in the store."""
        for k in super().keys(nested=nested, **kwargs):
            # only return those that have valid and supported compression extension
            # extension either registered by `fsspec` or has no `.` == no compression
            if fsspec.utils.infer_compression(k) is not None or "." not in k:
                yield k
