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
        compression_set_default: t.Union[str, None] = "gzip",
        **storage_options,
    ) -> None:
        """Initializes SquirrelStore.

        Args:
            url (str): Path to the root directory. If this path does not exist, it will be created.
            serializer (SquirrelSerializer): Serializer that is used to serialize data before persisting (see
                :py:meth:`set`) and to deserialize data after reading (see :py:meth:`get`). If not specified, data will
                not be (de)serialized. Defaults to None.
            clean (bool): If true, all files in the store will be removed recursively
            compression_set_default (Union[str, None]): Default compression method used during `set` (serialization).
                Use `None` to use no compression at all. The supported compression algorithms
                are given by :py:`fsspec.available_compressions`.
                Defaults to "gzip".
            **storage_options: Keyword arguments passed to filesystem initializer.
        """
        super().__init__(url=url, serializer=serializer, clean=clean, **storage_options)
        self.compression_set_default = compression_set_default

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
        The provided key will be extended by the extension of the selected compression method.
        By default, the compression method defined in `self.compression_set_default` is used.
        To use a different compression method, provide `compression` as a keyword argument.

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

        # TODO: Discuss restriction that key is not allowed to contain `.`
        # otherwise there is no way to guarantee that `compression=None` can be
        # retrieved from this key after setting it.
        compression = kwargs.get("compression", self.compression_set_default)
        # mapping created lazily to ensure that fsspec compressions can be registered during execution
        compression_to_ext = {v: k for k, v in fsspec.utils.compressions.items()}
        full_key = f"{key}.{compression_to_ext[compression]}" if compression is not None else f"{key}"
        fp = f"{self.url}/{full_key}"
        self.serializer.serialize_shard_to_file(value, fp, fs=self.fs, **kwargs)
        # TODO: Discuss if we would like to return the full-key
        # As this would make it probably easier to test the functionality

    def keys(self, nested: bool = False, **kwargs) -> t.Iterator[str]:
        """Yields all shard keys in the store."""
        for k in super().keys(nested=nested, **kwargs):
            # only return those that have valid and supported compression extension
            # extension either registered by `fsspec` or has no `.` == no compression
            if fsspec.utils.infer_compression(k) is not None or "." not in k:
                yield k
