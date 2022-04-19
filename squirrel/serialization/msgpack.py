from __future__ import annotations

import typing as t

import fsspec
import msgpack
import msgpack_numpy

from squirrel.serialization.serializer import SquirrelSerializer

if t.TYPE_CHECKING:
    from fsspec.spec import AbstractFileSystem

    from squirrel.constants import ShardType


class MessagepackSerializer(SquirrelSerializer):
    @staticmethod
    def serialize(obj: t.Any) -> bytes:
        """Returns the object serialized with msgpack."""
        return msgpack.packb(obj, use_bin_type=True, default=msgpack_numpy.encode)

    @staticmethod
    def deserialize(obj: bytes) -> t.Any:
        """Returns the object deserialized with msgpack."""
        return msgpack.unpackb(obj, object_hook=msgpack_numpy.decode)

    @staticmethod
    def serialize_shard_to_file(
        shard: ShardType,
        fp: str,
        fs: t.Optional[AbstractFileSystem] = None,
        mode: str = "wb",
        **open_kwargs,
    ) -> None:
        """Writes a shard to a file by only writing and serializing the values of its samples.

        Args:
            shard (ShardType): Shard to serialize and write to the file.
            fp (str): Path to the file to write.
            fs (AbstractFileSystem, optional): Filesystem to use for opening the file. If not provided, `fsspec` will
                pick a filesystem suitable for `fp`. Defaults to None.
            mode (str): IO mode to use. Passed to :py:meth:`fs.open`. Defaults to "wb".
            **open_kwargs: Other keyword arguments passed to :py:meth:`fs.open`. `open_kwargs` will always have
                `compression="gzip"` set.
        """
        open_kwargs["mode"] = mode
        open_kwargs["compression"] = "gzip"

        if fs is None:
            fs = fsspec

        with fs.open(fp, **open_kwargs) as f:
            for sample in shard:
                f.write(MessagepackSerializer.serialize(sample))

    @staticmethod
    def deserialize_shard_from_file(
        fp: str,
        fs: t.Optional[AbstractFileSystem] = None,
        mode: str = "rb",
        unpacker_kwargs: t.Optional[t.Dict] = None,
        **open_kwargs,
    ) -> t.Iterable[t.Any]:
        """Reads a shard from file and returns an iterable over its samples.

        Args:
            fp (str): Path to the file to write.
            fs (AbstractFileSystem, optional): Filesystem to use for opening the file. If not provided, `fsspec` will
                pick a filesystem suitable for `fp`. Defaults to None.
            mode (str): IO mode to use. Passed to :py:meth:`fs.open`. Defaults to "rb".
            unpacker_kwargs (Dict, optional): Kwargs to be passed to `msgpack.Unpacker()`.
                If `use_list` not given, it will be set to False.
            **open_kwargs: Other keyword arguments passed to :py:meth:`fs.open`. `open_kwargs` will always have
                `compression="gzip"` set.

        Yields:
            (Any) Values of the samples of the shard.
        """
        open_kwargs["mode"] = mode
        open_kwargs["compression"] = "gzip"
        unpacker_kwargs = {} if unpacker_kwargs is None else unpacker_kwargs
        if "use_list" not in unpacker_kwargs:
            unpacker_kwargs["use_list"] = False
        if "object_hook" not in unpacker_kwargs:
            unpacker_kwargs["object_hook"] = msgpack_numpy.decode

        if fs is None:
            fs = fsspec

        with fs.open(fp, **open_kwargs) as f:
            yield from msgpack.Unpacker(f, **unpacker_kwargs)
