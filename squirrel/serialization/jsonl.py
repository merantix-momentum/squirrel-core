from __future__ import annotations

import json
import typing as t

import fsspec
import numpy as np

from squirrel.serialization.serializer import SquirrelSerializer

if t.TYPE_CHECKING:
    from fsspec.spec import AbstractFileSystem

    from squirrel.constants import ShardType


class SquirrelJsonEncoder(json.JSONEncoder):
    """Json encoder for numpy types."""

    def default(self, obj: t.Any) -> t.Any:
        """The default function to encode numpy types."""
        if isinstance(
            obj,
            (
                np.int_,
                np.intc,
                np.intp,
                np.int8,
                np.int16,
                np.int32,
                np.int64,
                np.uint8,
                np.uint16,
                np.uint32,
                np.uint64,
            ),
        ):
            return int(obj)
        elif isinstance(obj, (np.float_, np.float16, np.float32, np.float64)):
            return float(obj)
        elif isinstance(obj, (np.ndarray,)):
            return {"data": obj.tolist(), "type": obj.dtype.str, "shape": obj.shape}
        return json.JSONEncoder.default(self, obj)


class SquirrelJsonDecoder(json.JSONDecoder):
    """Json decoder for numpy types."""

    def __init__(self, *args, **kwargs):
        """Initialize SquirrelJsonDecoder."""
        json.JSONDecoder.__init__(self, object_hook=self.object_hook, *args, **kwargs)

    def object_hook(self, dct: t.Dict) -> t.Any:
        """Decode custom types."""
        if "type" in dct and "data" in dct and "shape" in dct:
            return np.array(dct["data"], dtype=np.dtype(dct["type"])).reshape(dct["shape"])
        return dct


class JsonSerializer(SquirrelSerializer):
    def __init__(self, deser_hook: t.Optional[t.Callable] = None) -> None:
        """Initializes JsonSerializer.

        Args:
            deser_hook (Callable): Callable that is passed as `object_hook` to :py:class:`JsonDecoder` during json
                deserialization. Defaults to None.
        """
        super().__init__()
        self.deser_hook = deser_hook

    @staticmethod
    def serialize(obj: t.Any) -> bytes:
        """Returns the object serialized with json."""
        return json.dumps(obj, cls=SquirrelJsonEncoder).encode("utf-8")

    def deserialize(self, obj: bytes) -> t.Any:
        """Returns the object deserialized with json."""
        return json.loads(obj, cls=SquirrelJsonDecoder, object_hook=self.deser_hook)

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
                json_string = json.dumps(sample, cls=SquirrelJsonEncoder) + "\n"
                f.write(json_string.encode("utf-8"))

    @staticmethod
    def deserialize_shard_from_file(
        fp: str, fs: t.Optional[AbstractFileSystem] = None, mode: str = "rb", **open_kwargs
    ) -> t.Iterable[t.Any]:
        """Reads a shard from file and returns an iterable over its samples.

        Args:
            fp (str): Path to the file to write.
            fs (AbstractFileSystem, optional): Filesystem to use for opening the file. If not provided, `fsspec` will
                pick a filesystem suitable for `fp`. Defaults to None.
            mode (str): IO mode to use. Passed to :py:meth:`fs.open`. Defaults to "rb".
            **open_kwargs: Other keyword arguments passed to :py:meth:`fs.open`. `open_kwargs` will always have
                `compression="gzip"` set.

        Yields:
            (Any) Values of the samples of the shard.
        """
        open_kwargs["mode"] = mode
        open_kwargs["compression"] = "gzip"

        if fs is None:
            fs = fsspec

        with fs.open(fp, **open_kwargs) as f:
            for sample_bytes in f:
                yield json.loads(sample_bytes, cls=SquirrelJsonDecoder)
