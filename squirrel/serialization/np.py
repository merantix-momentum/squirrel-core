from __future__ import annotations

from typing import TYPE_CHECKING
import fsspec
import numpy as np

from squirrel.serialization.serializer import SquirrelFileSerializer

if TYPE_CHECKING:
    from fsspec.spec import AbstractFileSystem


class NumpySerializer(SquirrelFileSerializer):
    @property
    def file_extension(self) -> str:
        """File extension, i.e. npy"""
        return "npy"

    def serialize_shard_to_file(
        self,
        obj: np.ndarray,
        fp: str,
        fs: AbstractFileSystem | None = None,
        mode: str = "wb",
        open_kwargs: dict | None = None,
        **kwargs,
    ) -> None:
        """Store a numpy.array to `fp` with `numpy.save` method.
        Args:
            obj (np.ndarray): the numpy array to store
            fp (str): the full file path. Should include the file extension, i.e. .npy
            fs: filesystem instance
            mode (str): default to wb
            open_kwargs (Optional[Dict]): kwargs passed to fs.open() call
            kwargs: passed yo np.save()
        """
        if fs is None:
            fs = fsspec

        if open_kwargs is None:
            open_kwargs = {}

        with fs.open(fp, mode=mode, **open_kwargs) as f:
            np.save(file=f, arr=obj, allow_pickle=False, **kwargs)

    def deserialize_shard_from_file(
        self,
        fp: str,
        fs: AbstractFileSystem | None = None,
        mode: str = "rb",
        open_kwargs: dict | None = None,
        **kwargs,
    ) -> np.array:
        """Read a numpy array from `fp`
        Args:
            fp (str): the full file path from which, the array is read. Should include the file extension, i.e. .npy
            fs: filesystem instance
            mode (str): default to wb
            open_kwargs (Optional[Dict]): kwargs passed to fs.open() call
            kwargs: passed yo np.load()
        """
        if fs is None:
            fs = fsspec

        if open_kwargs is None:
            open_kwargs = {}

        with fs.open(fp, mode=mode, **open_kwargs) as f:
            data = np.load(file=f, allow_pickle=False, **kwargs)
            return data