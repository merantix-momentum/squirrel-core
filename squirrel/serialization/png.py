from __future__ import annotations

from typing import Any, TYPE_CHECKING
import numpy as np

from squirrel.serialization.serializer import SquirrelFileSerializer
from skimage.io import imsave, imread

if TYPE_CHECKING:
    from fsspec.spec import AbstractFileSystem


class PNGSerializer(SquirrelFileSerializer):
    @property
    def file_extension(self) -> str:
        """File Extension, i.e png"""
        return "png"

    def serialize(self, obj: Any) -> Any:
        """Not Implemented"""
        raise NotImplementedError()

    def deserialize(self, obj: Any) -> Any:
        """Not Implemented"""
        raise NotImplementedError()

    def serialize_shard_to_file(
        self,
        obj: np.ndarray,
        fp: str,
        fs: AbstractFileSystem | None = None,
        mode: str = "wb",
        open_kwargs: dict | None = None,
        **kwargs,
    ) -> None:
        """Store a numpy.ndarray to `fp` in PNG format

        Args:
            obj (np.ndarray): the numpy array to store
            fp (str): the full file path. Should include the file extension, i.e. .npy
            fs: filesystem instance
            mode (str): default to wb
            open_kwargs (Optional[Dict]): kwargs passed to fs.open() call, currently not implemented
            kwargs: passed yo skimage.io.imsave()
        """
        # TODO: use fs
        imsave(fp + f".{self.file_extension}", obj, **kwargs)

    def deserialize_shard_from_file(
        self,
        fp: str,
        fs: AbstractFileSystem | None = None,
        mode: str = "wb",
        open_kwargs: dict | None = None,
        **kwargs,
    ) -> np.ndarray:
        """Read a PNG image into numpy.ndarray"""
        # TODO: use fs
        return imread(fp, **kwargs)
