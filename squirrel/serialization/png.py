from typing import Any
import numpy as np

from squirrel.serialization.serializer import SquirrelFileSerializer
from skimage.io import imsave, imread


class PNGSerializer(SquirrelFileSerializer):

    @property
    def file_extension(self) -> str:
        return "png"

    def serialize(self, obj: Any) -> Any:
        """NotImplemented"""
        raise NotImplementedError()

    def deserialize(self, obj: Any) -> Any:
        """NotImplemented"""
        raise NotImplementedError()

    def serialize_shard_to_file(self, obj: np.ndarray, fp: str, **kwargs) -> None:
        """Store a numpy.ndarray to `fp` in PNG format

        Args:
            obj (np.ndarray): the numpy array to store
            fp (str): the full file path. Should include the file extension, i.e. .npy
        """
        imsave(fp + f".{self.file_extension}", obj, **kwargs)

    def deserialize_shard_from_file(self, fp: str, **kwargs) -> np.ndarray:
        """Read a PNG image into numpy.ndarray"""
        return imread(fp, **kwargs)
