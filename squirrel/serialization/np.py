from typing import Any
import numpy as np

from squirrel.serialization.serializer import SquirrelFileSerializer


class NumpySerializer(SquirrelFileSerializer):

    @property
    def file_extension(self) -> str:
        return "npy"

    def serialize(self, obj: Any) -> Any:
        """NotImplemented"""
        raise NotImplementedError()

    def deserialize(self, obj: Any) -> Any:
        """NotImplemented"""
        raise NotImplementedError()

    def serialize_shard_to_file(self, obj: np.ndarray, fp: str, **kwargs) -> None:
        """Store a numpy.array to `fp` with `numpy.save` method.

        Args:
            obj (np.ndarray): the numpy array to store
            fp (str): the full file path. Should include the file extension, i.e. .npy
        """
        np.save(file=fp, arr=obj, allow_pickle=False, **kwargs)

    def deserialize_shard_from_file(self, fp: str, **kwargs) -> np.array:
        """Read a numpy array from `fp`

        Args:
            fp (str): the full file path from which, the array is read. Should include the file extension, i.e. .npy
        """
        return np.load(file=fp, **kwargs)
