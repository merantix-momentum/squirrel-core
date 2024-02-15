from typing import Any, Optional
from fsspec import AbstractFileSystem
import numpy as np

from squirrel.serialization.serializer import SquirrelSerializer


class NumpySerializer(SquirrelSerializer):


    def serialize(self, obj: Any) -> Any:
        raise NotImplementedError()
    
    def deserialize(self, obj: Any) -> Any:
        raise NotImplementedError()
    
    def serialize_shard_to_file(
            self,
            obj: np.array,
            fp: str,
            fs: Optional[AbstractFileSystem] = None,
            **kwargs
        ) -> None:
        """
        Store a numpy.array to `fp` with `numpy.save` method.
        """
        np.save(file=fp, arr=obj, allow_pickle=False, **kwargs)

    def deserialize_shard_from_file(
            self,
            fp: str,
            **kwargs
        ) -> np.array:
        """Read a numpy array from `fp`"""
        return np.load(file=fp, **kwargs)