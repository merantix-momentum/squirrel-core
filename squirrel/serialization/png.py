from typing import Any
import numpy as np

from squirrel.serialization.serializer import SquirrelSerializer
from skimage.io import imsave, imread

class PNGSerializer(SquirrelSerializer):


    def serialize(self, obj: Any) -> Any:
        """NotImplementedError"""
        raise NotImplementedError()
    
    def deserialize(self, obj: Any) -> Any:
        """NotImplementedError"""
        raise NotImplementedError()
    
    def serialize_shard_to_file(
            self,
            obj: np.array,
            fp: str,
            **kwargs
        ) -> None:
        """
        Store a numpy.array to `fp` in PNG format.
        """
        imsave(fp + ".png", obj, **kwargs)

    def deserialize_shard_from_file(
            self,
            fp: str,
            **kwargs
        ) -> np.array:
        """Read a PNG image into numpy.array"""
        return imread(fp, **kwargs)