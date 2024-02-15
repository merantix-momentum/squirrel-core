from __future__ import annotations

from typing import Any

import ray

from squirrel.driver.store import StoreDriver
from squirrel.iterstream import Composable
from squirrel.serialization.png import PNGSerializer
from squirrel.serialization.np import NumpySerializer
from squirrel.store.directory_store import DirectoryStore

__all__ = [
    "DirectoryDriver",
]


class DirectoryDriver(StoreDriver):
    """A StoreDriver that by default uses SquirrelStore with messagepack serialization."""

    name = "directory"

    def __init__(self, url: str, file_format: str, storage_options: dict[str, Any] | None = None, **kwargs):
        if "store" in kwargs:
            raise ValueError("Store of MessagepackDriver is fixed, `store` cannot be provided.")
        
        if file_format == "npy":
            ser = NumpySerializer
        elif file_format == "png":
            ser = PNGSerializer
        else:
            raise NotImplementedError()
        super().__init__(url=url, serializer=ser, storage_options=storage_options, **kwargs)

    def get_iter(self, **kwargs) -> Composable:
        return super().get_iter(flatten=False, **kwargs)
    
    def get_iter_ray(self):
        """
        TODO: if the same api for get_iter can be used for ray, we should go for that. 
        """
        raise NotImplementedError()
    
    @property
    def store(self):
        return DirectoryStore(self.url, serializer=self.serializer, **self.storage_options)
    
    @property
    def ray(self):
        if self.serializer == PNGSerializer:
            return ray.data.read_images(self.url)
        elif self.serializer == NumpySerializer:
            return ray.data.read_numpy(self.url)
        else:
            raise NotImplementedError()
