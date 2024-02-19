from __future__ import annotations

from typing import Any

import ray

from squirrel.driver.store import StoreDriver
from squirrel.iterstream import Composable
from squirrel.serialization import PNGSerializer
from squirrel.serialization import NumpySerializer
from squirrel.store.directory_store import DirectoryStore

__all__ = [
    "DirectoryDriver",
]


class DirectoryDriver(StoreDriver):
    """A StoreDriver that by default uses SquirrelStore with messagepack serialization."""

    name = "directory"

    SERIALIZERS = {"npy": NumpySerializer, "png": PNGSerializer}

    def __init__(self, url: str, file_format: str, storage_options: dict[str, Any] | None = None, **kwargs):
        """
        A driver to access a directory of files in the same file formats. the class attribute
        SERIALIZE contains the supported formats along with the corresponding serializerd from
        `squirrel.serialization` package.
        """
        if "store" in kwargs:
            raise ValueError("Store of MessagepackDriver is fixed, `store` cannot be provided.")

        self._ser = self.SERIALIZERS.get(file_format, None)
        if self._ser is None:
            raise ValueError(
                f"""the file_format argument {file_format} is invalid,
                valid file_formats are {''.join(list(self._SERIALIZERS.keys))}"""
            )
        super().__init__(url=url, serializer=self._ser, storage_options=storage_options, **kwargs)

    def get_iter(self, **kwargs) -> Composable:
        """Get iter"""
        return super().get_iter(flatten=False, **kwargs)

    def get_iter_ray(self) -> None:
        """TODO: if the same api for get_iter can be used for ray, we should go for that."""
        raise NotImplementedError()

    @property
    def store(self) -> DirectoryStore:
        """Store that is used by the driver."""
        return DirectoryStore(self.url, serializer=self.serializer, **self.storage_options)

    @property
    def ray(self) -> ray.data.Dataset:
        """Ray Dataset"""
        if self._ser == PNGSerializer:
            return ray.data.read_images(self.url)
        elif self._ser == NumpySerializer:
            return ray.data.read_numpy(self.url)
