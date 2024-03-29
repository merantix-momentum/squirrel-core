from __future__ import annotations

from typing import Any

from squirrel.driver.store import StoreDriver
from squirrel.serialization import MessagepackSerializer

__all__ = [
    "MessagepackDriver",
]


class MessagepackDriver(StoreDriver):
    """A StoreDriver that by default uses SquirrelStore with messagepack serialization."""

    name = "messagepack"

    def __init__(self, url: str, storage_options: dict[str, Any] | None = None, **kwargs):
        """Initializes MessagepackDriver with default serializer. See parent class for more options.

        Args:
            url (str): Path to the root directory. If this path does not exist, it will be created.
            storage_options (Dict): a dictionary containing storage_options to be passed to fsspec.
                Example of storage_options if you want to enable `fsspec` caching:
                `storage_options={"protocol": "simplecache", "target_protocol": "gs", "cache_storage": "path/to/cache"}`
            **kwargs: Keyword arguments passed to the super class initializer.
        """
        if "store" in kwargs:
            raise ValueError("Store of MessagepackDriver is fixed, `store` cannot be provided.")
        super().__init__(url=url, serializer=MessagepackSerializer(), storage_options=storage_options, **kwargs)
