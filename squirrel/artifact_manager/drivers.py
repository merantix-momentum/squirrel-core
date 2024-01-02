from typing import Any, Optional

from squirrel.driver.store import StoreDriver
from squirrel.framework.plugins.plugin_manager import register_driver
from squirrel.store import AbstractStore, FilesystemStore


class DirectoryDriver(StoreDriver):
    name = "directory"

    def __init__(self, url: str, storage_options: Optional[dict[str, Any]] = None, **kwargs) -> None:
        """
        Initializes DirectoryDriver iterating over all files in the root directory.

        Args:
            url (str): the root url of the store
            storage_options (dict[str, Any], optional): Keyword arguments passed to filesystem handler.
            **kwargs: Keyword arguments to pass to the super class initializer.
        """
        if "store" in kwargs:
            raise ValueError("Store of DirectoryDriver is fixed, `store` cannot be provided.")

        super().__init__(url=url, serializer=None, storage_options=storage_options, **kwargs)

    @property
    def store(self) -> AbstractStore:
        """Store that is used by the driver."""
        if self._store is None:
            self._store = FilesystemStore(url=self.url, serializer=self.serializer, **self.storage_options)
        return self._store


register_driver(DirectoryDriver)
