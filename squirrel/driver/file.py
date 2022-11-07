from __future__ import annotations

from typing import IO, Any

import fsspec

from squirrel.constants import URL
from squirrel.driver.driver import Driver


class FileDriver(Driver):
    name = "file"

    def __init__(self, url: URL, storage_options: dict[str, Any] | None = None, **kwargs) -> None:
        """Initializes FileDriver.

        Args:
            url (URL): URL to file. Prefix with a protocol like ``s3://`` or ``gs://`` to read from other filesystems.
                       For a full list of supported types, refer to :func:`fsspec.open()`.
            storage_options (dict[str, Any] | None): A dict with keyword arguments passed to file system initializer.
            **kwargs: Keyword arguments passed to the super class initializer.
        """
        super().__init__(**kwargs)
        self.url = url
        self.storage_options = storage_options or {}

    def open(self, mode: str = "r", create_if_not_exists: bool = False, **kwargs) -> IO:
        """Returns a handler for the file.

        Uses :py:func:`squirrel.fsspec.fs.get_fs_from_url` to get a filesystem object corresponding to `self.url`.
        Simply returns the handler returned from the `open()` method of the filesystem.

        Args:
            mode (str): IO mode to use when opening the file. Will be forwarded to `filesystem.open()` method. Defaults
                to "r".
            create_if_not_exists (bool): If True, the file will be created if it does not exist (along with the parent
                directories). This is achieved by providing `auto_mkdir=create_if_not_exists` as a storage option to
                the filesystem. No matter what you set in the FileDriver's `storage_options`, `create_if_not_exists`
                will override the key `auto_mkdir`. Defaults to False.
            **kwargs: Keyword arguments that are passed to the `filesystem.open()` method.

        Return:
            (IO) File handler for the file at `self.path`.
        """
        kwargs = {**self.storage_options, "auto_mkdir": create_if_not_exists, **kwargs}
        return fsspec.open(self.url, mode=mode, **kwargs)
