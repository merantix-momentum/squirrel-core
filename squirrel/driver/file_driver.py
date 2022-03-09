from typing import IO

from squirrel.driver.driver import Driver


class FileDriver(Driver):
    name = "file"

    def __init__(self, path: str, **kwargs) -> None:
        """Initializes FileDriver.

        Args:
            path (str): Path to a file.
            **kwargs: Keyword arguments passed to the super class initializer.
        """
        super().__init__(**kwargs)
        self.path = path

    def open(self, mode: str = "r", create_if_not_exists: bool = False, **kwargs) -> IO:
        """Returns a handler for the file.

        Uses :py:func:`squirrel.fsspec.fs.get_fs_from_url` to get a filesystem object corresponding to `self.path`.
        Simply returns the handler returned from the `open()` method of the filesystem.

        Args:
            mode (str): IO mode to use when opening the file. Will be forwarded to `filesystem.open()` method. Defaults
                to "r".
            create_if_not_exists (bool): If True, the file will be created if it does not exist (along with the parent
                directories). This is achieved by providing `auto_mkdir=create_if_not_exists` as a storage option to
                the filesystem. Defaults to False.
            **kwargs: Keyword arguments that are passed to the `filesystem.open()` method.

        Return:
            (IO) File handler for the file at `self.path`.
        """
        from squirrel.fsspec.fs import get_fs_from_url

        fs = get_fs_from_url(self.path, storage_options={"auto_mkdir": create_if_not_exists})
        return fs.open(self.path, mode=mode, **kwargs)
