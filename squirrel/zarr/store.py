"""
At a low level, squirrel offers some store options that could enhance or modify zarr stores such as

- reducing number of HTTP calls zarr has to make in order to fetch items from cloud datasets
- fetching items from datasets in an asynchronous manner
- caching storage path keys and metadata

to allow fast cluster / full iteration through the dataset.

Currently, we offer the following store:

- :py:class:`~squirrel.zarr.store.SquirrelFSStore` is based on ``zarr.storage.FSStore``. In SquirrelFSStore, we use the
  :py:mod:`squirrel.fsspec` module to control HTTP connections to cloud buckets.
"""

import logging
import typing as t

from numcodecs import Blosc
from numcodecs.abc import Codec
from zarr.errors import FSPathExistNotDir
from zarr.storage import FSStore

from squirrel.constants import URL
from squirrel.fsspec.fs import get_fs_from_url
from squirrel.zarr.key import is_dir

logger = logging.getLogger(__name__)


def suggest_compression() -> Codec:
    """Suggested compression method from squirrel."""
    return Blosc(cname="lz4hc", clevel=5, shuffle=Blosc.BITSHUFFLE)


class SquirrelFSStore(FSStore):
    """Alters the default fs argument to :py:class:`~squirrel.fsspec.custom_gcsfs.CustomGCSFileSystem` if the url
    location directs to google cloud bucket. This allows us to use the custom gcsfs to open FSStore inside Zarr. The
    main purpose is too add google 400 error into retriable options when doing https requests to google cloud storage
    buckets so that 400 does not break our cloud build, tests, and other time consuming operations. For the main issue,
    see https://github.com/dask/gcsfs/issues/290.
    """

    def __init__(
        self,
        url: URL,
        key_separator: str = ".",
        mode: str = "w",
        exceptions: t.Sequence[t.Type[Exception]] = (KeyError, PermissionError, IOError),
        check_exists: bool = False,
        **storage_options,
    ) -> None:
        """Initialize SquirrelFSStore.

        Args:
            url (URL): Path to the store.
            key_separator (str, optional): Separator placed between the dimensions of a chunk. Defaults to ".".
            mode (str, optional): File IO mode to use. Defaults to "w".
            exceptions (Sequence[Type[Exception]], optional): When accessing data, any of these exceptions will be
                treated as a missing key. Defaults to (KeyError, PermissionError, IOError).
            check_exists (bool, optional): Whether to check that `url` corresponds to a directory. Defaults to False.
            **storage_options: Storage options to be passed to fsspec.

        Raises:
            FSPathExistNotDir: If `check_exists == True` and `url` does not correspond to a directory.
        """
        self.url = url
        self.normalize_keys = False
        self.key_separator = key_separator
        self.fs = get_fs_from_url(url, **storage_options)
        self.map = self.fs.get_mapper(url)
        self.path = self.fs._strip_protocol(url)
        self.mode = mode
        self.exceptions = exceptions

        if check_exists and self.fs.exists(self.path) and not self.fs.isdir(self.path):
            raise FSPathExistNotDir(url)

    def listdir(self, path: URL = None) -> t.List[str]:
        """List all dirs under the path, except for squirrel keys and zarr keys."""
        dir_path = self.dir_path(path)
        try:
            children = sorted(p.rstrip("/").rsplit("/", 1)[-1] for p in self.fs.ls(dir_path, detail=False))
            return [c for c in children if is_dir(c)]
        except IOError:
            return []

    def getsize(self, path: URL = None) -> int:
        """Get size of a subdir inside the store."""
        store_path = self.dir_path(path)
        return self.fs.du(store_path, total=True, maxdepth=None)
