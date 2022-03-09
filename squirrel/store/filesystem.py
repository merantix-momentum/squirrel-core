from __future__ import annotations

import os.path as osp
import random
import string
import typing as t

from squirrel.framework.io import read_from_file, write_to_file
from squirrel.fsspec.fs import create_dir_if_does_not_exist, get_fs_from_url
from squirrel.iterstream.source import FilePathGenerator
from squirrel.store.store import AbstractStore

if t.TYPE_CHECKING:
    from squirrel.serialization import SquirrelSerializer


def get_random_key(length: int = 16) -> str:
    """Generates a random key"""
    return "".join(random.choice(string.ascii_uppercase + string.digits) for _ in range(length))


class FilesystemStore(AbstractStore):
    """Store that uses fsspec to read from / write to files."""

    def __init__(
        self, url: str, serializer: t.Optional[SquirrelSerializer] = None, clean: bool = False, **storage_options
    ) -> None:
        """Initializes FilesystemStore.

        Args:
            url (str): Path to the root directory. If this path does not exist, it will be created.
            serializer (SquirrelSerializer, optional): Serializer that is used to serialize data before persisting (see
                :py:meth:`set`) and to deserialize data after reading (see :py:meth:`get`). If not specified, data will
                not be (de)serialized. Defaults to None.
            clean (bool): If true, all files in the store will be removed recursively
            **storage_options: Keyword arguments passed to filesystem initializer.
        """
        super().__init__()
        # gcs filesystem complains at some point when url ends with a "/"
        self.url = url.rstrip("/")
        self.storage_options = storage_options
        self.serializer = serializer
        self.fs = get_fs_from_url(self.url, **self.storage_options)
        if clean:
            self.fs.rm(self.url, recursive=True)
        create_dir_if_does_not_exist(self.fs, self.url)

    def get(self, key: str, mode: str = "rb", **open_kwargs) -> t.Any:
        """Yields the item with the given key.

        If the store has a serializer, data read from the file will be deserialized.

        Args:
            key (str): Key corresponding to the item to retrieve.
            mode (str): IO mode to use when opening the file. Defaults to "rb".
            **open_kwargs: Keyword arguments that will be forwarded to the filesystem object when opening the file.

        Yields:
            (Any) Item with the given key.
        """
        open_kwargs["mode"] = mode
        return read_from_file(f"{self.url}/{key}", self.fs, self.serializer, **open_kwargs)

    def set(self, value: t.Any, key: t.Optional[str] = None, mode: str = "wb", **open_kwargs) -> None:
        """Persists an item with the given key.

        If the store has a serializer, data item will be serialized before writing to a file.

        Args:
            value (Any): Item to be persisted.
            key (Optional[str]): Optional key corresponding to the item to persist.
            mode (str): IO mode to use when opening the file. Defaults to "wb".
            **open_kwargs: Keyword arguments that will be forwarded to the filesystem object when opening the file.
        """
        open_kwargs["mode"] = mode
        if key is None:
            key = get_random_key()
        write_to_file(f"{self.url}/{key}", value, self.fs, self.serializer, **open_kwargs)

    def keys(self, nested: bool = True, **kwargs) -> t.Iterator[str]:
        """Yields all paths in the store, relative to the root directory.

        Paths are generated using :py:class:`squirrel.iterstream.source.FilePathGenerator`.

        Args:
            nested (bool): Whether to return paths that are not direct children of the root directory. If True, all
                paths in the store will be yielded. Otherwise, only the top-level paths (i.e. direct children of the
                root path) will be yielded. This option is passed to FilePathGenerator initializer. Defaults to True.
            **kwargs: Other keyword arguments passed to the FilePathGenerator initializer.

        Yields:
            (str) Paths to files and directories in the store relative to the root directory.
        """
        fp_gen = FilePathGenerator(url=self.url, nested=nested, **kwargs)
        for path in fp_gen:
            yield osp.relpath(path, start=self.url)
