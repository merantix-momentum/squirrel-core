"""Contains methods that help with writing/reading from files."""

from typing import Any, Optional

import fsspec

from squirrel.constants import FILESYSTEM
from squirrel.serialization import SquirrelSerializer


def write_to_file(
    fp: str, obj: Any, fs: Optional[FILESYSTEM] = None, serializer: Optional[SquirrelSerializer] = None, **open_kwargs
) -> None:
    """Writes an object to a file.

    Args:
        fp (str): Path to the file.
        obj (Any): Object to be written.
        fs (FILESYSTEM, optional): Filesystem object to be used to open the file. If not provided, the suitable
            filesystem will be determined using :py:func:`squirrel.fsspec.fs.get_fs_from_url`. Defaults to None.
        serializer (SquirrelSerializer, optional): Serializer to be used to serialize `obj` before writing to file.
            If not provided, `obj` will not be serialized. Defaults to None.
        **open_kwargs: Keyword arguments that will be forwarded to the filesystem object when opening the file.
    """
    if fs is None:
        fs = fsspec

    if serializer is not None:
        obj = serializer.serialize(obj)

    with fs.open(fp, **open_kwargs) as f:
        f.write(obj)


def read_from_file(
    fp: str, fs: Optional[FILESYSTEM] = None, serializer: Optional[SquirrelSerializer] = None, **open_kwargs
) -> Any:
    """Reads from a file.

    Args:
        fp (str): Path to the file.
        fs (FILESYSTEM, optional): Filesystem object to be used to open the file. If not provided, the suitable
            filesystem will be determined using :py:func:`squirrel.fsspec.fs.get_fs_from_url`. Defaults to None.
        serializer (SquirrelSerializer, optional): Serializer to be used to deserialize the data read from the file.
            If not provided, read data will not be deserialized. Defaults to None.
        **open_kwargs: Keyword arguments that will be forwarded to the filesystem object when opening the file.

    Returns:
        (Any) Read (and possibly deserialized) data.
    """
    if fs is None:
        fs = fsspec

    with fs.open(fp, **open_kwargs) as f:
        data = f.read()
        if serializer is not None:
            data = serializer.deserialize(data)
        return data
