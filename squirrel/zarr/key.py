import itertools
import re
import typing as t
from collections import MutableMapping

from squirrel.constants import FILESYSTEM, SQUIRREL_DIR, SQUIRREL_PREFIX
from squirrel.dataset.stream_dataset import Fetcher, StreamDataset

# should match zarr chunks like 0.0.0 or 200.0.40.34.89
# regex logic: any repetition of (series of digits and .) , and then another digit series without . at the end
ZARR_CHUNK_PATTERN = re.compile(r"(\d+\.)+\d+")


def is_zarr_key(key: str) -> bool:
    """Check if the given key is a zarr key."""
    return key.endswith(".zarray") or key.endswith(".zgroup") or key.endswith(".zattrs")


def is_squirrel_key(key: str) -> bool:
    """Check if the given key is a key set by squirrel."""
    return key_end(key).startswith(SQUIRREL_PREFIX) or SQUIRREL_DIR in key


def is_zarr_chunk(key: str) -> bool:
    """Determines whether a key contains zarr chunk pattern (e.g. '0.0.0') or not."""
    return True if ZARR_CHUNK_PATTERN.match(key_end(key)) else False


def is_dir(key: str) -> bool:
    """Infers if a given key is for a folder or not (.zip files are considered as files)."""
    if is_zarr_key(key) or is_zarr_chunk(key) or key.endswith(".zip") or is_squirrel_key(key):
        return False
    return True


def key_end(key: str, separator: str = "/") -> str:
    """Split the key with `separator` and return the last token.

    Examples:
        >>> key_end("path/to/my.file")
        'my.file'

        >>> key_end("path/to/my.file", separator=".")
        'file'
    """
    return key.rsplit(separator, 1)[-1]


def normalize_key(key: str, key_separator: str = "/") -> str:
    """Normalize keys.

    Removes preceding and trailing whitespace and 'key_separator's. If the key corresponds to a folder, then it is made
    sure that the key ends with `key_separator`.
    """
    if key:
        key = key.strip()
        key = key.strip(key_separator)
        if is_dir(key.split(key_separator)[-1]):
            key += key_separator
    return key


def retrieve_all_keys(
    store: MutableMapping, prefix: t.Union[str, t.Sequence[str]] = None
) -> t.Generator[str, None, None]:
    """Retrieve keys from a store in a parallelized manner.

    Args:
        store (MutableMapping): Store to retrieve keys from. `store` is expected to have the attributes `fs` and
            `path`.
        prefix (Union[str, Sequence[str]], optional): If provided, only keys with this prefix will be retrieved.
        For example, if `prefix == "samples/sample_1"`, only the keys under "sample_1" will be retrieved. If a list of
        keys are provided, then all keys under each key will be retrieved, without checking for duplications. If
        `prefix` is pointing to a file rather than a folder, the file will be returned. Defaults to None.

    Yields:
        str: Keys in the store. The keys are relative to the store, i.e. they do not include the location of the store.

    Examples:
        >>> import numpy as np
        >>> from squirrel.zarr.convenience import get_group
        >>> g = get_group("path/to/my/group")
        >>> g["a_key"] = np.ones((2,2))
        >>> g["another_key"] = np.ones((2,2))
        >>> list(retrieve_all_keys(g.store))
        ['.zgroup', 'a_key/0.0', 'a_key/.zarray', 'another_key/0.0', 'another_key/.zarray']

        See above that "path/to/my/group" is not included in the keys.

        We can use `prefix` to restrict keys:

        >>> list(retrieve_all_keys(g.store), prefix="a_key")
        ['a_key/0.0', 'a_key/.zarray']
    """
    fs = store.fs
    path = store.path
    paths = []
    if prefix is None:
        paths.append(path)
    elif isinstance(prefix, str):
        paths.append(path + "/" + normalize_key(prefix))
    elif isinstance(prefix, t.Sequence):
        for k in prefix:
            paths.append(path + "/" + normalize_key(k))
    else:
        raise ValueError(f"Prefix cannot be identified: {prefix}")

    _keys = []
    _folders = []
    for p in paths:
        if is_dir(p):
            _folders.append(p)
        else:
            _keys.append(p)

    if len(_folders) == 0:
        for k in _keys:
            yield normalize_key(k[len(path) :])
        return

    fetcher = FSGroupFetcher(fs=fs)
    with StreamDataset(fetcher, cache_size=100_000) as zds:
        while len(_folders) > 0:
            len_folder = len(_folders)
            for _ in range(len_folder):
                ff = _folders.pop()
                zds.request(ff)

            for _ in range(len_folder):
                a_keys, a_folders = zds.retrieve()
                _keys.extend(a_keys)
                _folders.extend(a_folders)
                # TODO save folders?
            for k in _keys:
                yield normalize_key(k[len(path) :])
            _keys = []


class FSGroupFetcher(Fetcher):
    """Helper class for method :py:meth:`retrieve_all_keys`, to retrieve a group of keys in a async fashion."""

    def __init__(self, fs: FILESYSTEM, **kwargs) -> None:
        """Initialize FSGroupFetcher."""
        self.fs = fs

    def fetch(self, key: str) -> t.Tuple[t.List, t.List]:
        """Call fs.ls() to fetch files and directories under `key` and split them into two lists, keys and folders."""
        keys = []
        folder = []
        for k in self.fs.ls(path=key):
            if is_dir(k):
                folder.append(k)
            else:
                keys.append(k)
        return keys, folder

    def write(self, key: str, val: t.Any) -> None:
        """Write is not implemented."""
        raise NotImplementedError


class KeyFetcher(Fetcher):
    """Class used in write_shard_to_store for parallel key retrieval"""

    def __init__(self, store: MutableMapping, **kwargs) -> None:
        """Initialize the key fetcher"""
        self.store = store

    def fetch(self, key: str) -> t.Any:
        """Read key in store"""
        return self.store[key]

    def write(self, key: str, val: t.Any) -> None:
        """Write is not implemented"""
        raise NotImplementedError


def flatten(nested_keys: t.List[t.Iterable[str]]) -> t.List[str]:
    """Flatten a nested list of keys."""
    return list(itertools.chain.from_iterable(nested_keys))
