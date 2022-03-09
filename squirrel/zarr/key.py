import itertools
import re
import typing as t

from squirrel.constants import SQUIRREL_DIR, SQUIRREL_PREFIX

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


def flatten(nested_keys: t.List[t.Iterable[str]]) -> t.List[str]:
    """Flatten a nested list of keys."""
    return list(itertools.chain.from_iterable(nested_keys))
