from squirrel import fsspec
from squirrel.constants import FILESYSTEM, URL
from squirrel.fsspec import split_protocol

__all__ = ["get_fs_from_url", "get_protocol", "create_dir_if_does_not_exist"]


def get_fs_from_url(url: URL, **storage_options) -> FILESYSTEM:
    """Get filesystem suitable for a url.

    Only the protocol part of the url is used, thus there is no need to call this method several times for different
    stores accessed with the same protocol. Should be called outside of store initialization to allow buffering from
    the same container among different stores or shards.

    If the protocol "gs" is detected, will return a custom squirrel gcs file system instance which is more robust. For
    details, see :py:class:`squirrel.fsspec.custom_gcsfs.CustomGCSFileSystem`).
    """
    protocol, _ = split_protocol(url)
    return fsspec.filesystem(protocol, **storage_options)


def get_protocol(url: str) -> str:
    """Get the protocol from a url, return empty string if local"""
    return f"{url.split('://')[0]}://" if "://" in url else ""


def create_dir_if_does_not_exist(fs: fsspec.spec.AbstractFileSystem, path: str) -> None:
    """If the given path does not exist, creates a directory at it."""
    if not fs.exists(path):
        fs.mkdir(path)
