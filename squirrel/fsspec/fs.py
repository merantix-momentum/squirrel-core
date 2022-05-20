import fsspec
from fsspec.core import split_protocol

from squirrel.constants import FILESYSTEM, URL

__all__ = ["get_fs_from_url", "get_protocol", "create_dir_if_does_not_exist"]


def get_fs_from_url(url: URL, **storage_options) -> FILESYSTEM:
    """Get filesystem suitable for a url.

    Only the protocol part of the url is used, thus there is no need to call this method several times for different
    stores accessed with the same protocol. Should be called outside of store initialization to allow buffering from
    the same container among different stores or shards.

    If the protocol "gs" is detected, will return a custom squirrel gcs file system instance which is more robust. For
    details, see :py:class:`squirrel.fsspec.custom_gcsfs.CustomGCSFileSystem`).

    Users must have one of these IAM right in the respective projects to be able to access a requester pay bucket:
    (Copied from https://issuetracker.google.com/issues/156960628.)

    Requesters who include a billing project in their request. The project used in the request must be in good standing,
    and the user must have a role in the project that contains the serviceusage.services.use permission.
    The Editor and Owner roles contain the required permission.

    Requesters who don't include a billing project but have resourcemanager.projects.createBillingAssignment permission
    for the project that contains the bucket. The Billing Project Manager role contains the required permission.
    Access charges associated with these requests are billed to the project that contains the bucket.

    If you have encountered `ValueError: Bucket is requester pays. Set `requester_pays=True` when creating the
    GCSFileSystem.` I suggest you instead of passing `requester_pays=True` to `storage_options` in fsspec, simply
    switch to the right project by `gcloud config set project PROJECT_ID` where you have the right role(s). The problem
    should be resolved by itself.
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
