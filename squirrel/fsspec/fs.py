from __future__ import annotations
import os

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

    Allow users to access requester pay buckets, if a env var `REQUESTER_PAYS` is True. In such case, the user will
    inform gcloud to pay with its project it is currently in. To switch project, please use
    `gcloud config set project PROJECT_ID`. We do not support specifying another project other than your default
    project, this will cause a side effect, that we have to give cloud build service account too much IAM rights
    for testing to happen in cloudbuilds. For details, read below about IAM rights for requester pays.

    Users must have one of these IAM right in the respective projects to be able to access the requester pay bucket:
    (Copied from https://issuetracker.google.com/issues/156960628.)

    Requesters who include a billing project in their request. The project used in the request must be in good standing,
    and the user must have a role in the project that contains the serviceusage.services.use permission.
    The Editor and Owner roles contain the required permission.

    Requesters who don't include a billing project but have resourcemanager.projects.createBillingAssignment permission
    for the project that contains the bucket. The Billing Project Manager role contains the required permission.
    Access charges associated with these requests are billed to the project that contains the bucket.
    """
    protocol, _ = split_protocol(url)
    requester_pays = eval(os.environ.get("REQUESTER_PAYS").capitalize())
    validate_requester_pay(requester_pays)
    if requester_pays is True:  # filter other wield strings
        storage_options = {**storage_options, "requester_pays": requester_pays}
    return fsspec.filesystem(protocol, **storage_options)


def validate_requester_pay(requester_pays: bool) -> None:
    """Valide the requester pay env var, make sure there is no weird strings."""
    assert isinstance(requester_pays, bool), ValueError(
        "Environment variable 'REQUESTER_PAYS' only accepts value 'true', 'false', 'True' or 'False' in string."
    )


def get_protocol(url: str) -> str:
    """Get the protocol from a url, return empty string if local"""
    return f"{url.split('://')[0]}://" if "://" in url else ""


def create_dir_if_does_not_exist(fs: fsspec.spec.AbstractFileSystem, path: str) -> None:
    """If the given path does not exist, creates a directory at it."""
    if not fs.exists(path):
        fs.mkdir(path)
