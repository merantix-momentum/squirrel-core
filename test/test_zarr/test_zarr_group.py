import pytest
from zarr.errors import ReadOnlyError
from zarr.hierarchy import Group

from squirrel.constants import URL
from squirrel.zarr.group import get_group
from squirrel.zarr.key import normalize_key


def test_normalize_key() -> None:
    """Test if normalization of a key is consistent"""
    testcases = [
        ("a", "a/"),
        ("a/", "a/"),
        ("/a/", "a/"),
        ("/a", "a/"),
        ("a/ab/", "a/ab/"),
        ("/a/ab/", "a/ab/"),
        ("a/ab/0.0", "a/ab/0.0"),
        ("a/ab/0.0/", "a/ab/0.0"),
        ("/a/ab/0.0/", "a/ab/0.0"),
        (".zarray", ".zarray"),
        ("a/.zarray", "a/.zarray"),
        (".zattrs", ".zattrs"),
        ("a/.zattrs", "a/.zattrs"),
        (".zgroup", ".zgroup"),
        ("a/.zgroup", "a/.zgroup"),
        ("sample_id/file.ext", "sample_id/file.ext/"),  # real use case where we set tile id to original filename
    ]

    for testcase in testcases:
        assert normalize_key(testcase[0]) == testcase[1]


def test_del_item_r_group(test_path: URL, create_test_group: Group) -> None:
    """Test if deleting an item to a read only store raises an read only exception."""
    _ = create_test_group
    root = get_group(test_path, "r")
    with pytest.raises(ReadOnlyError):
        del root[list(root)[0]]


def test_set_item_r_group(test_path: URL, create_test_group: Group) -> None:
    """Test if setting an item to a read only store raises an read only exception."""
    _ = create_test_group
    root = get_group(test_path, "r")
    with pytest.raises(ReadOnlyError):
        root["test"] = 1
