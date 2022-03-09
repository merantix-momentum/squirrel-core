from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from squirrel.driver import ZarrDriver

if TYPE_CHECKING:
    from squirrel.integration_test.helpers import SHAPE
    from squirrel.zarr.group import SquirrelGroup


@pytest.mark.parametrize("max_workers", [0, 10])
def test_zarr_driver(create_test_group: SquirrelGroup, array_shape: SHAPE, max_workers: int) -> None:
    """Test loading zarr items from a store"""
    dl = ZarrDriver(url=create_test_group.store.url, max_workers=max_workers)
    it = dl.get_iter()
    for arr in it:
        assert arr.shape == array_shape
    root = dl.get_root_group()
    assert set(root.keys()).difference({"4", "2", "1", "3"}) == set()
