from __future__ import annotations

import random
from typing import TYPE_CHECKING

from squirrel.driver import StoreDriver
from squirrel.iterstream import FilePathGenerator

if TYPE_CHECKING:
    from squirrel.store import SquirrelStore


def test_keys(dummy_sq_store: SquirrelStore, num_samples: int) -> None:
    """Test keys method of the store."""
    keys = list(dummy_sq_store.keys())
    assert len(keys) == num_samples


def test_get_iter(dummy_sq_store: SquirrelStore) -> None:
    """Test StoreDriver.get_iter."""
    it = StoreDriver(url=dummy_sq_store.url, serializer=dummy_sq_store.serializer).get_iter().collect()
    assert isinstance(it, list)
    # TODO: This needs to be fixed to either inject the compression
    # suffix here, or to disable compression in dummy_sq_store
    # or to inject the correct compression suffix in `get_sample`
    keys = [sample["key"] for sample in it]
    it2 = (
        StoreDriver(url=dummy_sq_store.url, serializer=dummy_sq_store.serializer)
        .get_iter(keys_iterable=keys[:2])
        .collect()
    )
    assert len(it2) == 2
    assert set(keys[:2]) == {sample["key"] + ".gz" for sample in it2}
    random.shuffle(keys)
    it3 = (
        StoreDriver(url=dummy_sq_store.url, serializer=dummy_sq_store.serializer)
        .get_iter(keys_iterable=keys[:2], shuffle_key_buffer=1)
        .collect()
    )
    assert set(keys[:2]) == {sample["key"] for sample in it3}


def test_filepathgenerator(dummy_sq_store: SquirrelStore, num_samples: int) -> None:
    """Test FilePathGenerator."""
    assert len(FilePathGenerator(url=dummy_sq_store.url).collect()) == num_samples
