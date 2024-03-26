from __future__ import annotations

import random
import pytest
import tempfile

from squirrel.driver import StoreDriver
from squirrel.iterstream import FilePathGenerator
from squirrel.serialization import MessagepackSerializer
from squirrel.store import SquirrelStore
from squirrel.fsspec.fs import get_fs_from_url
from squirrel.integration_test.helpers import SHAPE, get_sample


def test_store_creation(dummy_sq_store: SquirrelStore, array_shape: SHAPE, num_samples: int) -> None:
    """Test store creation with non empty path."""
    uncleaned_store = SquirrelStore(url=dummy_sq_store.url, serializer=MessagepackSerializer(), clean=False)
    assert len(list(uncleaned_store.keys())) == num_samples
    uncleaned_store.set(get_sample(array_shape))
    assert len(list(uncleaned_store.keys())) == num_samples + 1

    cleaned_store = SquirrelStore(url=dummy_sq_store.url, serializer=MessagepackSerializer(), clean=True)
    assert not cleaned_store.fs.exists(dummy_sq_store.url)
    assert len(list(cleaned_store.keys())) == 0
    cleaned_store.set(get_sample(array_shape))
    assert len(list(cleaned_store.keys())) == 1


@pytest.mark.parametrize("clean", [True, False])
def test_store_creation_empty_path(test_path: str, array_shape: SHAPE, clean: bool) -> None:
    """Test store creation with an empty path."""
    fs = get_fs_from_url(test_path)
    assert not fs.exists(test_path)
    store = SquirrelStore(url=test_path, serializer=MessagepackSerializer(), clean=clean)
    assert not fs.exists(test_path)  # Directory should not be created at initialization
    assert list(store.keys()) == []
    store.set(get_sample(array_shape))
    assert len(list(store.keys())) == 1


def test_keys(dummy_sq_store: SquirrelStore, num_samples: int) -> None:
    """Test keys method of the store."""
    keys = list(dummy_sq_store.keys())
    assert len(keys) == num_samples


def test_get_iter(dummy_sq_store: SquirrelStore) -> None:
    """Test StoreDriver.get_iter."""
    it = StoreDriver(url=dummy_sq_store.url, serializer=dummy_sq_store.serializer).get_iter().collect()
    assert isinstance(it, list)
    keys = [sample["key"] for sample in it]
    it2 = (
        StoreDriver(url=dummy_sq_store.url, serializer=dummy_sq_store.serializer)
        .get_iter(keys_iterable=keys[:2])
        .collect()
    )
    assert len(it2) == 2
    assert set(keys[:2]) == {sample["key"] for sample in it2}
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

    with tempfile.TemporaryDirectory() as tmpdir:
        assert len(FilePathGenerator(url=tmpdir).collect()) == 0
