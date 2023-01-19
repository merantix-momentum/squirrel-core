import tempfile
from random import random
from typing import Tuple

import pytest
import torch.utils.data as tud

from squirrel.driver import MessagepackDriver
from squirrel.serialization import MessagepackSerializer, JsonSerializer
from squirrel.store.squirrel_store import CacheStore, SquirrelStore


def test_cache_store() -> None:
    """Test CacheStore"""
    with tempfile.TemporaryDirectory() as tmp_dir_1:
        with tempfile.TemporaryDirectory() as tmp_file_2:
            cs = CacheStore(
                url=tmp_dir_1,
                serializer=MessagepackSerializer(),
                cache_url=tmp_file_2,
            )
            cs.set({"a": 1}, key="test_key")
            assert len(list(cs._cache.keys())) == 0
            assert len(list(cs.keys())) == 1

            _ = list(cs.get("test_key"))

            assert len(list(cs._cache.keys())) == 1


def test_cached_store_with_driver(cached_uri_and_respective_driver: Tuple) -> None:
    """Test caching stores with drivers"""
    uri, uri_cache, driver_ = cached_uri_and_respective_driver
    driver = driver_(uri, cache_url=uri_cache)
    assert isinstance(driver.store, CacheStore)

    ser = MessagepackSerializer() if isinstance(driver, MessagepackDriver) else JsonSerializer()
    s = SquirrelStore(url=uri, serializer=ser)
    s.set(value=[{"a": 1}], key="test_key")
    assert len(list(s.keys())) == 1

    it = driver.get_iter().collect()
    assert it == [{"a": 1}]
    assert len(list(driver.store._cache.keys())) == 1

    it2 = driver.get_iter().collect()
    assert it2 == it
    assert list(driver.keys()) == list(driver.store.keys()) == list(driver.store._cache.keys()) == ["test_key"]


@pytest.mark.parametrize("num_workers", [0, 1, 2, 4])
def test_cache_with_pytorch(cached_uri_and_respective_driver: Tuple, num_workers: int) -> None:
    """Test if cache works with pytorch multiprocessing"""
    uri, uri_cache, driver_ = cached_uri_and_respective_driver
    samples = list(range(20))

    driver = driver_(uri, cache_url=uri_cache, storage_option={}, cash_storage_options={})
    ser = MessagepackSerializer() if isinstance(driver, MessagepackDriver) else JsonSerializer()
    s = SquirrelStore(url=uri, serializer=ser)
    for i in range(len(samples)):
        s.set(value=[{f"{i}": i}], key=f"{i}_key")

    if random() > 0.5:
        _ = driver.get_iter().split_by_worker_pytorch().to_torch_iterable().collect()
    it = driver.get_iter().split_by_worker_pytorch().to_torch_iterable().collect()
    dl = tud.DataLoader(it, num_workers=num_workers)

    out = [int(list(i.keys())[0]) for i in list(dl)]
    assert set(out) == set(samples)
