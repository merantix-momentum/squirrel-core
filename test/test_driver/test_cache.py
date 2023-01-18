import tempfile
from typing import Tuple

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
