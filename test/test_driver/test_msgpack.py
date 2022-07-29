import tempfile
import warnings
import pytest
import torch.utils.data as tud

from squirrel.constants import URL
from squirrel.driver import MessagepackDriver
from squirrel.iterstream import IterableSource
from squirrel.iterstream.torch_composables import TorchIterable, SplitByWorker
from squirrel.serialization import MessagepackSerializer
from squirrel.store import SquirrelStore


def test_invalid_url(local_msgpack_url: URL) -> None:
    """Test if an error is raised when an invalid url is passed"""

    # empty directory
    with tempfile.TemporaryDirectory() as tmp_dir:
        with pytest.warns(UserWarning):
            _ = MessagepackDriver(url=tmp_dir).get_iter().collect()

    # directory containing only empty directories
    with tempfile.TemporaryDirectory() as tmp_dir:
        with tempfile.TemporaryDirectory(dir=tmp_dir) as sub_dir:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                _ = MessagepackDriver(url=tmp_dir).get_iter().collect()
            with pytest.warns(UserWarning):
                _ = MessagepackDriver(url=sub_dir).get_iter().collect()

    # test invalid url
    invalid_url = "invalid_url"
    with pytest.warns(UserWarning):
        _ = MessagepackDriver(url=invalid_url).get_iter().collect()

    # test valid url containing samples
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        _ = MessagepackDriver(url=local_msgpack_url).get_iter().collect()


def test_dataloader_2_workers(local_msgpack_url: URL, num_samples: int) -> None:
    """Test passing an instance of a `Composable` to `torch.utils.data.DataLoader` with 2 workers"""
    it = MessagepackDriver(url=local_msgpack_url).get_iter(key_hooks=[SplitByWorker]).compose(TorchIterable)
    dl = tud.DataLoader(it, num_workers=2)
    assert len(list(dl)) == num_samples

    it2 = MessagepackDriver(url=local_msgpack_url).get_iter().split_by_worker_pytorch().to_torch_iterable()
    dl2 = tud.DataLoader(it2, num_workers=2)
    assert len(list(dl2)) == num_samples


def test_keys(dummy_msgpack_store: SquirrelStore, num_samples: int) -> None:
    """Test keys method of the store"""
    keys = list(dummy_msgpack_store.keys())
    assert len(keys) == 2


def test_get_iter(dummy_msgpack_store: SquirrelStore, num_samples: int) -> None:
    """Test MessagepackDriver.get_iter"""
    it = MessagepackDriver(url=dummy_msgpack_store.url).get_iter().collect()
    assert isinstance(it, list)
    assert len(list(it)) == num_samples


def test_clean_store() -> None:
    """Test instantiating a store and removing all it's content"""
    with tempfile.TemporaryDirectory() as tmp_dir:
        _ = SquirrelStore(tmp_dir, clean=True, serializer=MessagepackSerializer())
        driver = MessagepackDriver(url=tmp_dir)
        IterableSource([{f"k{i}": f"v{i}"} for i in range(4)]).batched(2).async_map(driver.store.set).join()
        assert len(list(driver.store.keys())) == 2
        store = SquirrelStore(tmp_dir, clean=True, serializer=MessagepackSerializer())
        assert len(list(store.keys())) == 0


def test_shard_no_key() -> None:
    """Test if samples and shards are correctly written to and retrieved from SquirrelStore when key is not given"""
    num = 15
    num_shards = 5
    num_samples_in_shard = 3
    samples = [{} for _ in range(num)]
    shards = [samples[idx : idx + num_samples_in_shard] for idx in range(num)[::num_samples_in_shard]]
    assert len(shards) == num_shards
    assert all(len(i) == num_samples_in_shard for i in shards)
    with tempfile.TemporaryDirectory() as tmp_dir:
        store = SquirrelStore(tmp_dir, serializer=MessagepackSerializer())
        for k, sh in enumerate(shards):
            store.set(value=sh, key=str(k))

        keys_ = []
        samples_ = []
        for k in store.keys():
            keys_.append(k)
            for sample in store.get(k):
                samples_.append(sample)

        assert len(keys_) == num_shards
        assert len(samples_) == num
