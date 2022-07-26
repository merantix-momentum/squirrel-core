import tempfile
from functools import partial
from typing import List, Any
from unittest import mock

import pytest
import torch
import torch.utils.data as tud

from squirrel.driver import MessagepackDriver
from squirrel.iterstream.iterators import map_
from squirrel.iterstream.source import IterableSource
from squirrel.iterstream.torch_composables import SplitByRank, SplitByWorker, TorchIterable, skip_k


@pytest.fixture(scope="module", autouse=True)
def samples() -> List[int]:
    """Fixture for this modules test data"""
    return list(range(100))


def test_convenience_compose_pytorch(samples: List[int]) -> None:
    """Test convenience functions for converting Composables to PyTorch"""
    batch_size = 5

    it1 = IterableSource(samples).compose(SplitByWorker).batched(batch_size).compose(TorchIterable)
    it2 = IterableSource(samples).split_by_worker_pytorch().batched(batch_size).to_torch_iterable()

    it3 = IterableSource(samples).compose(SplitByRank).batched(batch_size).compose(TorchIterable)
    it4 = IterableSource(samples).split_by_rank_pytorch().batched(batch_size).to_torch_iterable()

    assert it1.collect() == it2.collect()
    assert it3.collect() == it4.collect()


def test_skip_k() -> None:
    """Check if partial skip application successful."""
    it = range(10)
    fn = skip_k(0, 2)
    assert list(fn(it)) == list(it)[0::2]
    fn = skip_k(1, 2)
    assert list(fn(it)) == list(it)[1::2]


def test_torch_iterable(samples: List[int]) -> None:
    """Test TorchIterable mixin successful for torch dataloader"""
    num_workers = 4
    batch_size = 5

    it = IterableSource(samples).split_by_worker_pytorch().batched(batch_size).to_torch_iterable()

    dl = tud.DataLoader(it, num_workers=num_workers)

    out = torch.Tensor(list(dl))
    assert sorted(out.cpu().flatten().numpy().tolist()) == samples
    assert out.size() == (20, 5)


def _times_two(x: float) -> float:
    """Helper function to test map and async_map. Needs to be out here for picklability."""
    return x * 2


def test_multi_worker_torch_iterable_map(samples: List[int]) -> None:
    """Test map is picklable and forkable in pytorch multiprocessing context"""
    num_workers = 4
    batch_size = 5

    it = IterableSource(samples).map(_times_two).split_by_worker_pytorch().batched(batch_size).to_torch_iterable()

    dl = tud.DataLoader(it, num_workers=num_workers)

    out = torch.Tensor(list(dl))
    assert sorted(out.cpu().flatten().numpy().tolist()) == [2 * s for s in samples]
    assert out.size() == (20, 5)


def test_multi_worker_torch_iterable_async_map(samples: List[int]) -> None:
    """Test async_map is picklable and forkable in pytorch multiprocessing context"""
    num_workers = 4
    batch_size = 5

    it = IterableSource(samples).async_map(_times_two).split_by_worker_pytorch().batched(batch_size).to_torch_iterable()

    dl = tud.DataLoader(it, num_workers=num_workers)

    out = torch.Tensor(list(dl))
    assert sorted(out.cpu().flatten().numpy().tolist()) == [2 * s for s in samples]
    assert out.size() == (20, 5)


@mock.patch("torch.distributed.is_available", mock.MagicMock(return_value=True))
@mock.patch("torch.distributed.is_initialized", mock.MagicMock(return_value=True))
@mock.patch("torch.distributed.group.WORLD", mock.MagicMock(return_value="WORLD"))
@mock.patch("torch.distributed.get_world_size")
@mock.patch("torch.distributed.get_rank")
def test_multi_rank_torch_iterable(mock_get_rank: int, mock_get_world_size: int, samples: List[int]) -> None:
    """Test multi-rank split functionality"""
    world_size = 4
    mock_get_world_size.return_value = world_size

    for rank in range(world_size):
        mock_get_rank.return_value = rank
        out = IterableSource(samples).split_by_rank_pytorch().collect()
        assert out == samples[rank::world_size]


@mock.patch("torch.distributed.is_available", mock.MagicMock(return_value=True))
@mock.patch("torch.distributed.is_initialized", mock.MagicMock(return_value=True))
@mock.patch("torch.distributed.group.WORLD", mock.MagicMock(return_value="WORLD"))
@mock.patch("torch.distributed.get_world_size")
@mock.patch("torch.distributed.get_rank")
def test_multi_rank_multi_worker_torch_iterable(
    mock_get_rank: int, mock_get_world_size: int, samples: List[int]
) -> None:
    """
    Test multi-rank functionality as well as async_map forkability in pytorch multiprocessing context
    using multiple workers in the dataloader.
    """
    world_size = 2
    batch_size = 5
    num_workers = 2
    mock_get_world_size.return_value = world_size

    for rank in range(world_size):
        mock_get_rank.return_value = rank
        it = (
            IterableSource(samples)
            .split_by_rank_pytorch()
            .async_map(_times_two)
            .split_by_worker_pytorch()
            .batched(batch_size)
            .to_torch_iterable()
        )
        dl = tud.DataLoader(it, num_workers=num_workers)
        out = torch.Tensor(list(dl))
        assert sorted(out.cpu().flatten().numpy().tolist()) == [2.0 * s for s in samples[rank::world_size]]

        with tempfile.TemporaryDirectory() as tmp_dir:
            driver = MessagepackDriver(tmp_dir)
            store = driver.store
            keys_ = list(range(1000, 1100))
            for idx, sh in enumerate(samples):
                store.set(value=sh, key=keys_[idx])

            def _cb(x: Any) -> Any:
                return x

            it2 = (
                driver.get_iter(
                    key_hooks=[
                        _cb,
                        SplitByRank,
                        partial(map_, *[], **{"callback": _cb}),
                        SplitByWorker,
                    ]
                )
                .async_map(_times_two)
                .to_torch_iterable()
            )

            it3 = (
                driver.get_iter()
                .split_by_rank_pytorch()
                .split_by_worker_pytorch()
                .async_map(_times_two)
                .to_torch_iterable()
            )

            expected = [2.0 * s for s in samples[rank::world_size]]
            dl2 = tud.DataLoader(it2, num_workers=num_workers)
            out2 = torch.Tensor(list(dl2))
            assert sorted(out2.cpu().flatten().numpy().tolist()) == expected

            dl3 = tud.DataLoader(it3, num_workers=num_workers)
            out3 = torch.Tensor(list(dl3))
            assert sorted(out3.cpu().flatten().numpy().tolist()) == expected
