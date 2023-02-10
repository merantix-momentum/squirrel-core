import logging
from functools import partial
from itertools import islice
from typing import Callable, Iterable, Iterator, Optional

import torch
from torch.utils.data import IterableDataset

from squirrel.iterstream.base import Composable
from squirrel.framework.exceptions import PyTorchSplittingException

logger = logging.getLogger(__name__)


class SplitByRank(Composable):
    """Composable to split data between ranks of a multi-rank loading setup"""

    def __init__(
        self,
        torch_dist_group: Optional[str] = None,
    ) -> None:
        """Init the SplitByRank composable."""
        super().__init__()

        self.rank = 0
        self.size = 1
        if torch.distributed.is_available() and torch.distributed.is_initialized():
            group = torch_dist_group or torch.distributed.group.WORLD
            self.rank = torch.distributed.get_rank(group=group)
            self.size = torch.distributed.get_world_size(group=group)
            logger.debug(f"split keys to different nodes with rank {self.rank} in group {group}")

    def __iter__(self) -> Iterator:
        """Method to iterate over the source and yield the elements that will be processed by a particular node"""
        if torch.distributed.is_available() and self.size > 1:
            yield from islice(self.source, self.rank, None, self.size)
        else:
            yield from self.source


class SplitByWorker(Composable):
    """Composable to split data between PyTorch workers of a single rank"""

    def __init__(self) -> None:
        """Init"""
        super().__init__()

        # Needs to be instantiated lazily in order to be compatible with the forking behavior
        # of pytorch multiprocessing context in its dataloader.
        self.winfo = None

    def __iter__(self) -> Iterator:
        """Method to iterate over the source and yield the elements that will be processed by a particular worker"""
        self.winfo = torch.utils.data.get_worker_info()
        if self.winfo is None:
            yield from self.source
        else:
            yield from islice(self.source, self.winfo.id, None, self.winfo.num_workers)


class TorchIterable(Composable, IterableDataset):
    """Mixin-Composable to have squirrel pipeline inherit from PyTorch IterableDataset"""

    def __init__(self) -> None:
        """Init"""
        super().__init__()

    def __iter__(self) -> Iterator:
        """Method to iterate over the source"""
        if _in_multi_rank_env():
            if not self._contains_rank_split(self.source):
                raise PyTorchSplittingException(
                    "Composable was not split by rank. This will lead to unexpected iteration behaviour."
                    "Add a 'split_by_rank_pytorch' call to your composable to avoid this error. "
                )
        if _in_multi_worker_env():
            if not self._contains_worker_split(self.source):
                raise PyTorchSplittingException(
                    "Composable was not split by worker. This will lead to unexpected iteration behaviour."
                    "Add a 'split_by_worker_pytorch' call to your composable to avoid this error. "
                )
        yield from self.source

    def _contains_rank_split(self, source: Composable) -> bool:
        """Check if SplitByRank was chained to this Composable"""
        if isinstance(source, SplitByRank):
            return True
        elif not isinstance(source, Composable):
            return False
        else:
            return self._contains_rank_split(source.source)

    def _contains_worker_split(self, source: Composable) -> bool:
        """Check if SplitByWorker was chained to this Composable"""
        if isinstance(source, SplitByWorker):
            return True
        elif not isinstance(source, Composable):
            return False
        else:
            return self._contains_worker_split(source.source)


def _skip_k(it: Iterable, start: int, step: int) -> Iterator:
    """
    Hook method to skip over elements in an iterable.

    Args:
        it: Iterable
        start: int denoting the start of the skip slicing
        step: int denoting the step size of the skipping operation
    """
    if step > 1:
        yield from islice(it, start, None, step)
    else:
        yield from it


def skip_k(rank: int, world_size: int) -> Callable[[Iterable], Iterator]:
    """
    Returns a callable that takes an iterable and applies a skipping operation on it.

    Args:
        rank: int denoting the rank of the distributed training process.
        world_size: int denoting the full world size.
    """
    return partial(_skip_k, start=rank, step=world_size)


def _in_multi_worker_env() -> bool:
    """Check if currently in multi-worker environment"""
    return False if torch.utils.data.get_worker_info() is None else True


def _in_multi_rank_env() -> bool:
    """Check if currently in multi-rank environment"""
    if torch.distributed.is_available() and torch.distributed.is_initialized():
        group = torch.distributed.group.WORLD
        size = torch.distributed.get_world_size(group=group)
        return True if size > 1 else False
    else:
        return False
