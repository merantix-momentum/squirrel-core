import concurrent.futures
import logging
import typing as t
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor
from queue import Queue

from squirrel.framework.exceptions import EmptyQueueException

logger = logging.getLogger(__name__)

__all__ = ["Fetcher", "StreamDataset"]


class Fetcher(ABC):
    """Abstract class to read or write an item."""

    @abstractmethod
    def fetch(self, key: str) -> t.Any:
        """Read a single item by key.

        Args:
            key (str): Key for the item to be read.

        Returns:
            Any: Item corresponding to `key`.
        """
        pass

    @abstractmethod
    def write(self, key: str, val: t.Any) -> None:
        """Write a single item by key.

        Args:
            key (str): Key for the item to be written.
            val (Any): Value to write.
        """
        pass


class AsyncContent:
    """Represents content that can be fetched asynchronously."""

    def __init__(self, item: str, func: t.Callable, executor: concurrent.futures.Executor) -> None:
        """Initialize AsyncContent.

        Args:
            item (str): Key corresponding to a single item, will be passed to `fetch_func`.
            func (Callable): Function that fetches a given key.
            executor (concurrent.futures.Executor): Executor to submit `func` with `item`.
        """
        self.stack = 1
        self.future = executor.submit(func, item)

    def value(self, timeout: int = None) -> t.Any:
        """Get the value asynchronously.

        Args:
            timeout (int, optional): Number of seconds to wait for the result. If None, then the future is waited
                indefinitely. Defaults to None.

        Returns:
            Any: Content.
        """
        return self.future.result(timeout)


class StreamDataset(ABC):
    def __init__(
        self,
        fetcher: Fetcher,
        cache_size: int = 100,
        max_workers: int = None,
    ) -> None:
        """Initialize StreamDataset.

        Args:
            fetcher (Fetcher): Fetcher instance to be used for reading/writing items.
            cache_size (int, optional): Number of items to cache. Defaults to 100.
            max_workers (int, optional): Maximum number of workers that ThreadPoolExecutor uses. If None,
                ThreadPoolExecutor determines a reasonable number. Defaults to None.
        """
        self.q_keys = Queue()
        self.q_cache = Queue(cache_size)
        self.cache = dict()
        self.fetcher = fetcher
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.n = 0
        logger.info(
            f"new StreamDataset is constructed, fetcher_class: {self.fetcher.__class__}, "
            f"cache_size: {cache_size}, max_workers: {max_workers}"
        )

    def request(self, key: str) -> None:
        """Request to fetch an item by key and schedule it to be fetched.

        Args:
            key (str): Key for the requested item.
        """
        self.q_keys.put(key)
        self._schedule()

    def retrieve(self) -> t.Any:
        """Retrieve a previously requested item in a FIFO manner.

        Items must be requested by calling :py:meth:`request` before they can be retrieved.

        Raises:
            EmptyQueueException: If all requested items have already been retrieved.

        Returns:
            Any: Next item in the queue, in FIFO order.
        """
        self._schedule()

        if self._queues_empty():
            raise EmptyQueueException(
                "Retrieve is called but everything that was requested has already been retrieved."
            )

        key = self.q_cache.get()
        val = self.cache[key].value()
        if self.cache[key].stack == 1:
            del self.cache[key]
        else:
            self.cache[key].stack -= 1
        self._schedule()
        return val

    def _schedule(self) -> None:
        """Schedule items to be fetched."""
        while not self.q_cache.full() and not self.q_keys.empty():
            k = self.q_keys.get()
            if k in self.cache:
                self.cache[k].stack += 1
            else:
                self.cache[k] = AsyncContent(item=k, func=self.fetcher.fetch, executor=self.executor)
            self.q_cache.put(k)

    def _queues_empty(self) -> bool:
        """Checks whether both key and value queues are empty.

        Returns:
            bool: True if both queues are empty. False, otherwise.
        """
        return len(self.q_cache.queue) == 0 and len(self.q_keys.queue) == 0

    def write(self, key: str, val: t.Any) -> None:
        """Write an item asynchronously.

        Args:
            key (str): Key for the item to be written.
            val (Any): Value to write.
        """
        self.executor.submit(self.fetcher.write, key, val)

    def close(self) -> None:
        """Shutdown the ThreadPoolExecutor."""
        self.executor.shutdown(wait=True)
        logger.info("executor closed")

    def __enter__(self) -> "StreamDataset":
        """Use the stream in a context."""
        return self

    def __exit__(
        self,
        exctype: t.Optional[t.Type[BaseException]],
        excinst: t.Optional[BaseException],
        exctb: t.Optional[t.Any],
    ) -> bool:
        """Exit the context of using the stream."""
        self.close()
        return False
