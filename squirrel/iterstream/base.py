from __future__ import annotations

import inspect
import queue
import typing as t
from abc import abstractmethod
from concurrent.futures import Executor, ThreadPoolExecutor
from copy import deepcopy
from functools import partial

from numba import jit

from squirrel.constants import MetricsType
from squirrel.iterstream.iterators import (
    batched_,
    dask_delayed_,
    filter_,
    flatten_,
    map_,
    monitor_,
    shuffle_,
    take_,
    tqdm_,
)
from squirrel.iterstream.metrics import MetricsConf

__all__ = ["Composable", "AsyncContent"]


class Composable:
    """A mix-in class that provides stream manipulation functionalities."""

    def __init__(self, source: t.Optional[t.Union[t.Iterable, t.Callable]] = None):
        """Init"""
        self.source = source

    @abstractmethod
    def __iter__(self) -> t.Iterator:
        """Abstract iter"""
        pass

    def compose(self, constructor: t.Type[Composable], *args, **kw) -> Composable:
        """
        Apply the transformation expressed in the `__iter__` method of the `constructor` to items in the stream.
        If the provided constructor has an __init__ method, then the source argument should not be provided.
        """
        assert "source" not in kw
        if "__init__" in constructor.__dict__ and "source" in inspect.signature(constructor).parameters:
            raise ValueError(
                "If the provided constructor argument has an __init__ method, it should not "
                "have the source argument."
            )
        return constructor(*args, **kw).source_(self)

    def to(self, f: t.Callable, *args, **kw) -> _Iterable:
        """Pipe the iterable into another iterable which applies `f` callable on it"""
        assert "source" not in kw
        return _Iterable(self, f, *args, **kw)

    def source_(self, source: t.Union[t.Iterable, t.Callable]) -> Composable:
        """Set the source of the stream"""
        self.source = source
        return self

    def map(self, callback: t.Callable, **kw) -> _Iterable:
        """Applies the `callback` to each item in the stream. Specify key-word arguments for callback in **kw"""
        partial_callback = partial(callback, **kw)
        return self.to(map_, partial_callback)

    def filter(self, predicate: t.Callable) -> _Iterable:
        """Filters items by `predicate` callable"""
        return self.to(filter_, predicate)

    def async_map(
        self,
        callback: t.Callable,
        buffer: int = 100,
        max_workers: t.Optional[int] = None,
        executor: t.Optional[Executor] = None,
        **kw,
    ) -> _AsyncMap:
        """
        Applies the `callback` to the item in the self and returns the result.

        Args:
            callback (Callable): a callable to be applied to items in the stream
            buffer (int): the size of the buffer
            max_workers (int): number of workers in the
                :py:class:`ThreadPoolExecutor <concurrent.futures.ThreadPoolExecutor>`.
                `max_workers` is only used when `executor` is not provided, as the `executor`
                already includes the number of `max_workers`.
            executor (concurrent.futures.Executor, dask.distributed.Client): an optional executor to be used.
                By default a :py:class:`ThreadPoolExecutor <concurrent.futures.ThreadPoolExecutor>`
                is created, if no executor is provided. If you need a
                :py:class:`ProcessPoolExecutor <concurrent.futures.ProccessPoolExecutor>`,
                you can explicitly provide it here. It is also useful when chaining multiple
                `async_map`; you can pass the same `executor` to each `async_map` to share resources. If
                `dask.distributed.Client` is passed, tasks will be executed with the provided client (local or remote).

                **Note** if the executor is provided, it will not be closed in this function even after the iterator
                is exhausted.

                **Note** if executor is provided, the argument `max_workers` will be ignored. You should
                specify this in the executor that is being passed.
            **kw (dict): key-word arguments for callback

        Returns (_AsyncMap)
        """
        partial_callback = partial(callback, **kw)
        return _AsyncMap(
            source=self, callback=partial_callback, buffer=buffer, max_workers=max_workers, executor=executor
        )

    def dask_map(self, callback: t.Callable, **kw) -> _Iterable:
        """
        Converts each item in the stream into a dask.delayed object by applying the callback to the item.
        Specify additional keyword arguments via kw
        Args:
            callback (Callback): callback to be mapped over
            **kw: key-word arguments for callback

        Returns:
            mapped Composable (_Iterable)
        """
        partial_callback = partial(callback, **kw)
        return self.to(dask_delayed_, partial_callback)

    def materialize_dask(self, buffer: int = 10, max_workers: t.Optional[int] = None) -> _Iterable:
        """
        Materialize the dask.delayed object by calling `compute()` method on each item in a thread pool

        Args:
            buffer (int): size of the buffer that retrieves the data in parallel from dask
            max_workers (int): parameter passed to the
                :py:class:`ThreadPoolExecutor <concurrent.futures.ThreadPoolExecutor>`

        Returns (_Iterable)
        """
        return _DaskMaterializer(source=self, buffer=buffer, max_workers=max_workers).flatten()

    def numba_map(self, callback: t.Callable) -> _NumbaMap:
        """
        The iterator will be wrapped inside a `numba.jit` decorator to speed up the iteration.
        However, this is quite different from the standard asynchronous speed-up and does not always guarantee
        a better performance than the normal :py:class:`ThreadPoolExecutor <concurrent.futures.ThreadPoolExecutor>`,
        so please use with caution.

        Args:
            callback (Callable): a callback to be applied to each item in the stream

        Returns (_NumbaMap)
        """
        return _NumbaMap(source=self, callback=callback)

    def flatten(self) -> _Iterable:
        """When items in the stream are themselves iterables, flatten turn them back to individual items again"""
        return self.to(flatten_)

    def batched(
        self, batchsize: int, collation_fn: t.Optional[t.Callable] = None, drop_last_if_not_full: bool = True
    ) -> _Iterable:
        """Batch items in the stream.

        Args:
            batchsize: number of items to be batched together
            collation_fn: Collation function to use.
            drop_last_if_not_full (bool): if the length of the last batch is less than the `batchsize`, drop it
        """
        return self.to(
            batched_,
            batchsize=batchsize,
            collation_fn=collation_fn,
            drop_last_if_not_full=drop_last_if_not_full,
        )

    def shuffle(self, size: int, **kw) -> Composable:
        """Shuffles items in the buffer, defined by `size`, to simulate IID sample retrieval.

        Args:
            size (int, optional): Buffer size for shuffling. Defaults to 1000. Skip the shuffle step if `size < 2`.

        Acceptable keyword arguments:

        - initial (int, optional): Minimum number of elements in the buffer before yielding the first element.
          Must be less than or equal to `bufsize`, otherwise will be set to `bufsize`. Defaults to 100.

        - rng (random.Random, optional): Either `random` module or a :py:class:`random.Random` instance. If None,
          a `random.Random()` is used.

        - seed (Union[int, float, str, bytes, bytearray, None]): A data input that can be used for `random.seed()`.

        """
        if size < 2:
            return self
        return self.to(shuffle_, size, **kw)

    def take(self, n: t.Optional[int]) -> Composable:
        """Take n samples from iterable"""
        if n is None:
            return self
        return self.to(take_, n)

    def loop(self, n: t.Optional[int] = None) -> Composable:
        """Repeat the iterable n times.

        Args:
            n (int, Optional): number of times that the iterable is looped over. If None (the default), it loops forever

        Note: this method creates a deepcopy of the `source` attribute, i.e. all steps in the chain of Composables
        `before` the loop itself, which must be picklable.
        """
        return _LoopIterable(self, n)

    def zip_index(self, pad_length: int = None) -> Composable:
        """Zip the item in the stream with its index and yield Tuple[index, item]

        Args:
            pad_length: if provided, all indexes will be padded with zeros if they have less digits than pad_length,
                in which case all indexes are str rather than int.
        """
        return _ZipIndexIterable(self, pad_length=pad_length)

    def join(self) -> None:
        """A method to consume the stream"""
        for _ in self:
            pass

    def collect(self) -> t.List[t.Any]:
        """Collect and returns the result of the stream"""
        return list(self)

    def tqdm(self, **kw) -> _Iterable:
        """Add tqdm to iterator."""
        return self.to(tqdm_, **kw)

    def monitor(
        self,
        callback: t.Callable[[MetricsType], t.Any],
        prefix: t.Optional[str] = None,
        metrics_conf: MetricsConf = MetricsConf,
        window_size: int = 5,
        **kw,
    ) -> _Iterable:
        """Iterate through an iterable and calculate the metrics based on a rolling window. Notice that you can
        configure metrics to output only IOPS or throughput or None. All metrics are by default turned on and
        calculated.
        If only one metric is turned on, the calculation of the other metric will be skipped, and a dummy value `0`
        is reported instead. When all metrics are turned off, this method has no actual effect.

        Args:
            callback (Callable): `wandb.log`, `mlflow.log_metrics` or other metrics logger.
            prefix (str): If not None, will add this as a prefix to the metrics name. Can be used to monitor the same
                metric in different point in an iterstream in one run. Spaces are allowed.
            metrics_conf (MetricsConf): A config dataclass to control metrics calculated. Details see
                `squirrel.metrics.MetricsConf`
            window_size (int): How many items to average over the metrics calculation. Since each item passes by in a
                very small time window, for better accuracy, a rolling window cal is more accurate. Its value must
                be bigger than 0.
            **kw (dict): arguments to pass to your callback function.

        Returns:
            An _Iterable instance which can be chained by other funcs in this class.
        """
        return self.to(
            monitor_, callback=callback, prefix=prefix, metrics_conf=metrics_conf, window_size=window_size, **kw
        )

    def split_by_worker_pytorch(self) -> Composable:
        """Split the stream into multiple streams, one for each worker in the PyTorch distributed system."""
        from squirrel.iterstream.torch_composables import SplitByWorker

        return self.compose(SplitByWorker)

    def split_by_rank_pytorch(self, torch_dist_group: t.Optional[str] = None) -> Composable:
        """Split the stream into multiple streams, one for each rank in the PyTorch distributed system

        Args:
            torch_dist_group (str, optional): The group name of the PyTorch distributed system. Defaults to None.
        """
        from squirrel.iterstream.torch_composables import SplitByRank

        return self.compose(SplitByRank, torch_dist_group)

    def to_torch_iterable(self) -> Composable:
        """Convert the stream to a torch iterable."""
        from squirrel.iterstream.torch_composables import TorchIterable

        return self.compose(TorchIterable)


class _Iterable(Composable):
    """
    A class representing an iterable, which applies the callable `f` to the `source` items and returns the result in
    the :py:meth:`__iter__` method. This is used as the object being passed between steps of the stream.
    """

    def __init__(self, source: t.Iterable, f: t.Callable[..., t.Iterator], *args, **kw):
        """Initialize _Iterable.

        Args:
            source (Iterable): An iterable representing the source items.
            f (Callable): A callable to be applied to the iterator, which is built from `source`. Must return an
                iterator.
            *args: Arguments being passed to `f`.
            **kw: Kwargs passed to `f`.
        """
        super().__init__(source)
        assert callable(f)
        self.f = f
        self.args = args
        self.kw = kw

    def __iter__(self) -> t.Iterator:
        """Returns the iterator that is obtained by applying `self.f` to `self.source`."""
        assert self.source is not None, f"must set source before calling iter {self.f} {self.args} {self.kw}"
        assert callable(self.f), self.f
        return self.f(iter(self.source), *self.args, **self.kw)


class _LoopIterable(Composable):
    def __init__(self, source: t.Iterable, n: t.Optional[int]):
        """Init"""
        super().__init__(source=source)
        self.n = n

    def __iter__(self) -> t.Iterator:
        """Iterate over the iterable n times"""
        _started = False
        if self.n is None:
            current_ = iter(deepcopy(self.source))
            while True:
                try:
                    yield next(current_)
                    _started = True
                except StopIteration:
                    if not _started:
                        return
                current_ = iter(deepcopy(self.source))
        else:
            for _ in range(self.n):
                yield from iter(deepcopy(self.source))


class _ZipIndexIterable(Composable):
    def __init__(self, source: t.Iterable, pad_length: int = None) -> None:
        """Init"""
        super().__init__(source)
        self.idx = 0
        self.pad_length = pad_length

    def __iter__(self) -> t.Iterator:
        """Zip the index and the data"""
        for i in self.source:
            yield self._next_idx(), i

    def _next_idx(self) -> t.Union[int, str]:
        _idx = None
        if self.pad_length is not None:
            str_idx = str(self.idx)

            _idx = "0" * (self.pad_length - len(str_idx)) + str_idx
        else:
            _idx = self.idx
        self.idx += 1
        return _idx


class _AsyncMap(Composable):
    def __init__(
        self,
        source: t.Iterable,
        callback: t.Callable,
        buffer: int = 100,
        max_workers: t.Optional[int] = None,
        executor: t.Optional[Executor] = None,
    ):
        """A class that applies a `callback` asynchronously to the items in the `dataset`, using thread pool executor"""
        super().__init__(source)
        self.buffer = buffer
        self.callback = callback
        self.max_workers = max_workers
        self.executor = executor

        # Instantiate queue lazily in the __iter__ method
        # This is necessary to be compatible with the thread forking of PyTorch multiprocessing context
        # when using a multi-worker dataloader.
        self.queue = None

    def __iter__(self) -> t.Iterator:
        """An iterator"""

        self.queue = queue.Queue(self.buffer)
        it = iter(self.source)
        try:
            import dask.distributed
        except ImportError:
            pass

        if self._executor_not_provided():
            with ThreadPoolExecutor(max_workers=self.max_workers) as exec_:
                yield from self._iter(it, exec_)
        elif isinstance(self.executor, Executor):
            yield from self._iter(it, self.executor)
        elif isinstance(self.executor, dask.distributed.Client):
            yield from self._dask_iter(it)
        else:
            raise ValueError(f"Executor {self.executor} not recognized")

    def _executor_not_provided(self) -> bool:
        return self.executor is None

    def _iter(self, it: t.Iterator, executor: Executor) -> t.Iterator:
        sentinel = object()
        while True:
            # Fill queue
            while not self.queue.full():
                item = next(it, sentinel)
                if item is sentinel:
                    break
                self.queue.put(AsyncContent(item=item, func=self.callback, executor=executor))

            # stop iterating if all samples processed
            if self.queue.empty():
                break

            # yield sample
            yield self.queue.get().value()

    def _dask_iter(self, it: t.Iterator) -> t.Iterator:
        sentinel = object()
        while True:
            while not self.queue.full():
                item = next(it, sentinel)
                if item is sentinel:
                    break
                self.queue.put(self.executor.submit(self.callback, item))

            if self.queue.empty():
                break

            yield self.queue.get().result()


class _NumbaMap(Composable):
    def __init__(self, source: t.Iterable, callback: t.Callable):
        super().__init__(source)
        self.callback = callback

    def __iter__(self) -> t.Iterator:
        """An iterator"""
        yield from self._iter_in_jit(self.source, self.callback)

    @jit(forceobj=True)  # need `force object` mode to pass custom types of python objects to numba
    def _iter_in_jit(self, source: t.Iterable, callback: t.Callable) -> t.Iterator:
        """
        Wrap the iterator around numba JIT framework to speed up the iteration, instead of doing asynchronous speed
        up in a message queue (the default `_AsyncMap` behavior). Only evoked when `executor='numba'` is set.
        """
        for item in source:
            yield callback(item)


class _DaskMaterializer(_AsyncMap):
    def __init__(self, source: t.Iterable, buffer: int, max_workers: t.Optional[int] = None):
        """Call the init of the parent class `_AsyncMap` and pass dask.compute as the callback"""
        from dask import compute

        super().__init__(source, callback=compute, buffer=buffer, max_workers=max_workers, executor=None)


class AsyncContent:
    """Represents content that can be fetched asynchronously."""

    def __init__(self, item: str, func: t.Callable, executor: Executor) -> None:
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
