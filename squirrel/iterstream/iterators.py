import os
import pickle
import random
import time
import typing as t
from itertools import islice
from queue import Queue

import numpy as np
from tqdm import tqdm

from squirrel.constants import MetricsType, SeedType
from squirrel.iterstream.metrics import MetricsConf, metrics_iops, metrics_throughput


def _pick(buf: t.List, rng: random.Random) -> t.Any:
    """Pick a random element from a list.

    Args:
        buf (List): List of items.
        rng (random.Random): Random number generator to use.

    Returns:
        Any: One random item from `buf`.
    """
    k = rng.randint(0, len(buf) - 1)
    sample = buf[k]
    buf[k] = buf[-1]
    buf.pop()
    return sample


def shuffle_(
    iterable: t.Iterable,
    bufsize: int = 1000,
    initial: int = 100,
    rng: t.Optional[random.Random] = None,
    seed: SeedType = None,
) -> t.Iterator:
    """Shuffle the data in the stream.

    Uses a buffer of size `bufsize`. Shuffling at startup is less random; this is traded off against yielding samples
    quickly.

    Args:
        iterable (Iterable): Iterable to shuffle.
        bufsize (int, optional): Buffer size for shuffling. Defaults to 1000.
        initial (int, optional): Minimum number of elements in the buffer before yielding the first element. Must be
            less than or equal to `bufsize`, otherwise will be set to `bufsize`. Defaults to 100.
        rng (random.Random, optional): Either `random` module or a :py:class:`random.Random` instance. If None,
            a `random.Random()` is used.
        seed (Union[int, float, str, bytes, bytearray, None]): A data input that can be used for `random.seed()`.

    Yields:
        Any: Shuffled items of `iterable`.
    """
    rng = get_random_range(rng, seed)

    iterator = iter(iterable)
    initial = min(initial, bufsize)
    buf = []
    for sample in iterator:
        buf.append(sample)
        if len(buf) < bufsize:
            try:
                buf.append(next(iterator))
            except StopIteration:
                pass
        if len(buf) >= initial:
            yield _pick(buf, rng)
    while len(buf) > 0:
        yield _pick(buf, rng)


def take_(iterable: t.Iterable, n: int) -> t.Iterator:
    """Yield the first n elements from the iterable.

    Args:
        iterable (Iterable): Iterable to take from.
        n (int): Number of samples to take.

    Yields:
        Any: First `n` elements of `iterable`. Less elements can be yielded if the iterable does not have enough
        elements.
    """
    yield from islice(iterable, 0, n, 1)


def batched_(
    iterable: t.Iterable,
    batchsize: int = 20,
    collation_fn: t.Optional[t.Callable] = None,
    drop_last_if_not_full: bool = True,
) -> t.Iterator[t.List]:
    """Yield batches of the given size.

    Args:
        iterable (Iterable): Iterable to be batched.
        batchsize (int, optional): Target batch size. Defaults to 20.
        collation_fn (Callable, optional): Collation function. Defaults to None.
        drop_last_if_not_full (bool, optional): If the length of the last batch is less than `batchsize`, drop it.
            Defaults to True.

    Yields:
        Batches (i.e. lists) of samples.
    """
    it = iter(iterable)
    while True:
        batch = list(islice(it, batchsize))
        if drop_last_if_not_full and len(batch) < batchsize or len(batch) == 0:
            return
        if collation_fn is not None:
            batch = collation_fn(batch)
        yield batch


def map_(iterable: t.Iterable, callback: t.Callable) -> t.Iterator:
    """Apply the `callback` to each item in the `iterable` and yield the item."""
    for sample in iterable:
        yield callback(sample)


def filter_(iterable: t.Iterable, predicate: t.Callable) -> t.Iterator:
    """Filter items in the `iterable` by the `predicate` callable."""
    for sample in iterable:
        if predicate(sample):
            yield sample


def flatten_(iterables: t.Iterable[t.Iterable]) -> t.Iterator:
    """Iterate over iterables in the stream and yield their items."""
    for item in iterables:
        yield from item


def dask_delayed_(iterable: t.Iterable, callback: t.Callable) -> t.Iterator:
    """Convert items in the iterable into a dask.delayed object by applying callback"""
    from dask import delayed

    for item in iterable:
        yield delayed(callback)(item)


def tqdm_(iterable: t.Iterable, **kwargs) -> t.Iterator:
    """Iterate while using tqdm."""
    yield from tqdm(iterable, **kwargs)


def monitor_(
    iterable: t.Iterable,
    callback: t.Callable,
    prefix: t.Optional[str] = None,
    metrics_conf: MetricsConf = MetricsConf,
    *,
    window_size: int = 5,
    **kwargs,
) -> t.Iterator:
    """
    Iterate through an iterable and calculate the metrics based on a rolling window. Notice that you can configure
    metrics to output only IOPS or throughput or None. All metrics are by default turned on and calculated.
    If only one metric is turned on, the calculation of the other metric will be skipped, and a dummy value `0`
    is reported instead. When all metrics are turned off, this method has no actual effect.

    Args:
        iterable (Iterable): Any Iterable-like object.
        callback (Callable): `wandb.log`, `mlflow.log_metrics` or other metrics logger.
        prefix (str): If not None, will add this as a prefix to the metrics name. Can be used to monitor the same
            metric in different point in an iterstream in one run. Spaces are allowed.
        metrics_conf (MetricsConf): A config dataclass to control metrics calculated. Details see
            `squirrel.metrics.MetricsConf`
        window_size (int): How many items to average over the metrics calculation. Since each item passes by in a very
            small time window, for better accuracy, a rolling window cal is more accurate. Its value must be bigger than
            0.
        **kwargs: arguments to pass to your callback function.
    """
    assert window_size > 0, ValueError("`window_size` must > 0.")
    q_size = Queue(window_size)
    q_time = Queue(window_size)
    size_sum = 0
    for item in iterable:
        if metrics_conf.iops or metrics_conf.throughput:
            size_sum, metrics = _update_params(item, q_size, q_time, size_sum, metrics_conf, window_size, prefix)
            if metrics is not None:
                callback(metrics, **kwargs)
        yield item


def _update_params(
    item: t.Any,
    q_size: Queue,
    q_time: Queue,
    size_sum: float,
    metrics_conf: MetricsConf,
    window_size: int,
    prefix: t.Optional[str] = None,
) -> t.Tuple[float, t.Optional[MetricsType]]:
    """Update current rolling sum of item sizes of given `window_size` num of items, and use it to calculate current
    metrics. Returns current rolling size sum and metrics.
    """
    new_size = getsize(item) if metrics_conf.throughput is True else 0
    new_time = time.perf_counter()
    q_size.put(new_size)
    q_time.put(new_time)
    if q_size.full():
        # iterate through the rest items
        old_size = q_size.get()
        old_time = q_time.get()
        size_sum = size_sum + new_size - old_size
        time_diff = new_time - old_time
        metrics = _calculate_metrics(size_sum, time_diff, metrics_conf, window_size, prefix)
        return size_sum, metrics
    else:
        # iterate through the first (window_size - 1) items
        size_sum += new_size
        return size_sum, None


def _calculate_metrics(
    size_sum: float, time_diff: float, metrics_conf: MetricsConf, window_size: int, prefix: t.Optional[str] = None
) -> MetricsType:
    """Calculate metrics iops and throughput, then log them into the tracking server."""
    IOPS = metrics_iops(window_size, time_diff) if metrics_conf.iops is True else 0
    throughput = metrics_throughput(size_sum, time_diff, unit=metrics_conf.throughput_unit)
    if prefix is None:
        prefix = ""
    return {
        f"{prefix}IOPS number/s": IOPS,
        f"{prefix}throughput {metrics_conf.throughput_unit}/s": throughput,
    }


def get_random_range(rng: t.Optional[random.Random] = None, seed: SeedType = None) -> random.Random:
    """
    Returns a random number as range, calculated based on the input `rng` and `seed`.

    Args:
        rng (random.Random, optional): Either `random` module or a :py:class:`random.Random` instance. If None,
            a `random.Random()` is used.
        seed (Union[int, float, str, bytes, bytearray, None]): seed (Optional[int]): An int or other acceptable types
            that works for random.seed(). Will be used to seed `rng`. If None, a unique identifier will be used to seed.
    """
    if rng is None:
        rng = random.Random()
    if seed is None:
        seed = f"{os.getpid()}{time.time()}"
    rng.seed(seed)
    return rng


def getsize(item: t.Any) -> int:
    """Return estimated size (in terms of bytes) of a python object. Currently use numpy method to calculate size.
    Otherwise, the size is estimated through its pickled size. This is considered a better performant option than
    `zarr.storage.getsize()`.
    """
    if isinstance(item, np.ndarray):
        return item.nbytes
    return len(pickle.dumps(item))
