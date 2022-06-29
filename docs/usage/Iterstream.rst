IterStream
==========

Squirrel provides an API for chaining iterables.
The functionality is provided by **IterableSource** (:code:`squirrel.iterstream.source.IterableSource`).

Example Workflow
----------------

.. code-block:: python

    from squirrel.iterstream import IterableSource
    import time

    it = IterableSource([1, 2, 3, 4])
    for item in it:
        print(item)

:py:class:`IterableSource` has several methods to conveniently load data, given an iterable as the input:

.. code-block:: python

    it = IterableSource([1, 2, 3, 4]).map(lambda x: x + 1).async_map(lambda x: x ** 2).filter(lambda x: x % 2 == 0)
    for item in it:
        print(item)

:code:`map_async()` applies the provided function asynchronously. More on this in the following sections.
In addition to explicitly iterating over the items, it's also possible to call `collect()` to collect all items in a list, or `join()` to iterate over items without returning anything.

Items in the stream can be shuffled in the buffer and batched

.. code-block:: python

    it = IterableSource(range(10)).shuffle(size=5).map(lambda x: x+1).batched(batchsize=3, drop_last_if_not_full=True)
    for item in it:
        print(item)

Note that the argument `drop_last_if_not_full` (default True) will drop the last batch if its size is less than `batchsize` argument; so, only 3 items will be printed above.

Items in `IterableSource` can be composed by providing a Composable in the `compose()` method:

.. code-block:: python

    from squirrel.iterstream import Composable

    class MyIter(Composable):
        def __init__(self):
            super().__init__()

        def __iter__(self):
            for i in iter(self.source):
                yield f"_{i}", i

    it = IterableSource([1, 2, 3]).compose(MyIter)
    for item in it:
        print(item)


Combining multiple iterables can be achieved using `IterableSamplerSource`:

.. code-block:: python

    from squirrel.iterstream import IterableSamplerSource

    it1 = IterableSource([1, 2, 3]).map(lambda x: x + 1)
    it2 = [1, 2, 3]

    res = IterableSamplerSource(iterables=[it1, it2], probs=[.7, .3]).collect()
    print(res)
    assert sum(res) == 15

Note that you can pass the probabilities of sampling from each iterator. When an iterator is exhausted, the probabilities are normalized.

Asynchronous execution
----------------------
Part of the fast speed from iterstream thanks to :py:meth:`squirrel.iterstream.base.Composable.async_map`.
This method carries out the callback function you specified to each item in the stream asynchronously, therefore offers a large speed-up.

.. code-block:: python

    def io_bound(item):
        print(f"{item} io_bound")
        time.sleep(1)
        return item

    it = IterableSource([1, 2, 3]).async_map(io_bound, max_workers=4).async_map(io_bound, max_workers=None)
    t1 = time.time()
    for i in it:
        print(i)
    print(time.time() - t1)


By default, :py:meth:`async_map <squirrel.iterstream.base.Composable.async_map>`
instantiates a :py:class:`ThreadPoolExecutor <concurrent.futures.ThreadPoolExecutor>` (`executor=None`).
It also accepts :py:class:`ProcessPoolExecutor <concurrent.futures.ProcessPoolExecutor>`,
which is a good choice when performing cpu-bound operations on a single machine.

The argument `max_workers` defines the maximum number of workers/threads the
:py:class:`ThreadPoolExecutor <concurrent.futures.ThreadPoolExecutor>`
uses when `exectuor=None`.
By default, `max_workers=None` relies on an internal heuristic of
the :py:class:`ThreadPoolExecutor <concurrent.futures.ThreadPoolExecutor>`
to select a reasonable upper bound.
This may differ between Python versions.
See the documentation of
:py:class:`ThreadPoolExecutor <concurrent.futures.ThreadPoolExecutor>` for details.

In the above example, two :py:class:`ThreadPoolExecutor <concurrent.futures.ThreadPoolExecutor>`'s
are created, one with an upper bound of 4 threads and the other with a *smart* upper bound.
After the iterator is exhausted, both of these pools will be closed.

If `executor` is provided, no internal
:py:class:`ThreadPoolExecutor <concurrent.futures.ThreadPoolExecutor>` is
created and managed.
As a result, `max_workers` is *ignored* since the provided `executor` already includes
the information and the `executor` has to be manually closed.

.. code-block:: python


    from concurrent.futures import ThreadPoolExecutor
    tpool =  ThreadPoolExecutor(max_workers=4)

    def io_bound(item):
        print(f"{item} io_bound")
        time.sleep(1)
        return item

    it = IterableSource([1, 2, 3]).async_map(io_bound, executor=tpool).async_map(io_bound, executor=tpool)
    t1 = time.time()
    for i in it:
        print(i)
    print(time.time() - t1)

    # now the external pool needs to be manually closed
    tpool.shutdown()


In the above example, a
:py:class:`ThreadPoolExecutor <concurrent.futures.ThreadPoolExecutor>` is created with
a maximum number of 4 workers.
This pool of workers is shared among both
:py:meth:`async_map <squirrel.iterstream.base.Composable.async_map>` calls.



Cluster-mode
------------
Scaling out to a dask cluster only requires changing a single line of code:

.. code-block:: python

    from dask.distributed import Client
    client = Client()

    it = IterableSource([1, 2, 3]).async_map(io_bound, executor=client)
    t1 = time.time()
    for item in it:
        print(item)
    print(time.time() - t1)

In this example, a task is submitted and the result is gathered. An alternative would be to call :code:`dask_map` instead of :code:`async_map`, which transforms items in the stream into :code:`dask.delayed.Delayed` objects. This pattern makes it possible to load and transform the data in a dask cluster and only load the fully ready data into the local machine.

.. code-block:: python

    it = IterableSource([1, 2, 3]).dask_map(io_bound).dask_map(lambda item: item + 1).materialize_dask()
    t1 = time.time()
    for item in it:
        print(item)
    print(time.time() - t1)

Note that after calling :code:`dask_map` for the first time, you can chain more :code:`dask_map`\'s, which are then operating on the :code:`dask.delayed.Delayed` objects, so that the data and the operations live on the dask cluster until :code:`materialize_dask` is called.

Just-in-time compilation with numba
-----------------------------------
Squirrel uses :code:`numba` to jit-compile an iterator to speed up computation in the main process.

.. code-block:: python

    it = IterableSource([1, 2, 3]).numba_map(lambda x: x + 1)
    for item in it:
        print(item)


Here, the iterator itself is passed to the :code:`numba` decorator :code:`@numba.jit`. Then the speed-up will be entirely provided by :code:`numba`. Note that squirrel does not compile the user defined function. You may achieve a comparable speed-up by compiling your function and passing it to `map()`:

.. code-block:: python

    from numba import jit

    @jit(nopython=True)
    def runtime_transformation(x):
        return x

    it = IterableSource([1, 2, 3]).map(runtime_transformation)
    for item in it:
        print(item)

Compared to the other three options, :code:`numba` is more performant in some cases but not in others, and highly sensitive to the
actual data type and computation at hand. Therefore, we recommend you read the official `numba documentation`_
from :code:`numba`, and perform a benchmarking, before choosing this option in production.

.. note::

    Since numba only supports `limited types of python objects`_, and naturally does not include
    squirrel defined objects, we have to force object mode in numba, that means the decorator we have chosen in squirrel
    is of the following format: :code:`@numba.jit(forceobj=True)`.

.. _numba documentation: https://numba.pydata.org/numba-doc/latest/index.html
.. _limited types of python objects: https://numba.pydata.org/numba-doc/dev/reference/pysupported.html

PyTorch Distributed Dataloading
-------------------------------
The Squirrel api is designed to support fast streaming of datasets to a multi-rank, distributed system, as often encountered in modern deep learning applications involving multiple GPUs. To this end, we can use the `SplitByWorker` and `SplitByRank` composables and wrap the final iterator in a torch `Dataloader` object

.. code-block:: python

    import torch.utils.data as tud
    from squirrel.iterstream.source import IterableSource
    from squirrel.iterstream.torch_composables import SplitByRank, SplitByWorker, TorchIterable

    def times_two(x: float) -> float:
        return x * 2

    samples = list(range(100))
    batch_size = 5
    num_workers = 4
    it = (
            IterableSource(samples)
            .compose(SplitByRank)
            .async_map(times_two)
            .compose(SplitByWorker)
            .batched(batch_size)
            .compose(TorchIterable)
        )
    dl = tud.DataLoader(it, num_workers=num_workers)

Note that the rank of the distributed system depends on the torch distributed process group and is automatically determined.

And using :py:mod:`squirrel.driver` api:

.. code-block:: python

    from squirrel.driver import MessagepackDriver
    url = ""
    it = MessagepackDriver(url).get_iter(key_hooks=[SplitByWorker]).async_map(times_two).batched(batch_size).compose(TorchIterable)
    dl = DataLoader(it, num_workers=num_workers)

In this example, :code:`key_hooks=[SplitByWorker]` ensures that keys are split between workers before fetching the data and we achieve two level of parallelism; multi-processing provided by :code:`torch.utils.data.DataLoader`, and multi-threading inside each process for efficiently fetching samples by :code:`get_iter`.

Performance Monitoring
-----------------------
In squirrel, performance in :code:`iterstream` can be calculated and logged. This is done by applying an extra method
:py:func:`monitor()` into the original chaining iterstream. It can be added into any step in the above example where
:code:`it` is defined. For example, you can add :code:`.monitor(callback=wandb.log)` right after
:code:`async_map(times_two)` Then the performance of all the previous steps combined will be calculated at this point
and the calculated metrics will be passed to any user-specified callback such as :py:func:`wandb.log`.

The following is a complete example:

.. code-block:: python

    import wandb
    import mlflow
    import numpy as np

    def times_two(x: float) -> float:
        return x * 2

    samples = [np.random.rand(10, 10) for i in range(10 ** 4)]
    batch_size = 5

    with wandb.init(): # or mlflow.start_run()
        it = (
            IterableSource(samples)
            .async_map(times_two)
            .monitor(wandb.log) # or mlflow.log_metrics
            .batched(batch_size)
        )
        it.collect() # or it.take(<some int>).join()

This will create an iterstream with the same transformation logics as it was without the method :code:`monitor`, but the
calculated metrics at step `async_map` is sent to the callback function `wandb.log`. (The calculated metrics is of type
:code:`Dict[str, [int, float]]`, therefore any function takes such argument can be used to plug into
the callback of :code:`monitor`.)

By default, :code:`monitor` calculate two **metrics**: `IOPS` and `throughput`. However, this can be configured by
passing
a data class :py:class:`squirrel.metrics.MetricsConf` to the argument :code:`metrics_conf` in :code:`monitor`.
For details, see :py:mod:`squirrel.iterstream.metrics`.

**Monitoring at different locations** in an iterstream in one run can be achieved by inserting :code:`monitor` with
different `prefix`:

.. code-block:: python

    with wandb.init(): # or mlflow.start_run()
        it = (
            IterableSource(samples)
            .monitor(wandb.log, prefix="(before async_map) ")
            .async_map(times_two)
            .monitor(wandb.log, prefix="(after async_map) ") # or mlflow.log_metrics
            .batched(batch_size)
        )
        it.collect() # or it.take(<some int>).join()

This will generate 4 instead of 2 metrics with each original metric bifurcate into two with different prefixes to
track at which point the metrics are generated. (This does not interfere with :code:`metrics_conf` which determines
which metrics should be used in each :code:`monitor`.)
