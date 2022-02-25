IterStream
==============

Squirrel provides an API for chaining iterables. The functionality is provided by **IterableSource** (:code:`squirrel.iterstream.source.IterableSource`).

Example Workflow
----------------

.. code-block:: python

    from squirrel.iterstream.source import IterableSource

    it_loader = IterableSource([1, 2, 3, 4])
    for item in it_loader:
        print(i)

:py:class:`IterableSource` has several methods to conveniently load data, given an iterable as the input:

.. code-block:: python

    it_loader = IterableSource([1, 2, 3, 4]).map(lambda x: x + 1).async_map(lambda x: x ** 2).filter(lambda x: x % 2 == 0)
    for item in it_loader:
        print(i)

:code:`map_async()` applies the provided function asynchronously using :code:`concurrent.futures.ThreadPoolExecutor`, and it's therefore well suited for IO-bound operations (e.g. fetching data from remote storage for writing data to a remote storage concurrently). Please refer to the document `squirrel_store` for more information about `MemoryStore`

.. code-block:: python

    from squirrel.storage import MemoryStore
    st = MemoryStore()

    def get_sample():
        return {
            "id": f"id_{np.random.randint(1, 10000)}",
            "image": np.random.random(size=(1, 1, 1)),
            "label": np.random.choice([0, 1]),
            "meta": {
                "key": "value",
                "split": np.random.choice(["train", "test", "validation"])
            },
        }

    samples = [get_sample() for _ in range(10)]
    IterableSource(samples).async_map(lambda x: st.set(x)).join()

calling :code:`join()` is necessary here, as it iterates over the stream and pulls the items through it. It's equivalent to

.. code-block:: python

    samples = [get_sample() for _ in range(10)]
    it_ = IterableSource(samples).async_map(lambda x: st.set(x))

    for i in it_:
        pass

Once the data is stored in `st`, we can read it in an asynchronous way:

.. code-block:: python

    it_loader = IterableSource(st.keys()).async_map(lambda key: st.get(key))
    for item in it_loader:
        print(item)


To iterate over the data in an IID manner, keys in the stream can be shuffled in the buffer, and can be batched:

.. code-block:: python

    it_loader = IterableSource(st.keys()).shuffle(size=5).async_map(lambda key: st.get(key)).batched(batchsize=3, drop_last_if_not_full=True)
    for item in it_loader:
        print(i)

Note that the argument `drop_last_if_not_full` (default True) will drop the last batch if its size is less than `batchsize` argument.
Squirrel storage classes, such as `MemoryStore`, can be passes to `sink` or `prefetch`, for asynchronous persistence and retrieval:

.. code-block:: python

    st = MemoryStore()
    samples = [get_sample() for _ in range(10)]
    IterableSource(samples).sink(st)
    it_loader = IterableSource(st.keys()).shuffle(size=5).prefetch(st).batched(batchsize=2)
    for item in it_loader:
        print(i)

You can also collect the result using the `collect()` method on the iterator.

.. code-block:: python

    result = IterableSource([1, 2, 3, 4]).map(lambda x: x+1).collect()


items in `IterableSource` can be composed by providing a generator in the `compose()` method:

.. code-block:: python

    class MyIter(Composable):
        def __init__(self):
            super().__init__()

        def __iter__(self):
            for i in iter(self.source):
                yield f"_{i}", i

    it = IterableSource([1, 2, 3]).compose(MyIter)
    for i in it:
        print(i)


Combining multiple iterables can be achieved using `IterableSamplerSource`:

.. code-block:: python

    from squirrel.iterstream.source import IterableSamplerSource

    it1 = IterableSource([1, 2, 3]).map(lambda x: x + 1)
    it2 = [1, 2, 3]

    res = IterableSamplerSource(iterables=[it1, it2], probs=[.7, .3]).collect()
    assert sum(res) == 15

Note that you can pass the probabilities of sampling from each iterator. When an iterator is exhausted, the probabilities are normalized.

Advanced chaining
-----------------
Squirrel IterStream api allows flexible chaining of IO-bound and CPU-bound operations, where each operation can use the most appropriate type of executor. This is made possible by passing the `executor` argument to `async_map`. For instance, if there is a need for expensive runtime transformation after fetching samples, you may pass a :code:`concurrent.futures.ProcessPoolExecutor`:

.. code-block:: python

    if __name__ == "__main__":
        pool = ProcessPoolExecutor()
        def runtime_transformation(item):
            time.sleep(2)
            return item + 1

        it = IterableSource([1, 2, 3]).async_map(runtime_transformation, executor=pool)
        for i in it:
            print(i)

It is also possible to run transformation on a local or remote dask cluster

.. code-block:: python

    if __name__ == "__main__":
        client = dask.distributed.Client()
        def runtime_transformation(item):
            time.sleep(2)
            return item + 1

        it = IterableSource([1, 2, 3]).async_map(runtime_transformation, executor=client)
        for i in it:
            print(i)

This is useful, for instance, when items are fetched using a thread pool with minimal resources, but afterward there is an expensive transformation step. Note that squirrel will not clean up resources associated with the executors if it is provided by the user.

PyTorch Distributed Dataloading
-------------------------------

The Squirrel api is designed to support fast streaming of datasets to a multi-rank, distributed system, as often encountered in modern deep learning application involving multiple GPUs. To this end we can use the `SplitByWorker` and `SplitByRank` compsables and wrap the final iterator in a torch `Dataloader` object

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

    samples = [np.random.rand(1000, 1000) for i in range(10 ** 4)]
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
For details, see :ref:`metrics`.

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


The API reference of :code:`monitor` and other related methods can be found in
:ref:`Iterstream Composable and Chaining Methods`.

Modes of `async_map` in Iterstream
----------------------------------
Part of the fast speed from iterstream thanks to :py:func:`squirrel.iterstream.base.Composable.async_map`.
This method carries out the callback function you specified to each sample in the iterstream asynchronously,
therefore offer a large speed-up.

It is worth mentioning that there are currently 4 different modes of :code:`async_map` that you can use under the
argument :code:`executor`, namely :py:class:`concurrent.futures.ThreadPoolExecutor`,
:py:class:`concurrent.futures.ProcessPoolExecutor`, :py:class:`dask.distributed.Client` and :code:`numba`.
For the first three modes, we recommend to go the respective official documentations for their suitable use cases.
We would like to briefly mention what the last option does in :code:`async_map`.

The last option :code:`numba` will pass the iterator inside the :code:`async_map` to a :code:`numba`
decorator :code:`@numba.jit`. Then the speed-up will be entirely provided by :code:`numba` instead of
asynchronous message queues that we have used for the other modes.

Compared to the other three options, `numba` is more performant in some cases but not in others, and highly sensitive to the
actual data type and computation at hand. Therefore, we recommend you read the official `numba documentation`_
from :code:`numba`, and perform a benchmarking, before choose this option in production.

.. note::

    Since numba only supports `limited types of python objects`_, and naturally does not include
    squirrel defined objects, we have to force object mode in numba, that means the decorator we have chosen in squirrel
    is of the following format: :code:`@numba.jit(forceobj=True)`.

.. _numba documentation: https://numba.pydata.org/numba-doc/latest/index.html
.. _limited types of python objects: https://numba.pydata.org/numba-doc/dev/reference/pysupported.html
