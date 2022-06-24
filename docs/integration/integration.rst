Integration
===========
In this section, we show how Squirrel integrates with other libraries

PyTorch
--------------------
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

Dask
--------------------
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

Note that after calling :code:`dask_map` for the first time, you can chain more :code:`dask_map`s, which are then operating on the :code:`dask.delayed.Delayed` objects, so that the data and the operations live on the dask cluster until :code:`materialize_dask` is called.

Spark
---------------------

Numba
--------------------
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

Monitoring
---------------------
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

