Monitoring with MLflow and WandB
==============
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

