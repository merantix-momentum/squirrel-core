metrics
=======
There are two metrics used in :py:func:`squirrel.iterstream.base.Composable.monitor`: :py:func:`metrics_iops` and
:py:func:`metrics_throughput`. They can be turned on and off by passing an instance of :py:class:`MetricsConf` to
:code:`monitor(..., metrics_conf=MetricsConf(...))` whenever it is being used.


.. note::
    Both metrics are turned on be default. When one metric is on and the other is off, the labels of both metrics will
    still be returned, but the actual value of the turned-off metric will be reported as 0. When both are turned off,
    none will be handed over to the callback, and the original iterator simply passes through the method
    :code:`monitor`.

The APIs of these functions and class are listed below:

.. automodule:: squirrel.iterstream.metrics
    :members:
