Dask
========

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
