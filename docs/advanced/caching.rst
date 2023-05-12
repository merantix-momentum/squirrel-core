Caching
=======

Context
-------

There are advantages and disadvantages when fetching data from remote storage (e.g., buckets). Remote storage offers conveniences, such as easy data sharing between applications and users. Additionally, remote storage is often more cost-effective than local storage for long-term data repositories, which is particularly beneficial for large datasets.

Despite these advantages, there are potential drawbacks to fetching data from remote storage. One significant concern is slower data retrieval, which can impact the performance of your Squirrel application. Retrieving data over a network connection introduces additional latency over accessing data locally. Another factor to consider is the pricing structure associated with remote storage. Typically, remote storage costs are not solely based on how long which amount of data is stored but also on the amount of data transferred across the network and the number of requests made when retrieving the data. Consequently, you may incur extra costs, especially if you need to fetch a large number of shards across the network connection.

Caching to the Rescue
---------------------
In Machine Learning workloads, models are often trained over multiple epochs, meaning you may need to fetch the same data multiple times during a run. To optimize this process, imagine if you could load the remote data only in the first pass (first epoch), store it locally, and subsequently access the data from the fast and inexpensive local disk. Precisely this functionality is offered through caching.

Squirrel leverages the capabilities of the ``fsspec`` library, which includes a powerful caching feature out of the box. Each default Squirrel :py:class:`~squirrel.driver.Driver` such as :py:class:`~squirrel.driver.DataFrameDriver`, :py:class:`~squirrel.driver.FileDriver`, or :py:class:`~squirrel.driver.StoreDriver` accepts a ``storage_options`` argument, which is a dictionary passed down to the ``fsspec`` filesystem. This dictionary allows you to configure caching, among other things. For more detailed information, please refer to the ``fsspec`` `documentation <https://filesystem-spec.readthedocs.io/en/latest/features.html#caching-files-locally>`_ on local caching.

The code below shows an example of configuring caching for several drivers. Note that, as per the ``fsspec`` documentation, only ``simplecache`` is "guaranteed thread/process-safe".

.. code-block:: python

    from squirrel.driver import CsvDriver, FileDriver, MessagepackDriver

    so = {"protocol": "simplecache", "target_protocol": "gs", "cache_storage": "/tmp/cache"}
    
    CsvDriver("gs://bucket/data.csv", engine="pandas", storage_options=so)  # inherits from DataFrameDriver
    MessagepackDriver("gs://bucket/data-dir", storage_options=so)  # inherits from StoreDriver
    FileDriver("gs://bucket/file.txt", storage_options=so)

Let's observe the performance benefits of caching in action. The below code compares the performance of a :py:class:`~squirrel.driver.MessagepackDriver` with and without caching. The generated plot shows that the non-cached driver has a similar loading speed for all epochs. However, the cached driver stores the data on the local disk in the first epoch and reads it from the local disk in the subsequent epochs, making it much faster than the non-cached driver.

.. literalinclude:: ../examples/msgpack_caching.py
    :language: python

.. image:: ./msgpack_caching.svg
   :align: center
   :alt: MessagepackDriver with and without Caching

Storage Options and Catalogs
----------------------------

When using the :py:class:`~squirrel.catalog.Catalog`-API, some users will have written some ``storage_options`` to the catalog (e.g., that their Google Cloud Service (GCS) bucket is ``requester_pays=True``). As a new user, you might now want to provide additional ``storage_options`` (e.g., for caching). As shown below in the code, you can do so when you call :py:func:`~squirrel.catalog.catalog.CatalogSource.get_driver` on the :py:class:`~squirrel.catalog.catalog.CatalogSource`. Squirrel ensures that the new ``storage_options`` passed to :py:func:`~squirrel.catalog.catalog.CatalogSource.get_driver` are merged with the pre-existing ``storage_options``.

.. literalinclude:: ../examples/catalog_so_update.py
    :language: python