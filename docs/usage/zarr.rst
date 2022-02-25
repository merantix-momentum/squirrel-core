Work with Zarr
==============
Squirrel provides you with an interface to a `Zarr group <https://zarr.readthedocs.io/en/stable/api/hierarchy.html#zarr.hierarchy.group>`_ with some custom optimizations.

The core class to provide this functionality is called **ZarrDataset** (:code:`squirrel.dataset.zarr_dataset.ZarrDataset`), which provides many squirrel functionality of optimization for accessing zarr datasets.

Example Workflow
----------------

You can access a Zarr group with preconfigured setting by providing an `FSSpec <https://filesystem-spec.readthedocs.io/en/latest/>`_ path to the root of the group. Example:


.. code-block:: python

    from squirrel.dataset.zarr_dataset import ZarrDataset

    ds = ZarrDataset('gs://bucket_name/path/to/blob')
    root = ds.get_group(mode='r')

This returns a Zarr group named :code:`root`. For example (for those who are not familiar with zarr yet):

.. code-block:: python

    # suppose you have an array under sample 'sample0':
    array = root['sample0'][:] # get array value
    meta = root['sample0'].attrs['meta_key'] # get the metadata stored in the array using the predefined key



For optimized reading, Squirrel provides you with a streaming interface to Zarr. You can request keys from the Zarr group and get the results back in a stream. Example:

.. code-block:: python

    from squirrel.dataset.zarr_dataset import ZarrDataset

    zd = ZarrDataset('gs://bucket_name/path/to/blob')
    zds = ds.get_stream()
    zdg = ds.get_group(mode='r')

    # request all items in the zarr group
    for key in zdg:
        zds.request(key)

    # efficiently stream requested keys
    for key in zdg:
        item = zds.retrieve()
        ... do something with item

If you would like to provide custom fetcher:

.. code-block:: python

    from squirrel.dataset.zarr_dataset import ZarrItemFetcher

    class MyItemFetcher(ZarrItemFetcher):
        def __init__(self, path):
            super().__init__(path)

        def fetch(self, key):
            return self.store[key][:] # example case, returns value of the zarr array

        def write(self, key):
            pass

    kw1 = ...  # kwargs passed to fsspec and fetcher
    zds = ZarrDataset(path="path/to/data", **kw1)
    kw2 = ... # kwargs passed to fetcher init method
    stream = zds.get_stream(fetcher_class=MyItemFetcher, **kw2)

You can optimize the read only version of the Zarr group by using it's convenience functions in squirrel.zarr.convenience. The simplest way to optimize reading is caching the structure of the Zarr store. Example:

.. code-block:: python

    from squirrel.dataset.zarr_dataset import ZarrDataset
    from squirrel.zarr.convenience import optimize_for_reading

    ds = ZarrDataset('gs://bucket_name/path/to/blob')

    # optimize underlying store with the settings of your choice
    ds.optimize_for_reading(shard=True, worker=8)

    # reading from the group should be much faster now.
    root = ds.get_driver(mode='r')  # alias: get_group()

If you want to do the optimization in parallel, you can run the consolidation steps on your own and parallelize routing and shard writing.

.. code-block:: python

    from squirrel.dataset.zarr_dataset import ZarrDataset
    from squirrel.zarr.store import cache_meta_store, write_routing_to_store, write_shard_to_store
    from squirrel.zarr.sharding import suggest_shards

    ds = ZarrDataset('gs://bucket_name/path/to/blob')
    root_a = ds.get_group(mode='a')

    # cache metadata
    cache_meta_store(root_a.store, cache_keys=True, cache_meta=False, compress=True, clean=True)
    root_r = ds.get_group(mode='r')

    # suggest shards
    n_shards = 100
    compress = True
    routing = suggest_shards(root_r.store, shards=n_shards, shuffle_keys=True)

    # write routing which can be done in parallel
    for idx in range(n_shards):
        write_routing_to_store(root_a.store, idx, routing[idx], compress=compress)

    # write shards which can be done in parallel
    for idx in range(n_shards):
        write_shard_to_store(root_a.store, idx, compress=compress)

    # reading from the group should be much faster now.
    root = ds.get_driver(mode='r')  # alias: get_group()

ZarrDataset
-----------

:doc:`ZarrDataset <low_level_apis/dataset/zarr_dataset>` provides you a single entry point to a zarr dataset. Given a zarr dataset uri :code:`zarr_ds_uri = "/path/to/your/zarr/dataset"`, you can create an instance of zarr dataset, by calling :code:`ds = ZarrDataset (zarr_ds_uri)`, this includes many convenient methods for you to operate on a zarr dataset.

- :py:func:`ZarrDataset.get_group()` returns a root zarr group. For more details, see :doc:`Convenience Methods <low_level_apis/zarr/convenience>`.

- :py:func:`ZarrDataset.optimize_for_reading()` will cache all the path keys inside your zarr group. For more details, see :doc:`Convenience Methods <low_level_apis/zarr/convenience>`.

- :py:func:`ZarrDataset.get_stream` returns an instance of :py:class:`StreamDataset`, which you can utilize to fetch your zarr samples / tiles in a asynchronous fashion, which will reduce your IO time calling from cloud buckets. For more details, see :doc:`Stream Dataset <low_level_apis/dataset/stream_dataset>`.
