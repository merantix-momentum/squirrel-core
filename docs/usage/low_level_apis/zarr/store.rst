Stores in Squirrel
==================
At a low level, squirrel offers some store options that could enhance or modify zarr stores such as

* reducing number of HTTP calls zarr has to make in order to fetch items from cloud datasets
* fetching items from datasets in an asynchronous manner
* caching storage path keys and metadata

to allow fast cluster / full iteration through the dataset.

Currently, we offer the following stores:

* :py:class:`~squirrel.zarr.store.CachedStore` provides a squirrel store layer between ``zarr.hierarchy.Group`` and
  ``zarr.storage.FSStore`` to add multiple optimizations to zarr access.

  In particular, it provides a caching mechanism to store all storage path keys, directories and metadata inside a
  dataset in a single (or multiple) squirrel files, to allow fast access / iteration through the whole or a part of the
  dataset.

* :py:class:`~squirrel.zarr.store.SquirrelFSStore` is based on ``zarr.storage.FSStore``. In SquirrelFSStore, we use
  squirrel's :doc:`custom fsspec <fsspec>` module to control HTTP connections to cloud buckets.


squirrel.zarr.store
-------------------
.. automodule:: squirrel.zarr.store
    :members: