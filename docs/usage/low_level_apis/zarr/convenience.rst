Convenience Methods
===================

Squirrel provides the following convenience methods for easy access to and manipulation of zarr datasets:

#. :py:func:`suggested_store`: Determine the most appropriate store for the given parameters. 

#. :py:func:`get_group`: Instantiate a zarr group that is on a store most appropriate for the given parameters.
   This method is also available at the :py:class:`~squirrel.dataset.zarr_dataset.ZarrDataset` level. The store is
   identified by :py:func:`~squirrel.zarr.convenience.suggested_store`.

#. :py:func:`optimize_for_reading` Cache the dataset keys (item names), directories (absolute paths), dataset length
   (number of items) and metadata. This method is also available at the
   :py:class:`~squirrel.dataset.zarr_dataset.ZarrDataset` level.

   .. warning::
       :py:func:`optimize_for_reading` is in experimental phase and is not ready for large datasets yet. The
       :code:`shard=True` option is currently under development and is not stable.


squirrel.zarr.convenience
-------------------------
.. automodule:: squirrel.zarr.convenience
    :members:
