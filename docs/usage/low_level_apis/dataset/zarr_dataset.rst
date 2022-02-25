ZarrDataset
-----------

**ZarrDataset** provides you a single entry point to a zarr dataset. Given a zarr dataset uri :code:`zarr_ds_uri = "/path/to/your/zarr/dataset"`, you can create an instance of zarr dataset, by calling :code:`ds = ZarrDataset(zarr_ds_uri)`.

ZarrDataset includes many convenient methods for you to operate on a zarr dataset:

- :py:func:`~squirrel.dataset.zarr_dataset.ZarrDataset.get_group` returns the root zarr group.

- :py:func:`~squirrel.dataset.zarr_dataset.ZarrDataset.optimize_for_reading` will cache all the path keys inside your zarr group.

- :py:func:`~squirrel.dataset.zarr_dataset.ZarrDataset.get_stream` returns an instance of :py:class:`~squirrel.dataset.stream_dataset.StreamDataset`, which you can utilize to fetch your zarr samples / tiles in an asynchronous fashion, which will reduce your IO time, especially for datasets living in cloud buckets. For more details, see :doc:`stream_dataset`.

.. automodule:: squirrel.dataset.zarr_dataset
    :members:
