Driver
==============

Driver provides a programming interface to control and manage specific lower level interface that is often linked to a specific type of hardware, or other low-level service (`wikipedia <https://en.wikipedia.org/wiki/Driver_(software)>`_). Squirrel offer several drivers to read various data formats:

1. `record`
2. `messagepack`
3. `jsonl`
4. `zarr`
5. `csv`

Each driver consists of two building blocks, `store` and `loader`. Store exposes low-level api for accessing items, while dataloader provides higher-level api for reading data efficiently. To read the data using `loader`:

.. code-block:: python

    from squirrel.driver import RecordLoader, Sample, RecordStore

    url = "gs://bucket/dataset"
    for sample in RecordLoader(url).get_iter():
        assert isinstance(sample, Sample)

    record_store = RecordLoader(url).get_store()
    assert isinstance(record_store, RecordStore)

The record driver consists of a `loader` (i.e. `RecordLoader`) and a `store` (i.e. `RecordStore`). `loader` is a mechanism to efficiently read the data in a streaming manner, while `store` provides a map-style access to individual samples, with `get` and `set` method, and a `keys` method that returns all keys in of the samples in the store. `Sample` is a squirrel primitive to store and retrieve data from `RecordStore`. In order to store data to the `store`:

.. code-block:: python

    store = RecordStore("/path/to/store")
    samp = Sample(key="my_key", value={"image": np.random.random((100, 100, 3), "split": "train", "meta": {})})
    store.set(samp)
    retrieved_sample = list(store.get("my_key"))[0]
    assert isinstance(sample, Sample)
    assert list(store.keys()) == ["my_key"]

The key argument of the `Sample` is optional. If not provided, a random key is assigned. It should not contain `/` character.

Other drivers such as `messagepack` and `jsonl` have their specific `store` and `loader` and provide a similar api, with the notable difference that they store the collection of `Sample` as a `Shard`. Similar to `Sample`, the `key` argument of `Shard` is optional and auto-assigned if not provided.

.. code-block:: python

    from squirrel.driver import Shard, MessagepackStore

    store = MessagepackStore("/home/user/store")
    s1 = Sample(key="my_key", value={"image": np.random.random((100, 100, 3)), "split": "train", "meta": {}})
    s2 = Sample(key="another_key", value={"image": np.random.random((100, 100, 3)), "split": "train", "meta": {}})
    shard = Shard(key="shard_1", value=[s1, s2])
    store.set(shard)
    retrieved_shard = list(store.get("shard_1"))
    assert len(retrieved_shard) == 2  # it retrieves individual samples within the shard
    assert isinstance(retrieved_shard[0], Sample)
    assert list(store.keys()) == ["shard_1"]

    # This is for illustrative purpose only, please use MessagePackDataLoader for reading data (see below)
    for k in store.keys():
        for sam in store.get(k):
            print(sam)

Replacing `MessagepackStore` with `JsonlStore` in the above example would work the same way. messagepack have several advantages over jsonl, it's faster to write and read, produces smaller objects, and preserve types (which is particularly important for numpy arrays). The advantage of jsonl is that it's human-readable.

All drivers have a `get_iter` method, which can be accessed from the corresponding `loader`. It returns an iterable of type `Composable` (from the package :code:`squirrel.iterstream`), and as such, `map`, `filter`, and `async_map` work seamlessly on it:

.. code-block:: python

    from squirrel.driver import MessagePackDataLoader
    from squirrel.iterstream.base import Composable

    url = "gs://bucket/rec_dataset"
    record_loader = MessagePackDataLoader(url=url)
    it = record_loader.get_iter()
    assert isinstance(it, Composable)      # same for JsonlLoader, RecordLoader
    rec_store = record_loader.get_store()
    assert isinstance(rec_store, RecordStore)

    def _transform(sample):
        return sample

    for batch in it.filter(lambda sample: sample.values["split"] == "train").map(_transform).batched(10):
        # train your model
        pass

The `get_iter()` of each `loader` can be configured to pre-fetch, shuffle, and buffer the data. It also accepts `key_iterator` argument, if provided, only samples corresponding to these keys are retrieved, otherwise all samples will be retrieved. Please refer to the docstring of each one to learn more about this.

The recommended way to read `zarr` is via `ZarrLoader`:

.. code-block:: python

    from squirrel.driver import ZarrLoader
    zarr_url = "gs://bucket/dataset.zarr"
    def fetcher(store, key):
        return store[key]

    zarr_loader = ZarrLoader(zarr_url)
    it = zarr_loader.get_iter(fetcher_func=fetcher)
    root = zarr_loader.get_store()

.. warning::
    The ZarrDataset (:code:`squirrel.dataset.zarr_dataset.ZarrDataset`) will be deprecated in the future.

Squirrel can be used to also manage your models. WARNING: This is very experimental. A brief example:

Trace your model to TorchScript and save using file driver.

.. code-block:: python

    import torch
    import torch.nn as nn
    import torch.nn.functional as F

    from squirrel.driver.file_driver import FileDriver

    class Model(nn.Module):
        def __init__(self):
            super(Model, self).__init__()
            self.conv1 = nn.Conv2d(1, 20, 5)
            self.conv2 = nn.Conv2d(20, 20, 5)

        def forward(self, x):
            x = F.relu(self.conv1(x))
            return F.relu(self.conv2(x))

    my_model = Model()

    with FileDriver('gs://path/to/my/bucket/test_model.pt').get_store(mode='wb', create_if_not_exists=True) as f:
        my_scripted_model = torch.jit.script(my_model)
        torch.jit.save(my_scripted_model, f)

Load a TorchScript model from a remote bucket.

.. code-block:: python

    import torch
    from squirrel.driver.file_driver import FileDriver

    with FileDriver('gs://path/to/my/bucket/test_model.pt').get_store(mode='rb') as f:
        my_model = torch.jit.load(f)