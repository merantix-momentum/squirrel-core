Store
=====

:py:class:`Store` manages the storage and retrieval of data and serve as an abstraction layer under :py:class:`StoreDriver` to ease the
implementation of custom drivers.

Squirrel store API defines three methods:

* :py:meth:`Store.set`: Used to store a value with a key.

* :py:meth:`Store.get`: Used to retrieve a previously stored value.

* :py:meth:`Store.keys`: Returns all the keys for which the store has a value.

.. note::

    A Store permits persisting of value via the :py:meth:`set` method whereas a :ref:`Driver <driver>` can only read
    from a data source and cannot write to it.

SquirrelStore
--------------
:py:class:`SquirrelStore` is the recommended store to use with squirrel. It comes with several optimizations to
improve read/write speed and reduce storage size.

With SquirrelStore, it is possible to:

* Save shards (i.e. a collection of samples) in the store and retrieve them fast (see `Performance Benchmark`)

* Serialize shards using a :py:class:`SquirrelSerializer` instance

A Store can be initialized as below:

.. code-block:: python

    import tempfile

    from squirrel.serialization import MessagepackSerializer
    from squirrel.store import SquirrelStore

    tmpdir = tempfile.TemporaryDirectory()
    msg_store = SquirrelStore(url=tmpdir.name, serializer=MessagepackSerializer())

You can get an instance of a store from driver too.
This is the recommended approach, unless low-level control is needed.

.. code-block:: python

    from squirrel.driver import MessagepackDriver

    driver = MessagepackDriver(tmpdir.name)
    store = driver.store

Writing samples as shards using SquirrelStore
---------------------------------------------
#. Approach 1: Write/read shards sequentially
#############################################

.. code-block:: python

    import numpy as np


    def get_sample(i):
        return {
            "image": np.random.random((3, 3, 3)),
            "label": np.random.choice([1, 2]),
            "metadata": {"key": i},
        }


    N_SAMPLES, N_SHARDS = 100, 10
    samples = [get_sample(i) for i in range(N_SAMPLES)]
    shards = [samples[i : i + 10] for i in range(N_SHARDS)]

Shards can be saved by using the set() method.

.. code-block:: python

    for i, shard in enumerate(shards):
        store.set(
            shard,
            key=f"shard_{i}",  # dont need to set key, if omitted, a random key will be used
        )

    assert len(list(store.keys())) == N_SHARDS

Let's check out a sample:

.. code-block:: python

    for key in store.keys():
        shard = store.get(key)
        for sample in shard:
            print(sample)
            break
        break

    # Clean up
    tmpdir.cleanup()


Approach 2: Write/read shards asynchronously using iterstream
#############################################################
`SquirrelStore` does not buffer any data, as soon as `set()` is called, the data is written to the store.
Because of this, writing to the store can be easily parallelized.
In the following example, we use `async_map` from `Iterstream` module to write shards to the store in parallel and also
read from the store in parallel.

.. code-block:: python

    from squirrel.iterstream import IterableSource

    tmpdir = tempfile.TemporaryDirectory()
    store = MessagepackDriver(tmpdir.name).store

    # note that we are not providing keys for the shards here, random keys will be used
    IterableSource(shards).async_map(store.set).join()
    assert len(list(store.keys())) == 10

    samples = IterableSource(store.keys()).async_map(store.get).flatten().collect()
    assert len(samples) == 100

    # Clean up
    tmpdir.cleanup()