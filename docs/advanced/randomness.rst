Measure randomness of map driver
==================================
The :py:class:`~squirrel.driver.MapDriver` supports loading data from multiple shards.
A difficult choice is often to select an appropriate shard size when a dataset is created.
The choice of the shard size in combination with the size of the squirrel shuffel buffer determines
if the iid assumption holds approximately while we sample the data.
We provide a helper function to estimate the randomness of loading data from any data source using
the provided shuffle buffer size, number of shards and shard size.
We also allow to control the size of the initial shuffle buffer separately.
As a best practice, we assume that the shuffle buffer for shard keys is always set to
the maximum, i.e., corresponds to the number of shards.
Unless your dataset has a very significant number of shards, it should cost almost no
overhead to shuffle a simple list of keys entirely.

.. code-block:: python

    from squirrel.benchmark.quantify_randomness import quantify_randomness

    shards = 5
    shard_size = 25
    shuffle_buffer_size = 5

    print(quantify_randomness(shards, shard_size, shuffle_buffer_size, shuffle_buffer_size, n_samples=100))


We compute the randomness measure as the
`kendall tau coefficient <https://https://en.wikipedia.org/wiki/Kendall_rank_correlation_coefficient/>`_.
Values range between 0 and 1 while 1 means completely deterministic and 0 means random.
