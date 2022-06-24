PyTorch
===========
The Squirrel api is designed to support fast streaming of datasets to a multi-rank, distributed system, as often encountered in modern deep learning applications involving multiple GPUs. To this end, we can use the `SplitByWorker` and `SplitByRank` composables and wrap the final iterator in a torch `Dataloader` object

.. code-block:: python

    import torch.utils.data as tud
    from squirrel.iterstream.source import IterableSource
    from squirrel.iterstream.torch_composables import SplitByRank, SplitByWorker, TorchIterable

    def times_two(x: float) -> float:
        return x * 2

    samples = list(range(100))
    batch_size = 5
    num_workers = 4
    it = (
            IterableSource(samples)
            .compose(SplitByRank)
            .async_map(times_two)
            .compose(SplitByWorker)
            .batched(batch_size)
            .compose(TorchIterable)
        )
    dl = tud.DataLoader(it, num_workers=num_workers)

Note that the rank of the distributed system depends on the torch distributed process group and is automatically determined.

And using :py:mod:`squirrel.driver` api:

.. code-block:: python

    from squirrel.driver import MessagepackDriver
    url = ""
    it = MessagepackDriver(url).get_iter(key_hooks=[SplitByWorker]).async_map(times_two).batched(batch_size).compose(TorchIterable)
    dl = DataLoader(it, num_workers=num_workers)

In this example, :code:`key_hooks=[SplitByWorker]` ensures that keys are split between workers before fetching the data and we achieve two level of parallelism; multi-processing provided by :code:`torch.utils.data.DataLoader`, and multi-threading inside each process for efficiently fetching samples by :code:`get_iter`.
