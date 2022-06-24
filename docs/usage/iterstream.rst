IterStream
==========

Squirrel provides an API for chaining iterables.
The functionality is provided through the :py:class:`Composable`.

Stream Processing Methods
-------------------------
The :py:class:`Composable` class offers three kinds of methods for processing streams.

* *Source*: The first node in the stream that generates items or wraps an iterable, for instance :py:class:`IterableSource`.
* *Transformations* : Provide a way to apply transformations on items in the stream, such as :py:meth:`map` and :py:meth:`filter`, or manipulate the stream itself, such as :py:meth:`shuffle`, :py:meth:`batched`.
* *Terminal* : :py:meth:`join`, py:meth:`collect`. These methods are used to consume the stream.

Example Workflow
----------------

.. code-block:: python

    from squirrel.iterstream import IterableSource
    import time

    it = IterableSource([1, 2, 3, 4])
    for item in it:
        print(item)

:py:class:`IterableSource` has several methods to conveniently load data, given an iterable as the input:

.. code-block:: python

    it = IterableSource([1, 2, 3, 4]).map(lambda x: x + 1).async_map(lambda x: x ** 2).filter(lambda x: x % 2 == 0)
    for item in it:
        print(item)

:code:`map_async()` applies the provided function asynchronously. More on this in the following sections.
In addition to explicitly iterating over the items, it's also possible to call `collect()` to collect all items in a list, or `join()` to iterate over items without returning anything.

Items in the stream can be shuffled in the buffer and batched

.. code-block:: python

    it = IterableSource(range(10)).shuffle(size=5).map(lambda x: x+1).batched(batchsize=3, drop_last_if_not_full=True)
    for item in it:
        print(item)

Note that the argument `drop_last_if_not_full` (default True) will drop the last batch if its size is less than `batchsize` argument; so, only 3 items will be printed above.

Items in `IterableSource` can be composed by providing a Composable in the `compose()` method:

.. code-block:: python

    from squirrel.iterstream import Composable

    class MyIter(Composable):
        def __init__(self):
            super().__init__()

        def __iter__(self):
            for i in iter(self.source):
                yield f"_{i}", i

    it = IterableSource([1, 2, 3]).compose(MyIter)
    for item in it:
        print(item)


To see how you can chain custom Composables with `compose()`, see the advanced section :ref:`Driver <advanced/iterstream>`.

Combining multiple iterables can be achieved using `IterableSamplerSource`:

.. code-block:: python

    from squirrel.iterstream import IterableSamplerSource

    it1 = IterableSource([1, 2, 3]).map(lambda x: x + 1)
    it2 = [1, 2, 3]

    res = IterableSamplerSource(iterables=[it1, it2], probs=[.7, .3]).collect()
    print(res)
    assert sum(res) == 15

Note that you can pass the probabilities of sampling from each iterator. When an iterator is exhausted, the probabilities are normalized.

Asynchronous execution
----------------------
Part of the fast speed from iterstream thanks to :py:meth:`squirrel.iterstream.base.Composable.async_map`. This method carries out the callback function you specified to each item in the stream asynchronously, therefore offers a large speed-up.

.. code-block:: python


    from concurrent.futures import ThreadPoolExecutor
    tpool =  ThreadPoolExecutor()

    def io_bound(item):
        print(f"{item} io_bound")
        time.sleep(1)
        return item

    it = IterableSource([1, 2, 3]).async_map(io_bound, executor=tpool).async_map(io_bound)
    t1 = time.time()
    for i in it:
        print(i)
    print(time.time() - t1)

`async_map` instantiates a :code:`concurrent.futures.ThreadPoolExecutor` if the argument `executor` is `None` (default). It also accepts :code:`concurrent.futures.ProcessPoolExecutor`, which is a good choice when performing cpu-bound operations on a single machine.

Internally, a :py:class:`_AsyncMap` object is constructed when calling :py:meth:`async_map`.
:py:class:`_AsyncMap` maintains an internal queue and creates :py:class:`AsyncContent` that are inserted to the queue.
:py:class:`AsyncContent` objects are created by specifying a function callback, the item it operates on, and an executor.
When :py:class:`AsyncContent` object is created, the function callback is scheduled for asynchronous execution. We can simply fetch results
from the queue by iterating over the :py:class:`_AsyncMap` object.
