IterStream
==========

Squirrel provides an API for chaining iterables.
The functionality is provided through the :py:class:`Composable` class, which acts as a base class for most classes in IterStream.

Stream Processing Methods
-------------------------
The :py:class:`Composable` class offers three kinds of methods for processing streams.

* *Source*: The first node in the stream that generates items or wraps an iterable, for instance :py:class:`IterableSource`.
* *Transformations* : Provide a way to apply transformations on items in the stream, such as :py:meth:`map` and :py:meth:`filter`, or manipulate the stream itself, such as :py:meth:`shuffle`, :py:meth:`batched`.
* *Terminal* : :py:meth:`join`, :py:meth:`collect`. These methods are used to consume the stream.


Example Workflow
----------------

.. code-block:: python

    from squirrel.iterstream import IterableSource
    import time

    it = IterableSource([1, 2, 3, 4])
    for item in it:
        print(item)

:py:class:`IterableSource` is a :py:class:`Composable` and has several methods to conveniently load data, given an iterable as the input:

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

To see how you can chain custom Composables with `compose()`, see the advanced section for :ref:`IterStream <advanced/iterstream:IterStream>`.

.. note::

    Note that when defining a custom Composable, you have to omit the `source` argument in the constructor signature.
    This is because the `source` of your custom Composable is automatically set the Composable that is operating on.
    The only time the `source` argument is explicitly set is when creating a `IterableSource`,
    as this marks the beginning of the chain of Composables.

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
Part of the fast speed from iterstream thanks to :py:meth:`squirrel.iterstream.base.Composable.async_map`.
This method carries out the callback function you specified to each item in the stream asynchronously, therefore offers a large speed-up.

.. code-block:: python

    def io_bound(item):
        print(f"{item} io_bound")
        time.sleep(1)
        return item

    it = IterableSource([1, 2, 3]).async_map(io_bound, max_workers=4).async_map(io_bound, max_workers=None)
    t1 = time.time()
    for i in it:
        print(i)
    print(time.time() - t1)


By default, :py:meth:`async_map <squirrel.iterstream.base.Composable.async_map>`
instantiates a :py:class:`ThreadPoolExecutor <concurrent.futures.ThreadPoolExecutor>` (`executor=None`).
It also accepts :py:class:`ProcessPoolExecutor <concurrent.futures.ProcessPoolExecutor>`,
which is a good choice when performing cpu-bound operations on a single machine.

The argument `max_workers` defines the maximum number of workers/threads the
:py:class:`ThreadPoolExecutor <concurrent.futures.ThreadPoolExecutor>`
uses when `executor=None`.
By default, `max_workers=None` relies on an internal heuristic of
the :py:class:`ThreadPoolExecutor <concurrent.futures.ThreadPoolExecutor>`
to select a reasonable upper bound.
This may differ between Python versions.
See the documentation of
:py:class:`ThreadPoolExecutor <concurrent.futures.ThreadPoolExecutor>` for details.

In the above example, two :py:class:`ThreadPoolExecutor <concurrent.futures.ThreadPoolExecutor>`\s
are created, one with an upper bound of 4 threads and the other with a *smart* upper bound.
After the iterator is exhausted, both of these pools will be closed.

If `executor` is provided, no internal
:py:class:`ThreadPoolExecutor <concurrent.futures.ThreadPoolExecutor>` is
created and managed.
As a result, `max_workers` is *ignored* since the provided `executor` already includes
the information and the `executor` has to be manually closed.

.. code-block:: python


    from concurrent.futures import ThreadPoolExecutor
    tpool = ThreadPoolExecutor(max_workers=4)

    def io_bound(item):
        print(f"{item} io_bound")
        time.sleep(1)
        return item

    it = IterableSource([1, 2, 3]).async_map(io_bound, executor=tpool).async_map(io_bound, executor=tpool)
    t1 = time.time()
    for i in it:
        print(i)
    print(time.time() - t1)

    # now the external pool needs to be manually closed
    tpool.shutdown()


In the above example, a
:py:class:`ThreadPoolExecutor <concurrent.futures.ThreadPoolExecutor>` is created with
a maximum of 4 workers.
This pool of workers is shared among both
:py:meth:`async_map <squirrel.iterstream.base.Composable.async_map>` calls.
After exhausting the iterator, the `tpool` is shutdown.
