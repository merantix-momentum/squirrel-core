IterStream
==========
The IterStream module provides functionalities to chain iterables. This functionality
is provided through the :py:class:`Composable` class, which forms the base class for most classes in IterStream.

.. code-block:: python

    from squirrel.iterstream import IterableSource

    def add_1(x):
        print(f'add 1 to {x}')
        return x + 1

    def mult_10(x):
        print(f'multiply 10 to {x}')
        return x * 10

    it = IterableSource(range(3)).map(add_1).map(mult_10)
    next(iter(it))

Output::

    add 1 to 0
    multiply 10 to 1


In the example above we show how Composables are chained. We also call this chain of Composables a *stream*. We can see from the examples that
the transformations are executed lazily, that is the transformation is only executed when the iterator fetches the next item.

Custom Composable
--------------------
An alternative way of constructing streams is via :py:meth:`squirrel.iterstream.base.Composable.compose`.

.. code-block:: python

    from squirrel.iterstream import IterableSource
    from squirrel.iterstream import Composable

    class Add_1(Composable):
    def __init__(self):
        pass

    def __iter__(self):
        for x in self.source:
            print(f'add 1 to {x}')
            yield x + 1

    class Mult_10(Composable):
        def __init__(self):
            pass

        def __iter__(self):
            for x in self.source:
                print(f'multiply 10 to {x}')
                yield x * 10

    it = IterableSource(range(3)).compose(Add_1).compose(Mult_10)
    next(iter(it))

Output::

    add 1 to 0
    multiply 10 to 1

Similar as before, the execution is done lazily. The only difference is that we wrap the function inside a custom `Composable`
class. Writing custom Composable classes allows us to modify the iteration process. One use-case is for example when
we want to instantiate a expensive resource only once in the constructor e.g. a database connection
or a R-CNN feature extractor.

When using `compose()` note that the order of calling the `__iter__` method is from right ot left.

.. code-block:: python

    class Add_1(Composable):
        def __init__(self):
            print("Create Add_1")
            super().__init__()

        def __iter__(self):
            print("Start Add_1")
            for i in iter(self.source):
                print(f"add 1 to {i+1}")
                yield i+1


    class Mult_10(Composable):
        def __init__(self):
            print("Create Mult_10")
            super().__init__()

        def __iter__(self):
            print("Start Mult_10")
            for i in iter(self.source):
                print(f"multiply 10 to {10*i}")
                yield 10*i

    it = IterableSource(range(3)).compose(Add_1).compose(Mult_10)
    next(iter(it))

Output::

    Create Add_1
    Create Mult_10
    Start Mult_10
    Start Add_1
    add 1 to 0
    multiply 10 to 1

The constructors are called from left to right, as is the execution of the transformations. However, we can see
that the iterators are called from right to left.

.. admonition:: PyTorch

    There are already special Composables implemented for interfacing with PyTorch such as :py:class:`TorchIterable` or
    :py:class:`SplitByWorker`. Examples are given in :ref:`usage/iterstream:PyTorch Distributed Dataloading`.

    Note that PyTorch Dataloader requires the iterable passed to be pickable. That is, our custom Composable
    can't have a non-pickable object such as a `fssepc` object. A solution is to create the object in the `__iter__` method
    instead of inside the constructor.

Asynchronous Execution
-----------------------
The documentation in :ref:`usage/iterstream:IterStream` explain how asynchronous execution is performed with :py:meth:`async_map`.
Internally, a :py:class:`_AsyncMap` object is constructed when calling :py:meth:`async_map`.
:py:class:`_AsyncMap` maintains an internal queue and creates :py:class:`AsyncContent` that are inserted to the queue.
:py:class:`AsyncContent` objects are created by specifying a function callback, the item it operates on, and an executor.
When :py:class:`AsyncContent` object is created, the function callback is scheduled for asynchronous execution. We can simply fetch results
from the queue by iterating over the :py:class:`_AsyncMap` object.

Stream Processing Methods
-------------------------
The :py:class:`Composable` class offers three kinds of methods for processing streams.

* *Transformations* : :py:meth:`map`, :py:meth:`filter`. These methods can be used to apply a transformation over the stream.
* *Terminal* : :py:meth:`join`, py:meth:`collect`. These methods are used to materialize the stream.
* *Organization*: :py:meth:`shuffle`, py:meth:`batch`, py:meth:`take`. THe methods are used to order and organize the stream.

Architecture
--------------------
Most classes inherit from :py:class:`Composable` to implement methods for stream processing.
In the non-exhaustive UML diagram below, we show how the IterStream module is structured.

.. mermaid::

    classDiagram

        Composable <|-- _Iterable
        Composable <|-- IterableSource
        Composable <|-- _AsyncMap
        AsyncContent <.. _AsyncMap

        class Composable {
            source: Iterable or Callable

            __iter__() Iterator
            compose(constructor, *args, **kwargs) Composable
            map(callback) _Iterable
            async_map(callback, buffer, max_workers, executor) _Iterable
        }

       class _Iterable {
            source: Iterable
            callback: Callable

            __iter__() Iterator
       }

       class IterableSource {
            source: Iterable or Callable

            __iter__() Iterator
       }

        class _AsyncMap {
            source: Iterable
            callback: Callable
            int buffer
            int max_workers
            Executor executor

            __iter__() Iterator
       }

       class AsyncContent {
            future: executor.submit(func, item)

            value(): fetch results
       }



