IterStream
==========
The IterStream module provides functionalities to chain iterables. The core principle behind IterStream is
similar to chaining generators, that is lazy execution.

.. code-block:: python

    def add_1(x):
        print(f"add 1 to {x}")
        return x + 1

    def mult_10(x):
        print(f"multiply 10 to {x}")
        return x * 10

    g1 = (add_1(i) for i in range(3))
    g2 = (mult_10(i) for i in g1)
    next(iter(g2))

Output::

    add 1 to 0
    multiply 10 to 1



This functionality is provided through the :py:class:`~squirrel.iterstream.Composable` class, which forms the base
class for all classes in IterStream.

.. code-block:: python

    from squirrel.iterstream import Composable, IterableSource

    it = IterableSource(range(3))
    it1 = it.map(add_1)
    it2 = it.map(add_1).map(mult_10)

    assert isinstance(it, Composable)
    assert isinstance(it1, Composable)
    assert isinstance(it2, Composable)

    next(iter(it2))

Output::

    add 1 to 0
    multiply 10 to 1


In the example above we see how Composables are chained.
Each transformation again returns a Composable object.
We also call this chain of Composables **stream**.
The executions are done lazily, that is the transformation is only executed when the iterator fetches the next item.

Custom Composable
--------------------
An alternative way of constructing streams is via :py:meth:`squirrel.iterstream.base.Composable.compose`.

.. code-block:: python

    from squirrel.iterstream import Composable, IterableSource

    class Add_1(Composable):
        def __init__(self):
            pass

        def __iter__(self):
            for x in self.source:
                print(f"add 1 to {x}")
                yield x + 1

    class Mult_10(Composable):
        def __init__(self):
            pass

        def __iter__(self):
            for x in self.source:
                print(f"multiply 10 to {x}")
                yield x * 10

    it = IterableSource(range(3)).compose(Add_1).compose(Mult_10)
    next(iter(it))

Output::

    add 1 to 0
    multiply 10 to 1

Similar as before, the execution is done lazily and each transformation returns a Composable object.
The only difference is that we wrap the function inside a custom :py:class:`Composable` class.
Writing custom Composable classes allows us to modify the iteration process.
Some use-cases for custom Composables include:

    #. We need to instantiate an expensive resource such as a database connection or a CNN feature extractor only once
    for the entire stream instead of once per item.
    In this case it is advisable to instantiate this resource in the ``__iter__`` method of your custom class.
    One advantage is that the object is only instantiated once iterating over the stream starts.
    Additionally, if this resource is a non-picklable object, the stream can still be pickled before the iteration
    starts (e.g. in the multiprocessing context).

    #. When a very complex  stream processing is needed that is hard to achieve with standard methods.

The following example illustrates the control flow when chaining Composables.
Note that the order of calling the ``__iter__`` method is from right ot left.

.. code-block:: python

    class Add_1(Composable):
        def __init__(self):
            print("Create Add_1")
            super().__init__()

        def __iter__(self):
            print("Start Add_1")
            for i in iter(self.source):
                print(f"add 1 to {i+1}")
                yield i + 1

    class Mult_10(Composable):
        def __init__(self):
            print("Create Mult_10")
            super().__init__()

        def __iter__(self):
            print("Start Mult_10")
            for i in iter(self.source):
                print(f"multiply 10 to {10*i}")
                yield 10 * i

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

Source in a Stream
------------------------
In a stream, each `Composable` in the chain stores the iterable it operates on in the `source` attribute. That is if we
get the `source` from the *n*-th `Composable` in the chain, we can retrieve the intermediate
results up until the *n-1*-th `Composable` (including). However, note that after repeatedly calling
`source` we will end up with the original iterable, which will not have a `source` attribute.

.. code-block:: python

    def add_1(x):
        return x + 1

    def mult_10(x):
        return x * 10

    it = IterableSource(range(3)).map(add_1).map(mult_10)

    print(f"x: {it.source.source.collect()}")
    print(f"x + 1: {it.source.collect()}")
    print(f"(x + 1) * 10: {it.collect()}")

Output::

    x: [0, 1, 2]
    x + 1: [1, 2, 3]
    (x + 1) * 10: [10, 20, 30]


Asynchronous execution
----------------------
We have seen in :ref:`usage/iterstream:IterStream` how to apply functions on streams asynchronously with
:py:meth:`async_map`.
Internally, a :py:class:`squirrel.iterstream.base._AsyncMap` object is constructed when calling :py:meth:`async_map`.
:py:class:`_AsyncMap` maintains an internal queue and creates :py:class:`AsyncContent`\s that are inserted to the
queue.
:py:class:`AsyncContent` objects are created by specifying a function callback, the item it operates on, and an
executor.
When :py:class:`AsyncContent` object is created, the function callback is scheduled for asynchronous execution.
We can simply fetch results from the queue by iterating over the :py:class:`_AsyncMap` object.

Architecture
--------------------
Most classes inherit from :py:class:`Composable`, which provides many concrete stream processing methods such as
:py:meth:`map` and :py:meth:`filter`, and one abstract method ``__iter__`` which must be implemented by all subclasses.

In the non-exhaustive UML diagram below, we show how the IterStream module is structured.

.. mermaid::

    classDiagram

        Composable <|-- _Iterable
        Composable <|-- IterableSource
        Composable <|-- _AsyncMap
        AsyncContent <.. _AsyncMap

         <<abstract>> Composable
        class Composable {
            source: Iterable or Callable

            __iter__() Iterator
            compose(constructor, *args, **kwargs) Composable
            map(callback) _Iterable
            async_map(callback, buffer, max_workers, executor) _AsyncMap
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



