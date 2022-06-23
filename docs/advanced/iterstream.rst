IterStream
==========
The IterStream module provides functionalities to chain iterables. This functionality
is provided through the mix-in class :py:class:`Composable`, which implements methods such as map or filter.
The nice thing about it is that all operations are done lazily, that is no computation is executed until we ask for it.

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

Here, we see that the functions are only executed when the iterator is asked to fetch the next item.
An alternative way of chaining operations is via :py:meth:`squirrel.iterstream.base.Composable.compose`.

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

Similar as before, the execution is done lazily. The only difference is that we wrap the function inside a class inheriting
from `Composable`. One scenario why we might want to use `compose()` is when we need to instantiate an expensive resource
for the transformations such as a large network for preprocessing or a database connection.

An interesting fact about `compose()` is its order of execution:

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

    Create Add_1
    Create Mult_10
    Start Mult_10
    Start Add_1
    add 1 to 0
    multiply 10 to 1

The constructors are called from left to right, as is the execution of the transformations. However, we can see
that the iterators are called from right to left.

Architecture
--------------------
Most classes inherit from `Composable` to implement methods for stream manipulation.
:py:meth:`squirrel.iterstream.base._Iterable` and :py:meth:`squirrel.iterstream.base._AsyncMap` are both instantiated when
chaining stream manipulations. The difference is that `_AyncMap` deals with asynchronous execution. We show a non-exhaustive UML
diagram below. In particular, there is many more methods for stream manipulation not listed in the `Composable` class.

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
            buffer: int
            max_workers: int
            executor: Executor

            __iter__() Iterator
       }

       class AsyncContent {
            future: executor.submit(func, item)

            value(): fetch results
       }

Special Composables
---------------------
There are a few special `Composable` classes such as :py:class:`squirrel.iterstream.source.FilePathGenerator` or
classes that help you interface with PyTorch. `FilePathGenerator` for example is an iterable that returns a generator iterator
over the folder contents given an url.
We can also transform an iterable into a PyTorch compatible format using
:py:class:`squirrel.iterstream.torch_composables.TorchIterable`.
The tutorial on
`PyTorch training with Squirrel <https://github.com/merantix-momentum/squirrel-datasets-core/blob/main/examples/03.Pytorch_Model_Training.ipynb/>`_
provides a good example usage. Other special `Composable` classes such as :py:class:`squirrel.iterstream.torch_composables.SplitByWorker`
or :py:class:`squirrel.iterstream.torch_composables.SplitByRank` exists for
`distributed training in PyTorch <https://github.com/merantix-momentum/squirrel-datasets-core/blob/main/examples/10.Distributed_MNIST.py/>`_.


