IterStream
==========
The IterStream module provides functionalities to chain operations on iterables. This functionality
is provided through the mix-in class :py:class:`Composable`, which implements methods such as map or filter.
The nice thing about it is that all operations are done lazily, that is no computation is executed until we ask for it.

.. code-block:: python
    from squirrel.iterstream import IterableSource

    def add_1(x):
        print(f'add 1 to {x}')
        return x + 1

    def mult_10(x):
        print(f'multiply 10 with {x}')
        return x * 10

    it = IterableSource(range(3)).map(add_1).map(mult_10)
    next(iter(it))

Output::

    add 1 to 0
    multiply 10 with 1

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
                print(f'multiply 10 with {x}')
                yield x * 10

    it = IterableSource(range(3)).compose(Add_1).compose(Mult_10)
    next(iter(it))

Output::

    add 1 to 0
    multiply 10 with 1

Similar as before, the execution is done lazily. The only difference is that we wrapp the function inside a class inheriting
from `Composable`. One scenario why we might want to use `compose()` is when we need to instantiate an expensive resource
for the transformations such as a large network for preprocessing or a database connection.

An interesting fact about `compose()` is its order of execution..

Architecture
--------------------
To see how the IterStream module is structured, let's see the UML diagram. This not an exhaustive diagram and only
aims to show the most important concepts through the class relations.

.. mermaid::

    classDiagram

        Composable <|-- _Iterable
        Composable <|-- _AsyncMap
        Composable <|-- IterableSource

        class Composable {
            source: Iterable or Callable

            __iter__() Iterator
            map(callback) _Iterable
            async_map(callback, buffer, max_workers, executor) _Iterable
            to(callback, *args, **kwargs) _Iterable
            compose(constructor, *args, **kwargs) Composable
        }

       class _Iterable {
            source: Iterable
            callback: Callable
            buffer: int
            max_workers: int
            executor: Executor

            __iter__() Iterator
       }

       class _AsyncMap {
            source: Iterable
            callback: Callable

            __iter__() Iterator
       }

       class IterableSource {
            source: Iterable or Callable

            __iter__() Iterator
       }



Composable Class
---------------------
:py:class:`Composable` is a mix-in class that implements utility methods to chain operations on streams of data.
Each :py:class:`Composable` contains a source Iterable on which the methods operate.
The most prominent ones been :py:meth:`squirrel.iterstream.base.Composable.async_map` and :py:meth:`squirrel.iterstream.base.Composable.map` that
allow for transformations over the whole iterable. Other utility methods include

* :py:meth:`squirrel.iterstream.base.Composable.batched`:
* :py:meth:`squirrel.iterstream.base.Composable.shuffle`:
* :py:meth:`squirrel.iterstream.base.Composable.take`
* :py:meth:`squirrel.iterstream.base.Composable.filter`
* :py:meth:`squirrel.iterstream.base.Composable.join`
* :py:meth:`squirrel.iterstream.base.Composable.collect`
* :py:meth:`squirrel.iterstream.base.Composable.flatten`
* :py:meth:`squirrel.iterstream.base.Composable.to`
* :py:meth:`squirrel.iterstream.base.Composable.compose`

.. admonition:: to vs compose

    Both :py:meth:`squirrel.iterstream.base.Composable.to` and :py:meth:`squirrel.iterstream.base.Composable.compose`
    instantiate and return new :py:class:`Composable` objects from the source Iterable. In the latter, case any class inheriting
    from :py:class:`Composable` can be used. This can be for example an :py:class`TorchIterable` that
    allows for interfacing with the PyTorch DataLoader (`model training with PyTorch  <https://github.com/merantix-momentum/squirrel-datasets-core/blob/main/examples/03.Pytorch_Model_Training.ipynb/>`_).
    For the former case, :py:class:`_Iterable` object is constructed using the source and a callback function.
    This class is an Iterable that returns an Iterator where the callback function is applied over the source.

We have already came across the :py:class:`_Iterable` class when talking about :py:meth:`squirrel.iterstream.base.Composable.to`.
A very similar class is the :py:class:`__AsyncMap` class. :py:class:`__AsyncMap`

Chaining Iterables
----------------------
1. code example
2. highlight reversed order

Special Composables
---------------------
* FilePathGenerator
* TorchComposables



