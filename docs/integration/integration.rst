Integration
===========
In this section, we show how Squirrel integrates with other libraries



Spark
---------------------

Numba
--------------------
Squirrel uses :code:`numba` to jit-compile an iterator to speed up computation in the main process.

.. code-block:: python

    it = IterableSource([1, 2, 3]).numba_map(lambda x: x + 1)
    for item in it:
        print(item)


Here, the iterator itself is passed to the :code:`numba` decorator :code:`@numba.jit`. Then the speed-up will be entirely provided by :code:`numba`. Note that squirrel does not compile the user defined function. You may achieve a comparable speed-up by compiling your function and passing it to `map()`:

.. code-block:: python

    from numba import jit

    @jit(nopython=True)
    def runtime_transformation(x):
        return x

    it = IterableSource([1, 2, 3]).map(runtime_transformation)
    for item in it:
        print(item)

Compared to the other three options, :code:`numba` is more performant in some cases but not in others, and highly sensitive to the
actual data type and computation at hand. Therefore, we recommend you read the official `numba documentation`_
from :code:`numba`, and perform a benchmarking, before choosing this option in production.

.. note::

    Since numba only supports `limited types of python objects`_, and naturally does not include
    squirrel defined objects, we have to force object mode in numba, that means the decorator we have chosen in squirrel
    is of the following format: :code:`@numba.jit(forceobj=True)`.

.. _numba documentation: https://numba.pydata.org/numba-doc/latest/index.html
.. _limited types of python objects: https://numba.pydata.org/numba-doc/dev/reference/pysupported.html
