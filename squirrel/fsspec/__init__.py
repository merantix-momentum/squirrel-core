"""
In squirrel we customize the :py:mod:`fsspec` library to avoid common pitfalls with :py:mod:`fsspec`.

.. note::

    The two methods here are open to all end users to utilize whenever they feel the need to use fsspec directly in
    their projects.
    The squirrel.fsspec already circumvents all currently known issues with fsspec (And more reports are welcome to
    helps us improve its behavior.)
"""
