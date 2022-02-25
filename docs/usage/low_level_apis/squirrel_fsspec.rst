Squirrel fsspec usage
=====================

In squirrel we customize :py:class:`fsspec` libraries to avoid common pitfalls with :py:class:`fsspec`.

.. note::
    Though this section is under Low Level APis in squirrel, but the two methods here are open to all end users to utilize whenever they feel the need to use fsspec directly in their projects. The squirrel fsspec has already circumvent all currently known issues with fsspec. (And welcome to more reports to helps us improve its behavior.)

.. automodule:: squirrel.fsspec.fs
    :members:

.. autoclass:: squirrel.fsspec.custom_gcsfs.CustomGCSFileSystem
