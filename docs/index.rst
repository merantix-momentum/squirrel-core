.. squirrel documentation master file:

Welcome to Squirrel´s documentation!
====================================


.. toctree::
   :maxdepth: 1
   :caption: Contents:

   usage/driver
   usage/catalog
   usage/Iterstream
   usage/zarr
   usage/low_level_apis
   usage/code_of_conduct
   usage/contribute
   usage/glossary


Goal of the project
-----------------------------------------
Squirrel is a data catalog library that provides you with an efficient data infrastructure to serve data as a FAIR data product. See

1. :doc:`driver <usage/driver>` to learn about data loading functionalities for various formats.

2. :doc:`catalog <usage/catalog>` to learn more about how to use Squirrel catalog in your project (experimental)

3. :doc:`work with zarr <usage/zarr>` to learn how to use squirrel as your single entry point to zarr datasets.

4. :doc:`stream data with Iterstream <usage/Iterstream>` to learn about streaming capabilities of squirrel

5. Checkout whats possible by looking at some `examples <https://github.com/merantix/mxlabs-squirrel/examples>`_

6. :doc:`here <usage/contribute>` if you plan to contribute to this project.

**Disclaimer:** The catalog interface is still very WIP. Feel free to use Squirrel´s optimizations like the :doc:`ZarrDataset <usage/zarr>` in your projects directly.

**Disclaimer:** The  iterable dataloader, and squirrel storage are still very WIP.
