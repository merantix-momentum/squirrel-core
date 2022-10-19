Contribute
====================

To contribute to the development of this package, check out its Github repository and push commits there.

How do we handle pip requirements?
----------------------------------

We mostly follow `this workflow <https://kennethreitz.org/essays/2016/02/25/a-better-pip-workflow>`_:

#. Add packages to ``requirements.in``. Only pin versions that need to be pinned to make the code runnable.
#. Run the pip-compile command shown at the top of the ``requirements.txt`` to freeze requirements.
#. Commit ``requirements.in`` and ``requirements.txt`` in a PR. Once merged to main, Cloudbuild will build the
   image with the new dependencies.


Tests
-----

You can run tests by executing ``pytest``. Prior make sure that you installed the testing extras via
``pip install -e '.[dev,dask,gcp,torch,zarr]'``.

Build the documentation locally
-------------------------------

To build the documentation locally, make sure you install dev requirements by running
``pip install -r requirements.dev.in``. Running ``sphinx-build ./docs ./docs/build`` from the root
directory will generate the documentation.
