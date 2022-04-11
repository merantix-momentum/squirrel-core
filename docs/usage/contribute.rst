Contribute
====================

To contribute to developing this package, check out its Github repository and push commits there.

How do we handle pip requirements?
-------------------------------------

We mostly follow `this workflow <https://kennethreitz.org/essays/2016/02/25/a-better-pip-workflow>`_

#. Add packages to ``requirements.in``. Only pin versions that need to be pinned to make the code runable.
#. Ask our devs to freeze your requirements into ``requiremenets.txt``. This is not allowed from external users for
   security reasons.
#. Commit ``requirements.in`` and ``requirements.txt`` in a PR. Once merged to master, Cloudbuild will build the
   image with the new dependencies.


Tests
-------------------------------------

You can run tests by executing ``pytest``. Prior make sure that you installed the testing extras e.g. via
``pip install -e .[dev]`` or ``pip install -e '.[dev, zarr]`` if zarr is needed.
