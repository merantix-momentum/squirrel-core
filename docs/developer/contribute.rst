Contribute
====================

To contribute to the development of this package, check out its Github repository and push commits there.

How do we handle dependencies?
----------------------------------

We use poetry for resolving and installing dependencies.
For an overview of poetry basic commands, visit the `official documentation: <https://python-poetry.org/docs>`_

#. `Install poetry <https://python-poetry.org/docs/#installation>`_
#. Install the dependencies: ``poetry install --all-extras``. Poetry creates a virtual environment for you.
#. You can activate the venv using ``poetry shell`` or temporarily ``poetry run [command]``.
#. When adding new dependencies, use ``poetry add [my-package]`` or
   add them manually to ``pyproject.toml`` and update the lockfile ``poetry lock --no-update``.
#. ``requirements.txt`` will be updated via a pre-commit hook.
#. Commit ``poetry.lock`` and ``requirements.txt`` in a PR.
   Once merged to main, Cloudbuild will build the image with the new dependencies.


Tests
-----

You can run tests by executing ``poetry run pytest``.

Build the documentation locally
-------------------------------

Running ``poetry run sphinx-build ./docs ./docs/build`` from the root directory will generate the documentation.
Currently, this only works on python3.9.
You can use poetry with python3.9 by running ``poetry env use 3.9`` before ``poetry install --all-extras``.


Python Code Style Guide
-----------------------

We use PEP8 with some modifications.
We use `pre-commit <https://pre-commit.com>`_ to automatically check most of these points.
Visit `their website <https://pre-commit.com/#install>`_ to find out how to setup pre-commit for this repository and
check your contributions before opening a PR. You can run ``pre-commit run --all-files``. See the file
``.pre-commit-config.yaml`` in the root directory of the repo for more details.
