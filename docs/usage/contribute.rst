Contribute
====================

To contribute to developing this package, check out its Github repository and push commits there.

How do we handle pip requirements?
-------------------------------------

This section is intended for internal devs only. For safety reasons, we do not allow external contributors to add depedencies directly into our repo. If you wish to do, please contact our devs.

For internal devs:

#. Add packages to ``requirements.in``. Only pin versions that need to be pinned to make the code runable.
#. Run ``mx req freeze --extras_file=./requirements.azure.in --extras_file=./requirements.parquet.in --extras_file=./requirements.s3.in --extras_file=./requirements.zarr.in --extras_file=./requirements.dask.in --extras_file=./requirements.dev.in --extras_file=./requirements.gcp.in --extras_file=./requirements.torch.in``. This generates concrete version numbers and derived dependencies based on ``requirements.in`` and saves them under ``requirements.txt``. Only the later is used to install packages in the docker image of the project.
#. Commit ``requirements.in`` and ``requirements.txt`` and open a PR. Once merged to master, Cloudbuild will build the image with the new dependencies.


Tests
-------------------------------------

You can run tests by executing ``pytest``. Prior make sure that you installed the testing extras e.g. via ``pip install -e .[dev]``.
