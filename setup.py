#!/usr/bin/env python
# -*- coding: utf-8 -*-

import ast
import itertools
import os
import re
import sys

from setuptools import find_packages, setup

SOURCE_DIR = "squirrel"

# Read package information from other files so that just one version has to be maintained.
_version_re = re.compile(r"__version__\s+=\s+(.*)")
with open("%s/__init__.py" % SOURCE_DIR, "rb") as f:
    init_contents = f.read().decode("utf-8")

    def get_var(var_name: str) -> str:
        """Parsing of squirrel project infos defined in __init__.py"""
        pattern = re.compile(r"%s\s+=\s+(.*)" % var_name)
        match = pattern.search(init_contents).group(1)
        return str(ast.literal_eval(match))

    version = get_var("__version__")

# add tag to version if provided
if "--version_tag" in sys.argv:
    v_idx = sys.argv.index("--version_tag")
    version = version + sys.argv[v_idx + 1]
    sys.argv.remove("--version_tag")
    sys.argv.pop(v_idx)

if os.path.exists("README.md"):
    with open("README.md") as fh:
        readme = fh.read()
else:
    readme = ""
if os.path.exists("HISTORY.md"):
    with open("HISTORY.md") as fh:
        history = fh.read().replace(".. :changelog:", "")
else:
    history = ""

if os.path.exists("requirements.in"):
    with open("requirements.in") as fh:
        requirements = [r for r in fh.read().split("\n") if ";" not in r]
else:
    requirements = []

# generate extras based on requirements files
extras_require = dict()
for a_extra in ["dev", "gcp", "azure", "s3", "zarr", "parquet", "hdf5", "dask", "torch"]:
    req_file = f"requirements.{a_extra}.in"
    if os.path.exists(req_file):
        with open(req_file) as fh:
            extras_require[a_extra] = [r for r in fh.read().split("\n") if ";" not in r]
    else:
        extras_require[a_extra] = []
extras_require["all"] = list(itertools.chain.from_iterable(extras_require.values()))

PACKAGE_DIR = {
    SOURCE_DIR: SOURCE_DIR,
}

cmdclass = dict()
# try to import sphinx
try:
    from sphinx.setup_command import BuildDoc

    cmdclass["build_sphinx"] = BuildDoc
except ImportError:
    sys.stdout.write("WARNING: sphinx not available, not building docs")

# Setup package using PIP
if __name__ == "__main__":
    setup(
        name=f"{SOURCE_DIR}-core",
        version=version,
        python_requires=">=3.8.0",
        description="Squirrel is a Python library that enables ML teams to share, load, and transform data in a "
        "collaborative, flexible, and efficient way.",
        long_description=f"{readme}\n\n{history}",
        author="Merantix Labs GmbH",
        license="Apache 2.0",
        package_dir=PACKAGE_DIR,
        packages=find_packages(),
        include_package_data=True,
        install_requires=requirements,
        tests_require=extras_require["dev"],
        extras_require=extras_require,
        cmdclass=cmdclass,
        # register our custom filesystem to fsspec
        entry_points={
            "fsspec.specs": [
                "gs=squirrel.fsspec.custom_gcsfs.CustomGCSFileSystem",
            ],
        },
    )
