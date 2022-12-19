"""
Configuration file for the Sphinx documentation builder.

This file only contains a selection of the most common options. For a full list see the documentation:
https://www.sphinx-doc.org/en/master/usage/configuration.html
"""

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
from __future__ import annotations

import datetime
import os
import sys
import typing as t

if t.TYPE_CHECKING:
    from autoapi.mappers.python.objects import PythonPythonMapper
    from sphinx.application import Sphinx


sys.path.insert(0, os.path.abspath(os.path.join(__file__, os.pardir, os.pardir)))

# -- Project information -----------------------------------------------------

project = "Squirrel"
copyright = f"{datetime.datetime.now().year}, Merantix Momentum"
author = "Merantix Momentum"


# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx.ext.doctest",
    "sphinx.ext.autosummary",
    "sphinx.ext.intersphinx",
    "sphinx.ext.napoleon",
    "sphinx.ext.autosectionlabel",
    "sphinx.ext.todo",
    "autoapi.extension",
    "sphinxcontrib.mermaid",
    "myst_nb",
]
nb_execution_mode = "off"

# Add any paths that contain templates here, relative to this directory.
# Add any paths that contain templates here, relative to this directory.cv
templates_path = []

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

# configure sphinx-versions (sphinxcontrib-versioning => scv)
# to build versioned documentation only for branch "main"
scv_whitelist_branches = (r"^main",)
scv_root_ref = "main"
# sort by semantic versioning format
scv_sort = ("semver",)
# display branches before tags
scv_priority = "branches"

todo_include_todos = False

# Add links to other docs
intersphinx_mapping = {
    "python": ("https://docs.python.org/3/", None),
    "sphinx": ("https://www.sphinx-doc.org/en/master/", None),
    "dask": ("https://docs.dask.org/en/stable/", None),
    "pandas": ("https://pandas.pydata.org/pandas-docs/stable/", None),
    "fsspec": ("https://filesystem-spec.readthedocs.io/en/latest/", None),
    "squirrel-datasets": ("https://squirrel-datasets-core.readthedocs.io/en/latest", None),
}

autosectionlabel_prefix_document = True

# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = "sphinx_rtd_theme"

# Material theme options (see theme.conf for more information)
# TODO the following is unsupported by RTD, configure them in other ways.
# html_theme_options = {
#     "logo_url": "https://merantixlabs.com",
#     "logo": "mxl-logo.png",
#     "github_repository": "squirrel-core",
#     "path_to_documentation_dir": "docs",
#     "github_sphinx_locale": "",
#     "github_branch": "main",
# }

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ["_static"]

# Add logo and favicon
html_logo = "_static/logo.png"
html_favicon = "_static/favicon.ico"

# Document Python Code
autoapi_type = "python"
autoapi_dirs = ["../squirrel"]
autoapi_python_class_content = "both"
autoapi_member_order = "groupwise"
autoapi_options = [
    "members",
    "undoc-members",
    # "private-members",
    "show-inheritance",
    "show-module-summary",
    "special-members",
    "imported-members",
]


def skip_util_classes(
    app: Sphinx, what: str, name: str, obj: PythonPythonMapper, skip: bool, options: t.List[str]
) -> bool:
    """Called for each object to decide whether it should be skipped."""
    if what == "attribute" and name.endswith(".logger"):
        skip = True
    if name.startswith("squirrel.integration_test"):
        skip = True
    if name.startswith("test_"):
        skip = True
    return skip


def setup(sphinx: Sphinx) -> None:
    """Set up sphinx by registering custom skip function."""
    sphinx.connect("autoapi-skip-member", skip_util_classes)
