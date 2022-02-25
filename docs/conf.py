# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
import datetime
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(__file__, os.pardir)))

# -- Project information -----------------------------------------------------

project = "squirrel"
copyright = f"{datetime.datetime.now().year}, Merantix Labs GmbH"
author = "Merantix Labs GmbH"

# The full version, including alpha/beta/rc tags
release = "0.0.1"


# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = ["sphinx.ext.duration",
    "sphinx.ext.doctest",
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.intersphinx",
    "sphinx.ext.napoleon",
    "sphinx.ext.autosectionlabel",
]

exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

# configure sphinx-versions (sphinxcontrib-versioning => scv)
# to build versioned documentation only for branch "main"
scv_whitelist_branches = (r"^main",)
scv_root_ref = "main"
scv_sort = ("semver",)
# display branches before tags
scv_priority = "branches"

intersphinx_mapping = {
    "python": ("https://docs.python.org/3/", None),
    "sphinx": ("https://www.sphinx-doc.org/en/master/", None),
}
intersphinx_disabled_domains = ["std"]

templates_path = ["_templates"]

# -- Options for HTML output

html_theme = "sphinx_rtd_theme"
epub_show_urls = "footnote"
autoclass_content = "both"
todo_include_todos = False

# Add any paths that contain custom static files (such as style sheets) here
# html_static_path = ["_static"]
