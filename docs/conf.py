# Configuration file for the Sphinx documentation builder.
# https://www.sphinx-doc.org/en/master/usage/configuration.html

import os
import sys

# -- Path setup ----------------------------------------------------------------
sys.path.insert(0, os.path.abspath("../src"))

# -- Project information -------------------------------------------------------
project = "anyiostream"
copyright = "2025, randyt"
author = "randyt"
release = "0.1.0"

# -- General configuration -----------------------------------------------------
extensions = [
	"myst_parser",  # Markdown support
	"sphinx.ext.autodoc",  # Auto-generate API docs from docstrings
	"sphinx.ext.napoleon",  # Google/NumPy-style docstrings
	"sphinx.ext.viewcode",  # Add [source] links to API docs
	"sphinx.ext.intersphinx",  # Cross-reference external projects
	"sphinx_copybutton",  # Copy button on code blocks
	"sphinx_design",  # Tabs, cards, grids (replaces pymdownx.tabbed)
]

# MyST-Parser configuration
myst_enable_extensions = [
	"colon_fence",  # ::: fences for directives
	"deflist",  # Definition lists
	"fieldlist",  # Field lists
	"tasklist",  # Task lists (- [x])
]
myst_heading_anchors = 3  # Auto-generate anchors for h1-h3

# Source file suffixes
source_suffix = {
	".rst": "restructuredtext",
	".md": "markdown",
}

# The master toctree document
master_doc = "index"

# Patterns to exclude
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

# -- Options for HTML output ---------------------------------------------------
html_theme = "sphinx_rtd_theme"
html_theme_options = {
	"navigation_depth": 3,
	"collapse_navigation": False,
	"sticky_navigation": True,
	"includehidden": True,
	"titles_only": False,
}

html_static_path = ["_static"]

# -- Autodoc configuration ----------------------------------------------------
autodoc_member_order = "bysource"
autodoc_typehints = "description"
autodoc_class_signature = "separated"

# -- Intersphinx configuration -------------------------------------------------
intersphinx_mapping = {
	"python": ("https://docs.python.org/3", None),
	"anyio": ("https://anyio.readthedocs.io/en/stable/", None),
}

# -- Napoleon configuration ---------------------------------------------------
napoleon_google_docstring = True
napoleon_numpy_docstring = True
napoleon_include_init_with_doc = True
napoleon_use_param = True
napoleon_use_rtype = True
