"""Sphinx configuration for autotel documentation."""

import os
import sys

# Add source to path
sys.path.insert(0, os.path.abspath("../src"))

# Import version from package
from autotel.__version__ import __version__

# -- Project information -----------------------------------------------------
project = "autotel"
copyright = "2025, Jag Reehal"
author = "Jag Reehal"
release = __version__
version = __version__

# -- General configuration ---------------------------------------------------
extensions = [
    "sphinx.ext.autodoc",  # Auto-generate docs from docstrings
    "sphinx.ext.napoleon",  # Support for NumPy and Google style docstrings
    "sphinx.ext.viewcode",  # Add links to highlighted source code
    "sphinx.ext.intersphinx",  # Link to other project's documentation
    "sphinx.ext.autosummary",  # Generate autodoc summaries
    "myst_parser",  # Support for Markdown files
]

# Add any paths that contain templates here
templates_path = ["_templates"]

# List of patterns to exclude
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

# -- Options for HTML output -------------------------------------------------
html_theme = "furo"  # Modern, clean theme
html_static_path = ["_static"]
html_title = f"autotel {version}"

# Theme options
html_theme_options = {
    "sidebar_hide_name": False,
    "navigation_with_keys": True,
}

# -- Extension configuration -------------------------------------------------

# Napoleon settings for Google/NumPy style docstrings
napoleon_google_docstring = True
napoleon_numpy_docstring = True
napoleon_include_init_with_doc = True

# Autodoc settings
autodoc_default_options = {
    "members": True,
    "member-order": "bysource",
    "special-members": "__init__",
    "undoc-members": True,
    "exclude-members": "__weakref__",
}

# Autosummary settings
autosummary_generate = True

# Intersphinx mapping
intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
    "opentelemetry": ("https://opentelemetry-python.readthedocs.io/en/latest/", None),
}

# MyST parser settings (for Markdown)
myst_enable_extensions = [
    "colon_fence",
    "deflist",
    "substitution",
]
