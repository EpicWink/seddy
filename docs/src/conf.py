"""Configuration file for the Sphinx documentation builder."""

from sphinx.ext import apidoc

try:
    from importlib.metadata import version as importlib_metadata_version
except ImportError:
    # noinspection PyUnresolvedReferences
    from importlib_metadata import version as importlib_metadata_version

apidoc.main(["-eTf", "-t", "../templates", "-o", ".", "../../src"])  # gen API docs

project = "seddy"
copyright = "2020, Laurie O"
author = "Laurie O"

release = importlib_metadata_version("seddy")  # full version
version = ".".join(release.split(".")[:2])  # short version

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.viewcode",
    "sphinx.ext.napoleon",
    "autodocsumm",
]
html_theme = "furo"
master_doc = "index"  # support read-the-docs
