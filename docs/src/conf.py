"""Configuration file for the Sphinx documentation builder."""

import pkg_resources

from sphinx.ext import apidoc

apidoc.main(["-eTf", "-t", "../templates", "-o", ".", "../../src"])  # gen API docs

project = "seddy"
copyright = "2020, SiteSee"
author = "Laurie O"

release = pkg_resources.get_distribution("seddy").version  # full version
version = ".".join(release.split(".")[:2])  # short version

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.viewcode",
    "sphinx.ext.napoleon",
]
html_theme = "sphinx_rtd_theme"
master_doc = "index"  # support read-the-docs
