"""Configuration file for the Sphinx documentation builder."""

import pathlib

project = "seddy"
copyright = "2020, SiteSee"
author = "Laurie O"

_version_path = pathlib.Path(__file__).parent.parent.parent / "VERSION"
release = _version_path.read_text().strip()  # full version
version = ".".join(release.split(".")[:2])  # short version

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.viewcode",
    "sphinx.ext.napoleon",
]
html_theme = "sphinx_rtd_theme"
