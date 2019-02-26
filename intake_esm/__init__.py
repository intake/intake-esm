#!/usr/bin/env python
""" Top-level module for intake_esm. """
from .core import ESMMetadataStoreCatalog
from ._version import get_versions
from .config import set_options, get_options, SETTINGS, SOURCES

__version__ = get_versions()["version"]
del get_versions
