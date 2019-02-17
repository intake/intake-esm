#!/usr/bin/env python
""" Top-level module for intake_esm. """
from .core import ESMMetadataStoreCatalog
from .config import set_options, get_options, SETTINGS, SOURCES

from ._version import get_versions

__version__ = get_versions()["version"]
del get_versions
