#!/usr/bin/env python
""" Top-level module for intake_esm. """
from ._version import get_versions
from .config import SETTINGS, SOURCES, get_options, set_options
from .core import ESMMetadataStoreCatalog

__version__ = get_versions()["version"]
del get_versions
