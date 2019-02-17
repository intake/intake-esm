#!/usr/bin/env python
from .core import ESMMetadataStoreCatalog
from .config import set_options, get_options, SETTINGS, SOURCES

"""Top-level module for intake_esm."""
from ._version import get_versions

__version__ = get_versions()["version"]
del get_versions
