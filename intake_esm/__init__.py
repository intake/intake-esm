#!/usr/bin/env python
""" Top-level module for intake_esm. """
from . import config
from ._version import get_versions
from .core import ESMMetadataStoreCatalog

__version__ = get_versions()['version']
del get_versions
