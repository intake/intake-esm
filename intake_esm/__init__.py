#!/usr/bin/env python
""" Top-level module for intake_esm. """
from pkg_resources import DistributionNotFound, get_distribution

from . import config
from .core import ESMMetadataStoreCatalog

try:
    __version__ = get_distribution(__name__).version
except DistributionNotFound:
    # package is not installed
    pass
