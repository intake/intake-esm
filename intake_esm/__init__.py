#!/usr/bin/env python
""" Top-level module for intake_esm. """
# Import intake first to avoid circular imports during discovery.
import intake  # noqa: F401
from pkg_resources import DistributionNotFound, get_distribution

from . import config  # noqa: F401
from .core import ESMMetadataStoreCollection  # noqa: F401

try:
    __version__ = get_distribution(__name__).version
except DistributionNotFound:
    # package is not installed
    pass
