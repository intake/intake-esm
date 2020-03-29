#!/usr/bin/env python3
""" Top-level module for intake_esm. """
# Import intake first to avoid circular imports during discovery.
import intake  # noqa: F401
from pkg_resources import DistributionNotFound, get_distribution

from .core import esm_datastore  # noqa: F401

try:
    __version__ = get_distribution(__name__).version
except DistributionNotFound:  # noqa: F401
    __version__ = '0.0.0'
