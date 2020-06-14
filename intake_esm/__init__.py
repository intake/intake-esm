#!/usr/bin/env python3
# flake8: noqa
""" Top-level module for intake_esm. """
# Import intake first to avoid circular imports during discovery.
import intake
from pkg_resources import DistributionNotFound, get_distribution

from .core import esm_datastore

try:
    __version__ = get_distribution(__name__).version
except DistributionNotFound:
    __version__ = '0.0.0'
