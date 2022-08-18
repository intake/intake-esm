#!/usr/bin/env python3
# flake8: noqa
""" Top-level module for intake_esm. """
# Import intake first to avoid circular imports during discovery.
import intake
from pkg_resources import DistributionNotFound, get_distribution

from . import tutorial
from .core import esm_datastore
from .derived import DerivedVariableRegistry, default_registry
from .utils import set_options, show_versions

try:
    __version__ = get_distribution(__name__).version
except DistributionNotFound:  # pragma: no cover
    __version__ = '0.0.0'  # pragma: no cover
