#!/usr/bin/env python3
# flake8: noqa
"""Top-level module for intake_esm."""

# Import intake first to avoid circular imports during discovery.
import intake


from intake_esm import tutorial
from intake_esm.core import esm_datastore
from intake_esm.derived import DerivedVariableRegistry, default_registry
from intake_esm.utils import set_options, show_versions

from intake_esm._version import __version__
