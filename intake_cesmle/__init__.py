#!/usr/bin/env python
"""Top-level module for intake_cesmle."""
from ._version import get_versions
from . import database
__version__ = get_versions()["version"]
del get_versions

__all__ = ["database"]