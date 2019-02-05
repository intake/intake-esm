#!/usr/bin/env python
from .config import set_options
from .manage_collections import CESMCollections
from .core import CesmSource, CesmMetadataStoreCatalog

"""Top-level module for intake_cesm."""
from ._version import get_versions

__version__ = get_versions()["version"]
del get_versions
