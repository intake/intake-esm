#!/usr/bin/env python3
# flake8: noqa
"""Top-level module for intake_esm."""

# Import intake first to avoid circular imports during discovery.
import intake
import importlib


from intake_esm import tutorial
from intake_esm.core import esm_datastore
from intake_esm.derived import DerivedVariableRegistry, default_registry
from intake_esm.utils import set_options, show_versions
from intake_esm._imports import _to_opt_import_flag, _from_opt_import_flag
from intake_esm import _imports as _import_module

from intake_esm._version import __version__

import_flags = [_to_opt_import_flag(name) for name in _import_module._optional_imports]

__all__ = [
    'esm_datastore',
    'DerivedVariableRegistry',
    'default_registry',
    'set_options',
    'show_versions',
    'tutorial',
    '__version__',
] + import_flags


def __getattr__(attr: str) -> object:
    """
    Lazy load optional imports.
    """

    if attr in (gl := globals()):
        return gl[attr]

    try:
        return getattr(_import_module, attr)
    except AttributeError:
        raise AttributeError(
            f"Module '{__name__}' has no attribute '{attr}'. "
            f'Did you mean one of {", ".join(import_flags)}?'
        )
