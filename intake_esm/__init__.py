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

from intake_esm._version import __version__

_optional_imports: dict[str, bool | None] = {'esmvalcore': None}


def _to_opt_import_flag(name: str) -> str:
    """Dynamically create import flags for optional imports."""
    return f'_{name.upper()}_AVAILABLE'


def _from_opt_import_flag(name: str) -> str:
    """Dynamically retrive the optional import name from its flag."""
    if name.startswith('_') and name.endswith('_AVAILABLE'):
        return name[1:-10].lower()
    raise ValueError(
        f"Invalid optional import flag '{name}'. Expected format: '_<import_name>_AVAILABLE'."
    )


__all__ = [
    'esm_datastore',
    'DerivedVariableRegistry',
    'default_registry',
    'set_options',
    'show_versions',
    'tutorial',
] + [_to_opt_import_flag(name) for name in _optional_imports]


def __getattr__(attr: str) -> object:
    """
    Lazy load optional imports.
    """

    if attr in (gl := globals()):
        return gl[attr]

    import_flags = [_to_opt_import_flag(name) for name in _optional_imports]

    if attr in import_flags:
        import_name = _from_opt_import_flag(attr)
        if _optional_imports.get(import_name, None) is None:
            _optional_imports[import_name] = bool(importlib.util.find_spec(import_name))
            return _optional_imports[import_name]
        else:
            return _optional_imports[import_name]

    raise AttributeError(
        f"Module '{__name__}' has no attribute '{attr}'. "
        f'Did you mean one of {", ".join(import_flags)}?'
    )
