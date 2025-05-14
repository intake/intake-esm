import importlib

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
