#!/usr/bin/env python
""" The configuration script: set global settings.
"""

from __future__ import absolute_import, print_function

import os

import yaml

DATABASE_DIRECTORY = "database_directory"
CACHE_DIRECTORY = "cache_directory"

SETTINGS = {}
if "INTAKE_CESM_CONFIG" in os.environ:
    _config_dir = os.environ["INTAKE_CESM_CONFIG"]
else:
    _config_dir = os.path.join(os.path.expanduser("~"), ".intake-cesm")

SETTINGS = {
    DATABASE_DIRECTORY: os.path.join(_config_dir, "collections"),
    CACHE_DIRECTORY: os.path.join(_config_dir, "data-cache"),
}

for key in [DATABASE_DIRECTORY, CACHE_DIRECTORY]:
    os.makedirs(SETTINGS[key], exist_ok=True)


def _check_path_write_access(value):
    value = os.path.abspath(os.path.expanduser(value))
    if os.path.exists(value):
        if not os.access(value, os.W_OK):
            print(f"no write access to: {value}")
            return False
        return True

    try:
        os.makedirs(value)
        return True
    except (OSError, PermissionError) as err:
        print(f"could not make directory: {value}")
        raise err


def _full_path(value):
    return os.path.abspath(os.path.expanduser(value))


_VALIDATORS = {
    DATABASE_DIRECTORY: _check_path_write_access,
    CACHE_DIRECTORY: _check_path_write_access,
}


_SETTERS = {DATABASE_DIRECTORY: _full_path, CACHE_DIRECTORY: _full_path}


class set_options(object):
    """Set configurable settings."""

    def __init__(self, **kwargs):
        self.old = {}
        for key, val in kwargs.items():
            if key not in SETTINGS:
                raise ValueError(f"{key} is not in the set of valid settings:\n {set(SETTINGS)}")
            if key in _VALIDATORS and not _VALIDATORS[key](val):
                raise ValueError(f"{val} is not a valid value for {key}")
            self.old[key] = SETTINGS[key]
        self._apply_update(kwargs)

    def _apply_update(self, settings_dict):
        for key, val in settings_dict.items():
            if key in _SETTERS:
                settings_dict[key] = _SETTERS[key](val)
        SETTINGS.update(settings_dict)

    def __enter__(self):
        return

    def __exit__(self, type, value, traceback):
        self._apply_update(self.old)


_path_config_yml = os.path.join(_config_dir, "config.yml")
if os.path.exists(_path_config_yml):
    with open(_path_config_yml) as f:
        dot_file_settings = yaml.load(f)
    set_options(**dot_file_settings)
