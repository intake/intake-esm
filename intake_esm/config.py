#!/usr/bin/env python
""" The configuration script: set global settings.
"""

from __future__ import absolute_import, print_function

import os

import yaml

INTAKE_ESM_DIR = os.path.join(os.path.expanduser("~"), ".intake_esm")
INTAKE_ESM_CONFIG_FILE = os.path.join(INTAKE_ESM_DIR, "config.yaml")
DATABASE_DIRECTORY = os.path.join(INTAKE_ESM_DIR, "database_directory")
DATA_CACHE_DIRECTORY = os.path.join(INTAKE_ESM_DIR, "data_cache")
SOURCES = {"cesm": "intake_esm.cesm.CESMSource", "cmip": "intake_esm.esm.CMIPSource"}

cesm_definition = {
    "collection_columns": [
        "resource",
        "resource_type",
        "direct_access",
        "experiment",
        "case",
        "component",
        "stream",
        "variable",
        "date_range",
        "ensemble",
        "files",
        "files_basename",
        "files_dirname",
        "ctrl_branch_year",
        "year_offset",
        "sequence_order",
        "has_ocean_bgc",
        "grid",
    ],
    "replacements": {"freq": {"monthly": "month_1", "daily": "day_1", "yearly": "year_1"}},
    "component_streams": {
        "ocn": [
            "pop.h.nday1",
            "pop.h.nyear1",
            "pop.h.ecosys.nday1",
            "pop.h.ecosys.nyear1",
            "pop.h",
            "pop.h.sigma",
        ],
        "atm": [
            "cam.h0",
            "cam.h1",
            "cam.h2",
            "cam.h3",
            "cam.h4",
            "cam.h5",
            "cam.h6",
            "cam.h7",
            "cam.h8",
        ],
        "lnd": [
            "clm2.h0",
            "clm2.h1",
            "clm2.h2",
            "clm2.h3",
            "clm2.h4",
            "clm2.h5",
            "clm2.h6",
            "clm2.h7",
            "clm2.h8",
        ],
        "rof": [
            "rtm.h0",
            "rtm.h1",
            "rtm.h2",
            "rtm.h3",
            "rtm.h4",
            "rtm.h5",
            "rtm.h6",
            "rtm.h7",
            "rtm.h8",
            "mosart.h0",
            "mosart.h1",
            "mosart.h2",
            "mosart.h3",
            "mosart.h4",
            "mosart.h5",
            "mosart.h6",
            "mosart.h7",
            "mosart.h8",
        ],
        "ice": ["cice.h2_06h", "cice.h1", "cice.h"],
        "glc": [
            "cism.h",
            "cism.h0",
            "cism.h1",
            "cism.h2",
            "cism.h3",
            "cism.h4",
            "cism.h5",
            "cism.h6",
            "cism.h7",
            "cism.h8",
        ],
    },
}

cmip5_definition = {}
cmip6_definition = {}


SETTINGS = {
    "database_directory": DATABASE_DIRECTORY,
    "data_cache_directory": DATA_CACHE_DIRECTORY,
    "collections": {"cesm": cesm_definition, "cmip5": cmip5_definition, "cmip6": cmip6_definition},
}


for key in ["database_directory", "data_cache_directory"]:
    os.makedirs(SETTINGS[key], exist_ok=True)


def _check_path_write_access(value):
    value = os.path.abspath(os.path.expanduser(value))
    try:
        os.makedirs(value, exist_ok=True)
        return True

    except Exception:
        return False


def _full_path(value):
    return os.path.abspath(os.path.expanduser(value))


_VALIDATORS = {
    "database_directory": _check_path_write_access,
    "data_cache_directory": _check_path_write_access,
}


_SETTERS = {"database_directory": _full_path, "data_cache_directory": _full_path}


def save_to_disk():
    with open(INTAKE_ESM_CONFIG_FILE, 'w') as outfile:
        yaml.dump(get_options(), outfile, default_flow_style=False)


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
        save_to_disk()

    def __enter__(self):
        return

    def __exit__(self, type, value, traceback):
        self._apply_update(self.old)


def get_options():
    return SETTINGS


if os.path.exists(INTAKE_ESM_CONFIG_FILE):
    with open(INTAKE_ESM_CONFIG_FILE) as f:
        dot_file_settings = yaml.load(f)
    if dot_file_settings:
        set_options(**dot_file_settings)

else:
    save_to_disk()
