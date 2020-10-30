# -*- coding: utf-8 -*-
""" Helper functions for fetching and loading catalog"""
import importlib
import json
import sys
from pathlib import Path
from urllib.parse import ParseResult, urlparse, urlunparse

import pandas as pd
import requests


def _is_valid_url(url):
    """Check if path is URL or not
    Parameters
    ----------
    url : str
        path to check
    Returns
    -------
    bool
    """
    try:
        result = urlparse(url)
        return (
            result.scheme
            and result.netloc
            and result.path
            and (requests.get(url).status_code == 200)
        )
    except Exception:
        return False


def _fetch_and_parse_json(input_path):
    """Fetch and parse ESMCol file.
    Parameters
    ----------
    input_path : str
            ESMCol file to get and read
    Returns
    -------
    data : dict
    input_path : str
    """

    data = None

    try:
        if _is_valid_url(input_path):
            resp = requests.get(input_path)
            data = resp.json()
        else:
            input_path = Path(input_path).absolute().as_posix()
            with open(input_path) as filein:
                data = json.load(filein)

    except Exception as exc:
        raise exc

    return data, input_path


def _fetch_catalog(collection_data, esmcol_path, csv_kwargs=None):
    """Get the catalog file content, and load it into a pandas dataframe"""

    if csv_kwargs is None:
        csv_kwargs = {}
    catalog_path = None
    if 'catalog_file' in collection_data:
        if _is_valid_url(esmcol_path):
            catalog_path = collection_data['catalog_file']
            if not _is_valid_url(catalog_path):
                split_url = urlparse(esmcol_path)
                path = (Path(split_url.path).parent / collection_data['catalog_file']).as_posix()
                components = ParseResult(
                    scheme=split_url.scheme,
                    netloc=split_url.netloc,
                    path=path,
                    params=split_url.params,
                    query=split_url.query,
                    fragment=split_url.fragment,
                )
                catalog_path = urlunparse(components)
                if not _is_valid_url(catalog_path):
                    raise FileNotFoundError(f'Unable to find: {catalog_path}')
        else:
            catalog_path = Path(collection_data['catalog_file'])
            # If the catalog_path does not exist,
            # try constructing a path using the relative path
            if not catalog_path.exists():
                esmcol_path = Path(esmcol_path).absolute()
                catalog_path = esmcol_path.parent / collection_data['catalog_file']
                if not catalog_path.exists():
                    raise FileNotFoundError(f'Unable to find: {catalog_path}')
        return pd.read_csv(catalog_path, **csv_kwargs), catalog_path
    return pd.DataFrame(collection_data['catalog_dict']), None


def show_versions(file=sys.stdout):  # pragma: no cover
    """print the versions of intake-esm and its dependencies.
       Adapted from xarray/util/print_versions.py

    Parameters
    ----------
    file : file-like, optional
        print to the given file-like object. Defaults to sys.stdout.
    """

    deps = [
        ('xarray', lambda mod: mod.__version__),
        ('pandas', lambda mod: mod.__version__),
        ('intake', lambda mod: mod.__version__),
        ('intake_esm', lambda mod: mod.__version__),
        ('fsspec', lambda mod: mod.__version__),
        ('s3fs', lambda mod: mod.__version__),
        ('gcsfs', lambda mod: mod.__version__),
        ('fastprogress', lambda mod: mod.__version__),
        ('dask', lambda mod: mod.__version__),
        ('zarr', lambda mod: mod.__version__),
        ('cftime', lambda mod: mod.__version__),
        ('netCDF4', lambda mod: mod.__version__),
        ('requests', lambda mod: mod.__version__),
    ]

    deps_blob = []
    for (modname, ver_f) in deps:
        try:
            if modname in sys.modules:
                mod = sys.modules[modname]
            else:
                mod = importlib.import_module(modname)
        except Exception:
            deps_blob.append((modname, None))
        else:
            try:
                ver = ver_f(mod)
                deps_blob.append((modname, ver))
            except Exception:
                deps_blob.append((modname, 'installed'))

    print('\nINSTALLED VERSIONS', file=file)
    print('------------------', file=file)

    print('', file=file)
    for k, stat in sorted(deps_blob):
        print(f'{k}: {stat}', file=file)
