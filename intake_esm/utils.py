# -*- coding: utf-8 -*-
import json
import logging
from pathlib import Path
from urllib.parse import urlparse

import pandas as pd
import requests

logger = logging.getLogger('intake-esm')
handle = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s ' '- %(message)s')
handle.setFormatter(formatter)
logger.addHandler(handle)


def _is_valid_url(url):
    """ Check if path is URL or not

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
        return result.scheme and result.netloc and result.path
    except Exception:
        return False


def _fetch_and_parse_json(input_path):
    """ Fetch and parse ESMCol file.

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
            logger.info(f'Loading ESMCol from URL: {input_path}')
            resp = requests.get(input_path)
            data = resp.json()
        else:
            input_path = Path(input_path).absolute().as_posix()
            with open(input_path) as f:
                logger.info(f'Loading ESMCol from filesystem: {input_path}')
                data = json.load(f)

    except Exception as e:
        raise e

    return data, input_path


def _fetch_catalog(collection_data, esmcol_path):
    """Get the catalog file content, and load it into a pandas dataframe"""

    if 'catalog_file' in collection_data:

        if _is_valid_url(esmcol_path):
            catalog_path = collection_data['catalog_file']
            if not _is_valid_url(catalog_path):
                # Use pathlib. It's not explicitly for URLs, but it happens to work on them
                catalog = (Path(esmcol_path).parent / collection_data['catalog_file']).as_posix()
                if not _is_valid_url(catalog):
                    raise FileNotFoundError(f'Unable to find: {catalog_path}')
                else:
                    return pd.read_csv(catalog)
            return pd.read_csv(catalog_path)

        else:
            catalog_path = Path(collection_data['catalog_file'])
            # If the catalog_path does not exist,
            # try constructing a path using the relative path
            if not catalog_path.exists():
                esmcol_path = Path(esmcol_path).absolute()
                catalog = esmcol_path.parent / collection_data['catalog_file']
                if not catalog.exists():
                    raise FileNotFoundError(f'Unable to find: {catalog_path}')
                else:
                    return pd.read_csv(catalog)

            return pd.read_csv(catalog_path)

    else:
        return pd.DataFrame(collection_data['catalog_dict'])


def _get_dask_client():
    # Detect local default cluster already running
    # and use it for dataset group loading.
    client = None
    try:
        from distributed.client import _get_global_client

        client = _get_global_client()
        return client
    except ImportError:
        return client
