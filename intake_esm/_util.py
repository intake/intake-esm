# -*- coding: utf-8 -*-
import json
import logging
from urllib.parse import urlparse

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


def _fetch_and_parse_file(input_path):
    """ Fetch and parse ESMCol file.

    Parameters
    ----------
    input_path : str
            ESMCol file to get and read

    Returns
    -------
    data : dict
    """

    data = None

    try:
        if _is_valid_url(input_path):
            logger.info(f'Loading ESMCol from URL: {input_path}')
            resp = requests.get(input_path)
            data = resp.json()
        else:
            with open(input_path) as f:
                logger.info(f'Loading ESMCol from filesystem: {input_path}')
                data = json.load(f)

    except Exception as e:
        raise e

    return data


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
