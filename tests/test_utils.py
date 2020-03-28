import pandas as pd
import pytest
import requests

from intake_esm.utils import _fetch_and_parse_json, _fetch_catalog, _get_dask_client


def test_fetch_and_parse_json_url():
    url = 'https://raw.githubusercontent.com/NCAR/esm-collection-spec/master/collection-spec/examples/sample-glade-cmip6-netcdf-collection.json'
    data, path = _fetch_and_parse_json(url)
    assert path == url
    assert requests.get(url).json() == data


def test_fetch_and_parse_json_local(sample_cmip6):
    data, path = _fetch_and_parse_json(sample_cmip6)
    assert isinstance(data, dict)


def test_fetch_catalog_local_error(sample_bad_input):
    data, path = _fetch_and_parse_json(sample_bad_input)
    with pytest.raises(FileNotFoundError):
        _fetch_catalog(data, path)


def test_catalog_url_construction_from_relative_url():
    url = 'https://raw.githubusercontent.com/NCAR/cesm-lens-aws/master/intake-catalogs/aws-cesm1-le.json'
    data, path = _fetch_and_parse_json(url)
    df = _fetch_catalog(data, path)
    assert isinstance(df, pd.DataFrame)


def test_get_dask_client():
    from unittest import mock
    from distributed import Client
    import sys

    with Client() as client:
        c, _is_client_local = _get_dask_client()
        assert c is client

    with mock.patch.dict(sys.modules, {'distributed.client': None}):
        with pytest.raises(Exception):
            _get_dask_client()

    c, _is_client_local = _get_dask_client()
    assert isinstance(c, Client)
    assert _is_client_local
