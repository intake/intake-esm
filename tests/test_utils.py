import os
from unittest import mock

import pandas as pd
import pytest
import requests

from intake_esm.utils import _fetch_and_parse_json, _fetch_catalog, _is_valid_url

here = os.path.abspath(os.path.dirname(__file__))
multi_variable_col = os.path.join(here, 'sample-collections/multi-variable-collection.json')


@pytest.mark.parametrize('url', ['http://www.example.com/file[/].html', 55555])
def test_invalid_url(url):
    assert not _is_valid_url(url)


def test_is_valid_url_not_found():
    url = 'https://raw.githubusercontent.com/NCAR/esm-collection-spec/master/collection-spec/examples/sample-glade-cmip6-netcdf-collection.json'
    with mock.patch('requests.get') as mock_request:
        mock_request.return_value.status_code = 404
        assert not _is_valid_url(url)


def test_fetch_and_parse_json_url():
    url = 'https://raw.githubusercontent.com/NCAR/esm-collection-spec/master/collection-spec/examples/sample-glade-cmip6-netcdf-collection.json'
    data, path = _fetch_and_parse_json(url)
    assert path == url
    assert requests.get(url).json() == data


def test_fetch_and_parse_json_error():
    url = 'https://raw.githubusercontent.com/NCAR/esm-collection-spec/master/README.md'
    with pytest.raises(Exception):
        _fetch_and_parse_json(url)


def test_fetch_and_parse_json_local(sample_cmip6):
    data, path = _fetch_and_parse_json(sample_cmip6)
    assert isinstance(data, dict)


def test_fetch_catalog_local_error(sample_bad_input):
    data, path = _fetch_and_parse_json(sample_bad_input)
    with pytest.raises(FileNotFoundError):
        _fetch_catalog(data, path)


def test_catalog_url_construction_from_relative_url():
    url = 'https://raw.githubusercontent.com/intake/intake-esm/master/tests/sample-collections/cesm1-lens-netcdf.json'
    catalog_file = 'https://raw.githubusercontent.com/intake/intake-esm/master/tests/sample-collections/cesm1-lens-netcdf.csv'
    data, path = _fetch_and_parse_json(url)
    df, cat_file = _fetch_catalog(data, path)
    assert isinstance(df, pd.DataFrame)
    assert catalog_file == cat_file


def test_catalog_url_construction_from_relative_url_error():
    url = 'https://raw.githubusercontent.com/intake/intake-esm/master/tests/sample-collections/cesm1-lens-netcdf.json'
    data, path = _fetch_and_parse_json(url)
    data['catalog_file'] = 'DONT_EXIST'
    with pytest.raises(FileNotFoundError):
        _fetch_catalog(data, path)


@pytest.mark.parametrize(
    'esmcol_path,csv_kwargs', [(multi_variable_col, {'converters': {'variable': pd.eval}})]
)
def test_catalog_csv_kwargs(esmcol_path, csv_kwargs):
    data, path = _fetch_and_parse_json(esmcol_path)
    df, _ = _fetch_catalog(data, esmcol_path, csv_kwargs=csv_kwargs)
    assert isinstance(df.iloc[0].variable, list)
