import ast
import re

import intake
import pandas as pd
import pytest

from intake_esm._types import ESMCatalogModel, QueryModel
from intake_esm.data_source import ESMGroupedDataSource
from intake_esm.main import _is_pattern, _search, _search_apply_require_all_on

from .utils import (
    catalog_dict_records,
    cdf_col_sample_cesmle,
    cdf_col_sample_cmip5,
    cdf_col_sample_cmip6,
    multi_variable_col,
)


@pytest.mark.parametrize(
    'value, expected',
    [
        (2, False),
        ('foo', False),
        ('foo\\**bar', True),
        ('foo\\?*bar', True),
        ('foo\\?\\*bar', False),
        ('foo\\*bar', False),
        (r'foo\*bar*', True),
        ('^foo', True),
        ('^foo.*bar$', True),
        (re.compile('hist.*', flags=re.IGNORECASE), True),
    ],
)
def test_is_pattern(value, expected):
    assert _is_pattern(value) == expected


params = [
    ({}, None, []),
    (
        {'C': ['control', 'hist']},
        ['B', 'D'],
        [
            {'A': 'NCAR', 'B': 'CESM', 'C': 'hist', 'D': 'O2'},
            {'A': 'NCAR', 'B': 'CESM', 'C': 'control', 'D': 'O2'},
            {'A': 'IPSL', 'B': 'FOO', 'C': 'control', 'D': 'O2'},
            {'A': 'IPSL', 'B': 'FOO', 'C': 'hist', 'D': 'O2'},
        ],
    ),
    ({'C': ['control', 'hist'], 'D': ['NO2']}, 'B', []),
    (
        {'C': ['control', 'hist'], 'D': ['O2']},
        'B',
        [
            {'A': 'NCAR', 'B': 'CESM', 'C': 'hist', 'D': 'O2'},
            {'A': 'NCAR', 'B': 'CESM', 'C': 'control', 'D': 'O2'},
            {'A': 'IPSL', 'B': 'FOO', 'C': 'control', 'D': 'O2'},
            {'A': 'IPSL', 'B': 'FOO', 'C': 'hist', 'D': 'O2'},
        ],
    ),
    (
        {'C': ['hist'], 'D': ['NO2', 'O2']},
        'B',
        [
            {'A': 'IPSL', 'B': 'FOO', 'C': 'hist', 'D': 'O2'},
            {'A': 'IPSL', 'B': 'FOO', 'C': 'hist', 'D': 'NO2'},
        ],
    ),
    (
        {'C': 'hist', 'D': ['NO2', 'O2']},
        'B',
        [
            {'A': 'IPSL', 'B': 'FOO', 'C': 'hist', 'D': 'O2'},
            {'A': 'IPSL', 'B': 'FOO', 'C': 'hist', 'D': 'NO2'},
        ],
    ),
    (
        {'C': 'hist', 'D': ['NO2', 'O2'], 'B': 'FOO'},
        ['B'],
        [
            {'A': 'IPSL', 'B': 'FOO', 'C': 'hist', 'D': 'O2'},
            {'A': 'IPSL', 'B': 'FOO', 'C': 'hist', 'D': 'NO2'},
        ],
    ),
    (
        {'C': ['control']},
        None,
        [
            {'A': 'IPSL', 'B': 'FOO', 'C': 'control', 'D': 'O2'},
            {'A': 'CSIRO', 'B': 'BAR', 'C': 'control', 'D': 'O2'},
            {'A': 'NCAR', 'B': 'CESM', 'C': 'control', 'D': 'O2'},
        ],
    ),
    (
        {'D': [re.compile(r'^O2$'), 'NO2'], 'B': ['CESM', 'BAR']},
        None,
        [
            {'A': 'NCAR', 'B': 'CESM', 'C': 'hist', 'D': 'O2'},
            {'A': 'CSIRO', 'B': 'BAR', 'C': 'control', 'D': 'O2'},
            {'A': 'NCAR', 'B': 'CESM', 'C': 'control', 'D': 'O2'},
        ],
    ),
    (
        {'C': ['^co.*ol$']},
        None,
        [
            {'A': 'IPSL', 'B': 'FOO', 'C': 'control', 'D': 'O2'},
            {'A': 'CSIRO', 'B': 'BAR', 'C': 'control', 'D': 'O2'},
            {'A': 'NCAR', 'B': 'CESM', 'C': 'control', 'D': 'O2'},
        ],
    ),
    (
        {'C': ['hist'], 'D': ['TA']},
        None,
        [{'A': 'NCAR', 'B': 'WACM', 'C': 'hist', 'D': 'TA'}],
    ),
    (
        {
            'C': [re.compile('hist.*', flags=re.IGNORECASE)],
            'D': [re.compile('TA.*', flags=re.IGNORECASE)],
        },
        None,
        [
            {'A': 'NCAR', 'B': 'WACM', 'C': 'hist', 'D': 'TA'},
            {'A': 'NASA', 'B': 'foo', 'C': 'HiSt', 'D': 'tAs'},
        ],
    ),
]


@pytest.mark.parametrize('query, require_all_on, expected', params)
def test_search(query, require_all_on, expected):
    df = pd.DataFrame(
        {
            'A': ['NCAR', 'IPSL', 'IPSL', 'CSIRO', 'IPSL', 'NCAR', 'NOAA', 'NCAR', 'NASA'],
            'B': ['CESM', 'FOO', 'FOO', 'BAR', 'FOO', 'CESM', 'GCM', 'WACM', 'foo'],
            'C': ['hist', 'control', 'hist', 'control', 'hist', 'control', 'hist', 'hist', 'HiSt'],
            'D': ['O2', 'O2', 'O2', 'O2', 'NO2', 'O2', 'O2', 'TA', 'tAs'],
        }
    )
    query_model = QueryModel(
        query=query, columns=df.columns.tolist(), require_all_on=require_all_on
    )
    results = _search(query_model, df, set())
    assert isinstance(results, pd.DataFrame)
    if require_all_on:
        results = _search_apply_require_all_on(results, query_model)
    assert results.to_dict(orient='records') == expected


@pytest.mark.parametrize(
    'path, sep, read_csv_kwargs',
    [
        (multi_variable_col, '.', {'converters': {'variable': ast.literal_eval}}),
        (cdf_col_sample_cesmle, '/', None),
        (cdf_col_sample_cmip5, '.', None),
        (cdf_col_sample_cmip6, '*', None),
        (catalog_dict_records, '.', None),
    ],
)
def test_catalog_init(path, sep, read_csv_kwargs):
    cat = intake.open_esm_datastore_v2(path, sep=sep, read_csv_kwargs=read_csv_kwargs)
    assert isinstance(cat.df, pd.DataFrame)
    assert isinstance(cat.esmcat, ESMCatalogModel)
    if path == multi_variable_col:
        assert cat._columns_with_iterables == {'variable'}
        assert cat._multiple_variable_assets
    else:
        assert not cat._columns_with_iterables
        assert not cat._multiple_variable_assets

    assert len(cat) > 0


@pytest.mark.parametrize(
    'path, sep, read_csv_kwargs',
    [
        (multi_variable_col, '.', {'converters': {'variable': ast.literal_eval}}),
        (cdf_col_sample_cesmle, '/', None),
        (cdf_col_sample_cmip5, '.', None),
        (cdf_col_sample_cmip6, '*', None),
        (catalog_dict_records, '.', None),
    ],
)
def test_catalog_unique(path, sep, read_csv_kwargs):
    cat = intake.open_esm_datastore_v2(path, sep=sep, read_csv_kwargs=read_csv_kwargs)
    uniques = cat.unique()
    nuniques = cat.nunique()
    assert isinstance(uniques, pd.Series)
    assert isinstance(nuniques, pd.Series)
    assert set(uniques.keys()) == set(cat.df.columns)
    assert set(nuniques.keys()) == set(cat.df.columns)


@pytest.mark.parametrize('key', ['ocn.20C.pop.h', 'ocn.CTRL.pop.h', 'ocn.RCP85.pop.h'])
def test_catalog_getitem(key):
    cat = intake.open_esm_datastore_v2(cdf_col_sample_cesmle)
    entry = cat[key]
    assert isinstance(entry, ESMGroupedDataSource)


def test_catalog_getitem_error():
    cat = intake.open_esm_datastore_v2(cdf_col_sample_cesmle)
    with pytest.raises(KeyError):
        cat['foo']


def test_catalog_contains():
    cat = intake.open_esm_datastore_v2(cdf_col_sample_cesmle)
    assert 'ocn.20C.pop.h' in cat
    assert 'ocn.CTRL.pop.h' in cat
    assert 'ocn.RCP85.pop.h' in cat
    assert 'foo' not in cat


@pytest.mark.parametrize(
    'path, query, expected_size',
    [
        (cdf_col_sample_cesmle, {'experiment': 'CTRL'}, 1),
        (cdf_col_sample_cesmle, {'experiment': ['CTRL', '20C']}, 2),
        (cdf_col_sample_cesmle, {}, 0),
        (cdf_col_sample_cesmle, {'variable': 'SHF', 'time_range': ['200601-210012']}, 1),
    ],
)
def test_catalog_search(path, query, expected_size):
    cat = intake.open_esm_datastore_v2(path)
    new_cat = cat.search(**query)
    assert len(new_cat) == expected_size
