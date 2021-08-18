import ast

import intake
import pandas as pd
import pydantic
import pytest

from intake_esm._types import ESMCatalogModel
from intake_esm.data_source import ESMGroupedDataSource

from .utils import (
    catalog_dict_records,
    cdf_col_sample_cesmle,
    cdf_col_sample_cmip5,
    cdf_col_sample_cmip6,
    multi_variable_col,
)


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
def test_unique(path, sep, read_csv_kwargs):
    cat = intake.open_esm_datastore_v2(path, sep=sep, read_csv_kwargs=read_csv_kwargs)
    uniques = cat.unique()
    nuniques = cat.nunique()
    assert isinstance(uniques, pd.Series)
    assert isinstance(nuniques, pd.Series)
    assert set(uniques.keys()) == set(cat.df.columns)
    assert set(nuniques.keys()) == set(cat.df.columns)


@pytest.mark.parametrize('key', ['ocn.20C.pop.h', 'ocn.CTRL.pop.h', 'ocn.RCP85.pop.h'])
def test_getitem(key):
    cat = intake.open_esm_datastore_v2(cdf_col_sample_cesmle)
    entry = cat[key]
    assert isinstance(entry, ESMGroupedDataSource)


def test_getitem_error():
    cat = intake.open_esm_datastore_v2(cdf_col_sample_cesmle)
    with pytest.raises(KeyError):
        cat['foo']


def test_contains():
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
def test_search(path, query, expected_size):
    cat = intake.open_esm_datastore_v2(path)
    new_cat = cat.search(**query)
    assert len(new_cat) == expected_size


def test_query_validation_error():
    cat = intake.open_esm_datastore_v2(cdf_col_sample_cesmle)
    with pytest.raises(pydantic.ValidationError):
        cat.search(my_experiment='foo')
