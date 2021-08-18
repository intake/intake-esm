import ast

import intake
import pandas as pd
import pytest

from intake_esm._types import ESMCatalogModel

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
