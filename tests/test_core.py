import ast

import intake
import pandas as pd
import pytest

import intake_esm
from intake_esm.source import ESMDataSource

from .utils import (
    catalog_dict_records,
    cdf_col_sample_cesmle,
    cdf_col_sample_cmip5,
    cdf_col_sample_cmip6,
    multi_variable_col,
    sample_df,
    sample_esmcol_data,
    zarr_col_aws_cesm,
    zarr_col_pangeo_cmip6,
)


@pytest.mark.parametrize(
    'obj, sep, read_csv_kwargs',
    [
        (catalog_dict_records, '.', None),
        (cdf_col_sample_cmip6, '/', None),
        (zarr_col_aws_cesm, '.', None),
        (zarr_col_pangeo_cmip6, '*', None),
        (cdf_col_sample_cesmle, '.', None),
        (multi_variable_col, '*', {'converters': {'variable': ast.literal_eval}}),
        ({'esmcat': sample_esmcol_data, 'df': sample_df}, '.', None),
    ],
)
def test_catalog_init(obj, sep, read_csv_kwargs):
    """Test that the catalog can be initialized."""
    cat = intake.open_esm_datastore(obj, sep=sep, read_csv_kwargs=read_csv_kwargs)
    assert isinstance(cat.esmcat, intake_esm._types.ESMCatalogModel)
    assert isinstance(cat.df, pd.DataFrame)
    assert len(cat) > 0


@pytest.mark.parametrize(
    'obj, sep, read_csv_kwargs',
    [
        (multi_variable_col, '.', {'converters': {'variable': ast.literal_eval}}),
        (cdf_col_sample_cesmle, '/', None),
        (cdf_col_sample_cmip5, '.', None),
        (cdf_col_sample_cmip6, '*', None),
        (catalog_dict_records, '.', None),
        ({'esmcat': sample_esmcol_data, 'df': sample_df}, '.', None),
    ],
)
def test_catalog_unique(obj, sep, read_csv_kwargs):
    cat = intake.open_esm_datastore(obj, sep=sep, read_csv_kwargs=read_csv_kwargs)
    uniques = cat.unique()
    nuniques = cat.nunique()
    assert isinstance(uniques, pd.Series)
    assert isinstance(nuniques, pd.Series)
    assert set(uniques.keys()) == set(cat.df.columns)


def test_catalog_contains():
    cat = intake.open_esm_datastore(cdf_col_sample_cesmle)
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
    cat = intake.open_esm_datastore(path)
    new_cat = cat.search(**query)
    assert len(new_cat) == expected_size


@pytest.mark.parametrize('key', ['ocn.20C.pop.h', 'ocn.CTRL.pop.h', 'ocn.RCP85.pop.h'])
def test_catalog_getitem(key):
    cat = intake.open_esm_datastore(cdf_col_sample_cesmle)
    entry = cat[key]
    assert isinstance(entry, ESMDataSource)


def test_catalog_getitem_error():
    cat = intake.open_esm_datastore(cdf_col_sample_cesmle)
    with pytest.raises(KeyError):
        cat['foo']


@pytest.mark.xfail(reason='Needs to be fixed')
def test_serialize_to_csv(tmp_path):
    col = intake.open_esm_datastore(cdf_col_sample_cmip6)
    local_store = tmp_path
    col_subset = col.search(
        source_id='MRI-ESM2-0',
    )
    name = 'CMIP6-MRI-ESM2-0'
    col_subset.serialize(name=name, directory=local_store, catalog_type='file')
    col = intake.open_esm_datastore(f'{local_store}/{name}.json')
    pd.testing.assert_frame_equal(col_subset.df, col.df)
    assert col.esmcat.id == name


def test_empty_queries():
    col = intake.open_esm_datastore(cdf_col_sample_cmip6)
    with pytest.warns(UserWarning, match=r'Empty query: {} returned zero results.'):
        _ = col.search()

    with pytest.warns(UserWarning, match=r'Query:'):
        _ = col.search(variable_id='DONT_EXIST')

    cat = col.search()
    with pytest.warns(
        UserWarning, match=r'There are no datasets to load! Returning an empty dictionary.'
    ):
        dsets = cat.to_dataset_dict()
        assert not dsets
