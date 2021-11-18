import pandas as pd
import pydantic
import pytest

from intake_esm.cat import Assets, ESMCatalogModel, QueryModel

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
    'column_name, format, format_column_name', [('test', 'zarr', None), ('test', 'netcdf', None)]
)
def test_assets(column_name, format, format_column_name):
    a = Assets(column_name=column_name, format=format, format_column_name=format_column_name)
    assert a.column_name == column_name
    assert a.format == format
    assert a.format_column_name == format_column_name


def test_assets_mutually_exclusive():
    with pytest.raises(ValueError):
        Assets(column_name='test', format='netcdf', format_column_name='test')


@pytest.mark.parametrize(
    'file',
    [
        catalog_dict_records,
        cdf_col_sample_cmip6,
        cdf_col_sample_cmip5,
        zarr_col_aws_cesm,
        zarr_col_pangeo_cmip6,
        cdf_col_sample_cmip5,
        cdf_col_sample_cmip6,
        cdf_col_sample_cesmle,
        multi_variable_col,
    ],
)
def test_esmcatmodel_load(file):
    cat = ESMCatalogModel.load(file)
    assert isinstance(cat, ESMCatalogModel)
    assert isinstance(cat.df, pd.DataFrame)
    assert isinstance(cat.columns_with_iterables, set)
    assert isinstance(cat.has_multiple_variable_assets, bool)


def test_esmcatmodel_from_dict():
    cat = ESMCatalogModel.from_dict({'esmcat': sample_esmcol_data, 'df': sample_df})
    assert isinstance(cat, ESMCatalogModel)
    assert isinstance(cat.df, pd.DataFrame)
    assert isinstance(cat.columns_with_iterables, set)
    assert isinstance(cat.has_multiple_variable_assets, bool)


@pytest.mark.parametrize(
    'query, expected_unique_vals, expected_nunique_vals',
    [
        (
            {},
            {
                'component': [],
                'frequency': [],
                'experiment': [],
                'variable': [],
                'path': [],
                'format': [],
            },
            {
                'component': 0,
                'frequency': 0,
                'experiment': 0,
                'variable': 0,
                'path': 0,
                'format': 0,
            },
        ),
        (
            {'variable': ['FLNS']},
            {
                'component': ['atm'],
                'frequency': ['daily'],
                'experiment': ['20C'],
                'variable': ['FLNS'],
                'path': ['s3://ncar-cesm-lens/atm/daily/cesmLE-20C-FLNS.zarr'],
                'format': ['zarr'],
            },
            {
                'component': 1,
                'frequency': 1,
                'experiment': 1,
                'variable': 1,
                'path': 1,
                'format': 1,
            },
        ),
    ],
)
def test_esmcatmodel_unique_and_nunique(query, expected_unique_vals, expected_nunique_vals):
    cat = ESMCatalogModel.from_dict({'esmcat': sample_esmcol_data, 'df': sample_df})
    df_sub = cat.search(query=query)
    cat_sub = ESMCatalogModel.from_dict({'esmcat': sample_esmcol_data, 'df': df_sub})
    assert cat_sub.unique().to_dict() == expected_unique_vals
    assert cat_sub.nunique().to_dict() == expected_nunique_vals


@pytest.mark.parametrize(
    'query, columns, require_all_on',
    [({'foo': 1}, ['foo', 'bar'], ['bar']), ({'bar': 1}, ['foo', 'bar'], 'foo')],
)
def test_query_model(query, columns, require_all_on):
    q = QueryModel(query=query, columns=columns, require_all_on=require_all_on)
    assert q.columns == columns
    if not isinstance(require_all_on, str):
        assert q.require_all_on == require_all_on
    else:
        assert q.require_all_on == [require_all_on]

    assert set(q.query.keys()) == set(query.keys())


@pytest.mark.parametrize(
    'query, columns, require_all_on',
    [({'testing': 1}, ['foo', 'bar'], ['bar']), ({'bar': 1}, ['foo', 'bar'], 'testing')],
)
def test_query_model_validation_error(query, columns, require_all_on):
    with pytest.raises(pydantic.ValidationError):
        QueryModel(query=query, columns=columns, require_all_on=require_all_on)
