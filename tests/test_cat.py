import ast
import tempfile

import pandas as pd
import polars as pl
import pydantic
import pytest
from polars import testing as pl_testing

from intake_esm.cat import Assets, ESMCatalogModel, FramesModel, QueryModel

from .utils import (
    access_columns_with_lists_cat,
    access_columns_with_sets_cat,
    access_columns_with_tuples_cat,
    catalog_dict_records,
    cdf_cat_sample_cesmle,
    cdf_cat_sample_cmip5,
    cdf_cat_sample_cmip5_pq,
    cdf_cat_sample_cmip6,
    cdf_cat_sample_cmip6_noagg,
    multi_variable_cat,
    sample_df,
    sample_esmcat_data,
    sample_esmcat_data_without_agg,
    sample_lf,
    sample_pl_df,
    zarr_cat_aws_cesm,
    zarr_cat_pangeo_cmip6,
    zarr_v2_cat,
    zarr_v3_cat,
)


@pytest.mark.parametrize(
    'column_name, format, format_column_name',
    [
        ('test', 'zarr', None),
        ('test', 'zarr2', None),
        ('test', 'zarr3', None),
        ('test', 'netcdf', None),
    ],
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
        cdf_cat_sample_cmip6,
        cdf_cat_sample_cmip5,
        cdf_cat_sample_cmip5_pq,
        zarr_cat_aws_cesm,
        zarr_cat_pangeo_cmip6,
        cdf_cat_sample_cmip5,
        cdf_cat_sample_cmip6,
        cdf_cat_sample_cmip6_noagg,
        cdf_cat_sample_cesmle,
        multi_variable_cat,
        zarr_v2_cat,
        zarr_v3_cat,
    ],
)
@pytest.mark.flaky(max_runs=3, min_passes=1)  # Cold start related failures
def test_esmcatmodel_load(file):
    cat = ESMCatalogModel.load(file)
    assert isinstance(cat, ESMCatalogModel)
    assert isinstance(cat.df, pd.DataFrame)
    assert isinstance(cat.columns_with_iterables, set)
    assert isinstance(cat.has_multiple_variable_assets, bool)


@pytest.mark.parametrize(
    'esmcat_data',
    [sample_esmcat_data, sample_esmcat_data_without_agg],
)
def test_esmcatmodel_from_dict(esmcat_data):
    cat = ESMCatalogModel.from_dict({'esmcat': esmcat_data, 'df': sample_df})
    assert isinstance(cat, ESMCatalogModel)
    assert isinstance(cat.df, pd.DataFrame)
    assert isinstance(cat.columns_with_iterables, set)


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
    cat = ESMCatalogModel.from_dict({'esmcat': sample_esmcat_data, 'df': sample_df})
    df_sub = cat.search(query=query)
    cat_sub = ESMCatalogModel.from_dict({'esmcat': sample_esmcat_data, 'df': df_sub})
    assert cat_sub.unique().to_dict() == expected_unique_vals
    assert cat_sub.nunique().to_dict() == expected_nunique_vals


@pytest.mark.parametrize(
    'catalog_file, expected_type',
    [
        (access_columns_with_lists_cat, list),
        (access_columns_with_tuples_cat, tuple),
        (access_columns_with_sets_cat, tuple),
    ],
)
def test_esmcatmodel_roundtrip_itercols_type_stable(catalog_file, expected_type):
    """
    Test that if we open a catalog with list iterable column, they are saved as
    lists, tuples as tuples, etc.
    """
    cat = ESMCatalogModel.load(
        catalog_file, read_kwargs={'converters': {'variable': ast.literal_eval}}
    )
    # Create a tempdir & save it there, then open with pandas and literal eval it
    # to check the dtype
    assert cat.df['variable'].dtype == expected_type
    with tempfile.TemporaryDirectory() as tmpdir:
        cat.save(
            'catalog',
            directory=tmpdir,
            catalog_type='file',
        )
        serialised_cat = pd.read_csv(
            f'{tmpdir}/catalog.csv', converters={'variable': ast.literal_eval}
        )
        assert isinstance(serialised_cat['variable'].iloc[0], expected_type)


@pytest.mark.parametrize(
    'query, columns, require_all_on',
    [
        ({'foo': 1}, ['foo', 'bar'], ['bar']),
        ({'bar': 1}, ['foo', 'bar'], 'foo'),
        ({'foo': None}, ['foo', 'bar'], None),
    ],
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


@pytest.mark.parametrize(
    'pd_df, pl_df, lf, err',
    [
        (sample_df, sample_pl_df, sample_lf, False),
        (sample_df, None, None, False),
        (None, None, None, True),
        (None, sample_pl_df, None, False),
        (None, None, sample_lf, False),
    ],
)
def test_FramesModel_init(pd_df, pl_df, lf, err):
    """
    Make sure FramesModel works with different input combos
    """
    if not err:
        FramesModel(df=pd_df, pl_df=pl_df, lf=lf)
        assert True
    else:
        with pytest.raises(pydantic.ValidationError):
            FramesModel(df=pd_df, pl_df=pl_df, lf=lf)


@pytest.mark.parametrize(
    'pd_df, pl_df, lf',
    [
        (sample_df, sample_pl_df, sample_lf),
        (sample_df, None, None),
        (None, sample_pl_df, None),
        (None, None, sample_lf),
    ],
)
@pytest.mark.parametrize('attr', ['polars', 'lazy', 'columns_with_iterables'])
def test_FramesModel_no_accidental_pd(pd_df, pl_df, lf, attr):
    """
    Make sure that if we instantiate with a polars dataframe or a lazy frame, we
    don't accidentally trigger the creation of a pandas dataframe.
    """
    f = FramesModel(df=pd_df, pl_df=pl_df, lf=lf)

    if pd_df is not None:
        assert f.df is not None
    else:
        assert f.df is None

    # Now we just want to run through the properties to ensure they don't error
    # and that they don't trigger the creation of a pandas dataframe
    if pd_df is None:
        got_attr = getattr(f, attr)
        assert got_attr is not None
        assert f.df is None
    else:
        got_attr = getattr(f, attr)
        assert got_attr is not None
        assert f.df is not None


def test_FramesModel_set_manual_df():
    """
    Test that if we set esmcat._df, we don't cause an error. We also test that the
    creation of `cat._frames.pl_df` is deferred until we ask for it with the
    `cat.pl_df` property.
    """
    cat = ESMCatalogModel.from_dict({'esmcat': sample_esmcat_data, 'df': sample_df})

    assert cat._frames.pl_df is None

    new_df = pd.DataFrame({'numeric_col': [1, 2, 3], 'str_col': ['a', 'b', 'c']})
    cat._df = new_df

    assert getattr(cat, '_frames') is not None

    pd.testing.assert_frame_equal(cat.df, new_df)

    expected_pl_df = pl.DataFrame({'numeric_col': [1, 2, 3], 'str_col': ['a', 'b', 'c']})

    assert cat._frames.pl_df is None
    pl_testing.assert_frame_equal(cat.pl_df, expected_pl_df)
