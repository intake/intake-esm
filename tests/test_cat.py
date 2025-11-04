import ast
import datetime as dt
import tempfile

import pandas as pd
import polars as pl
import pydantic
import pytest
from polars import testing as pl_testing
from pydantic_core import ValidationError

from intake_esm.cat import Assets, DataFormat, ESMCatalogModel, FramesModel, QueryModel

from .utils import (
    access_columns_with_lists_cat,
    access_columns_with_sets_cat,
    access_columns_with_tuples_cat,
    access_single_item_iterables_cat,
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
        (access_columns_with_sets_cat, set),
        (access_single_item_iterables_cat, tuple),
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
    assert isinstance(cat.df.loc[0, 'variable'], tuple)
    with tempfile.TemporaryDirectory() as tmpdir:
        cat.save(
            'catalog',
            directory=tmpdir,
            catalog_type='file',
        )
        serialised_cat = pd.read_csv(
            f'{tmpdir}/catalog.csv', converters={'variable': ast.literal_eval}
        )
        assert isinstance(serialised_cat.loc[0, 'variable'], expected_type)


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


# -----------------------------
# Assets._validate_data_format
# -----------------------------


def test_assets_ok_with_format_column_name_only():
    a = Assets(column_name='asset', format=None, format_column_name='fmt_col')
    assert a.format is None
    assert a.format_column_name == 'fmt_col'


def test_assets_ok_with_format_only():
    # Assumes DataFormat is an Enum in your codebase; pick any valid member
    fmt = list(DataFormat)[0]
    a = Assets(column_name='asset', format=fmt, format_column_name=None)
    assert a.format == fmt
    assert a.format_column_name is None


def test_assets_error_when_both_set():
    fmt = list(DataFormat)[0]
    with pytest.raises(ValueError):
        Assets(column_name='asset', format=fmt, format_column_name='fmt_col')


def test_assets_error_when_neither_set():
    with pytest.raises(ValueError):
        Assets(column_name='asset', format=None, format_column_name=None)


# -------------------------------------
# ESMCatalogModel.validate_catalog
# -------------------------------------


def _minimal_assets():
    return Assets(column_name='asset', format=None, format_column_name='fmt')


def test_esm_catalog_ok_when_only_catalog_dict_set():
    m = ESMCatalogModel(
        esmcat_version='0.1.0',
        attributes=[],
        assets=_minimal_assets(),
        catalog_dict=[{'a': 1}],
        catalog_file=None,
    )
    assert m.catalog_dict == [{'a': 1}]
    assert m.catalog_file is None


def test_esm_catalog_ok_when_only_catalog_file_set(tmp_path):
    f = tmp_path / 'cat.json'
    f.write_text('{}')
    m = ESMCatalogModel(
        esmcat_version='0.1.0',
        attributes=[],
        assets=_minimal_assets(),
        catalog_dict=None,
        catalog_file=str(f),
    )
    assert m.catalog_file == str(f)
    assert m.catalog_dict is None


def test_esm_catalog_error_when_both_set(tmp_path):
    f = tmp_path / 'cat.json'
    f.write_text('{}')
    with pytest.raises(ValueError):
        ESMCatalogModel(
            esmcat_version='0.1.0',
            attributes=[],
            assets=_minimal_assets(),
            catalog_dict=[{'a': 1}],
            catalog_file=str(f),
        )


# ------------------------------
# QueryModel.validate_query
# ------------------------------


def test_query_model_ok_valid_columns_and_scalar_normalization():
    q = QueryModel(
        query={'var': 'tas', 'exp': 1, 'flag': True, 'noneval': None},
        columns=['var', 'exp', 'flag', 'noneval'],
        require_all_on=None,
    )
    # Scalars normalized to lists
    assert q.query['var'] == ['tas']
    assert q.query['exp'] == [1]
    assert q.query['flag'] == [True]
    assert q.query['noneval'] == [None]


def test_query_model_normalizes_pd_NA_to_list():
    q = QueryModel(
        query={'missing': pd.NA},
        columns=['missing'],
        require_all_on=None,
    )
    assert q.query['missing'] == [pd.NA]


def test_query_model_error_when_query_key_not_in_columns():
    with pytest.raises(ValueError, match=r"Column foo not in columns \['bar'\]"):
        QueryModel(query={'foo': 'x'}, columns=['bar'], require_all_on=None)


def test_query_model_require_all_on_string_becomes_list_and_is_validated():
    q = QueryModel(query={}, columns=['x', 'y'], require_all_on='x')
    assert q.require_all_on == ['x']

    # invalid require_all_on key
    with pytest.raises(ValueError, match=r"Column z not in columns \['x', 'y'\]"):
        QueryModel(query={}, columns=['x', 'y'], require_all_on=['z'])


def test_query_model_mixed_iterables_preserved():
    # Values that are already iterables should be preserved (no double-wrapping)
    q = QueryModel(
        query={'var': ['tas', 'pr']},
        columns=['var'],
        require_all_on=None,
    )
    assert q.query['var'] == ['tas', 'pr']


# ------------------------------
# FramesModel.ensure_some
# ------------------------------


def test_frames_model_ok_with_pandas_only():
    df = pd.DataFrame({'a': [1, 2]})
    m = FramesModel(df=df, pl_df=None, lf=None)
    assert m.df is not None
    assert m.pl_df is None
    assert m.lf is None


def test_frames_model_error_when_all_none():
    with pytest.raises(ValidationError):
        FramesModel(df=None, pl_df=None, lf=None)


def test_esm_catalog_dates_are_pass_through():
    m = ESMCatalogModel(
        esmcat_version='0.1.0',
        attributes=[],
        assets=_minimal_assets(),
        catalog_dict=[{}],
        last_updated=dt.date(2025, 1, 1),
    )
    assert m.last_updated == dt.date(2025, 1, 1)
