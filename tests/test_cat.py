import ast
import datetime as dt
import tempfile

import pandas as pd
import pydantic
import pytest
from pydantic_core import ValidationError

from intake_esm.cat import Assets, DataFormat, ESMCatalogModel, FramesModel, QueryModel

from .utils import (
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

    # Make sure both references to frames are the same object, if we have an iodriver object instantiated
    if cat._driver is not None:
        assert cat._frames is cat._driver._frames


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


def test_save_esmcat_invalidformat():
    cat = ESMCatalogModel.load(
        access_columns_with_tuples_cat, read_kwargs={'converters': {'variable': ast.literal_eval}}
    )
    with tempfile.TemporaryDirectory() as tmpdir:
        with pytest.raises(ValueError, match='file_format must be either "csv" or "parquet"'):
            cat.save(
                'catalog',
                directory=tmpdir,
                catalog_type='file',
                file_format='invalidformat',
            )


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


def test_frames_model_ok_with_pandas_only():
    df = pd.DataFrame({'a': [1, 2]})
    m = FramesModel(df=df, lf=None)
    assert m.df is not None
    assert m.lf is None


def test_frames_model_error_when_all_none():
    with pytest.raises(ValidationError):
        FramesModel(df=None, lf=None)


def test_esm_catalog_dates_are_pass_through():
    m = ESMCatalogModel(
        esmcat_version='0.1.0',
        attributes=[],
        assets=_minimal_assets(),
        catalog_dict=[{}],
        last_updated=dt.date(2025, 1, 1),
    )
    assert m.last_updated == dt.date(2025, 1, 1)


@pytest.mark.parametrize('materialised', [False, True])
@pytest.mark.parametrize('path', [cdf_cat_sample_cmip6, multi_variable_cat])
def test_frames_preserve_arrow_catalogue(path, materialised):
    read_kwargs = (
        {'converters': {'variable': ast.literal_eval}} if path == multi_variable_cat else {}
    )
    cat = ESMCatalogModel.load(path, read_kwargs=read_kwargs)
    lazy = cat.lf
    expected = lazy.collect().to_pandas(use_pyarrow_extension_array=True)
    frames = FramesModel(lf=lazy.collect().lazy()) if materialised else FramesModel(lf=lazy)
    pd.testing.assert_frame_equal(frames.pandas, expected)
    assert all(isinstance(dtype, pd.ArrowDtype) for dtype in frames.pandas.dtypes)
    assert frames.pandas is frames.df
