import ast
import json
import os
from unittest import mock

import intake
import packaging.version
import pandas as pd
import polars as pl
import pydantic
import pytest
import xarray as xr
from polars.testing import assert_frame_equal as assert_frame_equal_pl

if packaging.version.Version(xr.__version__) < packaging.version.Version('2024.10'):
    from datatree import DataTree
else:
    from xarray import DataTree

import intake_esm

from .utils import (
    access_columns_with_lists_cat,
    access_columns_with_tuples_cat,
    catalog_dict_records,
    cdf_cat_sample_cesmle,
    cdf_cat_sample_cmip5,
    cdf_cat_sample_cmip5_pq,
    cdf_cat_sample_cmip6,
    cdf_cat_sample_cmip6_noagg,
    mixed_cat_sample_cmip6,
    multi_variable_cat,
    opendap_cat_sample_noaa,
    sample_df,
    sample_esmcat_data,
    zarr_cat_aws_cesm,
    zarr_cat_pangeo_cmip6,
)

registry = intake_esm.DerivedVariableRegistry()


@registry.register(variable='FOO', query={'variable': ['FLNS', 'FLUT']})
def func(ds):
    return ds + 1


@registry.register(variable='BAR', query={'variable': ['FLUT']})
def funcs(ds):
    return ds + 1


@registry.register(variable='FLNS', query={'variable': ['FOO', 'FLUT']})
def func2(ds):
    return ds - 1


registry_multivar = intake_esm.DerivedVariableRegistry()


@registry_multivar.register(variable='FOO', query={'variable': ['TEMP']})
def func_multivar(ds):
    return ds + 1


@pytest.mark.parametrize(
    'obj, sep, read_kwargs, columns_with_iterables',
    [
        (catalog_dict_records, '.', None, None),
        (cdf_cat_sample_cmip6_noagg, '.', None, None),
        (cdf_cat_sample_cmip6, '/', None, None),
        (zarr_cat_aws_cesm, '.', None, None),
        (zarr_cat_pangeo_cmip6, '*', None, None),
        (cdf_cat_sample_cesmle, '.', None, None),
        (multi_variable_cat, '*', {'converters': {'variable': ast.literal_eval}}, None),
        (multi_variable_cat, '*', None, ['variable']),
        ({'esmcat': sample_esmcat_data, 'df': sample_df}, '.', None, None),
        (intake_esm.cat.ESMCatalogModel.load(cdf_cat_sample_cmip6), '.', None, None),
    ],
)
@pytest.mark.flaky(max_runs=3, min_passes=1)  # Cold start related failures
def test_catalog_init(capsys, obj, sep, read_kwargs, columns_with_iterables):
    """Test that the catalog can be initialized."""
    cat = intake.open_esm_datastore(
        obj, sep=sep, read_kwargs=read_kwargs, columns_with_iterables=columns_with_iterables
    )
    assert isinstance(cat.esmcat, intake_esm.cat.ESMCatalogModel)
    assert isinstance(cat.df, pd.DataFrame)
    assert len(cat) > 0

    print(repr(cat))
    # Use pytest-capturing method
    # https://docs.pytest.org/en/latest/capture.html#accessing-captured-output-from-a-test-function
    captured = capsys.readouterr()
    assert 'catalog with' in captured.out


@pytest.mark.parametrize(
    'obj, sep, read_kwargs, read_csv_kwargs',
    [
        (  # Both
            multi_variable_cat,
            '*',
            {'converters': {'variable': ast.literal_eval}},
            {'converters': {'variable': ast.literal_eval}},
        ),
        (  # Read kwargs only
            multi_variable_cat,
            '*',
            {'converters': {'variable': ast.literal_eval}},
            None,
        ),
        (  # Read csv kwargs only
            multi_variable_cat,
            '*',
            None,
            {'converters': {'variable': ast.literal_eval}},
        ),
    ],
)
@pytest.mark.flaky(max_runs=3, min_passes=1)  # Cold start related failures
def test_catalog_init_back_compat(capsys, obj, sep, read_kwargs, read_csv_kwargs):
    """Test that the catalog can be initialized with various combinations of read
    and read_csv kwargs, rasing and warning appropriately. Retains much of the logic
    of the test above to make sure behaviour is consistent."""

    if read_kwargs and read_csv_kwargs:
        with pytest.raises(
            ValueError, match='Cannot provide both `read_csv_kwargs` and `read_kwargs`. '
        ):
            intake.open_esm_datastore(
                obj, sep=sep, read_kwargs=read_kwargs, read_csv_kwargs=read_csv_kwargs
            )
        return None

    if read_csv_kwargs:
        with pytest.warns(
            DeprecationWarning,
            match='read_csv_kwargs is deprecated and will be removed in a future version. ',
        ):
            cat = intake.open_esm_datastore(obj, sep=sep, read_csv_kwargs=read_csv_kwargs)

    else:
        cat = intake.open_esm_datastore(obj, sep=sep, read_csv_kwargs=read_csv_kwargs)

    cat = intake.open_esm_datastore(obj, sep=sep, read_kwargs=read_kwargs)
    assert isinstance(cat.esmcat, intake_esm.cat.ESMCatalogModel)
    assert isinstance(cat.df, pd.DataFrame)
    assert len(cat) > 0

    print(repr(cat))
    # Use pytest-capturing method
    # https://docs.pytest.org/en/latest/capture.html#accessing-captured-output-from-a-test-function
    captured = capsys.readouterr()
    assert 'catalog with' in captured.out


@pytest.mark.parametrize(
    'obj, read_kwargs, columns_with_iterables',
    [
        (multi_variable_cat, {'converters': {'variable': ast.literal_eval}}, None),
        (multi_variable_cat, None, ['variable']),
    ],
)
def test_columns_with_iterables(capsys, obj, read_kwargs, columns_with_iterables):
    """Test that columns with iterables are successfully evaluated."""
    cat = intake.open_esm_datastore(
        obj, read_kwargs=read_kwargs, columns_with_iterables=columns_with_iterables
    )
    assert 'variable' in cat.esmcat.columns_with_iterables


def test_read_csv_conflict():
    """Test that error is raised when `columns_with_iterables` conflicts with `read_kwargs`."""
    # Work when inputs are consistent
    intake.open_esm_datastore(
        multi_variable_cat,
        read_kwargs={'converters': {'variable': ast.literal_eval}},
        columns_with_iterables=['variable'],
    )

    # Fails on conflict
    with pytest.raises(ValueError):
        intake.open_esm_datastore(
            multi_variable_cat,
            read_kwargs={'converters': {'variable': lambda x: x}},
            columns_with_iterables=['variable'],
        )


@pytest.mark.parametrize(
    'datastore, file_format',
    [
        (catalog_dict_records, 'csv'),
        (cdf_cat_sample_cmip6, 'csv'),
        (zarr_cat_aws_cesm, 'csv'),
        (zarr_cat_pangeo_cmip6, 'csv'),
        (cdf_cat_sample_cmip5_pq, 'parquet'),
        (multi_variable_cat, 'csv'),
        ({'esmcat': sample_esmcat_data, 'df': sample_df}, 'csv'),
        (intake_esm.cat.ESMCatalogModel.load(cdf_cat_sample_cmip6), 'csv'),
    ],
)
@pytest.mark.parametrize('catalog_type', ['file', 'dict'])
def test_write_csv_conflict(tmp_path, datastore, file_format, catalog_type):
    """Test that error is raised when `to_csv_kwargs` conflicts with `write_kwargs`."""
    cat = intake.open_esm_datastore(datastore)

    kwargs = {
        'name': 'test_catalog',
        'directory': tmp_path,
        'catalog_type': catalog_type,
        'file_format': file_format,
        'write_kwargs': {'compression': 'gzip'},
    }

    # Work when inputs are consistent
    cat.serialize(**kwargs)

    kwargs['to_csv_kwargs'] = kwargs['write_kwargs']
    # Fails on conflict
    with pytest.raises(ValueError):
        cat.serialize(
            name='test_catalog',
            directory=tmp_path,
            catalog_type='file',
            file_format=file_format,
            write_kwargs={'compression': 'gzip'},
            to_csv_kwargs={'compression': 'gzip'},
        )

    kwargs.pop('write_kwargs')

    with pytest.warns(
        DeprecationWarning,
        match='to_csv_kwargs is deprecated and will be removed in a future version. ',
    ):
        cat.serialize(
            name='test_catalog',
            directory=tmp_path,
            catalog_type=catalog_type,
            file_format=file_format,
            to_csv_kwargs={'compression': 'gzip'},
        )


@pytest.mark.parametrize(
    'file_format_1',
    [
        'csv',
        'parquet',
    ],
)
@pytest.mark.parametrize(
    'file_format_2',
    [
        'csv',
        'parquet',
    ],
)
def test_open_and_reserialize(tmp_path, file_format_1, file_format_2):
    """
    Open a catalog, and then re-serialize it into `tmp_path`. We want to
    make sure that the reserialised catalog is the same as the original, but that
    the catalog file is stored in the correct format.

    Note: Round-trip twice as it seems the defaults have changed since the test dataset was
    created. Only affects unspecified fields.
    """
    catalog = intake.open_esm_datastore(cdf_cat_sample_cmip5)

    catalog.serialize(
        name='serialized',
        directory=tmp_path,
        catalog_type='file',
        file_format=file_format_1,
        storage_options={},
    )

    catalog = intake.open_esm_datastore(tmp_path / 'serialized.json')

    catalog.serialize(
        name='reserialized',
        directory=tmp_path,
        catalog_type='file',
        file_format=file_format_2,
        storage_options={},
    )

    with open(tmp_path / 'serialized.json') as f:
        serialized = json.load(f)

    with open(tmp_path / 'reserialized.json') as f:
        reserialized = json.load(f)

    assert serialized.get('catalog_file', '').endswith(file_format_1)
    assert reserialized.get('catalog_file', '').endswith(file_format_2)

    # Remove fields that are expected to change
    changed_fieldnames = ['catalog_file', 'last_updated', 'id', 'title']
    for field in changed_fieldnames:
        serialized.pop(field, None)
        reserialized.pop(field, None)

    assert serialized == reserialized


@pytest.mark.parametrize(
    'query,regex',
    [
        ({'variables': ['FLNS', 'FLUT']}, r'Variable derivation requires'),
        ({'variable': ['FLNS', 'FLUT'], 'testing': 'foo'}, r'Derived variable'),
    ],
)
def test_invalid_derivedcat(query, regex):
    registry = intake_esm.DerivedVariableRegistry()

    @registry.register(variable='FOO', query=query)
    def func(ds):
        ds['FOO'] = ds.FLNS + ds.FLUT
        return ds

    with pytest.raises(ValueError, match=regex):
        intake.open_esm_datastore(catalog_dict_records, registry=registry)


def test_impossible_derivedcat():
    registry = intake_esm.DerivedVariableRegistry()

    @registry.register(variable='FOO', query={'variable': ['FLNS', 'FLUT']})
    def func(ds):
        ds['FOO'] = ds.FLNS + ds.FLUT
        return ds

    with pytest.raises(ValueError, match='Variable derivation requires `aggregation_control`'):
        intake.open_esm_datastore(cdf_cat_sample_cmip6_noagg, registry=registry)


@pytest.mark.parametrize(
    'obj, sep, read_kwargs',
    [
        (multi_variable_cat, '.', {'converters': {'variable': ast.literal_eval}}),
        (cdf_cat_sample_cesmle, '/', None),
        (cdf_cat_sample_cmip5, '.', None),
        (cdf_cat_sample_cmip6, '*', None),
        (catalog_dict_records, '.', None),
        (cdf_cat_sample_cmip6_noagg, '.', None),
        ({'esmcat': sample_esmcat_data, 'df': sample_df}, '.', None),
    ],
)
def test_catalog_unique(obj, sep, read_kwargs):
    cat = intake.open_esm_datastore(obj, sep=sep, read_kwargs=read_kwargs)
    uniques = cat.unique()
    nuniques = cat.nunique()
    assert isinstance(uniques, pd.Series)
    assert isinstance(nuniques, pd.Series)
    assert len(uniques.keys()) == len(cat.df.columns) + (
        0 if obj is cdf_cat_sample_cmip6_noagg else 1
    )  # for derived_variable entry


def test_catalog_contains():
    cat = intake.open_esm_datastore(cdf_cat_sample_cesmle)
    assert 'ocn.20C.pop.h' in cat
    assert 'ocn.CTRL.pop.h' in cat
    assert 'ocn.RCP85.pop.h' in cat
    assert 'foo' not in cat


@pytest.mark.parametrize(
    'path, query, expected_size',
    [
        (cdf_cat_sample_cesmle, {'experiment': 'CTRL'}, 1),
        (cdf_cat_sample_cesmle, {'experiment': ['CTRL', '20C']}, 2),
        (cdf_cat_sample_cesmle, {}, 0),
        (cdf_cat_sample_cesmle, {'variable': 'SHF', 'time_range': ['200601-210012']}, 1),
    ],
)
def test_catalog_search(path, query, expected_size):
    cat = intake.open_esm_datastore(path)
    new_cat = cat.search(**query)
    assert len(new_cat) == expected_size


@pytest.mark.parametrize(
    'path, columns_with_iterables, query, expected_size',
    [
        (access_columns_with_lists_cat, ['variable'], {'variable': ['aice_m']}, 1),
        (access_columns_with_tuples_cat, ['variable'], {'variable': ['aice_m']}, 1),
    ],
)
def test_catalog_search_columns_with_iterables(path, columns_with_iterables, query, expected_size):
    cat = intake.open_esm_datastore(path, columns_with_iterables=columns_with_iterables)

    for iter_col in columns_with_iterables:
        assert isinstance(cat.df[iter_col][0], tuple)
    new_cat = cat.search(**query)
    assert len(new_cat) == expected_size


def test_catalog_with_registry_search():
    cat = intake.open_esm_datastore(zarr_cat_aws_cesm, registry=registry)
    new_cat = cat.search(variable='FOO')
    assert len(cat) == 56
    assert len(new_cat) == 11

    assert len(cat.derivedcat) == 3
    assert len(new_cat.derivedcat) == 1

    new_cat = cat.search(variable='FOO', frequency='daily')
    assert len(new_cat) == 4
    assert len(new_cat.derivedcat) == 1

    new_cat = cat.search(frequency='daily')
    assert len(new_cat.derivedcat) == 3


@pytest.mark.parametrize('key', ['ocn.20C.pop.h', 'ocn.CTRL.pop.h', 'ocn.RCP85.pop.h'])
def test_catalog_getitem(key):
    cat = intake.open_esm_datastore(cdf_cat_sample_cesmle)
    entry = cat[key]
    assert isinstance(entry, intake_esm.source.ESMDataSource)


def test_catalog_getitem_error():
    cat = intake.open_esm_datastore(cdf_cat_sample_cesmle)
    with pytest.raises(KeyError):
        cat['foo']


@pytest.mark.parametrize('cat', [cdf_cat_sample_cesmle, cdf_cat_sample_cmip6_noagg])
def test_catalog_keys_info(cat):
    cat = intake.open_esm_datastore(cat)
    data = cat.keys_info()
    assert isinstance(data, pd.DataFrame)
    assert data.index.name == 'key'
    assert len(data) == len(cat)


@pytest.mark.parametrize(
    'catalog_type, to_csv_kwargs, json_dump_kwargs, directory',
    [
        ('file', {'compression': 'bz2'}, {}, '.'),
        ('file', {'compression': 'gzip'}, {}, None),
        ('dict', {}, {}, None),
    ],
)
def test_catalog_serialize(catalog_type, to_csv_kwargs, json_dump_kwargs, directory):
    cat = intake.open_esm_datastore(cdf_cat_sample_cmip6)
    cat_subset = cat.search(
        source_id='MRI-ESM2-0',
    )
    name = 'CMIP6-MRI-ESM2-0'
    cat_subset.serialize(
        name=name,
        directory=directory,
        catalog_type=catalog_type,
        to_csv_kwargs=to_csv_kwargs,
        json_dump_kwargs=json_dump_kwargs,
    )
    if directory is None:
        directory = os.getcwd()
    cat = intake.open_esm_datastore(f'{directory}/{name}.json')
    subset_df = cat_subset.esmcat.pl_df.with_columns(
        [
            pl.col(colname).cast(pl.Null)
            for colname in cat_subset.esmcat._frames.pl_df.columns
            if cat_subset.esmcat._frames.pl_df.get_column(colname).is_null().all()
        ]
    )

    df = cat.esmcat.pl_df.with_columns(
        [
            pl.col(colname).cast(pl.Null)
            for colname in cat.esmcat._frames.pl_df.columns
            if cat.esmcat._frames.pl_df.get_column(colname).is_null().all()
        ]
    )
    assert_frame_equal_pl(
        subset_df,
        df,
    )
    assert cat.esmcat.id == name


def test_empty_queries():
    cat = intake.open_esm_datastore(cdf_cat_sample_cmip6)
    sub_cat = cat.search()
    with pytest.warns(
        UserWarning, match=r'There are no datasets to load! Returning an empty dictionary.'
    ):
        dsets = sub_cat.to_dataset_dict()
        assert not dsets


@pytest.mark.parametrize(
    'key',
    [
        # 'CMIP.CNRM-CERFACS.CNRM-CM6-1.historical.Lmon.gr',
        # 'CMIP.CNRM-CERFACS.CNRM-CM6-1.piControl.Lmon.gr',
        'CMIP.CNRM-CERFACS.CNRM-ESM2-1.1pctCO2.Omon.gn',
        'CMIP.CNRM-CERFACS.CNRM-ESM2-1.abrupt-4xCO2.Amon.gr',
        'CMIP.CNRM-CERFACS.CNRM-ESM2-1.amip.Amon.gr',
    ],
)
@pytest.mark.parametrize('decode_times', [True, False])
def test_getitem(key, decode_times):
    cat = intake.open_esm_datastore(cdf_cat_sample_cmip6)
    x = cat[key]
    assert isinstance(x, intake_esm.source.ESMDataSource)
    ds = x(xarray_open_kwargs={'chunks': {}, 'decode_times': decode_times}).to_dask()
    assert isinstance(ds, xr.Dataset)
    assert set(x.df['member_id']) == set(ds['member_id'].values)


@pytest.mark.parametrize(
    'query', [dict(variable=['O2', 'TEMP']), dict(variable=['SHF']), dict(experiment='CTRL')]
)
def test_multi_variable_catalog(query):
    import ast

    cat = intake.open_esm_datastore(
        multi_variable_cat, read_kwargs={'converters': {'variable': ast.literal_eval}}
    )
    assert cat.esmcat.has_multiple_variable_assets

    cat_sub = cat.search(**query)
    assert set(cat_sub._requested_variables) == set(query.pop('variable', []))

    _, ds = cat_sub.to_dataset_dict().popitem()
    if cat_sub._requested_variables:
        assert set(ds.data_vars) == set(cat_sub._requested_variables)


def test_multi_variable_catalog_derived_cat():
    import ast

    cat = intake.open_esm_datastore(
        multi_variable_cat,
        read_kwargs={'converters': {'variable': ast.literal_eval}},
        registry=registry_multivar,
    )
    cat_sub = cat.search(variable=['FOO'])
    assert set(cat_sub._requested_variables) == {'TEMP', 'FOO'}


@pytest.mark.parametrize(
    'path, query, xarray_open_kwargs',
    [
        (
            zarr_cat_pangeo_cmip6,
            dict(
                variable_id=['pr'],
                experiment_id='ssp370',
                activity_id='AerChemMIP',
                source_id='BCC-ESM1',
                table_id='Amon',
                grid_label='gn',
            ),
            {'consolidated': True, 'backend_kwargs': {'storage_options': {'token': 'anon'}}},
        ),
        (
            cdf_cat_sample_cmip6,
            dict(source_id=['CNRM-ESM2-1', 'CNRM-CM6-1', 'BCC-ESM1'], variable_id=['tasmax']),
            {'chunks': {'time': 1}},
        ),
        (
            cdf_cat_sample_cmip6_noagg,
            dict(source_id=['CNRM-ESM2-1', 'CNRM-CM6-1', 'BCC-ESM1'], variable_id=['tasmax']),
            {'chunks': {'time': 1}},
        ),
        (mixed_cat_sample_cmip6, dict(institution_id='BCC'), {}),
    ],
)
@pytest.mark.flaky(max_runs=3, min_passes=1)  # Cold start related failures
def test_to_dataset_dict(path, query, xarray_open_kwargs):
    cat = intake.open_esm_datastore(path)
    cat_sub = cat.search(**query)
    _, ds = cat_sub.to_dataset_dict(xarray_open_kwargs=xarray_open_kwargs).popitem()
    if path != cdf_cat_sample_cmip6_noagg:
        assert 'member_id' in ds.dims
    assert len(ds.__dask_keys__()) > 0
    assert ds.time.encoding


@pytest.mark.parametrize(
    'path, query, xarray_open_kwargs',
    [
        (
            zarr_cat_pangeo_cmip6,
            dict(
                variable_id=['pr'],
                experiment_id='ssp370',
                activity_id='AerChemMIP',
                source_id='BCC-ESM1',
                table_id='Amon',
                grid_label='gn',
            ),
            {'consolidated': True, 'backend_kwargs': {'storage_options': {'token': 'anon'}}},
        ),
        (
            cdf_cat_sample_cmip6,
            dict(source_id=['CNRM-ESM2-1', 'CNRM-CM6-1', 'BCC-ESM1'], variable_id=['tasmax']),
            {'chunks': {'time': 1}},
        ),
    ],
)
@pytest.mark.flaky(max_runs=3, min_passes=1)  # Cold start related failures
def test_to_datatree(path, query, xarray_open_kwargs):
    cat = intake.open_esm_datastore(path)
    cat_sub = cat.search(**query)
    tree = cat_sub.to_datatree(xarray_open_kwargs=xarray_open_kwargs)
    _, ds = tree.to_dict().popitem()
    assert 'member_id' in ds.dims
    assert len(ds.__dask_keys__()) > 0
    assert ds.time.encoding
    assert isinstance(tree, DataTree)


def test_to_datatree_levels():
    cat = intake.open_esm_datastore(zarr_cat_pangeo_cmip6)
    cat_sub = cat.search(
        **dict(
            variable_id=['pr'],
            experiment_id='ssp370',
            activity_id='AerChemMIP',
            source_id='BCC-ESM1',
            table_id='Amon',
            grid_label='gn',
        ),
    )

    tree = cat_sub.to_datatree(
        xarray_open_kwargs={
            'consolidated': True,
            'backend_kwargs': {'storage_options': {'token': 'anon'}},
        },
        levels=['source_id'],
    )
    assert list(tree.keys()) == ['BCC-ESM1']


@pytest.mark.parametrize(
    'path, query, xarray_open_kwargs',
    [
        (
            zarr_cat_pangeo_cmip6,
            dict(
                variable_id=['pr'],
                experiment_id='ssp370',
                activity_id='AerChemMIP',
                source_id='BCC-ESM1',
                table_id='Amon',
                grid_label='gn',
            ),
            {'consolidated': True, 'backend_kwargs': {'storage_options': {'token': 'anon'}}},
        ),
        (
            cdf_cat_sample_cmip6,
            dict(
                source_id=['CNRM-ESM2-1', 'CNRM-CM6-1', 'BCC-ESM1'],
                variable_id=['tasmax'],
                experiment_id='piControl',
            ),
            {'chunks': {'time': 1}},
        ),
    ],
)
def test_to_dask(path, query, xarray_open_kwargs):
    cat = intake.open_esm_datastore(path)
    cat_sub = cat.search(**query)
    ds = cat_sub.to_dask(xarray_open_kwargs=xarray_open_kwargs)
    assert 'member_id' in ds.dims
    assert len(ds.__dask_keys__()) > 0
    assert ds.time.encoding


@pytest.mark.parametrize(
    'path, query',
    [
        (cdf_cat_sample_cmip6, {'experiment_id': ['historical', 'rcp85']}),
        (cdf_cat_sample_cmip6_noagg, {'experiment_id': ['historical', 'rcp85']}),
        (cdf_cat_sample_cmip5, {'experiment': ['historical', 'rcp85']}),
    ],
)
def test_to_dataset_dict_aggfalse(path, query):
    cat = intake.open_esm_datastore(path)
    cat_sub = cat.search(**query)
    nds = len(cat_sub.df)
    dsets = cat_sub.to_dataset_dict(xarray_open_kwargs={'chunks': {'time': 1}}, aggregate=False)
    assert len(dsets.keys()) == nds


@pytest.mark.parametrize(
    'path, query',
    [
        (
            cdf_cat_sample_cmip6,
            dict(source_id=['CNRM-ESM2-1', 'CNRM-CM6-1', 'BCC-ESM1'], variable_id=['tasmax']),
        )
    ],
)
def test_to_dataset_dict_w_preprocess(path, query):
    def rename_coords(ds):
        return ds.rename({'lon': 'longitude', 'lat': 'latitude'})

    cat = intake.open_esm_datastore(path)
    cat_sub = cat.search(**query)
    dsets = cat_sub.to_dataset_dict(
        xarray_open_kwargs={'chunks': {'time': 1}}, preprocess=rename_coords
    )
    _, ds = dsets.popitem()
    assert 'latitude' in ds.dims
    assert 'longitude' in ds.dims


def test_to_dataset_dict_cdf_zarr_kwargs_deprecation():
    cat = intake.open_esm_datastore(cdf_cat_sample_cmip6)
    cat_sub = cat.search(
        **dict(source_id=['CNRM-ESM2-1', 'CNRM-CM6-1', 'BCC-ESM1'], variable_id=['tasmax'])
    )
    with pytest.warns(
        DeprecationWarning,
        match=r'cdf_kwargs and zarr_kwargs are deprecated and will be removed in a future version. Please use xarray_open_kwargs instead.',
    ):
        cat_sub.to_dataset_dict(cdf_kwargs={'chunks': {'time': 1}})


def test_to_dataset_dict_w_preprocess_error():
    cat = intake.open_esm_datastore(cdf_cat_sample_cmip5)
    with pytest.raises(pydantic.ValidationError):
        cat.to_dataset_dict(preprocess='foo')


def test_to_dataset_dict_skip_error():
    cat = intake.open_esm_datastore(catalog_dict_records)
    with pytest.raises(intake_esm.source.ESMDataSourceError):
        dsets = cat.to_dataset_dict(
            xarray_open_kwargs={'backend_kwargsd': {'storage_options': {'anon': True}}},
            skip_on_error=False,
        )

    dsets = cat.to_dataset_dict(
        xarray_open_kwargs={'backend_kwargsd': {'storage_options': {'anon': True}}},
        skip_on_error=True,
    )

    assert len(dsets.keys()) == 0


def test_to_dataset_dict_with_registry():
    registry = intake_esm.DerivedVariableRegistry()

    @registry.register(variable='FOO', query={'variable': ['FLNS', 'FLUT']})
    def func(ds):
        ds['FOO'] = ds.FLNS + ds.FLUT
        return ds

    @registry.register(variable='BAR', query={'variable': ['FLUT']})
    def funcs(ds):
        ds['BAR'] = ds.FLUT * 1000
        return ds

    cat = intake.open_esm_datastore(catalog_dict_records, registry=registry)
    new_cat = cat.search(variable=['FOO', 'BAR'])
    _, ds = new_cat.to_dataset_dict(
        xarray_open_kwargs={'backend_kwargs': {'storage_options': {'anon': True}}}
    ).popitem()

    assert 'FOO' in ds.data_vars
    assert 'BAR' in ds.data_vars
    assert len(ds.data_vars) == 4

    with pytest.raises(NotImplementedError):
        new_cat.esmcat.aggregation_control.groupby_attrs += ['variable']
        new_cat.to_dataset_dict(
            xarray_open_kwargs={'backend_kwargs': {'storage_options': {'anon': True}}}
        )


def test_to_dask_opendap():
    cat = intake.open_esm_datastore(opendap_cat_sample_noaa)
    new_cat = cat.search(variable='sst', first_swap='2005001', scode=482)
    ds = new_cat.to_dask(xarray_open_kwargs=dict(engine='pydap'))
    assert 'sst' in ds.data_vars
    assert len(ds.__dask_keys__()) > 0


def test_subclassing_catalog():
    class ChildCatalog(intake_esm.esm_datastore):
        pass

    cat = ChildCatalog(catalog_dict_records)
    scat = cat.search(variable=['FLNS'])
    assert type(scat) is ChildCatalog


def test_options():
    cat = intake.open_esm_datastore(catalog_dict_records)
    scat = cat.search(variable=['FLNS'])
    with intake_esm.set_options(attrs_prefix='myprefix'):
        _, ds = scat.to_dataset_dict(
            xarray_open_kwargs={'backend_kwargs': {'storage_options': {'anon': True}}},
        ).popitem()
        assert ds.attrs['myprefix:component'] == 'atm'


@pytest.mark.parametrize(
    'threaded, ITK_ESM_THREADING, expected',
    [
        (True, 'True', True),
        (True, 'False', True),
        (False, 'True', False),
        (False, 'False', False),
        (None, 'True', True),  # Keep previous default behavior
        (None, 'False', False),
        (None, 1, False),
    ],
)
@mock.patch('os.getenv')
def test__get_threaded(mock_get_env, threaded, ITK_ESM_THREADING, expected):
    mock_get_env.return_value = ITK_ESM_THREADING
    if isinstance(ITK_ESM_THREADING, int):
        with pytest.raises(
            ValueError,
            match='The environment variable ITK_ESM_THREADING must be a boolean, if set.',
        ):
            intake_esm.core._get_threaded(threaded)
    else:
        assert intake_esm.core._get_threaded(threaded) == expected
