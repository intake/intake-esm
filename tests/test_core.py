import ast

import intake
import pandas as pd
import pydantic
import pytest
import xarray as xr

import intake_esm

registry = intake_esm.DerivedVariableRegistry()


@registry.register(variable='FOO', query={'variable': ['FLNS', 'FLUT']})
def func(ds):
    return ds + 1


@registry.register(variable='BAR', query={'variable': ['FLUT']})
def funcs(ds):
    return ds + 1


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
def test_catalog_init(capsys, obj, sep, read_csv_kwargs):
    """Test that the catalog can be initialized."""
    cat = intake.open_esm_datastore(obj, sep=sep, read_csv_kwargs=read_csv_kwargs)
    assert isinstance(cat.esmcat, intake_esm.cat.ESMCatalogModel)
    assert isinstance(cat.df, pd.DataFrame)
    assert len(cat) > 0

    print(repr(cat))
    # Use pytest-capturing method
    # https://docs.pytest.org/en/latest/capture.html#accessing-captured-output-from-a-test-function
    captured = capsys.readouterr()
    assert 'catalog with' in captured.out


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
    assert len(uniques.keys()) == len(cat.df.columns) + 1  # for derived_variable entry


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


def test_catalog_with_registry_search():
    cat = intake.open_esm_datastore(catalog_dict_records, registry=registry)
    new_cat = cat.search(variable='FOO')
    assert len(cat) == 1
    assert len(new_cat) == 1

    assert len(cat.derivedcat) == 2
    assert len(new_cat.derivedcat) == 1


@pytest.mark.parametrize('key', ['ocn.20C.pop.h', 'ocn.CTRL.pop.h', 'ocn.RCP85.pop.h'])
def test_catalog_getitem(key):
    cat = intake.open_esm_datastore(cdf_col_sample_cesmle)
    entry = cat[key]
    assert isinstance(entry, intake_esm.source.ESMDataSource)


def test_catalog_getitem_error():
    cat = intake.open_esm_datastore(cdf_col_sample_cesmle)
    with pytest.raises(KeyError):
        cat['foo']


@pytest.mark.parametrize('catalog_type', ['file', 'dict'])
def test_catalog_serialize(tmp_path, catalog_type):
    cat = intake.open_esm_datastore(cdf_col_sample_cmip6)
    local_store = tmp_path
    cat_subset = cat.search(
        source_id='MRI-ESM2-0',
    )
    name = 'CMIP6-MRI-ESM2-0'
    cat_subset.serialize(name=name, directory=local_store, catalog_type=catalog_type)
    cat = intake.open_esm_datastore(f'{local_store}/{name}.json')
    pd.testing.assert_frame_equal(
        cat_subset.df.reset_index(drop=True), cat.df.reset_index(drop=True)
    )
    assert cat.esmcat.id == name


def test_empty_queries():
    col = intake.open_esm_datastore(cdf_col_sample_cmip6)
    cat = col.search()
    with pytest.warns(
        UserWarning, match=r'There are no datasets to load! Returning an empty dictionary.'
    ):
        dsets = cat.to_dataset_dict()
        assert not dsets


@pytest.mark.parametrize(
    'key',
    [
        'CMIP.CNRM-CERFACS.CNRM-CM6-1.historical.Lmon.gr',
        'CMIP.CNRM-CERFACS.CNRM-CM6-1.piControl.Lmon.gr',
        'CMIP.CNRM-CERFACS.CNRM-ESM2-1.1pctCO2.Omon.gn',
        'CMIP.CNRM-CERFACS.CNRM-ESM2-1.abrupt-4xCO2.Amon.gr',
        'CMIP.CNRM-CERFACS.CNRM-ESM2-1.amip.Amon.gr',
    ],
)
@pytest.mark.parametrize('decode_times', [True, False])
def test_getitem(key, decode_times):
    col = intake.open_esm_datastore(cdf_col_sample_cmip6)
    x = col[key]
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
        multi_variable_col, read_csv_kwargs={'converters': {'variable': ast.literal_eval}}
    )
    assert cat.esmcat.has_multiple_variable_assets

    cat_sub = cat.search(**query)
    assert set(cat_sub._requested_variables) == set(query.pop('variable', []))

    _, ds = cat_sub.to_dataset_dict().popitem()
    if cat_sub._requested_variables:
        assert set(ds.data_vars) == set(cat_sub._requested_variables)


@pytest.mark.parametrize(
    'path, query, xarray_open_kwargs',
    [
        (
            zarr_col_pangeo_cmip6,
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
            cdf_col_sample_cmip6,
            dict(source_id=['CNRM-ESM2-1', 'CNRM-CM6-1', 'BCC-ESM1'], variable_id=['tasmax']),
            {'chunks': {'time': 1}},
        ),
    ],
)
def test_to_dataset_dict(path, query, xarray_open_kwargs):
    cat = intake.open_esm_datastore(path)
    cat_sub = cat.search(**query)
    _, ds = cat_sub.to_dataset_dict(xarray_open_kwargs=xarray_open_kwargs).popitem()
    assert 'member_id' in ds.dims
    assert len(ds.__dask_keys__()) > 0
    assert ds.time.encoding


@pytest.mark.parametrize(
    'path, query',
    [
        (cdf_col_sample_cmip6, {'experiment_id': ['historical', 'rcp85']}),
        (cdf_col_sample_cmip5, {'experiment': ['historical', 'rcp85']}),
    ],
)
def test_to_dataset_dict_aggfalse(path, query):
    col = intake.open_esm_datastore(path)
    cat = col.search(**query)
    nds = len(cat.df)
    dsets = cat.to_dataset_dict(xarray_open_kwargs={'chunks': {'time': 1}}, aggregate=False)
    assert len(dsets.keys()) == nds


@pytest.mark.parametrize(
    'path, query',
    [
        (
            cdf_col_sample_cmip6,
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
    cat = intake.open_esm_datastore(cdf_col_sample_cmip6)
    cat_sub = cat.search(
        **dict(source_id=['CNRM-ESM2-1', 'CNRM-CM6-1', 'BCC-ESM1'], variable_id=['tasmax'])
    )
    with pytest.warns(
        DeprecationWarning,
        match=r'cdf_kwargs and zarr_kwargs are deprecated and will be removed in a future version. Please use xarray_open_kwargs instead.',
    ):
        cat_sub.to_dataset_dict(cdf_kwargs={'chunks': {'time': 1}})


def test_to_dataset_dict_w_preprocess_error():
    cat = intake.open_esm_datastore(cdf_col_sample_cmip5)
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
