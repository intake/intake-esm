import os
from tempfile import TemporaryDirectory

import intake
import numpy as np
import pandas as pd
import pytest
import xarray as xr

from intake_esm.core import _get_subset, _normalize_query, _unique

here = os.path.abspath(os.path.dirname(__file__))
zarr_col_pangeo_cmip6 = (
    'https://raw.githubusercontent.com/NCAR/intake-esm-datastore/master/catalogs/pangeo-cmip6.json'
)
cdf_col_sample_cmip6 = os.path.join(here, 'sample-collections/cmip6-netcdf.json')
cdf_col_sample_cmip5 = os.path.join(here, 'sample-collections/cmip5-netcdf.json')
cdf_col_sample_cesmle = os.path.join(here, 'sample-collections/cesm1-lens-netcdf.json')
catalog_dict_records = os.path.join(here, 'sample-collections/catalog-dict-records.json')


@pytest.fixture(scope='module')
def pangeo_cmip6_col():
    url = 'https://raw.githubusercontent.com/NCAR/intake-esm-datastore/master/catalogs/pangeo-cmip6.json'
    return intake.open_esm_datastore(url)


zarr_query = dict(
    variable_id=['pr'],
    experiment_id='ssp370',
    activity_id='AerChemMIP',
    source_id='BCC-ESM1',
    table_id='Amon',
    grid_label='gn',
)
cdf_query = dict(source_id=['CNRM-ESM2-1', 'CNRM-CM6-1', 'BCC-ESM1'], variable_id=['tasmax'])


def test_repr(pangeo_cmip6_col):
    assert 'ESM Collection' in repr(pangeo_cmip6_col)


def test_log_level_error():
    with pytest.raises(ValueError):
        intake.open_esm_datastore(cdf_col_sample_cmip6, log_level='VERBOSE')


def test_col_unique(pangeo_cmip6_col):
    uniques = pangeo_cmip6_col.unique(columns=['activity_id', 'experiment_id'])
    assert isinstance(uniques, dict)
    assert isinstance(pangeo_cmip6_col.nunique(), pd.Series)


def test_unique():
    df = pd.DataFrame(
        {
            'path': ['file1', 'file2', 'file3', 'file4'],
            'variable': [['A', 'B'], ['A', 'B', 'C'], ['C', 'D', 'A'], 'C'],
            'attr': [1, 2, 3, np.nan],
            'random': [set(['bx', 'by']), set(['bx', 'bz']), set(['bx', 'by']), None],
        }
    )
    expected = {
        'path': {'count': 4, 'values': ['file1', 'file2', 'file3', 'file4']},
        'variable': {'count': 4, 'values': ['A', 'B', 'C', 'D']},
        'attr': {'count': 3, 'values': [1.0, 2.0, 3.0]},
        'random': {'count': 3, 'values': ['bx', 'by', 'bz']},
    }
    actual = _unique(df, df.columns.tolist())
    assert actual == expected

    actual = _unique(df)
    assert actual == expected

    actual = _unique(df, columns='random')
    expected = {'random': {'count': 3, 'values': ['bx', 'by', 'bz']}}
    assert actual == expected


def test_load_esmcol_remote(pangeo_cmip6_col):
    assert isinstance(pangeo_cmip6_col.df, pd.DataFrame)


def test_serialize_to_json():
    with TemporaryDirectory() as local_store:
        col = intake.open_esm_datastore(catalog_dict_records)

        name = 'test_serialize_dict'
        col.serialize(name=name, directory=local_store, catalog_type='dict')

        output_catalog = os.path.join(local_store, name + '.json')

        col2 = intake.open_esm_datastore(output_catalog)
        pd.testing.assert_frame_equal(col.df, col2.df)


def test_serialize_to_csv(pangeo_cmip6_col):
    with TemporaryDirectory() as local_store:
        col_subset = pangeo_cmip6_col.search(
            source_id='BCC-ESM1', grid_label='gn', table_id='Amon', experiment_id='historical',
        )
        name = 'cmip6_bcc_esm1'
        col_subset.serialize(name=name, directory=local_store, catalog_type='file')
        col = intake.open_esm_datastore(f'{local_store}/cmip6_bcc_esm1.json')
        pd.testing.assert_frame_equal(col_subset.df, col.df)
        assert col._col_data['id'] == name


@pytest.mark.parametrize(
    'esmcol_path, query', [(zarr_col_pangeo_cmip6, zarr_query), (cdf_col_sample_cmip6, cdf_query)],
)
def test_search(esmcol_path, query):
    col = intake.open_esm_datastore(esmcol_path)
    cat = col.search(**query)
    assert len(cat.df) > 0
    assert len(col.df.columns) == len(cat.df.columns)


@pytest.mark.parametrize(
    'esmcol_path, query, kwargs',
    [
        (zarr_col_pangeo_cmip6, zarr_query, {}),
        (cdf_col_sample_cmip6, cdf_query, {'chunks': {'time': 1}}),
    ],
)
def test_to_dataset_dict(esmcol_path, query, kwargs):
    col = intake.open_esm_datastore(esmcol_path)
    cat = col.search(**query)
    if kwargs:
        _, ds = cat.to_dataset_dict(zarr_kwargs={'consolidated': True}, cdf_kwargs=kwargs).popitem()
    else:
        _, ds = cat.to_dataset_dict().popitem()
    assert 'member_id' in ds.dims
    assert len(ds.__dask_keys__()) > 0
    assert ds.time.encoding


@pytest.mark.parametrize('esmcol_path, query', [(cdf_col_sample_cmip6, cdf_query)])
def test_to_dataset_dict_aggfalse(esmcol_path, query):
    col = intake.open_esm_datastore(esmcol_path)
    cat = col.search(**query)
    nds = len(cat.df)
    dsets = cat.to_dataset_dict(zarr_kwargs={'consolidated': True}, aggregate=False)
    assert len(dsets.keys()) == nds
    key, ds = dsets.popitem()
    assert 'tasmax' in key


@pytest.mark.parametrize(
    'esmcol_path, query, kwargs',
    [
        (zarr_col_pangeo_cmip6, zarr_query, {}),
        (cdf_col_sample_cmip6, cdf_query, {'chunks': {'time': 1}}),
    ],
)
def test_to_dataset_dict_w_preprocess(esmcol_path, query, kwargs):
    def rename_coords(ds):
        return ds.rename({'lon': 'longitude', 'lat': 'latitude'})

    col = intake.open_esm_datastore(esmcol_path)
    col_sub = col.search(**query)
    dsets = col_sub.to_dataset_dict(zarr_kwargs={'consolidated': True}, preprocess=rename_coords)
    _, ds = dsets.popitem()
    assert 'latitude' in ds.dims
    assert 'longitude' in ds.dims


def test_to_dataset_dict_w_cmip6preprocessing(pangeo_cmip6_col):
    pytest.importorskip('cmip6_preprocessing')
    from cmip6_preprocessing.preprocessing import combined_preprocessing

    cat = pangeo_cmip6_col.search(
        source_id='BCC-CSM2-MR',
        experiment_id='historical',
        table_id='Omon',
        variable_id='thetao',
        member_id='r1i1p1f1',
    )
    _, ds = cat.to_dataset_dict(
        zarr_kwargs={'consolidated': True, 'decode_times': False}, preprocess=combined_preprocessing
    ).popitem()
    assert isinstance(ds, xr.Dataset)


@pytest.mark.parametrize(
    'esmcol_path, query', [(zarr_col_pangeo_cmip6, zarr_query), (cdf_col_sample_cmip6, cdf_query)],
)
def test_to_dataset_dict_nocache(esmcol_path, query):
    col = intake.open_esm_datastore(esmcol_path)
    cat = col.search(**query)
    _, ds = cat.to_dataset_dict(zarr_kwargs={'consolidated': True}).popitem()
    id1 = id(ds)
    cat = col.search(**query)
    _, ds = cat.to_dataset_dict(zarr_kwargs={'consolidated': True}).popitem()
    assert id1 != id(ds)


@pytest.mark.skip(reason='LDEO opendap servers seem not be working properly')
def test_opendap_endpoint():
    col = intake.open_esm_datastore('http://haden.ldeo.columbia.edu/catalogs/hyrax_cmip6.json')
    cat = col.search(
        source_id='CAMS-CSM1-0',
        experiment_id='historical',
        member_id='r1i1p1f1',
        table_id='Amon',
        grid_label='gn',
        version='v1',
    )
    dsets = cat.to_dataset_dict(cdf_kwargs={'chunks': {'time': 36}})
    _, ds = dsets.popitem()
    assert isinstance(ds, xr.Dataset)


@pytest.mark.parametrize(
    'chunks, expected_chunks',
    [
        ({'time': 1, 'lat': 2, 'lon': 2}, (1, 1, 2, 2)),
        ({'time': 2, 'lat': 1, 'lon': 1}, (1, 2, 1, 1)),
    ],
)
def test_to_dataset_dict_chunking(chunks, expected_chunks):
    c = intake.open_esm_datastore(cdf_col_sample_cmip5)
    cat = c.search(variable=['hfls'], frequency='mon', modeling_realm='atmos', model=['CNRM-CM5'])

    dset = cat.to_dataset_dict(cdf_kwargs=dict(chunks=chunks))
    _, ds = dset.popitem()
    assert ds['hfls'].data.chunksize == expected_chunks


@pytest.mark.parametrize('progressbar', [False, True])
def test_progressbar(progressbar):
    c = intake.open_esm_datastore(cdf_col_sample_cmip5)
    cat = c.search(variable=['hfls'], frequency='mon', modeling_realm='atmos', model=['CNRM-CM5'])
    _ = cat.to_dataset_dict(cdf_kwargs=dict(chunks={}), progressbar=progressbar)


def test_to_dataset_dict_s3():
    pytest.importorskip('s3fs')
    zarr_col_aws_cesmle = 'https://raw.githubusercontent.com/NCAR/cesm-lens-aws/master/intake-catalogs/aws-cesm1-le.json'
    col = intake.open_esm_datastore(zarr_col_aws_cesmle)
    cat = col.search(variable='RAIN', experiment='20C')
    dsets = cat.to_dataset_dict(storage_options={'anon': True})
    _, ds = dsets.popitem()
    assert isinstance(ds, xr.Dataset)


def test_read_catalog_dict():
    col = intake.open_esm_datastore(catalog_dict_records)
    assert isinstance(col.df, pd.DataFrame)


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
]


@pytest.mark.parametrize('query, require_all_on, expected', params)
def test_get_subset(query, require_all_on, expected):
    df = pd.DataFrame(
        {
            'A': ['NCAR', 'IPSL', 'IPSL', 'CSIRO', 'IPSL', 'NCAR', 'NOAA', 'NCAR'],
            'B': ['CESM', 'FOO', 'FOO', 'BAR', 'FOO', 'CESM', 'GCM', 'WACM'],
            'C': ['hist', 'control', 'hist', 'control', 'hist', 'control', 'hist', 'hist'],
            'D': ['O2', 'O2', 'O2', 'O2', 'NO2', 'O2', 'O2', 'TA'],
        }
    )

    x = _get_subset(df, require_all_on=require_all_on, **query).to_dict(orient='records')
    assert x == expected


def test_normalize_query():
    query = {'experiment_id': ['historical', 'piControl'], 'variable_id': 'tas', 'table_id': 'Amon'}
    expected = {
        'experiment_id': ['historical', 'piControl'],
        'variable_id': ['tas'],
        'table_id': ['Amon'],
    }
    actual = _normalize_query(query)
    assert actual == expected
