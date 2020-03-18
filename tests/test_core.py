import os
from tempfile import TemporaryDirectory

import intake
import pandas as pd
import pytest
import xarray as xr

import intake_esm
from intake_esm.core import _get_dask_client, _get_subset, _normalize_query

here = os.path.abspath(os.path.dirname(__file__))
zarr_col_pangeo_cmip6 = (
    'https://raw.githubusercontent.com/NCAR/intake-esm-datastore/master/catalogs/pangeo-cmip6.json'
)
cdf_col_sample_cmip6 = os.path.join(here, 'sample-collections/cmip6-netcdf.json')
cdf_col_sample_cmip5 = os.path.join(here, 'sample-collections/cmip5-netcdf.json')
zarr_col_aws_cesmle = (
    'https://raw.githubusercontent.com/NCAR/cesm-lens-aws/master/intake-catalogs/aws-cesm1-le.json'
)
cdf_col_sample_cesmle = os.path.join(here, 'sample-collections/cesm1-lens-netcdf.json')
catalog_dict_records = os.path.join(here, 'sample-collections/catalog-dict-records.json')


zarr_query = dict(
    variable_id=['pr'],
    experiment_id='ssp370',
    activity_id='AerChemMIP',
    source_id='BCC-ESM1',
    table_id='Amon',
    grid_label='gn',
)
cdf_query = dict(source_id=['CNRM-ESM2-1', 'CNRM-CM6-1', 'BCC-ESM1'], variable_id=['tasmax'])


def test_repr():
    col = intake.open_esm_datastore(zarr_col_pangeo_cmip6)
    assert 'ESM Collection' in repr(col)


def test_unique():
    col = intake.open_esm_datastore(zarr_col_pangeo_cmip6)
    uniques = col.unique(columns=['activity_id', 'experiment_id'])
    assert isinstance(uniques, dict)


def test_load_esmcol_remote():
    col = intake.open_esm_datastore(
        'https://raw.githubusercontent.com/NCAR/intake-esm-datastore/master/catalogs/pangeo-cmip6.json'
    )
    assert isinstance(col.df, pd.DataFrame)


params = [
    ('CMIP.CNRM-CERFACS.CNRM-CM6-1.historical.*.Amon.*.gr.*', intake_esm.core.esm_datastore),
    ('CMIP.CNRM-CERFACS.CNRM-CM6-1.historical.r4i1p1f2.Amon.tasmax.gr.*', dict),
    ('CMIP.IPSL.IPSL-CM6A-LR.piControl', intake_esm.core.esm_datastore),
    ('CMIP', intake_esm.core.esm_datastore),
    (
        './tests/sample_data/cmip/CMIP6/CMIP/IPSL/IPSL-CM6A-LR/historical/r23i1p1f1/Omon/prsn/gr/v20180803/prsn/prsn_Omon_IPSL-CM6A-LR_historical_r23i1p1f1_gr_185001-201412.nc',
        dict,
    ),
]


@pytest.mark.parametrize('key, object_type', params)
def test_getitem(key, object_type):
    col = intake.open_esm_datastore(cdf_col_sample_cmip6)
    x = col[key]
    assert isinstance(x, object_type)


def test_getitem_error():
    col = intake.open_esm_datastore(cdf_col_sample_cmip6)
    with pytest.raises(KeyError):
        key = 'DOES.NOT.EXIST'
        col[key]


def test_serialize_to_json():
    with TemporaryDirectory() as local_store:
        col = intake.open_esm_datastore(catalog_dict_records)

        name = 'test_serialize_dict'
        col.serialize(name=name, directory=local_store, catalog_type='dict')

        output_catalog = os.path.join(local_store, name + '.json')

        col2 = intake.open_esm_datastore(output_catalog)
        pd.testing.assert_frame_equal(col.df, col2.df)


def test_serialize_to_csv():
    with TemporaryDirectory() as local_store:
        col = intake.open_esm_datastore(
            'https://raw.githubusercontent.com/NCAR/intake-esm-datastore/master/catalogs/pangeo-cmip6.json'
        )
        col_subset = col.search(
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
    col = intake.open_esm_datastore(zarr_col_aws_cesmle)
    cat = col.search(variable='RAIN', experiment='20C')
    dsets = cat.to_dataset_dict(storage_options={'anon': True})
    _, ds = dsets.popitem()
    assert isinstance(ds, xr.Dataset)


def test_read_catalog_dict():
    col = intake.open_esm_datastore(catalog_dict_records)
    assert isinstance(col.df, pd.DataFrame)


def test_to_dataset_dict_w_dask_cluster():
    from distributed import Client

    with Client():
        col = intake.open_esm_datastore(zarr_col_aws_cesmle)
        cat = col.search(variable='RAIN', experiment='20C')
        dsets = cat.to_dataset_dict(storage_options={'anon': True})
        _, ds = dsets.popitem()
        assert isinstance(ds, xr.Dataset)


def test_get_dask_client():
    from unittest import mock
    from distributed import Client
    import sys

    with Client() as client:
        c = _get_dask_client()
        assert c is client

    with mock.patch.dict(sys.modules, {'distributed.client': None}):
        c = _get_dask_client()
        assert c is None

    c = _get_dask_client()
    assert c is None


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
