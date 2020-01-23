import os
from tempfile import TemporaryDirectory

import intake
import pandas as pd
import pytest
import xarray as xr

here = os.path.abspath(os.path.dirname(__file__))
zarr_col_pangeo_cmip6 = os.path.join(here, 'pangeo-cmip6-zarr.json')
cdf_col_sample_cmip6 = os.path.join(here, 'cmip6-netcdf.json')
cdf_col_sample_cmip5 = os.path.join(here, 'cmip5-netcdf.json')
zarr_col_aws_cesmle = os.path.join(here, 'cesm1-lens-zarr.json')
cdf_col_sample_cesmle = os.path.join(here, 'cesm1-lens-netcdf.json')
catalog_dict_records = os.path.join(here, 'catalog-dict-records.json')


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
            source_id='BCC-ESM1', grid_label='gn', table_id='Amon', experiment_id='historical'
        )

        name = 'cmip6_bcc_esm1'
        col_subset.serialize(name=name, directory=local_store, catalog_type='file')

        col = intake.open_esm_datastore(f'{local_store}/cmip6_bcc_esm1.json')
        pd.testing.assert_frame_equal(col_subset.df, col.df)

        assert col._col_data['id'] == name


@pytest.mark.parametrize(
    'esmcol_path, query', [(zarr_col_pangeo_cmip6, zarr_query), (cdf_col_sample_cmip6, cdf_query)]
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
    'esmcol_path, query', [(zarr_col_pangeo_cmip6, zarr_query), (cdf_col_sample_cmip6, cdf_query)]
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


def test_to_dataset_dict_s3():
    col = intake.open_esm_datastore(zarr_col_aws_cesmle)
    cat = col.search(variable='RAIN', experiment='20C')
    dsets = cat.to_dataset_dict(storage_options={'anon': True})
    _, ds = dsets.popitem()
    assert isinstance(ds, xr.Dataset)


@pytest.mark.parametrize(
    'chunks, expected_chunks',
    [
        ({'time': 100, 'nlat': 2, 'nlon': 2}, (1, 100, 2, 2)),
        ({'time': 200, 'nlat': 1, 'nlon': 1}, (1, 200, 1, 1)),
    ],
)
def test_to_dataset_dict_chunking_2(chunks, expected_chunks):
    c = intake.open_esm_datastore(cdf_col_sample_cesmle)
    query = {'variable': ['SHF'], 'member_id': [1, 3, 9], 'experiment': ['20C', 'RCP85']}
    cat = c.search(**query)
    dset = cat.to_dataset_dict(cdf_kwargs=dict(chunks=chunks))
    _, ds = dset.popitem()
    assert ds['SHF'].data.chunksize == expected_chunks


def test_read_catalog_dict():
    col = intake.open_esm_datastore(catalog_dict_records)
    assert isinstance(col.df, pd.DataFrame)
