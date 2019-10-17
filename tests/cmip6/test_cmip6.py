import os

import intake
import pandas as pd
import pytest

here = os.path.abspath(os.path.dirname(__file__))
zarr_col = os.path.join(here, 'pangeo-cmip6-zarr.json')
cdf_col = os.path.join(here, 'cmip6-netcdf.json')
zarr_query = dict(
    variable_id=['pr'],
    experiment_id='ssp370',
    activity_id='AerChemMIP',
    source_id='BCC-ESM1',
    table_id='Amon',
    grid_label='gn',
)
cdf_query = dict(source_id=['CNRM-ESM2-1', 'CNRM-CM6-1', 'BCC-ESM1'], variable_id=['tasmax'])


@pytest.mark.parametrize('esmcol_path, query', [(zarr_col, zarr_query), (cdf_col, cdf_query)])
def test_search(esmcol_path, query):
    col = intake.open_esm_datastore(esmcol_path)
    cat = col.search(**query)
    assert len(cat.df) > 0
    assert len(col.df.columns) == len(cat.df.columns)


@pytest.mark.parametrize(
    'esmcol_path, query, kwargs',
    [(zarr_col, zarr_query, {}), (cdf_col, cdf_query, {'chunks': {'time': 1}})],
)
def test_to_dataset_dict(esmcol_path, query, kwargs):
    col = intake.open_esm_datastore(esmcol_path)
    cat = col.search(**query)
    if kwargs:
        _, ds = cat.to_dataset_dict(cdf_kwargs=kwargs).popitem()
    _, ds = cat.to_dataset_dict().popitem()
    assert 'member_id' in ds.dims
    assert len(ds.__dask_keys__()) > 0


@pytest.mark.parametrize(
    'esmcol_path, query, kwargs',
    [(zarr_col, zarr_query, {}), (cdf_col, cdf_query, {'chunks': {'time': 1}})],
)
def test_to_dataset_dict_w_preprocess(esmcol_path, query, kwargs):
    def rename_coords(ds):
        return ds.rename({'lon': 'longitude', 'lat': 'latitude'})

    col = intake.open_esm_datastore(esmcol_path)
    col_sub = col.search(**query)

    dsets = col_sub.to_dataset_dict(preprocess=rename_coords)
    _, ds = dsets.popitem()
    assert 'latitude' in ds.dims
    assert 'longitude' in ds.dims


@pytest.mark.parametrize('esmcol_path, query', [(zarr_col, zarr_query), (cdf_col, cdf_query)])
def test_to_dataset_dict_nocache(esmcol_path, query):
    col = intake.open_esm_datastore(esmcol_path)

    cat = col.search(**query)
    _, ds = cat.to_dataset_dict().popitem()

    id1 = id(ds)

    cat = col.search(**query)
    _, ds = cat.to_dataset_dict().popitem()

    assert id1 != id(ds)


def test_repr():
    col = intake.open_esm_datastore(zarr_col)
    assert 'ESM Collection' in repr(col)


def test_unique():
    col = intake.open_esm_datastore(zarr_col)
    uniques = col.unique(columns=['activity_id', 'experiment_id'])
    assert isinstance(uniques, dict)


def test_load_esmcol_remote():
    col = intake.open_esm_datastore(
        'https://raw.githubusercontent.com/NCAR/intake-esm-datastore/master/catalogs/pangeo-cmip6.json'
    )
    assert isinstance(col.df, pd.DataFrame)
