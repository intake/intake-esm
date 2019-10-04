import os

import intake
import pytest
import xarray as xr

here = os.path.abspath(os.path.dirname(__file__))
csv_zarr = os.path.join(here, 'cmip6-zarr-consolidated-stores.csv')
csv_cdf = os.path.join(here, 'cmip6-netcdf-test.csv')
zarr_query = dict(
    variable_id=['pr'],
    experiment_id='ssp370',
    activity_id='AerChemMIP',
    source_id='BCC-ESM1',
    table_id='Amon',
    grid_label='gn',
)
cdf_query = dict(source_id=['CNRM-ESM2-1', 'CNRM-CM6-1', 'BCC-ESM1'], variable_id=['tasmax'])


@pytest.mark.parametrize('path, query', [(csv_zarr, zarr_query), (csv_cdf, cdf_query)])
def test_search(path, query):
    col = intake.open_esm_metadatastore(path=csv_zarr)
    cat = col.search(**query)
    assert len(cat.df) > 0
    assert len(col.df.columns) == len(cat.df.columns)


@pytest.mark.parametrize('path, query', [(csv_zarr, zarr_query)])
def test_to_xarray(path, query):
    col = intake.open_esm_metadatastore(path=csv_zarr)
    cat = col.search(**query)
    _, ds = cat.to_xarray().popitem()
    assert isinstance(ds, xr.Dataset)
