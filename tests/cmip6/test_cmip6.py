import os

import intake
import pytest
import xarray as xr

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
    col = intake.open_esm_metadatastore(esmcol_path)
    cat = col.search(**query)
    assert len(cat.df) > 0
    assert len(col.df.columns) == len(cat.df.columns)


@pytest.mark.parametrize('esmcol_path, query', [(zarr_col, zarr_query)])
def test_to_xarray(esmcol_path, query):
    col = intake.open_esm_metadatastore(esmcol_path)
    cat = col.search(**query)
    _, ds = cat.to_xarray().popitem()
    assert isinstance(ds, xr.Dataset)
