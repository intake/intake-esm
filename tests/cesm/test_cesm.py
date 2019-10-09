import os

import intake
import pytest
import xarray as xr

here = os.path.abspath(os.path.dirname(__file__))
zarr_col = os.path.join(here, 'cesm1-lens-zarr.json')
cdf_col = os.path.join(here, 'cesm1-lens-netcdf.json')


def test_search():
    col = intake.open_esm_metadatastore(cdf_col)
    cat = col.search(variable=['SHF'])
    assert len(cat.df) > 0


def test_to_xarray_zarr():
    col = intake.open_esm_metadatastore(zarr_col)
    cat = col.search(variable='RAIN', experiment='20C')
    dsets = cat.to_xarray()
    _, ds = dsets.popitem()
    assert isinstance(ds, xr.Dataset)


@pytest.mark.parametrize(
    'chunks, expected_chunks',
    [
        ({'time': 100, 'nlat': 2, 'nlon': 2}, (100, 2, 2)),
        ({'time': 200, 'nlat': 1, 'nlon': 1}, (200, 1, 1)),
    ],
)
def test_to_xarray_cesm_netcdf(chunks, expected_chunks):
    c = intake.open_esm_metadatastore(cdf_col)
    query = {'variable': ['SHF'], 'member_id': [1, 3, 9], 'experiment': ['20C', 'RCP85']}
    cat = c.search(**query)
    dset = cat.to_xarray(cdf_kwargs=dict(chunks=chunks))
    _, ds = dset.popitem()
    assert ds['SHF'].data.chunksize == expected_chunks
