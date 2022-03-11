import os

import pytest
import xarray

from intake_esm.source import _get_xarray_open_kwargs, _open_dataset

here = os.path.abspath(os.path.dirname(__file__))


f1 = os.path.join(
    here,
    'sample_data/cmip/cmip5/output1/NIMR-KMA/HadGEM2-AO/rcp85/mon/atmos/Amon/r1i1p1/v20130815/tasmax/tasmax_Amon_HadGEM2-AO_rcp85_r1i1p1_200511-200512.nc',
)
f2 = os.path.join(
    here,
    'sample_data/cmip/cmip5/output1/NIMR-KMA/HadGEM2-AO/rcp85/mon/atmos/Amon/r1i1p1/v20130815/tasmax/tasmax_Amon_HadGEM2-AO_rcp85_r1i1p1_200601-210012.nc',
)

multi_path = os.path.dirname(f1) + '/*.nc'


def _common_open(fpath, varname='tasmax'):
    _xarray_open_kwargs = _get_xarray_open_kwargs('netcdf')
    return _open_dataset(fpath, varname, xarray_open_kwargs=_xarray_open_kwargs).compute()


@pytest.mark.parametrize('fpath,expected_time_size', [(f1, 2), (f2, 2), (multi_path, 4)])
def test_open_dataset(fpath, expected_time_size):
    ds = _common_open(fpath)
    assert isinstance(ds, xarray.Dataset)
    assert len(ds.time) == expected_time_size
    
    
@pytest.mark.parametrize('storage_options',[{'anon': True}, {}])
def test_get_xarray_open_kwargs(storage_options):
    xarray_open_kwargs = _get_xarray_open_kwargs('zarr', storage_options=storage_options)
    assert xarray_open_kwargs['backend_kwargs']['storage_options'] == storage_options
