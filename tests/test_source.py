import os
import xarray

from intake_esm.source import _open_dataset, _get_xarray_open_kwargs

here = os.path.abspath(os.path.dirname(__file__))


f1 = os.path.join(here, "sample_data/cmip/cmip5/output1/NIMR-KMA/HadGEM2-AO/rcp85/mon/atmos/Amon/r1i1p1/v20130815/tasmax/tasmax_Amon_HadGEM2-AO_rcp85_r1i1p1_200511-200512.nc")
f2 = os.path.join(here, "sample_data/cmip/cmip5/output1/NIMR-KMA/HadGEM2-AO/rcp85/mon/atmos/Amon/r1i1p1/v20130815/tasmax/tasmax_Amon_HadGEM2-AO_rcp85_r1i1p1_200601-210012.nc")

multi_path = os.path.dirname(f1) + "/*.nc"


def _common_open(fpath, varname="tasmax"):
    _xarray_open_kwargs = _get_xarray_open_kwargs("netcdf")
    return _open_dataset(fpath, varname, xarray_open_kwargs=_xarray_open_kwargs).compute()


def test_open_dataset_single():
    ds1 = _common_open(f1)
    ds2 = _common_open(f2)

    assert isinstance(ds1, xarray.Dataset)
    assert ds1.time.values[0].isoformat() == "2005-11-16T00:00:00"
    assert ds2.time.values[-1].isoformat() == "2006-02-16T00:00:00" 


def test_open_dataset_multi():
    ds = _common_open(multi_path)

    assert isinstance(ds, xarray.Dataset)
    assert len(ds.time) == 4
    assert ds.time.values[0].isoformat() == "2005-11-16T00:00:00"
    assert ds.time.values[-1].isoformat() == "2006-02-16T00:00:00"
 
