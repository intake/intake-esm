import os
import tarfile
import tempfile

import dask
import pytest
import xarray

from intake_esm.source import _get_xarray_open_kwargs, _open_dataset, _update_attrs

dask.config.set(scheduler='single-threaded')


here = os.path.abspath(os.path.dirname(__file__))


f1 = os.path.join(
    here,
    'sample_data/cmip/cmip5/output1/NIMR-KMA/HadGEM2-AO/rcp85/mon/atmos/Amon/r1i1p1/v20130815/tasmax/tasmax_Amon_HadGEM2-AO_rcp85_r1i1p1_200511-200512.nc',
)
f2 = os.path.join(
    here,
    'sample_data/cmip/cmip5/output1/NIMR-KMA/HadGEM2-AO/rcp85/mon/atmos/Amon/r1i1p1/v20130815/tasmax/tasmax_Amon_HadGEM2-AO_rcp85_r1i1p1_200601-210012.nc',
)

kerchunk_file = os.path.join(
    here,
    'sample_data/kerchunk-files/noaa-nwm-test-reference.json',
)

multi_path = f'{os.path.dirname(f1)}/*.nc'


def _create_tmp_folder():
    tmpdir = tempfile.mkdtemp()
    return tmpdir


def _create_tar_file(ipath):
    tmp_folder = _create_tmp_folder()
    tar_fn = tmp_folder + '/test.tar'
    basename = os.path.basename(ipath)
    with tarfile.open(tar_fn, 'w') as tar:
        tar.add(ipath, arcname=basename)
    return tar_fn


tar_path = _create_tar_file(f1)
tar_url = f'tar://{os.path.basename(f1)}::{tar_path}'


def _common_open(fpath, varname='tasmax', engine=None):
    _xarray_open_kwargs = _get_xarray_open_kwargs('netcdf')
    if engine is not None:
        _xarray_open_kwargs['engine'] = engine
    return _open_dataset(fpath, varname, xarray_open_kwargs=_xarray_open_kwargs).compute()


@pytest.mark.parametrize(
    'fpath,expected_time_size,engine',
    [(f1, 2, None), (f2, 2, None), (multi_path, 4, None), (tar_url, 2, 'scipy')],
)
def test_open_dataset(fpath, expected_time_size, engine):
    ds = _common_open(fpath, engine=engine)
    assert isinstance(ds, xarray.Dataset)
    assert len(ds.time) == expected_time_size


@pytest.mark.parametrize('storage_options', [{'anon': True}, {}])
def test_get_xarray_open_kwargs(storage_options):
    xarray_open_kwargs = _get_xarray_open_kwargs('zarr', storage_options=storage_options)
    assert xarray_open_kwargs['backend_kwargs']['storage_options'] == storage_options


def test_open_dataset_kerchunk(kerchunk_file=kerchunk_file):
    xarray_open_kwargs = _get_xarray_open_kwargs(
        'reference',
        dict(engine='zarr', consolidated=False),
        storage_options={'remote_protocol': 's3', 'remote_options': {'anon': True}},
    )
    ds = _open_dataset(
        data_format='reference',
        urlpath=kerchunk_file,
        varname=None,
        xarray_open_kwargs=xarray_open_kwargs,
    ).compute()
    assert isinstance(ds, xarray.Dataset)


@pytest.mark.parametrize('data_format', ['zarr', 'netcdf'])
@pytest.mark.parametrize('attrs', [{}, {'units': 'K'}, {'variables': ['foo', 'bar']}])
def test_update_attrs(tmp_path, data_format, attrs):
    fpath = tmp_path / 'test.nc' if data_format == 'netcdf' else tmp_path / 'test.zarr'
    fpath = str(fpath)
    ds = _common_open(f1)
    ds = _update_attrs(ds=ds, additional_attrs=attrs)
    if data_format == 'netcdf':
        ds.to_netcdf(fpath)
    else:
        ds.to_zarr(fpath)

    _xarray_open_kwargs = _get_xarray_open_kwargs(data_format=data_format)
    ds_new = _open_dataset(fpath, 'tasmax', xarray_open_kwargs=_xarray_open_kwargs).compute()
    assert ds_new.attrs == ds.attrs
