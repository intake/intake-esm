import os
import tarfile
import tempfile
from unittest import mock

import dask
import pytest
import xarray
from dask.delayed import DelayedLeaf

from intake_esm.source import (
    _delayed_open_ds,
    _eager_open_ds,
    _get_open_func,
    _get_xarray_open_kwargs,
    _open_dataset,
    _update_attrs,
)
from intake_esm.utils import _zarr_async

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


@pytest.mark.parametrize('xr_version, chunk_default', [('2025.10.0', {}), ('2025.11.0', 'auto')])
def test_get_xarray_open_kwargs_chunk_default(xr_version, chunk_default):
    # patch xr.__version__ to test different default chunking behavior
    with mock.patch('xarray.__version__', xr_version):
        xarray_open_kwargs = _get_xarray_open_kwargs('zarr')
    assert xarray_open_kwargs.get('chunks', None) == chunk_default


def test_open_dataset_kerchunk(kerchunk_file=kerchunk_file):
    # Need to drop crs here as it's an object dtype and not cftime - breaks auto
    # chunking
    xarray_open_kwargs = _get_xarray_open_kwargs(
        'reference',
        dict(engine='zarr', consolidated=False, drop_variables='crs'),
        storage_options={
            'remote_protocol': 's3',
            'remote_options': {'anon': True, 'asynchronous': _zarr_async()},
        },
    )

    ds = _open_dataset(
        data_format='reference',
        urlpath=kerchunk_file,
        varname=None,
        xarray_open_kwargs=xarray_open_kwargs,
    )
    assert isinstance(ds, xarray.Dataset)


@pytest.mark.parametrize(
    'urlpath',
    [
        'https://data.gdex.ucar.edu/d633000/kerchunk/meanflux/Mean_convective_precipitation_rate-https.json'
    ],
)
@pytest.mark.parametrize('varname', ['tmp2m-hgt-an-gauss'])
def test_open_dataset_kerchunk_engine(urlpath, varname):
    """
    Test opening kerchunk datasets with the kerchunk engine.
    This tests the code path: `elif xarray_open_kwargs['engine'] == 'kerchunk' and data_format == 'reference'`

    Tests remote HTTPS URLs to ensure the kerchunk engine
    workflow handles correctly.
    """
    xarray_open_kwargs = _get_xarray_open_kwargs('reference', dict(engine='kerchunk', chunks={}))

    ds = _open_dataset(
        data_format='reference',
        urlpath=urlpath,
        varname=varname,
        xarray_open_kwargs=xarray_open_kwargs,
    )
    assert isinstance(ds, xarray.Dataset)


def test_open_dataset_kerchunk_engine_local(kerchunk_file=kerchunk_file):
    """
    Test opening kerchunk datasets with the kerchunk engine for local reference file
    This tests the code path:
    `elif fsspec.utils.can_be_local(urlpath) and xarray_open_kwargs['engine'] != 'kerchunk':`

    Tests local path to ensure the kerchunk engine
    workflow handles correctly.
    """
    xarray_open_kwargs = _get_xarray_open_kwargs(
        'reference',
        dict(
            engine='kerchunk',
            chunks={},
            backend_kwargs={
                'storage_options': {'remote_protocol': 's3', 'remote_options': {'anon': True}}
            },
        ),
    )

    ds = _open_dataset(
        data_format='reference',
        urlpath=kerchunk_file,
        varname=None,
        xarray_open_kwargs=xarray_open_kwargs,
    )
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


@pytest.mark.parametrize(
    'fpath,dvars,cvars,expected',
    [
        (
            f1,
            ['time_bnds'],
            [''],
            ['time_bnds', 'height', 'time'],
        ),
        (f1, ['tasmax'], [''], ['tasmax', 'height', 'time', 'lat', 'lon']),
        (
            f1,
            [],
            ['height'],
            ['height'],
        ),
        (
            f1,
            [],
            [],
            ['height', 'time_bnds', 'lon_bnds', 'lat_bnds', 'tasmax', 'time', 'lat', 'lon'],
        ),
        (multi_path, ['time_bnds'], [''], ['time_bnds', 'height', 'time']),
        (
            multi_path,
            ['tasmax'],
            [''],
            ['tasmax', 'time', 'height', 'lat', 'lon'],
        ),
        (multi_path, [], ['height'], ['height']),
        (
            multi_path,
            [],
            [],
            ['time_bnds', 'lon_bnds', 'lat_bnds', 'tasmax', 'time', 'height', 'lat', 'lon'],
        ),
    ],
)
def test_request_coord_vars(fpath, dvars, cvars, expected):
    """
    Test requesting a combination of data & coordinate variables.
    """
    requested_vars = [*dvars, *cvars]
    xarray_open_kwargs = _get_xarray_open_kwargs('netcdf')
    ds = _open_dataset(
        urlpath=fpath,
        varname=['height', 'lat', 'lat_bnds', 'lon', 'lon_bnds', 'tasmax', 'time', 'time_bnds'],
        xarray_open_kwargs=xarray_open_kwargs,
        requested_variables=requested_vars,
    ).compute()

    ds_dvars = ds.data_vars or set()
    ds_cvars = ds.coords or set()

    found_vars = set(ds_dvars) | set(ds_cvars)

    assert found_vars == set(expected)


@pytest.mark.parametrize(
    'threaded, expected',
    [
        (True, _delayed_open_ds),
        (False, _eager_open_ds),
    ],
)
def test_get_open_func(threaded, expected):
    """Test that the correct open function is returned based on the threaded argument."""
    open_func = _get_open_func(threaded)
    if not threaded:
        assert open_func == _eager_open_ds
    else:
        assert isinstance(open_func, DelayedLeaf)
