import os
import tarfile
import tempfile
import warnings
from unittest import mock

import dask
import intake
import numpy as np
import pytest
import xarray
from dask.delayed import DelayedLeaf

from intake_esm.source import (
    ConcatenationWarning,
    ESMDataSourceError,
    _delayed_open_ds,
    _eager_open_ds,
    _expand_dims,
    _get_open_func,
    _get_xarray_open_kwargs,
    _open_dataset,
    _update_attrs,
)
from intake_esm.utils import _zarr_async

from .utils import cdf_cat_sample_cmip6

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
    if storage_options:
        assert xarray_open_kwargs['backend_kwargs']['storage_options'] == storage_options
    else:
        # don't add empty or none options
        assert 'storage_options' not in xarray_open_kwargs['backend_kwargs']


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
        dict(engine='zarr', consolidated=False),
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


def test_no_concat_coordinates_warns_and_retains_first_dataset():
    cat = intake.open_esm_datastore(cdf_cat_sample_cmip6).search(
        source_id='CNRM-CM6-1', variable_id='tasmax', experiment_id='historical'
    )
    # These real member files share coordinates. Without join_new they have
    # no dimension along which combine_by_coords can order the two datasets.
    cat.esmcat.aggregation_control.aggregations = [
        agg for agg in cat.esmcat.aggregation_control.aggregations if agg.type != 'join_new'
    ]
    assert len(cat.df) == 2
    with pytest.warns(ConcatenationWarning, match='retaining only first dataset'):
        ds = cat.to_dask(
            threaded=False,
            xarray_open_kwargs={'engine': 'h5netcdf', 'chunks': {}},
            progressbar=False,
        )
    try:
        with xarray.open_dataset(cat.df.path.iloc[0], engine='h5netcdf') as first:
            xarray.testing.assert_equal(ds.tasmax, first.tasmax)
    finally:
        ds.close()


def test_other_combine_value_errors_are_not_swallowed():
    cat = intake.open_esm_datastore(cdf_cat_sample_cmip6).search(
        source_id='CNRM-CM6-1', variable_id='tasmax', experiment_id='historical'
    )
    cat.esmcat.aggregation_control.aggregations = [
        agg for agg in cat.esmcat.aggregation_control.aggregations if agg.type != 'join_new'
    ]

    def reverse_second_member(ds):
        if ds.attrs['variant_label'] == 'r6i1p1f2':
            return ds.isel(time=slice(None, None, -1))
        return ds

    with warnings.catch_warnings(record=True) as caught:
        with pytest.raises(ESMDataSourceError) as exc:
            cat.to_dask(
                preprocess=reverse_second_member,
                threaded=False,
                xarray_open_kwargs={'engine': 'h5netcdf', 'chunks': {}},
                progressbar=False,
            )
    assert isinstance(exc.value.__cause__, ValueError)
    assert not any(issubclass(w.category, ConcatenationWarning) for w in caught)


def test_expand_dims_coord_variable_regression():
    """A catalog whose `variable` column includes a dimension coordinate (`time`)
    must still load. Previously `_expand_dims` expanded `time` into a
    `(member, time)` coordinate, destroying its index and breaking
    `combine_by_coords` with an `ESMDataSourceError`.
    """
    cat_json = os.path.join(here, 'sample_data/expand-dims-coord/catalog.json')
    cat = intake.open_esm_datastore(cat_json, columns_with_iterables=['variable'], threaded=False)
    dsets = cat.to_dataset_dict(progressbar=False)
    ds = dsets['by647.mon']
    assert 'time' in ds.dims  # time index preserved, not expanded away
    assert ds.sizes['time'] == 3
    assert ds.time.dims == ('time',)
    assert 'time' in ds.indexes
    assert set(ds.member.values) == {'r1', 'r2'}
    for member in ['r1', 'r2']:
        path = os.path.join(here, 'sample_data/expand-dims-coord', f'regression_{member}.nc')
        with xarray.open_dataset(path) as original:
            np.testing.assert_array_equal(ds.time.values, original.time.values)
            np.testing.assert_allclose(ds.tas.sel(member=member).values, original.tas.values)


def test_expand_dims_still_expands_data_variables():
    ds = xarray.Dataset({'tas': ('time', [1.0, 2.0])}, coords={'time': [0, 1]})
    ds.attrs['intake_esm_vars'] = ['tas', 'time']
    result = _expand_dims({'member': ['r1']}, ds)
    assert result.tas.dims == ('member', 'time')
    assert result.time.dims == ('time',)
    np.testing.assert_array_equal(result.tas.values, [[1.0, 2.0]])


@pytest.fixture
def object_dtype_zarr(tmp_path):
    # Keep the existing real NetCDF values, adding a variable-length string
    # variable that Dask cannot automatically chunk.
    with xarray.open_dataset(f1, engine='scipy') as source:
        expected = source.load()
    expected['labels'] = ('sample', np.array(['first', 'second'], dtype=object))
    path = tmp_path / 'object.zarr'
    expected.to_zarr(path, zarr_format=2)
    return str(path), expected


@pytest.mark.parametrize('multi_file', [False, True])
def test_object_dtype_auto_chunk_fallback(object_dtype_zarr, multi_file):
    path, expected = object_dtype_zarr
    kwargs = {'engine': 'zarr', 'chunks': 'auto'}
    open_func = xarray.open_mfdataset if multi_file else xarray.open_dataset
    with pytest.raises(NotImplementedError, match='auto rechunking with object dtype'):
        open_func(path, **kwargs)
    with _open_dataset([path] if multi_file else path, None, xarray_open_kwargs=kwargs) as actual:
        xarray.testing.assert_equal(actual.compute(), expected)


@pytest.mark.parametrize('chunks', [None, {}, {'sample': 1}])
def test_object_dtype_preserves_explicit_chunks(object_dtype_zarr, chunks):
    path, expected = object_dtype_zarr
    kwargs = {'engine': 'zarr', 'chunks': chunks}
    with _open_dataset(path, None, xarray_open_kwargs=kwargs) as actual:
        xarray.testing.assert_equal(actual.compute(), expected)
        if chunks is None:
            assert actual.labels.chunks is None
        elif chunks == {'sample': 1}:
            assert actual.labels.chunks == ((1, 1),)
    assert kwargs['chunks'] == chunks


def test_object_dtype_does_not_override_explicit_auto_dimension(object_dtype_zarr):
    path, _ = object_dtype_zarr
    with pytest.raises(NotImplementedError, match='auto rechunking with object dtype'):
        _open_dataset(
            path, None, xarray_open_kwargs={'engine': 'zarr', 'chunks': {'sample': 'auto'}}
        )


def test_chunk_fallback_preserves_unrelated_error():
    with pytest.raises(ValueError, match='unrecognized engine'):
        _open_dataset(f1, None, xarray_open_kwargs={'engine': 'not-an-engine', 'chunks': 'auto'})


def test_numeric_and_cftime_keep_auto_chunks():
    kwargs = {'engine': 'scipy', 'chunks': 'auto'}
    with _open_dataset(f1, None, xarray_open_kwargs=kwargs) as actual:
        with xarray.open_dataset(f1, engine='scipy') as expected:
            xarray.testing.assert_equal(actual.compute(), expected)
        assert actual.tasmax.chunks is not None
    assert kwargs['chunks'] == 'auto'
