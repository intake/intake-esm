import pytest
import xarray as xr

from intake_esm.merge_util import _open_asset, join_existing, join_new, union


@pytest.fixture(scope='module')
def datasets():
    ds = xr.tutorial.open_dataset('rasm', decode_times=False)
    return [ds, ds]


def test_join_new(datasets):
    ds = join_new(datasets, 'member_id', ['one', 'two'], 'Tair')
    assert 'member_id' in ds.coords
    assert set(['one', 'two']) == set(ds.member_id.values)


def test_join_new_error(datasets):
    with pytest.raises(ValueError):
        _ = join_new(datasets, 'time', ['one', 'two'], 'Tair')


def test_join_existing(datasets):
    ds = join_existing(datasets, options={'dim': 'time'})
    assert len(ds.time) == len(datasets[0].time) * 2


def test_join_existing_error(datasets):
    with pytest.raises(TypeError):
        join_existing(datasets)

    with pytest.raises(ValueError):
        datasets[0] = datasets[0].rename({'time': 'times'})
        join_existing(datasets, options={'dim': 'time'})


def test_union(datasets):
    from copy import deepcopy

    dsets = deepcopy(datasets)
    dsets[0] = dsets[0].rename({'Tair': 'air'})
    ds = union(dsets)
    assert len(ds.data_vars) == 2
    assert set(['Tair', 'air']) == set(ds.data_vars)


def test_union_error():
    ds = xr.tutorial.open_dataset('rasm', decode_times=False)
    datasets = [ds, ds]
    with pytest.raises(xr.MergeError):
        datasets[0] = datasets[0].rename({'time': 'times'})
        union(datasets)


@pytest.mark.parametrize(
    'path, data_format, error',
    [('file://test', 'zarr', ValueError), ('file://test', 'netcdf', FileNotFoundError)],
)
def test_open_asset_error(path, data_format, error):
    with pytest.raises(error):
        _open_asset(path, data_format, {}, {}, None, 'Tair')
