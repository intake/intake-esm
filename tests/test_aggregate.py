import pytest
import xarray as xr

from intake_esm import aggregate


@pytest.fixture
def dsets():
    ds = xr.tutorial.open_dataset('air_temperature', decode_times=False, chunks={'time': 12})
    ds1 = ds.copy(True)
    ds2 = ds.copy(True)
    ds1.attrs['tracking_id'] = 'abc'
    ds2.attrs['tracking_id'] = 'xyz'
    return [ds1, ds2]


def test_ensure_time_coord_name(dsets):
    t = aggregate.ensure_time_coord_name(dsets[0], 'time')
    assert t == 'time'


def test_dict_union(dsets):
    u = aggregate.dict_union(*[ds.attrs for ds in dsets])
    assert isinstance(u, dict)

    v = aggregate.dict_union(dsets[0].attrs)
    assert v == dsets[0].attrs


def test_merge(dsets):
    d = dsets[0]
    d['Tair'] = d['air']
    dsets[0] = d
    ds = aggregate.merge(dsets)
    assert isinstance(ds, xr.Dataset)
    assert 'Tair' in ds.data_vars
    xr.testing.assert_identical(ds.time, dsets[0].time)


def test_concat_time_levels(dsets):
    dsets[0]['time'].data = (dsets[0].time + (2 * 2920.0)).data
    ds = aggregate.concat_time_levels(dsets, 'time')
    assert ds.time.shape == (5840,)


def test_concat_ensembles(dsets):
    ds = aggregate.concat_ensembles(dsets)
    assert 'member_id' in ds.coords
    assert ds.air.shape == (2, 2920, 25, 53)


def test_concat_ensembles_round_diff(dsets):
    dsets[1]['lat'].data = (dsets[1].lat + 0.0000001).data
    ds = aggregate.concat_ensembles(dsets)
    assert 'member_id' in ds.coords
    assert ds.air.shape == (2, 2920, 25, 53)
    xr.testing.assert_identical(ds.lat, dsets[0].lat)


def test_drop_additional_coord_dims(dsets):
    dsets[1]['random_coord'] = dsets[1].lat * 0.001
    dsets[1] = dsets[1].set_coords('random_coord')
    dsets[1]['random_var'] = xr.DataArray(range(len(dsets[1].lat)), dims=['random_coord'])

    ds = aggregate.concat_ensembles(dsets)
    assert 'random_var' not in set(ds.variables)
    assert 'random_coord' not in set(ds.variables)
    assert ds.air.shape == (2, 2920, 25, 53)
