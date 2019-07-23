import pytest
import xarray as xr

from intake_esm import aggregate


@pytest.fixture
def dsets():
    ds = xr.tutorial.open_dataset('rasm', decode_times=False, chunks={'time': 12})
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
    d['TS'] = d['Tair']
    dsets[0] = d
    ds = aggregate.merge(dsets)
    assert isinstance(ds, xr.Dataset)
    assert 'TS' in ds.data_vars
    xr.testing.assert_identical(ds.time, dsets[0].time)


def test_concat_time_levels(dsets):
    dsets[0]['time'].data = (dsets[0].time + (3 * 365.0)).data
    ds = aggregate.concat_time_levels(dsets, 'time')
    assert ds.time.shape == (72,)


def test_concat_ensembles(dsets):
    ds = aggregate.concat_ensembles(dsets)
    assert 'member_id' in ds.coords
    assert ds.Tair.shape == (2, 36, 205, 275)


def test_concat_ensembles_round_diff(dsets):
    dsets[1]['xc'].data = (dsets[1].xc + 0.0000001).data
    ds = aggregate.concat_ensembles(dsets)
    ds = ds.set_coords(['xc', 'yc'])
    assert 'member_id' in ds.coords
    assert ds.Tair.shape == (2, 36, 205, 275)
    xr.testing.assert_identical(ds.xc, dsets[0].xc)
