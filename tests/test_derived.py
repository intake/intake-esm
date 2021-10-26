import sys

import pytest
import xarray as xr

from intake_esm.derived import DerivedVariable, DerivedVariableError, DerivedVariableRegistry

from .utils import here


def test_registry_init():
    """
    Test the DerivedVariableRegistry class.
    """
    dvr = DerivedVariableRegistry()

    assert dvr._registry == {}
    assert len(dvr.keys()) == 0


def test_registry_load():

    sys.path.insert(0, f'{here}/')
    dvr = DerivedVariableRegistry.load('my_registry')
    assert len(dvr) > 0
    assert 'FOO' in dvr

    # Test for errors/ invalid inputs, wrong return type
    with pytest.raises(ValueError):
        DerivedVariableRegistry.load('utils')


def test_registry_register():
    dvr = DerivedVariableRegistry()

    @dvr.register(variable='FOO', query={'variable': ['BAR']})
    def func(ds):
        return ds.x + ds.y

    assert dvr.keys() == ['FOO']
    assert dvr['FOO'].func == func
    assert 'FOO' in dvr
    assert isinstance(dvr['FOO'], DerivedVariable)
    assert isinstance(iter(dvr), type(iter(dvr._registry)))
    assert len(dvr) == 1


def test_registry_search():
    dvr = DerivedVariableRegistry()

    @dvr.register(variable='FOO', query={'variable': ['BAR']})
    def func(ds):
        return ds.x + ds.y

    @dvr.register(variable='BAZ', query={'variable': ['BAR']})
    def func_b(ds):
        return ds.x + ds.y

    subset = dvr.search(variable='BAZ')
    assert len(subset) == 1
    assert subset['BAZ'].func == func_b


def test_registry_derive_variables():
    ds = xr.tutorial.open_dataset('air_temperature')

    dvr = DerivedVariableRegistry()

    @dvr.register(variable='FOO', query={'variable': 'air'})
    def func(ds):
        ds['FOO'] = ds.air // 100
        return ds

    dsets = dvr.update_datasets(datasets={'test': ds.copy()}, variable_key_name='variable')
    assert 'test' in dsets
    assert 'FOO' in dsets['test']
    assert isinstance(dsets['test']['FOO'], xr.DataArray)


def test_registry_derive_variables_error():
    ds = xr.tutorial.open_dataset('air_temperature')
    dvr = DerivedVariableRegistry()

    @dvr.register(variable='FOO', query={'variable': 'air'})
    def func(ds):
        ds['FOO'] = ds.air // 100
        return ds

    # Test for errors/ invalid inputs, wrong return type
    with pytest.raises(DerivedVariableError):
        dvr['FOO']({})

    @dvr.register(variable='FOO', query={'variable': 'air'})
    def funcb(ds):
        ds['FOO'] = 1 / 0
        return ds

    dsets = dvr.update_datasets(
        datasets={'test': ds.copy()}, variable_key_name='variable', skip_on_error=True
    )

    assert 'FOO' not in dsets['test']

    with pytest.raises(DerivedVariableError):
        dsets = dvr.update_datasets(
            datasets={'test': ds.copy()}, variable_key_name='variable', skip_on_error=False
        )
