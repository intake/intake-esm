import os
from unittest import mock as mock

import dask
import pandas as pd
import pytest
import xarray as xr

from intake_esm.search import search
from intake_esm.source import ESMDataSource, ESMGroupDataSource

here = os.path.abspath(os.path.dirname(__file__))
path = os.path.join(here, 'sample-collections/cmip6-netcdf-test.csv')
df = pd.read_csv(path)

aggregation_dict = {
    'variable_id': {'type': 'union'},
    'member_id': {'type': 'join_new', 'options': {'coords': 'minimal', 'compat': 'override'}},
    'time_range': {'type': 'join_existing', 'options': {'dim': 'time'}},
}

subset_df = search(
    df,
    activity_id='CMIP',
    institution_id='CNRM-CERFACS',
    source_id='CNRM-CM6-1',
    experiment_id='historical',
    table_id='Amon',
    grid_label='gr',
)
group_args = dict(
    key='foo',
    df=subset_df,
    aggregation_dict=aggregation_dict,
    path_column='path',
    variable_column='variable_id',
    data_format='netcdf',
    format_column=None,
    cdf_kwargs={'chunks': {'time': 2}},
)

single_row_args = dict(
    key='foo',
    row=df.iloc[0],
    path_column='path',
    data_format='netcdf',
    format_column=None,
    cdf_kwargs={'chunks': {'time': 2}},
)


def test_esm_group():
    source = ESMGroupDataSource(**group_args)
    assert source._ds is None
    ds = source.to_dask()
    assert dask.is_dask_collection(ds['tasmax'])
    assert ds.attrs['intake_esm_dataset_key'] == 'foo'
    assert isinstance(ds, xr.Dataset)
    assert set(subset_df['member_id']) == set(ds['member_id'].values)
    source.close()
    assert source._ds is None


def test_esm_group_repr(capsys):
    source = ESMGroupDataSource(**group_args)
    print(repr(source))
    captured = capsys.readouterr()
    assert 'assets:' in captured.out


def test_esm_group_ipython_display():
    pytest.importorskip('IPython')
    source = ESMGroupDataSource(**group_args)
    with mock.patch('IPython.display.display') as ipy_display:
        source._ipython_display_()
        ipy_display.assert_called_once()


@pytest.mark.parametrize('x', [pd.DataFrame(), pd.Series(dtype='object'), {}, None])
def test_esm_group_invalid_df(x):
    args = dict(
        key='foo',
        df=x,
        aggregation_dict=aggregation_dict,
        path_column='path',
        variable_column='variable_id',
        data_format='netcdf',
        format_column=None,
        cdf_kwargs={'chunks': {'time': 2}},
    )

    with pytest.raises(ValueError, match=r'`df` must be a non-empty pandas.DataFrame'):
        _ = ESMGroupDataSource(**args)


def test_esm_single_source():
    source = ESMDataSource(**single_row_args)
    assert source._ds is None
    ds = source.to_dask()
    assert dask.is_dask_collection(ds['tasmax'])
    assert ds.attrs['intake_esm_dataset_key'] == 'foo'
    assert isinstance(ds, xr.Dataset)
    source.close()
    assert source._ds is None


def test_esm_single_source_repr(capsys):
    source = ESMDataSource(**single_row_args)
    print(repr(source))
    captured = capsys.readouterr()
    assert 'asset: 1' in captured.out


def test_esm_single_source_ipython_display():
    pytest.importorskip('IPython')
    source = ESMDataSource(**single_row_args)
    with mock.patch('IPython.display.display') as ipy_display:
        source._ipython_display_()
        ipy_display.assert_called_once()


@pytest.mark.parametrize('row', [pd.DataFrame(), pd.Series(dtype='object'), {}, None])
def test_esm_single_source_invalid_row(row):
    args = dict(
        key='foo',
        row=row,
        path_column='path',
        data_format='netcdf',
        format_column=None,
        cdf_kwargs={'chunks': {'time': 2}},
    )

    with pytest.raises(ValueError, match=r'`row` must be a non-empty pandas.Series'):
        _ = ESMDataSource(**args)
