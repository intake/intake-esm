import os

import pandas as pd
import pytest
import xarray as xr

from intake_esm.core import _get_subset
from intake_esm.source import ESMGroupDataSource


@pytest.fixture(scope='module')
def df():
    here = os.path.abspath(os.path.dirname(__file__))
    path = os.path.join(here, 'sample-collections/cmip6-netcdf-test.csv')
    return pd.read_csv(path)


@pytest.fixture(scope='module')
def aggregation_dict():
    x = {
        'variable_id': {'type': 'union'},
        'member_id': {'type': 'join_new', 'options': {'coords': 'minimal', 'compat': 'override'}},
        'time_range': {'type': 'join_existing', 'options': {'dim': 'time'}},
    }
    return x


def test_esm_group(df, aggregation_dict):
    subset_df = _get_subset(
        df,
        activity_id='CMIP',
        institution_id='CNRM-CERFACS',
        source_id='CNRM-CM6-1',
        experiment_id='historical',
        table_id='Amon',
        grid_label='gr',
    )
    args = dict(
        df=subset_df,
        aggregation_dict=aggregation_dict,
        path_column='path',
        variable_column='variable_id',
        data_format='netcdf',
        format_column=None,
        cdf_kwargs={'chunks': {'time': 2}},
    )
    source = ESMGroupDataSource(**args)
    assert source._ds is None
    ds = source.to_dask()
    assert isinstance(ds, xr.Dataset)
    assert set(subset_df['member_id']) == set(ds['member_id'].values)
    source.close()
    assert source._ds is None


def test_esm_group_empty_df(df, aggregation_dict):
    empty_df = pd.DataFrame(columns=df.columns)
    args = dict(
        df=empty_df,
        aggregation_dict=aggregation_dict,
        path_column='path',
        variable_column='variable_id',
        data_format='netcdf',
        format_column=None,
        cdf_kwargs={'chunks': {'time': 2}},
    )

    with pytest.raises(ValueError, match=r'`df` must be a non-empty pandas.DataFrame'):
        _ = ESMGroupDataSource(**args)
