import os

import intake
import pandas as pd
import pytest
import xarray as xr

here = os.path.abspath(os.path.dirname(__file__))
csv_spec = os.path.join(here, 'cmip6-zarr-consolidated-stores.csv')


def test_search():
    col = intake.open_esm_metadatastore(path=csv_spec)
    cat = col.search(
        variable_id=['pr'],
        experiment_id='ssp370',
        activity_id='AerChemMIP',
        source_id='BCC-ESM1',
        table_id='Amon',
        grid_label='gn',
    )
    assert len(cat.df) > 0
    assert len(col.df.columns) == len(cat.df.columns)


def test_to_xarray():
    col = intake.open_esm_metadatastore(path=csv_spec)
    cat = col.search(
        variable_id=['pr'],
        experiment_id='ssp370',
        activity_id='AerChemMIP',
        source_id='BCC-ESM1',
        table_id='Amon',
        grid_label='gn',
    )
    _, ds = cat.to_xarray().popitem()
    assert isinstance(ds, xr.Dataset)
