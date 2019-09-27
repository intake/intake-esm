import os

import intake
import pandas as pd
import pytest
import xarray as xr
import yaml

from intake_esm import config

cdef = yaml.safe_load(
    """
name: AWS-CESM1-LE
collection_type: cesm-aws
data_sources:
  land:
    locations:
      - name: land-monthly
        loc_type: s3
        direct_access: True
        urlpath: s3://ncar-cesm-lens/lnd/monthly
        data_format: zarr

  ocean:
    locations:
      - name: ocean-monthly
        loc_type: s3
        direct_access: True
        urlpath: s3://ncar-cesm-lens/ocn/monthly
        data_format: zarr
"""
)


def test_build_collection_cesm1_aws_le():
    with config.set({'database-directory': './tests/test_collections'}):
        col = intake.open_esm_metadatastore(
            collection_input_definition=cdef, overwrite_existing=True
        )
        assert isinstance(col.df, pd.DataFrame)


def test_search():
    with config.set({'database-directory': './tests/test_collections'}):
        col = intake.open_esm_metadatastore(collection_name='AWS-CESM1-LE')
        cat = col.search(variable=['RAIN', 'FSNO'])
        assert len(cat.df) > 0


def test_to_xarray():
    with config.set({'database-directory': './tests/test_collections'}):
        col = intake.open_esm_metadatastore(collection_name='AWS-CESM1-LE')
        cat = col.search(variable='SALT', experiment='20C')
        dsets = cat.to_xarray()
        _, ds = dsets.popitem()
        assert isinstance(ds, xr.Dataset)
