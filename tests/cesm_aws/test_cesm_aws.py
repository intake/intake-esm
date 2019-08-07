import os

import intake
import pandas as pd
import pytest
import xarray as xr
import yaml

from intake_esm import config

CIRCLE_CI_CHECK = os.environ.get('CIRCLECI', False)
if CIRCLE_CI_CHECK:
    profile_name = None

else:
    profile_name = 'intake-esm-tester'

storage_options = {'anon': False, 'profile_name': profile_name}
cdef = yaml.safe_load(
    """
name: AWS-CESM1-LE
collection_type: cesm-aws
data_sources:
  land:
    locations:
      - name: land-monthly
        loc_type: aws-s3
        direct_access: True
        urlpath: s3://ncar-cesm-lens/lnd/monthly
        file_extension: .zarr

  ocean:
    locations:
      - name: ocean-monthly
        loc_type: aws-s3
        direct_access: True
        urlpath: s3://ncar-cesm-lens/ocn/monthly
        file_extension: .zarr
"""
)


def test_build_collection_cesm1_aws_le():
    with config.set({'database-directory': './tests/test_collections'}):
        col = intake.open_esm_metadatastore(
            collection_input_definition=cdef,
            overwrite_existing=True,
            storage_options=storage_options,
        )
        assert isinstance(col._ds, xr.Dataset)


def test_search():
    with config.set({'database-directory': './tests/test_collections'}):
        col = intake.open_esm_metadatastore(
            collection_name='AWS-CESM1-LE', storage_options=storage_options
        )
        cat = col.search(variable=['RAIN', 'FSNO'])
        assert len(cat.query_results.index) > 0


def test_to_xarray():
    with config.set({'database-directory': './tests/test_collections'}):
        col = intake.open_esm_metadatastore(
            collection_name='AWS-CESM1-LE', storage_options=storage_options
        )
        cat = col.search(variable='FSNO', experiment='20C', component='lnd')
        dsets = cat.to_xarray()
        _, ds = dsets.popitem()
        assert isinstance(ds, xr.Dataset)
