import os

import intake
import pandas as pd
import pytest
import xarray as xr
import yaml

from intake_esm import config

cdef = yaml.load(
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
            storage_options={'anon': False, 'profile_name': 'default'},
        )
        assert isinstance(col.df, pd.DataFrame)


def test_search():
    with config.set({'database-directory': './tests/test_collections'}):
        col = intake.open_esm_metadatastore(collection_name='AWS-CESM1-LE')
        cat = col.search(variable='RAIN')
        assert not cat.query_results.empty
