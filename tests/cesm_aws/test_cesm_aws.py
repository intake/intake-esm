import os

import intake
import pandas as pd
import pytest
import xarray as xr

from intake_esm import config

here = os.path.abspath(os.path.dirname(__file__))


def test_build_collection_cesm1_aws_le():
    with config.set({'database-directory': './tests/test_collections'}):
        collection_input_definition = os.path.join(here, 'cesm1-le_aws_collection-input.yml')
        col = intake.open_esm_metadatastore(
            collection_input_definition=collection_input_definition, overwrite_existing=True
        )
        assert isinstance(col.df, pd.DataFrame)


def test_search():
    with config.set({'database-directory': './tests/test_collections'}):
        col = intake.open_esm_metadatastore(collection_name='AWS-CESM1-LE')
        cat = col.search(variable='RAIN')
        assert not cat.query_results.empty
