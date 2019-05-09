import os

import intake
import pandas as pd
import pytest
import xarray as xr

from intake_esm import config
from intake_esm.core import ESMMetadataStoreCatalog

here = os.path.abspath(os.path.dirname(__file__))


def test_build_collection():
    with config.set({'database-directory': './tests/test_collections'}):
        collection_input_definition = os.path.join(here, 'era5-test.yml')
        col = intake.open_esm_metadatastore(
            collection_input_definition=collection_input_definition, overwrite_existing=True
        )
        assert isinstance(col.df, pd.DataFrame)


def test_search():
    with config.set({'database-directory': './tests/test_collections'}):
        col = intake.open_esm_metadatastore(collection_name='era5_test')
        cat = col.search(
            variable_short_name=['mn2t', 'mx2t'],
            start_year=[2015, 2005],
            start_month=[5, 6],
            start_hour=[0, 6, 12, 18],
        )

        assert isinstance(cat.query_results, pd.DataFrame)
        assert not cat.query_results.empty