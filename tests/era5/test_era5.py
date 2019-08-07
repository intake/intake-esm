import os

import intake
import pandas as pd
import pytest
import xarray as xr

from intake_esm import config

here = os.path.abspath(os.path.dirname(__file__))


def test_build_collection():
    with config.set({'database-directory': './tests/test_collections'}):
        collection_input_definition = os.path.join(here, 'era5-test.yml')
        col = intake.open_esm_metadatastore(
            collection_input_definition=collection_input_definition, overwrite_existing=True
        )
        assert isinstance(col._ds, xr.Dataset)


def test_search():
    with config.set({'database-directory': './tests/test_collections'}):
        col = intake.open_esm_metadatastore(collection_name='era5_test')
        cat = col.search(
            variable_short_name=['mn2t', 'mx2t'], forecast_initial_date=['2002-02-01', '2002-02-16']
        )

        assert isinstance(cat.query_results, xr.Dataset)
        assert len(cat.query_results.index) > 0
