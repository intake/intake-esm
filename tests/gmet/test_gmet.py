import os

import intake
import pandas as pd
import pytest
import xarray as xr

from intake_esm import config

here = os.path.abspath(os.path.dirname(__file__))


def test_build_collection():
    with config.set({'database-directory': './tests/test_collections'}):
        collection_input_definition = os.path.join(here, 'gmet-test.yml')
        col = intake.open_esm_metadatastore(
            collection_input_definition=collection_input_definition, overwrite_existing=True
        )
        assert isinstance(col.df, pd.DataFrame)


def test_search():
    with config.set({'database-directory': './tests/test_collections'}):
        col = intake.open_esm_metadatastore(collection_name='gmet_test')
        cat = col.search(member_id=[1, 2])

        assert isinstance(cat.df, pd.DataFrame)
        assert len(cat.df) > 0


def test_to_xarray():
    with config.set({'database-directory': './tests/test_collections'}):
        col = intake.open_esm_metadatastore(collection_name='gmet_test')
        cat = col.search(direct_access=True)
        ds = cat.to_xarray(chunks={'time': 1}, decode_times=True)
        assert isinstance(ds, xr.Dataset)
        assert 'member_id' in ds.coords
