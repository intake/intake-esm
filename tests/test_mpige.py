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
        collection_input_definition = os.path.join(here, 'mpi-ge.yml')
        col = intake.open_esm_metadatastore(
            collection_input_definition=collection_input_definition, overwrite_existing=True
        )
        assert isinstance(col.df, pd.DataFrame)


def test_search():
    with config.set({'database-directory': './tests/test_collections'}):
        c = intake.open_esm_metadatastore(collection_name='mpige_test')
        cat = c.search(component='mpiom', stream='monitoring_ym')

        assert isinstance(cat.query_results, pd.DataFrame)
        assert not cat.query_results.empty


def test_to_xarray():
    with config.set({'database-directory': './tests/test_collections'}):
        c = intake.open_esm_metadatastore(collection_name='mpige_test')
        cat = c.search(component='mpiom', stream='monitoring_ym')
        ds = cat.to_xarray()
        assert isinstance(ds, dict)
        ds = ds['hist.mpiom']
        assert isinstance(ds, xr.Dataset)
