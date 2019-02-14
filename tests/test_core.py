import os

import intake
import pandas as pd
import pytest
import xarray as xr

# pytest imports this package last, so plugin is not auto-added
from intake_cesm.core import CesmMetadataStoreCatalog

intake.registry["cesm_metadatastore"] = CesmMetadataStoreCatalog

here = os.path.abspath(os.path.dirname(__file__))


def test_build_collection():
    collection_input_file = os.path.join(here, 'collection_input_test.yml')
    collection_type_def_file = os.path.join('intake_cesm/cesm_definitions.yml')
    build_args = {
        'collection_input_file': collection_input_file,
        'collection_type_def_file': collection_type_def_file,
        'overwrite_existing': True,
    }
    col = intake.open_cesm_metadatastore(collection='test', build_args=build_args)
    assert isinstance(col.df, pd.DataFrame)


@pytest.mark.parametrize("collection", ["cesm_dple", "cesm_dple_test_collection"])
def test_constructor(collection):
    c = intake.open_cesm_metadatastore(collection)
    assert isinstance(c, CesmMetadataStoreCatalog)


def test_search():
    c = intake.open_cesm_metadatastore("cesm_dple")
    cat = c.search(experiment="g.e11_LENS.GECOIAF.T62_g16.009", component="ocn", variable="FG_CO2")

    assert isinstance(cat.results, pd.DataFrame)
    assert not cat.results.empty


def test_cat():
    cat = intake.open_catalog(os.path.join(here, "catalog.yaml"))
    cat = cat["cesm_dple-cff53aef-6938-4c6e-b6ae-efa5035bed7e"]
    assert isinstance(cat.results, pd.DataFrame)


def test_to_xarray():
    c = intake.open_cesm_metadatastore("cesm_dple_test_collection")
    cat = c.search(variable='O2', direct_access=True)
    ds = cat.to_xarray()
    assert isinstance(ds, xr.Dataset)
