import os

import intake
import pandas as pd
import pytest

# pytest imports this package last, so plugin is not auto-added
from intake_cesm.core import CesmMetadataStoreCatalog

# intake.registry["cesm_metadatastore"] = CesmMetadataStoreCatalog

here = os.path.abspath(os.path.dirname(__file__))

# import intake_cesm
# @pytest.mark.parametrize("collection", ["cesm1_le", "cesm_dple"])
# def test_build_collection(collection):
#     collection_type_def_file = 'intake_cesm/cesm_definitions.yml'
#     if collection == "cesm1_le":
#         collection_input_file = 'collection_input_cesm1_le.yml'
#     if collection == "cesm2_runs":
#         collection_input_file = "collection_input_cmip6_cesm.yml"
#     if collection == "cesm_dple":
#         collection_input_file = "collection_input_cesm_dple.yml"
#
#     intake_cesm.CESMCollections(collection_input_file=collection_input_file,
#                                 collection_type_def_file=collection_type_def_file)


@pytest.mark.parametrize("collection", ["cesm_dple"])
def test_constructor(collection):
    c = intake.open_cesm_metadatastore(collection)
    assert isinstance(c, CesmMetadataStoreCatalog)


def test_set_collection_success():
    c = intake.open_cesm_metadatastore("cesm_dple")
    c.set_collection("cesm_dple")
    assert isinstance(c, CesmMetadataStoreCatalog)


def test_set_collection_fail():
    with pytest.raises(FileNotFoundError):
        c = intake.open_cesm_metadatastore("cesm_dple")
        c.set_collection("cesm")


def test_search():
    c = intake.open_cesm_metadatastore("cesm_dple")
    cat = c.search(
        experiment="g.e11_LENS.GECOIAF.T62_g16.009", component="ocn", variable="FG_CO2"
    )

    assert isinstance(cat.results, pd.DataFrame)
    assert not cat.results.empty


def test_cat():
    cat = intake.open_catalog(os.path.join(here, "catalog.yaml"))
    cat = cat["cesm_dple-cff53aef-6938-4c6e-b6ae-efa5035bed7e"]
    assert isinstance(cat.results, pd.DataFrame)
