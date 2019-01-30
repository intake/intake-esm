import os

import intake
import pandas as pd
import pytest

# pytest imports this package last, so plugin is not auto-added
from intake_cesm.core import CesmMetadataStoreCatalog

intake.registry["cesm_metadatastore"] = CesmMetadataStoreCatalog

here = os.path.abspath(os.path.dirname(__file__))


@pytest.mark.parametrize("collection", ["cesm1_le", "cesm2_runs", "cesm_dple"])
def test_constructor(collection):
    c = intake.open_cesm_metadatastore(collection)
    assert isinstance(c, CesmMetadataStoreCatalog)


def test_set_collection_success():
    c = intake.open_cesm_metadatastore("cesm1_le")
    c.set_collection("cesm2_runs")
    assert isinstance(c, CesmMetadataStoreCatalog)


def test_set_collection_fail():
    with pytest.raises(KeyError):
        c = intake.open_cesm_metadatastore("cesm1_le")
        c.set_collection("cesm")


def test_search():
    c = intake.open_cesm_metadatastore("cesm1_le")
    cat = c.search(experiment=["20C", "RCP85"], component="ocn", ensemble=1, variable="FG_CO2")

    assert isinstance(cat.results, pd.DataFrame)
    assert not cat.results.empty


def test_cat():
    cat = intake.open_catalog(os.path.join(here, "catalog.yaml"))
    cat = cat["cesm1_le-90b7bc60-f946-4ae3-b727-7caeed65d974"]
    assert isinstance(cat.results, pd.DataFrame)
