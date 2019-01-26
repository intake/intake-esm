import pandas as pd
import pytest

from intake_cesm.cat import CesmCatalog


@pytest.mark.parametrize("collection", ["cesm1_le", "cesm2_runs", "cesm_dple"])
def test_constructor(collection):
    c = CesmCatalog(collection)
    assert isinstance(c, CesmCatalog)


def test_set_collection_success():
    c = CesmCatalog("cesm1_le")
    c.set_collection("cesm2_runs")
    assert isinstance(c, CesmCatalog)


def test_set_collection_fail():
    with pytest.raises(KeyError):
        c = CesmCatalog("cesm1_le")
        c.set_collection("cesm")


def test_search():
    c = CesmCatalog("cesm1_le")
    df = c.search(
        experiment=["20C", "RCP85"], component="ocn", ensemble=1, variable="FG_CO2"
    )

    assert isinstance(df, pd.DataFrame)
    assert not df.empty
