import sys

import intake
import pytest
import xarray as xr

from intake_esm import tutorial


@pytest.mark.network
class TestGetURL:
    def setUp(self):
        self.aws_cesm2_le = (
            'https://raw.githubusercontent.com/intake/intake-esm/main/tutorial-catalogs/AWS-CESM2-LENS.json',
        )
        self.aws_cmip6 = (
            'https://raw.githubusercontent.com/intake/intake-esm/main/tutorial-catalogs/AWS-CMIP6.json',
        )
        self.google_cmip6 = 'https://raw.githubusercontent.com/intake/intake-esm/main/tutorial-catalogs/GOOGLE-CMIP6.json'

    def test_get_url(self) -> None:
        cat_url = tutorial.get_url(self.testfile)
        assert isinstance(cat_url, str)

    def test_open_from_url(self) -> None:
        cat_url = tutorial.get_url(self.testfile)
        cat = intake.open_esm_datastore(cat_url)
        assert isinstance(cat, intake_esm.cat.ESMCatalogModel)
