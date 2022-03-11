import intake
import pytest

import intake_esm

tutorial_cats = ['aws_cesm2_le', 'aws_cmip6', 'google_cmip6']


@pytest.mark.paramtrize('names', tutorial_cats)
class TestGetURL:
    def test_get_url(names):
        cat_url = intake_esm.tutorial.get_url(names)
        assert isinstance(cat_url, str)

    def test_open_from_url(names):
        cat_url = intake_esm.tutorial.get_url(names)
        cat = intake.open_esm_datastore(cat_url)
        assert isinstance(cat, intake_esm.cat.ESMCatalogModel)
