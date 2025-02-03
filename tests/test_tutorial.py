import intake
import pytest

import intake_esm
from intake_esm.tutorial import DEFAULT_CATALOGS

tutorial_cats = list(zip(DEFAULT_CATALOGS.keys(), DEFAULT_CATALOGS.values())) + [
    pytest.param('bad_key', 'bad_url', marks=pytest.mark.xfail)
]


@pytest.mark.parametrize('name,url', tutorial_cats)
@pytest.mark.flaky(max_runs=3, min_passes=1)  # Cold start related failures
def test_get_url(name, url):
    cat_url = intake_esm.tutorial.get_url(name)
    assert isinstance(cat_url, str)
    assert cat_url == url


@pytest.mark.network
@pytest.mark.parametrize('name,url', tutorial_cats)
@pytest.mark.flaky(max_runs=3, min_passes=1)  # Cold start related failures
def test_open_from_url(name, url):
    cat_url = intake_esm.tutorial.get_url(name)
    cat = intake.open_esm_datastore(cat_url)
    assert isinstance(cat.esmcat, intake_esm.cat.ESMCatalogModel)
    assert cat == intake.open_esm_datastore(url)


def test_get_available_cats():
    cats = intake_esm.tutorial.get_available_cats()
    assert cats == list(DEFAULT_CATALOGS.keys())
