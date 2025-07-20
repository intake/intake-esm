import intake
import pytest

import intake_esm
from intake_esm.tutorial import DEFAULT_CATALOGS

tutorial_cats = list(zip(DEFAULT_CATALOGS.keys(), DEFAULT_CATALOGS.values()))


@pytest.mark.parametrize('name,url', tutorial_cats)
@pytest.mark.flaky(max_runs=3, min_passes=1)  # Cold start related failures
def test_get_url(name, url):
    cat_url = intake_esm.tutorial.get_url(name)
    assert isinstance(cat_url, str)
    assert cat_url == url


@pytest.mark.xfail
def test_get_url_xfail():
    cat_url = intake_esm.tutorial.get_url('bad_key')
    assert isinstance(cat_url, str)
    assert 'bad_url' == 'bad_url'


@pytest.mark.network
@pytest.mark.parametrize('name,url', tutorial_cats)
@pytest.mark.flaky(max_runs=3, min_passes=1)  # Cold start related failures
def test_open_from_url(name, url):
    cat_url = intake_esm.tutorial.get_url(name)
    cat = intake.open_esm_datastore(cat_url)
    assert isinstance(cat.esmcat, intake_esm.cat.ESMCatalogModel)
    assert cat == intake.open_esm_datastore(url)


@pytest.mark.network
@pytest.mark.xfail
def test_open_from_url_xfail():
    cat_url = intake_esm.tutorial.get_url('bad_url')
    cat = intake.open_esm_datastore(cat_url)
    assert isinstance(cat.esmcat, intake_esm.cat.ESMCatalogModel)
    assert cat == intake.open_esm_datastore('bad_url')


def test_get_available_cats():
    cats = intake_esm.tutorial.get_available_cats()
    assert cats == list(DEFAULT_CATALOGS.keys())
