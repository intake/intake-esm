import intake
import pytest

import intake_esm

tutorial_cats = [
    (
        'aws_cesm2_le',
        'https://raw.githubusercontent.com/intake/intake-esm/main/tutorial-catalogs/AWS-CESM2-LENS.json',
    ),
    (
        'aws_cmip6',
        'https://raw.githubusercontent.com/intake/intake-esm/main/tutorial-catalogs/AWS-CMIP6.json',
    ),
    (
        'google_cmip6',
        'https://raw.githubusercontent.com/intake/intake-esm/main/tutorial-catalogs/GOOGLE-CMIP6.json',
    ),
    pytest.param('bad_key', 'bad_url', marks=pytest.mark.xfail),
]


@pytest.mark.parametrize('name,url', tutorial_cats)
def test_get_url(name, url):
    cat_url = intake_esm.tutorial.get_url(name)
    assert isinstance(cat_url, str)
    assert cat_url == url


@pytest.mark.network
@pytest.mark.parametrize('name,url', tutorial_cats)
def test_open_from_url(name, url):
    cat_url = intake_esm.tutorial.get_url(name)
    cat = intake.open_esm_datastore(cat_url)
    assert isinstance(cat.esmcat, intake_esm.cat.ESMCatalogModel)
    assert cat == intake.open_esm_datastore(url)


@pytest.fixture
def test_get_keys(tutorial_cats):
    tutorial_cats_dict = dict(tutorial_cats[0:3])
    keys = intake_esm.tutorial.get_keys(tutorial_cats_dict)
    assert keys == list(tutorial_cats_dict.keys())
