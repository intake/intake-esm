import os

import pytest

import intake_esm

here = os.path.abspath(os.path.dirname(__file__))


@pytest.fixture
def sample_cmip6():
    return os.path.join(here, 'sample-catalogs/cmip6-netcdf.json')


@pytest.fixture
def sample_bad_input():
    return os.path.join(here, 'sample-catalogs/bad.json')


@pytest.fixture
def cleanup_init():
    """
    This resets the _optional_imports dictionary in intake_esm to it's default
    state, so we can test lazy loading and whatnot
    """
    yield

    intake_esm._optional_imports = {'esmvalcore': None}
