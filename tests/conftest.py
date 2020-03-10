import os

import pytest

here = os.path.abspath(os.path.dirname(__file__))


@pytest.fixture
def sample_cmip6():
    return os.path.join(here, 'sample-collections/cmip6-netcdf.json')


@pytest.fixture
def sample_bad_input():
    return os.path.join(here, 'sample-collections/bad.json')
