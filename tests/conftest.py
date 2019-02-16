import pytest

from intake_esm.config import get_options, set_options


@pytest.fixture
def default_settings():
    set_options(database_directory="./tests/test_collections")
    return get_options()
