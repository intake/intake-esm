import intake
import pytest

from intake_esm.config import get_options, set_options
from intake_esm.core import CesmMetadataStoreCatalog

intake.registry["cesm_metadatastore"] = CesmMetadataStoreCatalog


@pytest.fixture
def default_settings():
    set_options(database_directory="./tests/test_collections")
    return get_options()
