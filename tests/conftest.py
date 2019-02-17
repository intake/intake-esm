import intake
import pytest

from intake_esm.config import get_options, set_options
from intake_esm.core import ESMMetadataStoreCatalog

intake.registry["esm_metadatastore"] = ESMMetadataStoreCatalog


@pytest.fixture
def default_settings():
    set_options(database_directory="./tests/test_collections")
    return get_options()
