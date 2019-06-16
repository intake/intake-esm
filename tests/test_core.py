import pytest

from intake_esm import ESMMetadataStoreCatalog


def test_esm_metadastore_empty():
    with pytest.raises(ValueError):
        ESMMetadataStoreCatalog()
