import pytest

from intake_esm.cmip import CMIPCollection, CMIPSource


def test_collection_constructor():
    with pytest.raises(NotImplementedError):
        CMIPCollection()


def test_source_constructor():
    with pytest.raises(NotImplementedError):
        CMIPSource()
