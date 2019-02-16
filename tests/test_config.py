import os

import pytest

from intake_esm.config import get_options, save_to_disk, set_options


def test_default_config():
    SETTINGS = get_options()
    assert isinstance(SETTINGS["collections"]["cesm"], dict)


def test_set_options():
    set_options(database_directory="/tmp/collections")
    s1 = get_options()["database_directory"]
    assert s1 == os.path.abspath(os.path.expanduser("/tmp/collections"))
    set_options(database_directory="./tests/test_collections")
    s2 = get_options()["database_directory"]

    assert not s1 == s2


def test_set_options_failure():
    with pytest.raises(ValueError):
        set_options(database="/tmp/test")
