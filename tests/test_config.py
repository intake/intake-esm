import os

import intake

from intake_esm.config import get_options, save_to_disk, set_options


def test_default_config():
    SETTINGS = get_options()
    assert isinstance(SETTINGS["collections"]["cesm"], dict)


def test_set_options():
    set_options(database_directory="./tests/test_collections_2")
    s1 = get_options()["database_directory"]
    assert s1 == os.path.abspath(os.path.expanduser("./tests/test_collections_2"))
    set_options(database_directory="./tests/test_collections")
    s2 = get_options()["database_directory"]

    assert not s1 == s2
