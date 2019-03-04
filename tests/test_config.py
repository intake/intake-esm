import os

import pytest

from intake_esm import config


def test_default_config():
    assert isinstance(config.get('collections')['cesm'], dict)


def test_set_options():
    config.set({'database_directory': '/tmp/collections'})
    s1 = config.get('database_directory')
    assert s1 == os.path.abspath(os.path.expanduser('/tmp/collections'))
    config.set({'database_directory': '/tests/test_collections'})
    s2 = config.get('database_directory')

    assert not s1 == s2
