import os

import pytest

from intake_esm import config
from intake_esm.bld_collection_utils import FILE_ALIAS_DICT, load_collection_input_file

filepaths = list(FILE_ALIAS_DICT.keys())

try:
    TMPDIR = os.environ['TMPDIR']
except:
    TMPDIR = './tests/tmpdir'


@pytest.mark.parametrize('filepath', filepaths)
def test_open_collection_def_locally(filepath):
    """Opens all files listed in file_alias_dict."""
    data_cache_dir = f'{TMPDIR}/intake-esm-tests'
    with config.set(
        {'database-directory': './tests/test_collections', 'data-cache-directory': data_cache_dir}
    ):
        d = load_collection_input_file(filepath, cache=False)
        assert isinstance(d, dict)
        assert len(d) > 0


def test_load_collection_def_empty():
    actual = load_collection_input_file()
    assert actual is None
