import os
import re
import shutil
import socket

import intake
import pandas as pd
import pytest

from intake_esm import config
from intake_esm.storage import StorageResource, _ensure_file_access

here = os.path.abspath(os.path.dirname(__file__))

regex = re.compile(r'cheyenne|casper')
hostname = socket.gethostname()
match = regex.search(hostname)

try:
    TMPDIR = os.environ['TMPDIR']
except:
    TMPDIR = './tests/tmpdir'


def test_storage_input_file():
    input_file = os.path.join(here, 'input-filelist-test.txt')
    urlpath = input_file
    type_ = 'input-file'
    exclude_dirs = ['*/avoid-this-dir/*']
    file_extension = '.nc'

    SR = StorageResource(urlpath, type_, exclude_dirs, file_extension)
    assert isinstance(SR, StorageResource)
    files = SR.filelist
    assert isinstance(files, list)
    assert len(files) == 4


@pytest.mark.skipif(
    not match, reason='does not run outside of Cheyenne login nodes or Casper nodes'
)
def test_storage_hsi():
    urlpath = '/CCSM/csm/CESM-CAM5-BGC-LE/ocn/proc/tseries/daily/SST'
    loc_type = 'hsi'
    exclude_dirs = []
    file_extension = '.nc'
    SR = StorageResource(urlpath, loc_type, exclude_dirs, file_extension)

    files = SR.filelist
    assert isinstance(files, list)
    assert len(files) != 0


def test_file_transfer_symlink():
    data_cache_dir = f'{TMPDIR}/intake-esm-tests/transferred-data'
    with config.set(
        {'database-directory': './tests/test_collections', 'data-cache-directory': data_cache_dir}
    ):
        collection_input_definition = os.path.join(here, 'copy-to-cache-collection-input.yml')
        col = intake.open_esm_metadatastore(
            collection_input_definition=collection_input_definition, overwrite_existing=True
        )

        cat = col.search(variable=['STF_O2', 'SHF'])

        query_results = _ensure_file_access(cat.query_results)
        local_urlpaths = query_results['file_fullpath'].tolist()
        assert isinstance(local_urlpaths, list)
        assert len(local_urlpaths) > 0

        shutil.rmtree(data_cache_dir)


@pytest.mark.skipif(
    not match, reason='does not run outside of Cheyenne login nodes or Casper nodes'
)
def test_file_transfer_hsi():
    data_cache_dir = f'{TMPDIR}/intake-esm-tests/transferred-data'
    with config.set(
        {'database-directory': './tests/test_collections', 'data-cache-directory': data_cache_dir}
    ):
        collection_input_definition = os.path.join(
            here, 'ensure-file-hsi-transfer-collection-input.yml'
        )
        col = intake.open_esm_metadatastore(
            collection_input_definition=collection_input_definition, overwrite_existing=True
        )

        cat = col.search(variable=['SST'])

        query_results = _ensure_file_access(cat.query_results)
        local_urlpaths = query_results['file_fullpath'].tolist()
        assert isinstance(local_urlpaths, list)
        assert len(local_urlpaths) > 0

        shutil.rmtree(data_cache_dir)
