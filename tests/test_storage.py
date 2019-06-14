import os
import re
import socket

import pytest

from intake_esm.storage import StorageResource

here = os.path.abspath(os.path.dirname(__file__))

regex = re.compile(r'cheyenne|casper')
hostname = socket.gethostname()
match = regex.search(hostname)


def test_storage_input_file():
    input_file = os.path.join(here, 'input-filelist-test.txt')
    urlpath = input_file
    type_ = 'input-file'
    exclude_dirs = ['*/avoid-this-dir/*']
    file_extension = '.nc'

    SR = StorageResource(urlpath, type_, exclude_dirs, file_extension)
    assert isinstance(SR, StorageResource)
    files = SR._list_files_input_file()
    assert isinstance(files, list)
    assert len(files) == 5


@pytest.mark.skipif(
    not match, reason='does not run outside of Cheyenne login nodes or Casper nodes'
)
def test_storage_hsi():
    urlpath = '/CCSM/csm/CESM-CAM5-BGC-LE'
    loc_type = 'hsi'
    exclude_dirs = []
    file_extension = '.nc'
    SR = StorageResource(urlpath, loc_type, exclude_dirs, file_extension)

    files = SR._list_files_hsi()
    assert isinstance(files, list)
    assert len(files) != 0
