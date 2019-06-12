import os

import pytest

from intake_esm.storage import StorageResource

here = os.path.abspath(os.path.dirname(__file__))


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
