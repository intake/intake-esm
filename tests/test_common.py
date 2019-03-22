import os

import pytest

from intake_esm.common import StorageResource, _open_collection

here = os.path.abspath(os.path.dirname(__file__))


def test_storage_resource_init():
    input_file = os.path.join(here, 'intake-cesm-test-filelist')
    urlpath = input_file
    type_ = 'input-file'
    exclude_dirs = ['*/avoid-this-dir/*']
    file_extension = '.nc'

    SR = StorageResource(urlpath, type_, exclude_dirs, file_extension)
    assert isinstance(SR, StorageResource)
    assert isinstance(SR._list_files_input_file(), list)
