import os
import re
import shutil
import socket

import intake
import pandas as pd
import pytest
import xarray as xr

from intake_esm import config

here = os.path.abspath(os.path.dirname(__file__))

regex = re.compile(r'cheyenne|casper')
hostname = socket.gethostname()
match = regex.search(hostname)

try:
    TMPDIR = os.environ['TMPDIR']
except:
    TMPDIR = './tests/tmpdir'


def test_build_collection_cesm1_le():
    with config.set({'database-directory': './tests/test_collections'}):
        collection_input_definition = os.path.join(here, 'cesm1-le_collection-input.yml')
        col = intake.open_esm_metadatastore(
            collection_input_definition=collection_input_definition, overwrite_existing=True
        )
        assert isinstance(col.df, pd.DataFrame)


@pytest.mark.xfail(reason='Feature not yet implemented')
def test_search_regex():
    with config.set({'database-directory': './tests/test_collections'}):
        col = intake.open_esm_metadatastore(collection_name='cesm1-le')
        cases = sorted(
            col.search(case='b.e11.B20TRC5CNBDRD.f09_g16.10*').query_results.case.unique()
        )
        expected = sorted(
            [
                'b.e11.B20TRC5CNBDRD.f09_g16.101',
                'b.e11.B20TRC5CNBDRD.f09_g16.102',
                'b.e11.B20TRC5CNBDRD.f09_g16.103',
                'b.e11.B20TRC5CNBDRD.f09_g16.104',
                'b.e11.B20TRC5CNBDRD.f09_g16.105',
            ]
        )
        assert cases == expected

        date_ranges = set(col.search(date_range=['192001-']).query_results.date_range.unique())
        assert date_ranges == set(['192001-200512'])


@pytest.mark.parametrize(
    'chunks, expected_chunks',
    [
        ({'time': 100, 'nlat': 2, 'nlon': 2}, (1, 100, 2, 2)),
        ({'time': 200, 'nlat': 1, 'nlon': 1}, (1, 200, 1, 1)),
    ],
)
def test_to_xarray_cesm(chunks, expected_chunks):
    with config.set({'database-directory': './tests/test_collections'}):
        c = intake.open_esm_metadatastore(collection_name='cesm1-le')
        query = {
            'variable': ['STF_O2', 'SHF'],
            'member_id': [1, 3, 9],
            'experiment': ['20C', 'RCP85'],
        }
        cat = c.search(**query)
        dset = cat.to_xarray(chunks=chunks)
        _, ds = dset.popitem()
        assert ds['SHF'].data.chunksize == expected_chunks


@pytest.mark.parametrize(
    'query',
    [
        ({'variable': ['STF_O2', 'SHF'], 'member_id': [1, 3, 9], 'experiment': ['20C', 'RCP85']}),
        ({'variable': ['STF_O2', 'SHF'], 'member_id': [1], 'experiment': ['20C', 'RCP85']}),
    ],
)
def test_to_xarray_restore_non_coords(query):
    with config.set({'database-directory': './tests/test_collections'}):
        c = intake.open_esm_metadatastore(collection_name='cesm1-le')
        cat = c.search(**query)
        dset = cat.to_xarray(decode_times=False)
        _, ds = dset.popitem()
        assert 'TAREA' in ds.data_vars


@pytest.mark.skipif(
    not match, reason='does not run outside of Cheyenne login nodes or Casper nodes'
)
def test_to_xarray_cesm_hsi():
    data_cache_dir = f'{TMPDIR}/intake-esm-tests/transferred-data'
    with config.set(
        {'database-directory': './tests/test_collections', 'data-cache-directory': data_cache_dir}
    ):
        collection_input_definition = os.path.join(
            here, '../ensure-file-hsi-transfer-collection-input.yml'
        )
        col = intake.open_esm_metadatastore(
            collection_input_definition=collection_input_definition, overwrite_existing=False
        )

        cat = col.search(variable=['SST'])
        dset = cat.to_xarray(chunks={'time': 365})
        _, ds = dset.popitem()
        assert isinstance(ds['SST'], xr.DataArray)
