import os

import intake
import pandas as pd
import pytest
import xarray as xr

from intake_esm import config
from intake_esm.core import ESMMetadataStoreCatalog

here = os.path.abspath(os.path.dirname(__file__))


def test_build_collection_cesm1_le():
    with config.set({'database-directory': './tests/test_collections'}):
        collection_input_definition = os.path.join(here, 'cesm1-le_collection-input.yml')
        col = intake.open_esm_metadatastore(
            collection_input_definition=collection_input_definition, overwrite_existing=True
        )
        assert isinstance(col.df, pd.DataFrame)


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
        cat = c.search(
            variable=['STF_O2', 'SHF'],
            ensemble=[1, 3, 9],
            experiment=['20C', 'RCP85'],
            direct_access=True,
        )
        dset = cat.to_xarray(chunks=chunks)
        ds = dset['pop.h.ocn']
        assert ds['SHF'].data.chunksize == expected_chunks
