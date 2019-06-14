import os

import intake
import pandas as pd
import pytest
import xarray as xr

from intake_esm import config

here = os.path.abspath(os.path.dirname(__file__))


def test_build_collection_file():
    with config.set({'database-directory': './tests/test_collections'}):
        collection_input_definition = os.path.join(here, 'cmip6_collection_input_test.yml')
        col = intake.open_esm_metadatastore(
            collection_input_definition=collection_input_definition, overwrite_existing=True
        )
        assert isinstance(col.df, pd.DataFrame)


def test_search():
    with config.set({'database-directory': './tests/test_collections'}):
        c = intake.open_esm_metadatastore(collection_name='cmip6_test_collection')
        cat = c.search(source_id=['CNRM-ESM2-1', 'GISS-E2-1-G'])
        assert isinstance(cat.query_results, pd.DataFrame)
        assert not cat.query_results.empty


@pytest.mark.parametrize(
    'chunks, expected_chunks',
    [({'time': 1, 'lat': 2, 'lon': 2}, (1, 2, 2)), ({'time': 2, 'lat': 1, 'lon': 1}, (2, 1, 1))],
)
def test_to_xarray_cmip(chunks, expected_chunks):
    with config.set({'database-directory': './tests/test_collections'}):
        c = intake.open_esm_metadatastore(collection_name='cmip6_test_collection')

        # Test for data from multiple institutions
        cat = c.search(source_id=['CNRM-ESM2-1', 'GISS-E2-1-G'], variable_id=['prra', 'tasmax'])
        ds = cat.to_xarray(decode_times=False, chunks=chunks)
        print(ds)
        assert isinstance(ds, dict)
        nasa_dset = ds['NASA-GISS.GISS-E2-1-G.amip.Omon.gn']
        assert nasa_dset['prra'].data.chunksize == expected_chunks
