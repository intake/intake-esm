import os

import intake
import pandas as pd
import pytest
import xarray as xr

from intake_esm import config

here = os.path.abspath(os.path.dirname(__file__))


def test_build_collection():
    with config.set({'database-directory': './tests/test_collections'}):
        collection_input_file = os.path.join(here, 'cmip_collection_input_test.yml')
        col = intake.open_esm_metadatastore(
            collection_input_file=collection_input_file, overwrite_existing=True
        )
        assert isinstance(col.df, pd.DataFrame)


def test_search():
    with config.set({'database-directory': './tests/test_collections'}):
        c = intake.open_esm_metadatastore(
            collection_name='cmip_test_collection', collection_type='cmip'
        )
        cat = c.search(model=['CanESM2', 'CSIRO-Mk3-6-0'])
        assert isinstance(cat.results, pd.DataFrame)
        assert not cat.results.empty


def test_cat():
    with config.set({'database-directory': './tests/test_collections'}):
        cat = intake.open_catalog(os.path.join(here, 'cmip_catalog.yaml'))
        cat = cat['cmip_test_collection_a4fa3aaa-d4f5-4da0-9f6e-dc10e79d1452']
        assert isinstance(cat.results, pd.DataFrame)


def test_to_xarray_cmip_empty():
    with config.set({'database-directory': './tests/test_collections'}):
        c = intake.open_esm_metadatastore(
            collection_name='cmip_test_collection', collection_type='cmip'
        )
        cat = c.search(
            model='CanESM2', experiment='rcp85', frequency='mon', realm='atmos', ensemble='r2i1p1'
        )

        with pytest.raises(ValueError):
            cat.to_xarray()


@pytest.mark.parametrize(
    'chunks, expected_chunks',
    [
        ({'member_id': 1, 'time': 1, 'lat': 2, 'lon': 2}, (1, 1, 2, 2)),
        ({'member_id': 1, 'time': 2, 'lat': 1, 'lon': 1}, (1, 2, 1, 1)),
    ],
)
def test_to_xarray_cmip(chunks, expected_chunks):
    with config.set({'database-directory': './tests/test_collections'}):
        c = intake.open_esm_metadatastore(
            collection_name='cmip_test_collection', collection_type='cmip'
        )
        cat = c.search(variable=['hfls'], frequency='mon', realm='atmos', model=['CNRM-CM5'])

        ds = cat.to_xarray(decode_times=True, chunks=chunks)
        assert ds['hfls'].data.chunksize == expected_chunks
