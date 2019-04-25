import os

import intake
import pandas as pd
import pytest
import xarray as xr

from intake_esm import config

here = os.path.abspath(os.path.dirname(__file__))


def test_build_collection_file():
    with config.set({'database-directory': './tests/test_collections'}):
        collection_input_definition = os.path.join(here, 'cmip5_collection_input_test.yml')
        col = intake.open_esm_metadatastore(
            collection_input_definition=collection_input_definition, overwrite_existing=True
        )
        assert isinstance(col.df, pd.DataFrame)


def test_build_collection_dict():
    with config.set({'database-directory': './tests/test_collections'}):
        collection_definition = {
            'name': 'cmip5_test_collection',
            'collection_type': 'cmip5',
            'data_sources': {
                'root_dir': {
                    'name': 'GLADE',
                    'loc_type': 'posix',
                    'direct_access': True,
                    'urlpath': './tests/sample_data/cmip/cmip5',
                }
            },
        }
        col = intake.open_esm_metadatastore(
            collection_input_definition=collection_definition, overwrite_existing=True
        )
        assert isinstance(col.df, pd.DataFrame)


def test_search():
    with config.set({'database-directory': './tests/test_collections'}):
        c = intake.open_esm_metadatastore(collection_name='cmip5_test_collection')
        cat = c.search(model=['CanESM2', 'CSIRO-Mk3-6-0'])
        assert isinstance(cat.query_results, pd.DataFrame)
        assert not cat.query_results.empty


def test_cat():
    with config.set({'database-directory': './tests/test_collections'}):
        cat = intake.open_catalog(os.path.join(here, 'cmip5_catalog.yaml'))
        cat = cat['cmip5_test_collection_b4cf52c3-4879-44c6-955e-f341b1f9b2d9']
        assert isinstance(cat.query_results, pd.DataFrame)


def test_to_xarray_cmip_empty():
    with config.set({'database-directory': './tests/test_collections'}):
        c = intake.open_esm_metadatastore(collection_name='cmip5_test_collection')
        cat = c.search(
            model='CanESM2',
            experiment='rcp85',
            frequency='mon',
            modeling_realm='atmos',
            ensemble_member='r2i1p1',
        )

        with pytest.raises(ValueError):
            cat.to_xarray()


@pytest.mark.parametrize(
    'chunks, expected_chunks',
    [
        ({'time': 1, 'lat': 2, 'lon': 2}, (1, 1, 2, 2)),
        ({'time': 2, 'lat': 1, 'lon': 1}, (1, 2, 1, 1)),
    ],
)
def test_to_xarray_cmip(chunks, expected_chunks):
    with config.set({'database-directory': './tests/test_collections'}):
        c = intake.open_esm_metadatastore(collection_name='cmip5_test_collection')
        cat = c.search(
            variable=['hfls'], frequency='mon', modeling_realm='atmos', model=['CNRM-CM5']
        )

        dset = cat.to_xarray(decode_times=True, chunks=chunks)
        ds = dset['CNRM-CERFACS.CNRM-CM5.historical.mon.atmos']
        assert ds['hfls'].data.chunksize == expected_chunks

        # Test for data from multiple institutions
        cat = c.search(variable=['hfls'], frequency='mon', modeling_realm='atmos')
        ds = cat.to_xarray(decode_times=False, chunks=chunks)
        assert isinstance(ds, dict)
        assert 'CCCma.CanCM4.historical.mon.atmos' in ds.keys()
