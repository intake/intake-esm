import os

import intake
import pandas as pd
import pytest
import xarray as xr
import yaml

from intake_esm import config

here = os.path.abspath(os.path.dirname(__file__))

cdef = yaml.safe_load(
    """name: cmip5_test_collection
collection_type: cmip5
data_sources:
  BNU-ESM:
    locations:
     -  name: SAMPLE-DATA
        loc_type: posix
        direct_access: True
        urlpath: ./tests/sample_data/cmip/cmip5/output1/BNU/BNU-ESM
        exclude_dirs: ['*/files/*', 'latest']
        file_extension: .nc

    extra_attributes:
      activity: CMIP5
      product: output1
      institute: BNU

  CanCM4:
    locations:
     -  name: SAMPLE-DATA
        loc_type: posix
        direct_access: True
        urlpath: ./tests/sample_data/cmip/cmip5/output1/CCCma/CanCM4
        exclude_dirs: ['*/files/*', 'latest']
        file_extension: .nc

    extra_attributes:
      activity: CMIP5
      product: output1
      institute: CCCma


  CanESM2:
    locations:
     -  name: SAMPLE-DATA
        loc_type: posix
        direct_access: True
        urlpath: ./tests/sample_data/cmip/cmip5/output1/CCCma/CanESM2
        exclude_dirs: ['*/files/*', 'latest']
        file_extension: .nc

    extra_attributes:
      activity: CMIP5
      product: output1
      institute: CCCma


  CNRM-CM5:
    locations:
     -  name: SAMPLE-DATA
        loc_type: posix
        direct_access: True
        urlpath: ./tests/sample_data/cmip/cmip5/output1/CNRM-CERFACS/CNRM-CM5
        exclude_dirs: ['*/files/*', 'latest']
        file_extension: .nc

    extra_attributes:
      activity: CMIP5
      product: output1
      institute: CNRM-CERFACS


  CESM1-BGC:
    locations:
     -  name: SAMPLE-DATA
        loc_type: posix
        direct_access: True
        urlpath: ./tests/sample_data/cmip/cmip5/output1/NSF-DOE-NCAR/CESM1-BGC
        exclude_dirs: ['*/files/*', 'latest']
        file_extension: .nc

    extra_attributes:
      activity: CMIP5
      product: output1
      institute: NSF-DOE-NCAR


  CESM1-CAM5:
    locations:
     -  name: SAMPLE-DATA
        loc_type: posix
        direct_access: True
        urlpath: ./tests/sample_data/cmip/cmip5/output1/NSF-DOE-NCAR/CESM1-CAM5
        exclude_dirs: ['*/files/*', 'latest']
        file_extension: .nc

    extra_attributes:
      activity: CMIP5
      product: output1
      institute: NSF-DOE-NCAR
"""
)


def test_build_collection_file():
    with config.set({'database-directory': './tests/test_collections'}):

        col = intake.open_esm_metadatastore(
            collection_input_definition=cdef, overwrite_existing=True
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
