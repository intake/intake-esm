import intake
import pandas as pd
import pytest
import xarray as xr
import yaml

from intake_esm import config

cdef = yaml.safe_load(
    """
name: cmip6_test_collection
collection_type: cmip6
data_sources:
  BCC-CSM2-MR:
    locations:
     -  name: SAMPLE-DATA
        loc_type: posix
        direct_access: True
        urlpath: ./intake-esm/tests/sample_data/cmip/CMIP6/CMIP/BCC/BCC-CSM2-MR
        exclude_dirs: ['*/files/*', 'latest']
        file_extension: .nc

    extra_attributes:
      mip_era: CMIP6
      activity_id: CMIP
      institution_id: BCC

  BCC-ESM1:
    locations:
     -  name: SAMPLE-DATA
        loc_type: posix
        direct_access: True
        urlpath: ./tests/sample_data/cmip/CMIP6/CMIP/BCC/BCC-ESM1
        exclude_dirs: ['*/files/*', 'latest']
        file_extension: .nc

    extra_attributes:
      mip_era: CMIP6
      activity_id: CMIP
      institution_id: BCC

  CNRM-CM6-1:
    locations:
     -  name: SAMPLE-DATA
        loc_type: posix
        direct_access: True
        urlpath: ./tests/sample_data/cmip/CMIP6/CMIP/CNRM-CERFACS/CNRM-CM6-1
        exclude_dirs: ['*/files/*', 'latest']
        file_extension: .nc

    extra_attributes:
      mip_era: CMIP6
      activity_id: CMIP
      institution_id: CNRM-CERFACS

  CNRM-ESM2-1:
    locations:
     -  name: SAMPLE-DATA
        loc_type: posix
        direct_access: True
        urlpath: ./tests/sample_data/cmip/CMIP6/CMIP/CNRM-CERFACS/CNRM-ESM2-1
        exclude_dirs: ['*/files/*', 'latest']
        file_extension: .nc

    extra_attributes:
      mip_era: CMIP6
      activity_id: CMIP
      institution_id: CNRM-CERFACS

  # GISS-E2-1-G:
  #   locations:
  #    -  name: SAMPLE-DATA
  #       loc_type: posix
  #       direct_access: True
  #       urlpath: ./tests/sample_data/cmip/CMIP6/CMIP/NASA-GISS/GISS-E2-1-G
  #       exclude_dirs: ['*/files/*', 'latest', *_historical]
  #       file_extension: .nc

  #   extra_attributes:
  #     mip_era: CMIP6
  #     activity_id: CMIP
  #     institution_id: NASA-GISS
"""
)


def test_build_collection_file():
    with config.set({'database-directory': './tests/test_collections'}):
        col = intake.open_esm_metadatastore(
            collection_input_definition=cdef, overwrite_existing=True
        )
        col = intake.open_esm_metadatastore(collection_name='cmip6_test_collection')
        assert isinstance(col.df, pd.DataFrame)


def test_search():
    with config.set({'database-directory': './tests/test_collections'}):
        c = intake.open_esm_metadatastore(collection_name='cmip6_test_collection')
        cat = c.search(source_id=['BCC-ESM1', 'CNRM-CM6-1', 'CNRM-ESM2-1'])
        assert isinstance(cat.query_results, pd.DataFrame)
        assert not cat.query_results.empty


@pytest.mark.parametrize(
    'chunks, expected_chunks',
    [({'time': 1, 'lat': 2, 'lon': 2}, (1, 2, 2)), ({'time': 2, 'lat': 1, 'lon': 1}, (2, 1, 1))],
)
def test_to_xarray_cmip(chunks, expected_chunks):
    with config.set({'database-directory': './tests/test_collections'}):
        c = intake.open_esm_metadatastore(collection_name='cmip6_test_collection')

        # Test for data from multiple models
        cat = c.search(source_id=['CNRM-ESM2-1', 'CNRM-CM6-1', 'BCC-ESM1'], variable_id=['tasmax'])
        ds = cat.to_xarray(decode_times=False, chunks=chunks)
        print(ds)
        assert isinstance(ds, dict)
        _, dset = ds.popitem()
        assert dset['tasmax'].data.chunksize == expected_chunks
