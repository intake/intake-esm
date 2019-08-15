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
    TEST:
      locations:
      -  name: SAMPLE-DATA
         loc_type: posix
         direct_access: True
         urlpath: ./tests/sample_data/cmip/CMIP6
         exclude_dirs: ['*/files/*', 'latest']
         file_extension: .nc
  """
)


def test_build_collection_file():
    with config.set({'database-directory': './tests/test_collections'}):
        col = intake.open_esm_metadatastore(
            collection_input_definition=cdef, overwrite_existing=True
        )
        col = intake.open_esm_metadatastore(collection_name='cmip6_test_collection')
        assert isinstance(col.ds, xr.Dataset)
        assert isinstance(col.df, pd.DataFrame)
        assert set(col.df.grid_label.unique()) == set(['gr', 'gn'])
        assert set(col.df.variable_id.unique()) == set(
            [
                'prsn',
                'prra',
                'tasmax',
                'evspsblveg',
                'landCoverFrac',
                'mrso',
                'co3',
                'gpp',
                'residualFrac',
                'mrfso',
            ]
        )


def test_search():
    with config.set({'database-directory': './tests/test_collections'}):
        c = intake.open_esm_metadatastore(collection_name='cmip6_test_collection')
        cat = c.search(source_id=['BCC-ESM1', 'CNRM-CM6-1', 'CNRM-ESM2-1'])
        assert isinstance(cat.ds, xr.Dataset)
        assert len(cat.ds.index) > 0

        assert isinstance(cat.df, pd.DataFrame)


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
