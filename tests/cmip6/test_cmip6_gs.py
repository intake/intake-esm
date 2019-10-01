import os

import intake
import pandas as pd
import pytest
import xarray as xr
import yaml

from intake_esm import config

here = os.path.abspath(os.path.dirname(__file__))

cdef = yaml.safe_load(
    """
  name: PANGEO-CMIP6
  collection_type: cmip6
  data_sources:
    AR6_WG1:
      locations:
        - name: AerChemMIP
          loc_type: gs
          direct_access: True
          urlpath: gs://cmip6/AerChemMIP
          data_format: zarr
  """
)

storelist = os.path.join(here, 'storelist.txt')
cdef2 = yaml.safe_load(
    f"""
  name: PANGEO-CMIP6
  collection_type: cmip6
  data_sources:
    CMIP6:
      locations:
        - name: Google Storage
          loc_type: input-file
          direct_access: True
          urlpath: {storelist}
          data_format: zarr
  """
)


def test_build_collection_file():
    with config.set({'database-directory': './tests/test_collections'}):
        col = intake.open_esm_metadatastore(
            collection_input_definition=cdef2, overwrite_existing=True
        )
        col = intake.open_esm_metadatastore(collection_name='PANGEO-CMIP6')
        assert isinstance(col._collection, xr.Dataset)


def test_search():
    with config.set({'database-directory': './tests/test_collections'}):
        col = intake.open_esm_metadatastore(collection_name='PANGEO-CMIP6')
        cat = col.search(
            variable_id=['pr'],
            experiment_id='ssp370',
            activity_id='AerChemMIP',
            source_id='BCC-ESM1',
            table_id='Amon',
            grid_label='gn',
        )
        assert len(cat.df) > 0
        assert len(col.df.columns) == len(cat.df.columns)
        assert len(cat._df.columns) >= len(cat.df.columns)


def test_to_xarray():
    with config.set({'database-directory': './tests/test_collections'}):
        col = intake.open_esm_metadatastore(collection_name='PANGEO-CMIP6')
        cat = col.search(variable_id=['pr'], experiment_id='ssp370')
        _, ds = cat.to_xarray().popitem()
        assert isinstance(ds, xr.Dataset)
