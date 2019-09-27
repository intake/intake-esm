import intake
import pandas as pd
import pytest
import xarray as xr
import yaml

from intake_esm import config

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
          urlpath: gs://pangeo-cmip6/AR6_WG1/AerChemMIP
          data_format: zarr
  """
)


def test_build_collection_file():
    with config.set({'database-directory': './tests/test_collections'}):
        col = intake.open_esm_metadatastore(
            collection_input_definition=cdef, overwrite_existing=True
        )
        col = intake.open_esm_metadatastore(collection_name='PANGEO-CMIP6')
        assert isinstance(col._collection, xr.Dataset)


def test_search():
    with config.set({'database-directory': './tests/test_collections'}):
        col = intake.open_esm_metadatastore(collection_name='PANGEO-CMIP6')
        cat = col.search(variable_id=['pr', 'ts'], experiment_id='ssp370')
        assert len(cat.df) > 0


def test_to_xarray():
    with config.set({'database-directory': './tests/test_collections'}):
        col = intake.open_esm_metadatastore(collection_name='PANGEO-CMIP6')
        cat = col.search(variable_id=['pr'], experiment_id='ssp370')
        _, ds = cat.to_xarray().popitem()
        assert isinstance(ds, xr.Dataset)
