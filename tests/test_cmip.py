import os

import intake
import pandas as pd
import pytest
import xarray as xr

here = os.path.abspath(os.path.dirname(__file__))


def test_build_collection():
    collection_input_file = os.path.join(here, 'cmip_collection_input_test.yml')
    col = intake.open_esm_metadatastore(collection_input_file=collection_input_file)
    assert isinstance(col.df, pd.DataFrame)


def test_search():
    c = intake.open_esm_metadatastore(
        collection_name='cmip_test_collection', collection_type='cmip'
    )
    cat = c.search(
        model='CanESM2', experiment='rcp85', frequency='mon', realm='atmos', ensemble='r2i1p1'
    )
    assert isinstance(cat.results, pd.DataFrame)
    assert not cat.results.empty


def test_cat():
    cat = intake.open_catalog(os.path.join(here, 'cmip_catalog.yaml'))
    cat = cat['cmip_test_collection_a4fa3aaa-d4f5-4da0-9f6e-dc10e79d1452']
    assert isinstance(cat.results, pd.DataFrame)


def test_to_xarray():
    c = intake.open_esm_metadatastore(
        collection_name='cmip_test_collection', collection_type='cmip'
    )
    cat = c.search(
        model='CanESM2', experiment='rcp85', frequency='mon', realm='atmos', ensemble='r2i1p1'
    )

    ds = cat.to_xarray()
    assert isinstance(ds, xr.Dataset)
