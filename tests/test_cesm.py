import os

import intake
import pandas as pd
import pytest
import xarray as xr

# pytest imports this package last, so plugin is not auto-added
from intake_esm.core import ESMMetadataStoreCatalog

here = os.path.abspath(os.path.dirname(__file__))


def test_build_collection():
    collection_input_file = os.path.join(here, 'collection_input_test.yml')
    col = intake.open_esm_metadatastore(collection_input_file=collection_input_file)
    assert isinstance(col.df, pd.DataFrame)

    with pytest.raises(ValueError):
        col = intake.open_esm_metadatastore(
            collection_input_file=collection_input_file,
            collection_name='cesm_dple',
            collection_type='cesm',
        )


@pytest.mark.parametrize('collection', ['cesm_dple_test_collection'])
def test_constructor(collection):
    c = intake.open_esm_metadatastore(collection_name=collection, collection_type='cesm')
    assert isinstance(c, ESMMetadataStoreCatalog)


def test_search():
    c = intake.open_esm_metadatastore(
        collection_name='cesm_dple_test_collection', collection_type='cesm'
    )
    cat = c.search(variable='O2', direct_access=True)

    assert isinstance(cat.results, pd.DataFrame)
    assert not cat.results.empty


def test_cat():
    cat = intake.open_catalog(os.path.join(here, 'catalog.yaml'))
    cat = cat['cesm_dple_test_collection_7afe8a3a-8d2f-40a8-9ede-899f48ce83b2']
    assert isinstance(cat.results, pd.DataFrame)


def test_to_xarray():
    c = intake.open_esm_metadatastore(
        collection_name='cesm_dple_test_collection', collection_type='cesm'
    )
    cat = c.search(variable='O2', direct_access=True)
    ds = cat.to_xarray()
    assert isinstance(ds, xr.Dataset)
