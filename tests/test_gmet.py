import os

import intake
import pandas as pd
import pytest
import xarray as xr

from intake_esm import config
from intake_esm.core import ESMMetadataStoreCatalog

here = os.path.abspath(os.path.dirname(__file__))


def test_build_collection():
    with config.set({'database-directory': './tests/test_collections'}):
        collection_input_definition = os.path.join(here, 'gmet-list.yml')
        col = intake.open_esm_metadatastore(
            collection_input_definition=collection_input_definition, overwrite_existing=True
        )
        assert isinstance(col.df, pd.DataFrame)


def test_search():
    with config.set({'database-directory': './tests/test_collections'}):
        col = intake.open_esm_metadatastore(collection_name='gmet_test')
        cat = col.search(
            member_id=[1, 2],
            time_range=['19800101-19801231', '19810101-19811231', '19820101-19821231'],
        )

        assert isinstance(cat.query_results, pd.DataFrame)
        assert not cat.query_results.empty


@pytest.mark.skip
def test_to_xarray():
    with config.set({'database-directory': './tests/test_collections'}):
        col = intake.open_esm_metadatastore(collection_name='mpige_test')
        cat = col.search(component='mpiom', stream='monitoring_ym')
        with pytest.warns(UserWarning):
            ds = cat.to_xarray()
            assert isinstance(ds, dict)

        cat = col.search(
            component=['mpiom', 'hamocc'],
            stream='monitoring_ym',
            experiment=['hist', 'rcp85'],
            ensemble=[2, 3],
        )
        ds = cat.to_xarray(merge_exp=False)
        assert 'experiment_id' in ds.coords
