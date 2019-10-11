import os

import intake
import pandas as pd
import pytest

here = os.path.abspath(os.path.dirname(__file__))
esmcol_path = os.path.join(here, 'cmip5-netcdf.json')


def test_search():
    c = intake.open_esm_metadatastore(esmcol_path)
    cat = c.search(model=['CanESM2', 'CSIRO-Mk3-6-0'])
    assert isinstance(cat.df, pd.DataFrame)
    assert len(cat.df) > 0


@pytest.mark.parametrize(
    'chunks, expected_chunks',
    [
        ({'time': 1, 'lat': 2, 'lon': 2}, (1, 1, 2, 2)),
        ({'time': 2, 'lat': 1, 'lon': 1}, (1, 2, 1, 1)),
    ],
)
def test_to_xarray_cmip(chunks, expected_chunks):
    c = intake.open_esm_metadatastore(esmcol_path)
    cat = c.search(variable=['hfls'], frequency='mon', modeling_realm='atmos', model=['CNRM-CM5'])

    dset = cat.to_dataset_dict(cdf_kwargs=dict(chunks=chunks))
    _, ds = dset.popitem()
    assert ds['hfls'].data.chunksize == expected_chunks
