import ast

import intake
import pandas as pd
import pytest

import intake_esm

from .utils import (
    catalog_dict_records,
    cdf_col_sample_cesmle,
    cdf_col_sample_cmip5,
    cdf_col_sample_cmip6,
    multi_variable_col,
    sample_df,
    sample_esmcol_data,
    zarr_col_aws_cesm,
    zarr_col_pangeo_cmip6,
)


@pytest.mark.parametrize(
    'obj, sep, read_csv_kwargs',
    [
        (catalog_dict_records, '.', None),
        (cdf_col_sample_cmip6, '/', None),
        (zarr_col_aws_cesm, '.', None),
        (zarr_col_pangeo_cmip6, '*', None),
        (cdf_col_sample_cesmle, '.', None),
        (multi_variable_col, '*', {'converters': {'variable': ast.literal_eval}}),
        ({'esmcat': sample_esmcol_data, 'df': sample_df}, '.', None),
    ],
)
def test_catalog_init(obj, sep, read_csv_kwargs):
    """Test that the catalog can be initialized."""
    cat = intake.open_esm_datastore(obj, sep=sep, read_csv_kwargs=read_csv_kwargs)
    assert isinstance(cat.esmcat, intake_esm._types.ESMCatalogModel)
    assert isinstance(cat.df, pd.DataFrame)
    assert len(cat) > 0


@pytest.mark.parametrize(
    'obj, sep, read_csv_kwargs',
    [
        (multi_variable_col, '.', {'converters': {'variable': ast.literal_eval}}),
        (cdf_col_sample_cesmle, '/', None),
        (cdf_col_sample_cmip5, '.', None),
        (cdf_col_sample_cmip6, '*', None),
        (catalog_dict_records, '.', None),
        ({'esmcat': sample_esmcol_data, 'df': sample_df}, '.', None),
    ],
)
def test_catalog_unique(obj, sep, read_csv_kwargs):
    cat = intake.open_esm_datastore(obj, sep=sep, read_csv_kwargs=read_csv_kwargs)
    uniques = cat.unique()
    nuniques = cat.nunique()
    assert isinstance(uniques, pd.Series)
    assert isinstance(nuniques, pd.Series)
    assert set(uniques.keys()) == set(cat.df.columns)
