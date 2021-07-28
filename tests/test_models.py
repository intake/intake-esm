import pytest

from intake_esm.models import Assets, ESMCatalog

from .utils import (
    catalog_dict_records,
    cdf_col_sample_cesmle,
    cdf_col_sample_cmip5,
    cdf_col_sample_cmip6,
    multi_variable_col,
    zarr_col_aws_cesm,
    zarr_col_pangeo_cmip6,
)


@pytest.mark.parametrize(
    'column_name, format, format_column_name', [('test', 'zarr', None), ('test', 'netcdf', None)]
)
def test_assets(column_name, format, format_column_name):
    a = Assets(column_name=column_name, format=format, format_column_name=format_column_name)
    assert a.column_name == column_name
    assert a.format == format
    assert a.format_column_name == format_column_name


def test_assets_mutually_exclusive():
    with pytest.raises(ValueError):
        Assets(column_name='test', format='netcdf', format_column_name='test')


@pytest.mark.parametrize(
    'file',
    [
        catalog_dict_records,
        cdf_col_sample_cmip6,
        cdf_col_sample_cmip5,
        zarr_col_aws_cesm,
        zarr_col_pangeo_cmip6,
        cdf_col_sample_cmip5,
        cdf_col_sample_cmip6,
        cdf_col_sample_cesmle,
        multi_variable_col,
    ],
)
def test_assets_from_file(file):
    cat = ESMCatalog.load_catalog_file(file)
    assert isinstance(cat, ESMCatalog)
