import os

import pandas as pd
import polars as pl

here = os.path.abspath(os.path.dirname(__file__))
zarr_cat_pangeo_cmip6 = 'https://storage.googleapis.com/cmip6/pangeo-cmip6.json'
cdf_cat_sample_cmip6 = os.path.join(here, 'sample-catalogs/cmip6-netcdf.json')
multi_variable_cat = os.path.join(here, 'sample-catalogs/multi-variable-catalog.json')
cdf_cat_sample_cmip5 = os.path.join(here, 'sample-catalogs/cmip5-netcdf.json')
cdf_cat_sample_cmip5_pq = os.path.join(here, 'sample-catalogs/cmip5-netcdf-parquet.json')
cdf_cat_sample_cesmle = os.path.join(here, 'sample-catalogs/cesm1-lens-netcdf.json')
catalog_dict_records = os.path.join(here, 'sample-catalogs/catalog-dict-records.json')
cdf_cat_sample_cmip6_noagg = os.path.join(here, 'sample-catalogs/cmip6-netcdf-noagg.json')
opendap_cat_sample_noaa = os.path.join(here, 'sample-catalogs/noaa-pathfinder-opendap.json')
zarr_cat_aws_cesm = (
    'https://raw.githubusercontent.com/NCAR/cesm-lens-aws/master/intake-catalogs/aws-cesm1-le.json'
)
mixed_cat_sample_cmip6 = os.path.join(here, 'sample-catalogs/cmip6-bcc-mixed-formats.json')
access_columns_with_lists_cat = os.path.join(here, 'sample-catalogs/access-columns-with-lists.json')
access_columns_with_lists_bz2_cat = os.path.join(
    here, 'sample-catalogs/access-columns-with-lists-bz2.json'
)

access_columns_with_tuples_cat = os.path.join(
    here, 'sample-catalogs/access-columns-with-tuples.json'
)
access_columns_with_sets_cat = os.path.join(here, 'sample-catalogs/access-columns-with-sets.json')
access_single_item_iterables_cat = os.path.join(
    here, 'sample-catalogs/access-single-item-iterables.json'
)
zarr_v2_cat = os.path.join(here, 'sample-catalogs/cesm1-lens-zarr2.json')
zarr_v3_cat = os.path.join(here, 'sample-catalogs/cesm1-lens-zarr2.json')


sample_df = pd.DataFrame(
    [
        {
            'component': 'atm',
            'frequency': 'daily',
            'experiment': '20C',
            'variable': 'FLNS',
            'path': 's3://ncar-cesm-lens/atm/daily/cesmLE-20C-FLNS.zarr',
            'format': 'zarr',
        },
        {
            'component': 'atm',
            'frequency': 'daily',
            'experiment': '20C',
            'variable': 'FLNSC',
            'path': 's3://ncar-cesm-lens/atm/daily/cesmLE-20C-FLNSC.zarr',
            'format': 'zarr',
        },
    ]
)


sample_pl_df = pl.from_pandas(sample_df)
sample_lf = sample_pl_df.lazy()

sample_esmcat_data = {
    'esmcat_version': '0.1.0',
    'id': 'aws-cesm1-le',
    'description': '',
    'catalog_file': '',
    'attributes': [],
    'assets': {'column_name': 'path', 'format': 'zarr'},
    'aggregation_control': {
        'variable_column_name': 'variable',
        'groupby_attrs': ['component', 'experiment', 'frequency'],
        'aggregations': [
            {'type': 'union', 'attribute_name': 'variable', 'options': {'compat': 'override'}}
        ],
    },
}

sample_esmcat_data_without_agg = {
    'esmcat_version': '0.1.0',
    'id': 'aws-cesm1-le',
    'description': '',
    'catalog_file': '',
    'attributes': [],
    'assets': {'column_name': 'path', 'format': 'zarr'},
}
