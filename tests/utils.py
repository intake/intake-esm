import os

import pandas as pd

here = os.path.abspath(os.path.dirname(__file__))
zarr_col_pangeo_cmip6 = 'https://storage.googleapis.com/cmip6/pangeo-cmip6.json'
cdf_col_sample_cmip6 = os.path.join(here, 'sample-collections/cmip6-netcdf.json')
multi_variable_col = os.path.join(here, 'sample-collections/multi-variable-collection.json')
cdf_col_sample_cmip5 = os.path.join(here, 'sample-collections/cmip5-netcdf.json')
cdf_col_sample_cesmle = os.path.join(here, 'sample-collections/cesm1-lens-netcdf.json')
catalog_dict_records = os.path.join(here, 'sample-collections/catalog-dict-records.json')
zarr_col_aws_cesm = (
    'https://raw.githubusercontent.com/NCAR/cesm-lens-aws/master/intake-catalogs/aws-cesm1-le.json'
)
mixed_col_sample_cmip6 = os.path.join(here, 'sample-collections/cmip6-bcc-mixed-formats.json')


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

sample_esmcol_data = {
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

sample_esmcol_data_without_agg = {
    'esmcat_version': '0.1.0',
    'id': 'aws-cesm1-le',
    'description': '',
    'catalog_file': '',
    'attributes': [],
    'assets': {'column_name': 'path', 'format': 'zarr'},
}
