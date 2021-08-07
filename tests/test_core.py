import os
from tempfile import TemporaryDirectory
from unittest import mock as mock

import intake
import pandas as pd
import pytest
import xarray as xr

import intake_esm

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
    'assets': {'column_name': 'path', 'format_column_name': 'format'},
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

zarr_query = dict(
    variable_id=['pr'],
    experiment_id='ssp370',
    activity_id='AerChemMIP',
    source_id='BCC-ESM1',
    table_id='Amon',
    grid_label='gn',
)
cdf_query = dict(source_id=['CNRM-ESM2-1', 'CNRM-CM6-1', 'BCC-ESM1'], variable_id=['tasmax'])


@pytest.mark.parametrize(
    'url',
    [
        zarr_col_aws_cesm,
        catalog_dict_records,
        cdf_col_sample_cesmle,
        cdf_col_sample_cmip5,
        cdf_col_sample_cmip6,
    ],
)
def test_init(capsys, url):
    col = intake.open_esm_datastore(url)
    assert isinstance(col.df, pd.DataFrame)
    print(repr(col))
    # Use pytest-capturing method
    # https://docs.pytest.org/en/latest/capture.html#accessing-captured-output-from-a-test-function
    captured = capsys.readouterr()
    assert 'catalog with' in captured.out


def test_ipython_display():
    col = intake.open_esm_datastore(catalog_dict_records)
    pytest.importorskip('IPython')
    with mock.patch('IPython.display.display') as ipy_display:
        col._ipython_display_()
        ipy_display.assert_called_once()


@pytest.mark.parametrize(
    'df, esmcol_data, data_format, data_format_column',
    [
        (sample_df, sample_esmcol_data, None, 'format'),
        (sample_df, sample_esmcol_data_without_agg, 'zarr', None),
    ],
)
def test_init_from_df(df, esmcol_data, data_format, data_format_column):
    col = intake.open_esm_datastore(df, esmcol_data)
    pd.testing.assert_frame_equal(df, col.df)
    assert col.data_format == data_format
    assert col.format_column_name == data_format_column
    assert col.path_column_name == 'path'


@pytest.mark.parametrize('esmcol_obj', [None, open(catalog_dict_records), sample_df])
def test_init_error(esmcol_obj):
    with pytest.raises(ValueError):
        intake.open_esm_datastore(esmcol_obj)


@pytest.mark.parametrize(
    'url',
    [
        zarr_col_aws_cesm,
        catalog_dict_records,
        cdf_col_sample_cesmle,
        cdf_col_sample_cmip5,
        cdf_col_sample_cmip6,
        multi_variable_col,
    ],
)
def test_repr_html(url):
    col = intake.open_esm_datastore(url)
    text = col._repr_html_()
    assert 'unique' in text
    columns = col.df.columns.tolist()
    for column in columns:
        assert column in text


def test_ipython_key_completions():
    col = intake.open_esm_datastore(cdf_col_sample_cmip6)
    rv = [
        'df',
        'to_dataset_dict',
        'from_df',
        'keys',
        'serialize',
        'search',
        'unique',
        'nunique',
        'update_aggregation',
        'key_template',
        'groupby_attrs',
        'variable_column_name',
        'aggregations',
        'agg_columns',
        'aggregation_dict',
        'path_column_name',
        'data_format',
        'format_column_name',
    ]
    keys = col._ipython_key_completions_()
    for key in rv:
        assert key in keys


@pytest.mark.parametrize(
    'url, columns',
    [
        (zarr_col_aws_cesm, None),
        (catalog_dict_records, None),
        (cdf_col_sample_cesmle, ['experiment', 'component']),
        (cdf_col_sample_cmip5, None),
        (cdf_col_sample_cmip6, ['experiment_id']),
        (multi_variable_col, None),
    ],
)
def test_col_unique(url, columns):
    col = intake.open_esm_datastore(url)
    uniques = col.unique(columns=columns)
    assert isinstance(uniques, dict)
    if columns is None:
        assert set(uniques.keys()) == set(col.df.columns.tolist())
    else:
        assert set(uniques.keys()) == set(columns)
    assert isinstance(col.nunique(), pd.Series)


@pytest.mark.parametrize(
    'key',
    [
        'CMIP.CNRM-CERFACS.CNRM-CM6-1.historical.Lmon.gr',
        'CMIP.CNRM-CERFACS.CNRM-CM6-1.piControl.Lmon.gr',
        'CMIP.CNRM-CERFACS.CNRM-ESM2-1.1pctCO2.Omon.gn',
        'CMIP.CNRM-CERFACS.CNRM-ESM2-1.abrupt-4xCO2.Amon.gr',
        'CMIP.CNRM-CERFACS.CNRM-ESM2-1.amip.Amon.gr',
    ],
)
@pytest.mark.parametrize('decode_times', [True, False])
def test_getitem(key, decode_times):
    col = intake.open_esm_datastore(cdf_col_sample_cmip6)
    x = col[key]
    assert isinstance(x, intake_esm.source.ESMGroupDataSource)
    ds = x(cdf_kwargs={'chunks': {}, 'decode_times': decode_times}).to_dask()
    assert isinstance(ds, xr.Dataset)
    assert set(x.df['member_id']) == set(ds['member_id'].values)


def test_getitem_error():
    col = intake.open_esm_datastore(cdf_col_sample_cmip6)
    with pytest.raises(KeyError):
        key = 'DOES.NOT.EXIST'
        col[key]


@pytest.mark.parametrize(
    'key, expected',
    [
        ('CMIP.CNRM-CERFACS.CNRM-CM6-1.historical.Amon.gr', True),
        (
            './tests/sample_data/cmip/CMIP6/CMIP/IPSL/IPSL-CM6A-LR/historical/r23i1p1f1/Omon/prsn/gr/v20180803/prsn/prsn_Omon_IPSL-CM6A-LR_historical_r23i1p1f1_gr_185001-201412.nc',
            False,
        ),
        ('DOES_NOT_EXIST', False),
    ],
)
def test_contains(key, expected):
    col = intake.open_esm_datastore(cdf_col_sample_cmip6)
    actual = key in col
    assert actual == expected


def test_df_property():
    col = intake.open_esm_datastore(catalog_dict_records)
    assert len(col.df) == 5
    col.df = col.df.iloc[0:2, :]
    assert isinstance(col.df, pd.DataFrame)
    assert len(col) == 1
    assert len(col.df) == 2


@pytest.mark.parametrize(
    'property, expected',
    [
        ('groupby_attrs', ['component', 'experiment', 'frequency']),
        ('variable_column_name', 'variable'),
        (
            'aggregations',
            [{'type': 'union', 'attribute_name': 'variable', 'options': {'compat': 'override'}}],
        ),
        ('agg_columns', ['variable']),
        ('aggregation_dict', {'variable': {'type': 'union', 'options': {'compat': 'override'}}}),
        ('path_column_name', 'path'),
        ('data_format', 'zarr'),
        ('format_column_name', None),
    ],
)
def test_aggregation_properties_getter(property, expected):
    col = intake.open_esm_datastore(catalog_dict_records)
    value = getattr(col, property)
    assert value == expected


@pytest.mark.parametrize(
    'property, value',
    [
        ('data_format', 'netcdf'),
        ('groupby_attrs', []),
        ('variable_column_name', 'foo'),
        ('path_column_name', 'bar'),
        ('format_column_name', 'foobar'),
    ],
)
def test_aggregation_properties_setter(property, value):
    col = intake.open_esm_datastore(catalog_dict_records)
    setattr(col, property, value)
    res_value = getattr(col, property)
    assert res_value == value


@pytest.mark.parametrize(
    'attribute_name, agg_type, options, delete',
    [
        ('variable', 'union', {'dim': 'time'}, False),
        ('variable', 'union', {}, False),
        ('experiment', 'union', None, False),
        ('component', 'join_existing', {'compat': 'override'}, False),
        ('experiment', 'join_new', {'compat': 'override'}, False),
        ('experiment', None, None, True),
        ('variable', None, None, True),
    ],
)
def test_update_aggregation(attribute_name, agg_type, options, delete):
    col = intake.open_esm_datastore(catalog_dict_records)
    col.update_aggregation(attribute_name, agg_type, options, delete)
    if not delete:
        if options is None:
            options = {}
        assert col.aggregation_dict[attribute_name] == {'type': agg_type, 'options': options}
    else:
        assert attribute_name not in col.aggregation_dict


@pytest.mark.parametrize(
    'attribute_name, agg_type, options',
    [
        ('foo', 'union', {'dim': 'time'}),
        ('variable', 'merge', {}),
        ('experiment', 'join_new', 'foo'),
        ('variable', 'join_existing', 'bar'),
    ],
)
def test_update_aggregation_error(attribute_name, agg_type, options):
    col = intake.open_esm_datastore(catalog_dict_records)
    with pytest.raises(AssertionError):
        col.update_aggregation(attribute_name, agg_type, options)


@pytest.mark.parametrize(
    'aggregations, expected_aggregations, expected_aggregation_dict, expected_agg_columns',
    [
        (
            [
                {'type': 'union', 'attribute_name': 'variable_id'},
                {
                    'type': 'join_new',
                    'attribute_name': 'member_id',
                    'options': {'coords': 'minimal', 'compat': 'override'},
                },
                {
                    'type': 'join_new',
                    'attribute_name': 'dcpp_init_year',
                    'options': {'coords': 'minimal', 'compat': 'override'},
                },
            ],
            [
                {'type': 'union', 'attribute_name': 'variable_id'},
                {
                    'type': 'join_new',
                    'attribute_name': 'member_id',
                    'options': {'coords': 'minimal', 'compat': 'override'},
                },
                {
                    'type': 'join_new',
                    'attribute_name': 'dcpp_init_year',
                    'options': {'coords': 'minimal', 'compat': 'override'},
                },
            ],
            {
                'variable_id': {'type': 'union'},
                'member_id': {
                    'type': 'join_new',
                    'options': {'coords': 'minimal', 'compat': 'override'},
                },
                'dcpp_init_year': {
                    'type': 'join_new',
                    'options': {'coords': 'minimal', 'compat': 'override'},
                },
            },
            ['variable_id', 'member_id', 'dcpp_init_year'],
        ),
        (
            [
                {
                    'type': 'join_new',
                    'attribute_name': 'member_id',
                    'options': {'coords': 'minimal', 'compat': 'override'},
                },
                {'type': 'union', 'attribute_name': 'variable_id'},
                {
                    'type': 'join_new',
                    'attribute_name': 'dcpp_init_year',
                    'options': {'coords': 'minimal', 'compat': 'override'},
                },
            ],
            [
                {'type': 'union', 'attribute_name': 'variable_id'},
                {
                    'type': 'join_new',
                    'attribute_name': 'member_id',
                    'options': {'coords': 'minimal', 'compat': 'override'},
                },
                {
                    'type': 'join_new',
                    'attribute_name': 'dcpp_init_year',
                    'options': {'coords': 'minimal', 'compat': 'override'},
                },
            ],
            {
                'variable_id': {'type': 'union'},
                'member_id': {
                    'type': 'join_new',
                    'options': {'coords': 'minimal', 'compat': 'override'},
                },
                'dcpp_init_year': {
                    'type': 'join_new',
                    'options': {'coords': 'minimal', 'compat': 'override'},
                },
            },
            ['variable_id', 'member_id', 'dcpp_init_year'],
        ),
        ([], [], {}, []),
        (
            [{'type': 'join_existing', 'attribute_name': 'dcpp_init_year', 'options': {}}],
            [{'type': 'join_existing', 'attribute_name': 'dcpp_init_year', 'options': {}}],
            {'dcpp_init_year': {'type': 'join_existing', 'options': {}}},
            ['dcpp_init_year'],
        ),
    ],
)
def test_construct_agg_info(
    aggregations, expected_aggregations, expected_aggregation_dict, expected_agg_columns
):
    r_agg, r_agg_dict, r_agg_colums = intake_esm.core._construct_agg_info(aggregations)
    assert r_agg == expected_aggregations
    assert r_agg_dict == expected_aggregation_dict
    assert r_agg_colums == expected_agg_columns


def test_serialize_to_json():
    with TemporaryDirectory() as local_store:
        col = intake.open_esm_datastore(catalog_dict_records)
        name = 'test_serialize_dict'
        col.serialize(name=name, directory=local_store, catalog_type='dict')
        output_catalog = os.path.join(local_store, name + '.json')
        col2 = intake.open_esm_datastore(output_catalog)
        pd.testing.assert_frame_equal(col.df, col2.df)


def test_serialize_to_csv():
    col = intake.open_esm_datastore(cdf_col_sample_cmip6)
    with TemporaryDirectory() as local_store:
        col_subset = col.search(
            source_id='MRI-ESM2-0',
        )
        name = 'CMIP6-MRI-ESM2-0'
        col_subset.serialize(name=name, directory=local_store, catalog_type='file')
        col = intake.open_esm_datastore(f'{local_store}/{name}.json')
        pd.testing.assert_frame_equal(col_subset.df, col.df)
        assert col.esmcol_data['id'] == name


@pytest.mark.parametrize(
    'esmcol_path, query',
    [(zarr_col_pangeo_cmip6, zarr_query), (cdf_col_sample_cmip6, cdf_query)],
)
def test_search(esmcol_path, query):
    col = intake.open_esm_datastore(esmcol_path)
    cat = col.search(**query)
    assert len(cat.df) > 0
    assert len(col.df.columns) == len(cat.df.columns)


def test_empty_queries():
    col = intake.open_esm_datastore(cdf_col_sample_cmip6)
    msg = r'Query returned zero results.'
    with pytest.warns(UserWarning, match=msg):
        _ = col.search()

    with pytest.warns(UserWarning, match=msg):
        _ = col.search(variable_id='DONT_EXIST')

    cat = col.search()
    with pytest.warns(
        UserWarning, match=r'There are no datasets to load! Returning an empty dictionary.'
    ):
        dsets = cat.to_dataset_dict()
        assert not dsets


@pytest.mark.parametrize(
    'esmcol_path, query', [(zarr_col_pangeo_cmip6, zarr_query), (cdf_col_sample_cmip6, cdf_query)]
)
def test_to_dataset_dict(esmcol_path, query):
    col = intake.open_esm_datastore(esmcol_path)
    cat = col.search(**query)
    _, ds = cat.to_dataset_dict(
        zarr_kwargs={'consolidated': True},
        cdf_kwargs={'chunks': {'time': 1}},
        storage_options={'token': 'anon'},
    ).popitem()
    assert 'member_id' in ds.dims
    assert len(ds.__dask_keys__()) > 0
    assert ds.time.encoding


@pytest.mark.parametrize(
    'esmcol_path, query',
    [
        (cdf_col_sample_cmip6, {'experiment_id': ['historical', 'rcp85']}),
        (cdf_col_sample_cmip5, {'experiment': ['historical', 'rcp85']}),
    ],
)
def test_to_aggregations_off(esmcol_path, query):
    col = intake.open_esm_datastore(esmcol_path)
    cat = col.search(**query)
    nds = len(cat.df)
    cat.groupby_attrs = []
    assert len(cat.keys()) == nds
    assert isinstance(cat._grouped, pd.DataFrame)
    assert isinstance(col._grouped, pd.core.groupby.generic.DataFrameGroupBy)


@pytest.mark.parametrize(
    'esmcol_path, query',
    [
        (cdf_col_sample_cmip6, {'experiment_id': ['historical', 'rcp85']}),
        (cdf_col_sample_cmip5, {'experiment': ['historical', 'rcp85']}),
    ],
)
def test_to_dataset_dict_aggfalse(esmcol_path, query):
    col = intake.open_esm_datastore(esmcol_path)
    cat = col.search(**query)
    nds = len(cat.df)
    dsets = cat.to_dataset_dict(zarr_kwargs={'consolidated': True}, aggregate=False)
    assert len(dsets.keys()) == nds


@pytest.mark.parametrize(
    'esmcol_path, query',
    [(zarr_col_pangeo_cmip6, zarr_query), (cdf_col_sample_cmip6, cdf_query)],
)
def test_to_dataset_dict_w_preprocess(esmcol_path, query):
    def rename_coords(ds):
        return ds.rename({'lon': 'longitude', 'lat': 'latitude'})

    col = intake.open_esm_datastore(esmcol_path)
    col_sub = col.search(**query)
    dsets = col_sub.to_dataset_dict(
        zarr_kwargs={'consolidated': True},
        cdf_kwargs={'chunks': {'time': 1}},
        preprocess=rename_coords,
        storage_options={'token': 'anon'},
    )
    _, ds = dsets.popitem()
    assert 'latitude' in ds.dims
    assert 'longitude' in ds.dims


def test_to_dataset_dict_w_preprocess_error():
    col = intake.open_esm_datastore(cdf_col_sample_cmip5)
    with pytest.raises(ValueError, match=r'preprocess argument must be callable'):
        col.to_dataset_dict(preprocess='foo')


@pytest.mark.xskip(reason='Disable CMIP6 preprocessing for the time being')
def test_to_dataset_dict_w_cmip6preprocessing():
    col = intake.open_esm_datastore(zarr_col_pangeo_cmip6)
    pytest.importorskip('cmip6_preprocessing')
    from cmip6_preprocessing.preprocessing import combined_preprocessing

    cat = col.search(
        source_id='BCC-CSM2-MR',
        experiment_id='historical',
        table_id='Omon',
        variable_id='thetao',
        member_id='r1i1p1f1',
    )
    _, ds = cat.to_dataset_dict(
        zarr_kwargs={'consolidated': True, 'decode_times': False},
        preprocess=combined_preprocessing,
        storage_options={'token': 'anon'},
    ).popitem()
    assert isinstance(ds, xr.Dataset)


@pytest.mark.parametrize(
    'esmcol_path, query',
    [(zarr_col_pangeo_cmip6, zarr_query), (cdf_col_sample_cmip6, cdf_query)],
)
def test_to_dataset_dict_nocache(esmcol_path, query):
    col = intake.open_esm_datastore(esmcol_path)
    cat = col.search(**query)
    _, ds = cat.to_dataset_dict(
        zarr_kwargs={'consolidated': True}, storage_options={'token': 'anon'}
    ).popitem()
    id1 = id(ds)
    cat = col.search(**query)
    _, ds = cat.to_dataset_dict(
        zarr_kwargs={'consolidated': True}, storage_options={'token': 'anon'}
    ).popitem()
    assert id1 != id(ds)


@pytest.mark.skip(reason='LDEO opendap servers seem not be working properly')
def test_opendap_endpoint():
    col = intake.open_esm_datastore('http://haden.ldeo.columbia.edu/catalogs/hyrax_cmip6.json')
    cat = col.search(
        source_id='CAMS-CSM1-0',
        experiment_id='historical',
        member_id='r1i1p1f1',
        table_id='Amon',
        grid_label='gn',
        version='v1',
    )
    dsets = cat.to_dataset_dict(cdf_kwargs={'chunks': {'time': 36}})
    _, ds = dsets.popitem()
    assert isinstance(ds, xr.Dataset)


@pytest.mark.parametrize(
    'chunks, expected_chunks',
    [
        ({'time': 1, 'lat': 2, 'lon': 2}, (1, 1, 2, 2)),
        ({'time': 2, 'lat': 1, 'lon': 1}, (1, 2, 1, 1)),
    ],
)
def test_to_dataset_dict_chunking(chunks, expected_chunks):
    c = intake.open_esm_datastore(cdf_col_sample_cmip5)
    cat = c.search(variable=['hfls'], frequency='mon', modeling_realm='atmos', model=['CNRM-CM5'])

    dset = cat.to_dataset_dict(cdf_kwargs=dict(chunks=chunks))
    _, ds = dset.popitem()
    assert ds['hfls'].data.chunksize == expected_chunks


@pytest.mark.parametrize('progressbar', [False, True])
def test_progressbar(progressbar):
    c = intake.open_esm_datastore(cdf_col_sample_cmip5)
    cat = c.search(variable=['hfls'], frequency='mon', modeling_realm='atmos', model=['CNRM-CM5'])
    _ = cat.to_dataset_dict(cdf_kwargs=dict(chunks={}), progressbar=progressbar)


def test_to_dataset_dict_s3():
    pytest.importorskip('s3fs')
    col = intake.open_esm_datastore(zarr_col_aws_cesm)
    cat = col.search(variable='RAIN', experiment='20C')
    dsets = cat.to_dataset_dict(storage_options={'anon': True})
    _, ds = dsets.popitem()
    assert isinstance(ds, xr.Dataset)


@pytest.mark.parametrize(
    'query', [dict(variable=['O2', 'TEMP']), dict(variable=['SHF']), dict(experiment='CTRL')]
)
def test_multi_variable_catalog(query):
    import ast

    col = intake.open_esm_datastore(
        multi_variable_col, csv_kwargs={'converters': {'variable': ast.literal_eval}}
    )
    assert col._multiple_variable_assets

    col_sub = col.search(**query)
    assert set(col_sub._requested_variables) == set(query.pop('variable', []))

    _, ds = col_sub.to_dataset_dict().popitem()
    if col_sub._requested_variables:
        assert set(ds.data_vars) == set(col_sub._requested_variables)
