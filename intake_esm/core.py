import copy
import json
from urllib.parse import urlparse

import dask
import dask.delayed
import fsspec
import intake
import intake_xarray
import numpy as np
import pandas as pd
import requests

from .merge_util import (
    _create_asset_info_lookup,
    _restore_non_dim_coords,
    aggregate,
    to_nested_dict,
)


class ESMMetadataStoreCollection(intake.catalog.Catalog):
    name = 'esm_metadatastore'

    def __init__(self, esmcol_path, **kwargs):
        """ This Catalog is backed by a CSV file.

        The in-memory representation for this catalog is a Pandas DataFrame.

        Parameters
        ----------

        esmcol_path : str
           Path to an ESM collection JSON file
        **kwargs :
            Additional keyword arguments are passed through to the base class,
            Catalog.

        """
        self.esmcol_path = esmcol_path
        self._col_data = _fetch_and_parse_file(esmcol_path)
        self.df = pd.read_csv(self._col_data['catalog_file'])
        self._entries = {}
        super().__init__(**kwargs)

    def search(self, **query):
        """ Search for entries in the collection catalog

        Returns
        -------
        cat : Catalog
          A new Catalog with a subset of the entries in this Catalog.

        Examples
        --------
        >>> import intake
        >>> col = intake.open_esm_metadatastore("pangeo-cmip6.json")
        >>> cat = col.search(source_id=['BCC-CSM2-MR', 'CNRM-CM6-1', 'CNRM-ESM2-1'],
        ...                       experiment_id=['historical', 'ssp585'], variable_id='pr',
        ...                       table_id='Amon', grid_label='gn')
        >>> cat.df.head()
            activity_id institution_id  ... grid_label                                             zstore
        216           CMIP            BCC  ...         gn  gs://cmip6/CMIP/BCC/BCC-CSM2-MR/historical/r1i...
        302           CMIP            BCC  ...         gn  gs://cmip6/CMIP/BCC/BCC-CSM2-MR/historical/r2i...
        357           CMIP            BCC  ...         gn  gs://cmip6/CMIP/BCC/BCC-CSM2-MR/historical/r3i...
        17859  ScenarioMIP            BCC  ...         gn  gs://cmip6/ScenarioMIP/BCC/BCC-CSM2-MR/ssp585/...

        [4 rows x 9 columns]
        """

        import uuid

        args = {'esmcol_path': self.esmcol_path, 'query': query}
        name = f'{self._col_data["id"]}-esm-collection-{str(uuid.uuid4())}'
        description = ''
        driver = 'intake_esm.core.ESMDatasetSource'
        cat = intake.catalog.local.LocalCatalogEntry(
            name=name,
            description=description,
            driver=driver,
            direct_access=True,
            args=args,
            cache={},
            parameters={},
            metadata={},
            catalog_dir='',
            getenv=False,
            getshell=False,
        )
        self._entries[name] = cat
        return cat

    def nunique(self):
        """Count distinct observations across dataframe columns"""
        return self.df.nunique()

    def unique(self, columns=None):
        """ Return unique values for given columns
        Parameters
        ----------
        columns : str, list
           name of columns for which to get unique values

        info : dict
           dictionary containing count, and unique values

        """
        return _unique(self.df, columns)

    def __repr__(self):
        """Make string representation of object."""
        info = self.nunique().to_dict()
        output = []
        for key, values in info.items():
            output.append(f'{values} {key}(s)\n')
        output = '\n\t> '.join(output)
        items = len(self.df.index)
        return f'{self._col_data["id"]}-ESM Collection with {items} entries:\n\t> {output}'


def _unique(df, columns):
    if isinstance(columns, str):
        columns = [columns]
    if not columns:
        columns = df.columns

    info = {}
    for col in columns:
        uniques = df[col].unique().tolist()
        info[col] = {'count': len(uniques), 'values': uniques}
    return info


class ESMDatasetSource(intake_xarray.base.DataSourceMixin):
    container = 'xarray'
    name = 'esm-dataset-source'

    def __init__(self, esmcol_path, query, **kwargs):
        super().__init__(metadata={})
        self.esmcol_path = esmcol_path
        self._col_data = _fetch_and_parse_file(esmcol_path)
        self.df = self._get_subset(**query)
        self.urlpath = ''
        self._ds = None

    def _get_subset(self, **query):
        df = pd.read_csv(self._col_data['catalog_file'])
        if not query:
            return pd.DataFrame(columns=df.columns)
        condition = np.ones(len(df), dtype=bool)
        for key, val in query.items():
            if isinstance(val, list):
                condition_i = np.zeros(len(df), dtype=bool)
                for val_i in val:
                    condition_i = condition_i | (df[key] == val_i)
                condition = condition & condition_i
            elif val is not None:
                condition = condition & (df[key] == val)
        query_results = df.loc[condition]
        return query_results

    def unique(self, columns=None):
        """ Return unique values for given columns
        Parameters
        ----------
        columns : str, list
           name of columns for which to get unique values

        info : dict
           dictionary containing count, and unique values

        """
        return _unique(self.df, columns)

    def nunique(self):
        """Count distinct observations across dataframe columns"""
        return self.df.nunique()

    def to_dataset_dict(self, zarr_kwargs={}, cdf_kwargs={'chunks': {}}):
        """ Load catalog entries into a dictionary of xarray datasets.
        Parameters
        ----------
        zarr_kwargs : dict
            Keyword arguments to pass to `xarray.open_zarr()` function
        cdf_kwargs : dict
            Keyword arguments to pass to `xarray.open_dataset()` function

        Returns
        -------
        dsets : dict
           A dictionary of xarray datasets.

        Examples
        --------
        >>> import intake
        >>> col = intake.open_esm_metadatastore("glade-cmip6.json")
        >>> cat = col.search(source_id=['BCC-CSM2-MR', 'CNRM-CM6-1', 'CNRM-ESM2-1'],
        ...                       experiment_id=['historical', 'ssp585'], variable_id='pr',
        ...                       table_id='Amon', grid_label='gn')
        >>> dsets = cat.to_dataset_dict(cdf_kwargs={'chunks': {'time' : 36}, 'decode_times': False})
        --> The keys in the returned dictionary of datasets are constructed as follows:
                'activity_id.institution_id.source_id.experiment_id.table_id.grid_label'
        >>> dsets.keys()
        dict_keys(['CMIP.BCC.BCC-CSM2-MR.historical.Amon.gn', 'ScenarioMIP.BCC.BCC-CSM2-MR.ssp585.Amon.gn'])


        """
        if 'chunks' in cdf_kwargs and not cdf_kwargs['chunks']:
            print('xarray will load the datasets with dask using a single chunk for all arrays.')

        self.zarr_kwargs = zarr_kwargs
        self.cdf_kwargs = cdf_kwargs
        return self.to_dask()

    def _get_schema(self):
        from intake.source.base import Schema

        self._open_dataset()
        self._schema = Schema(
            datashape=None, dtype=None, shape=None, npartitions=None, extra_metadata={}
        )
        return self._schema

    def _open_dataset(self):

        path_column_name = self._col_data['assets']['column_name']
        if 'format' in self._col_data['assets']:
            use_format_column = False
        else:
            use_format_column = True

        mapper_dict = {
            path: _path_to_mapper(path) for path in self.df[path_column_name]
        }  # replace path column with mapper (dependent on filesystem type)

        groupby_attrs = self._col_data['aggregation_control'].get('groupby_attrs', [])
        aggregations = self._col_data['aggregation_control'].get('aggregations', [])
        variable_column_name = self._col_data['aggregation_control']['variable_column_name']

        aggregation_dict = {}
        for agg in aggregations:
            key = agg['attribute_name']
            rest = agg.copy()
            del rest['attribute_name']
            aggregation_dict[key] = rest

        agg_columns = list(aggregation_dict.keys())

        if groupby_attrs:
            groups = self.df.groupby(groupby_attrs)
        else:
            groups = self.df.groupby(self.df.columns.tolist())
        print(
            f"""--> The keys in the returned dictionary of datasets are constructed as follows:\n\t'{".".join(groupby_attrs)}'"""
        )
        print(f'\n--> There will be {len(groups)} group(s)')

        dsets = [
            _load_group_dataset(
                key,
                df,
                self._col_data,
                agg_columns,
                aggregation_dict,
                path_column_name,
                variable_column_name,
                use_format_column,
                mapper_dict,
                self.zarr_kwargs,
                self.cdf_kwargs,
            )
            for key, df in groups
        ]

        dsets = dask.compute(*dsets)
        del mapper_dict

        self._ds = {dset[0]: dset[1] for dset in dsets}


@dask.delayed
def _load_group_dataset(
    key,
    df,
    col_data,
    agg_columns,
    aggregation_dict,
    path_column_name,
    variable_column_name,
    use_format_column,
    mapper_dict,
    zarr_kwargs,
    cdf_kwargs,
):

    aggregation_dict = copy.deepcopy(aggregation_dict)
    agg_columns = agg_columns.copy()
    drop_cols = []
    for col in agg_columns:
        if df[col].isnull().all():
            drop_cols.append(col)
            del aggregation_dict[col]

    agg_columns = list(filter(lambda x: x not in drop_cols, agg_columns))
    # the number of aggregation columns determines the level of recursion
    n_agg = len(agg_columns)

    mi = df.set_index(agg_columns)
    nd = to_nested_dict(mi[path_column_name])

    if use_format_column:
        format_column_name = col_data['assets']['format_column_name']
        lookup = _create_asset_info_lookup(
            df, path_column_name, variable_column_name, format_column_name=format_column_name
        )
    else:

        lookup = _create_asset_info_lookup(
            df, path_column_name, variable_column_name, data_format=col_data['assets']['format']
        )

    ds = aggregate(
        aggregation_dict, agg_columns, n_agg, nd, lookup, mapper_dict, zarr_kwargs, cdf_kwargs
    )
    group_id = '.'.join(key)
    return group_id, _restore_non_dim_coords(ds)


def _is_valid_url(url):
    """ Check if path is URL or not
    Parameters
    ----------
    url : str
        path to check
    Returns
    -------
    boolean
    """
    try:
        result = urlparse(url)
        return result.scheme and result.netloc and result.path
    except Exception:
        return False


def _fetch_and_parse_file(input_path):
    """ Fetch and parse ESMCol file.
    Parameters
    ----------
    input_path : str
            ESMCol file to get and read
    Returns
    -------
    data : dict
    """

    data = None

    try:
        if _is_valid_url(input_path):
            print('Loading ESMCol from URL')
            resp = requests.get(input_path)
            data = resp.json()
        else:
            with open(input_path) as f:
                print('Loading ESMCol from filesystem')
                data = json.load(f)

    except Exception as e:
        raise e

    return data


def _path_to_mapper(path):
    """Convert path to mapper if necessary."""
    if fsspec.core.split_protocol(path)[0] is not None:
        return fsspec.get_mapper(path)
    else:
        return path
