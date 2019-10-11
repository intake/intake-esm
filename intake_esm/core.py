import json
import logging
from urllib.parse import urlparse

import intake
import intake_xarray
import numpy as np
import pandas as pd
import requests
from tqdm.auto import tqdm

from .merge_util import (
    _create_asset_info_lookup,
    _restore_non_dim_coords,
    aggregate,
    to_nested_dict,
)

logger = logging.getLogger(__name__)


class ESMMetadataStoreCollection(intake.catalog.Catalog):
    name = 'esm_metadatastore'

    def __init__(self, esmcol_path):
        super().__init__()
        self.esmcol_path = esmcol_path
        self._col_data = _fetch_and_parse_file(esmcol_path)
        self.df = pd.read_csv(self._col_data['catalog_file'])
        self._entries = {}

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
        name = f'esm-collection-{str(uuid.uuid4())}'
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
        """ Return unique values for given columns"""
        if isinstance(columns, str):
            columns = [columns]
        if not columns:
            columns = self.df.columns

        info = {}
        for col in columns:
            uniques = self.df[col].unique().tolist()
            info[col] = {'count': len(uniques), 'values': uniques}
        return info

    def __repr__(self):
        """Make string representation of object."""
        info = self.nunique().to_dict()
        output = []
        for key, values in info.items():
            output.append(f'{values} {key}(s)\n')
        output = '\n\t> '.join(output)
        items = len(self.df.index)
        return f'ESM Collection with {items} entries:\n\t> {output}'


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

    def to_xarray(self, zarr_kwargs={}, cdf_kwargs={}):
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
        >>> dsets = cat.to_xarray(cdf_kwargs={'chunks': {'time' : 36}, 'decode_times': False})
        --> The keys in the returned dictionary of datasets are constructed as follows:
                'activity_id.institution_id.source_id.experiment_id.table_id.grid_label'
        Dataset(s): 100%|██████████████████████████████████████████████████████████| 2/2 [00:17<00:00,  8.57s/it]
        >>> dsets.keys()
        dict_keys(['CMIP.BCC.BCC-CSM2-MR.historical.Amon.gn', 'ScenarioMIP.BCC.BCC-CSM2-MR.ssp585.Amon.gn'])


        """
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

        if use_format_column:
            format_column_name = self._col_data['assets']['format_column_name']

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

        # the number of aggregation columns determines the level of recursion
        n_agg = len(agg_columns)

        print(
            f"""--> The keys in the returned dictionary of datasets are constructed as follows:\n\t'{".".join(groupby_attrs)}'"""
        )

        if groupby_attrs:
            groups = self.df.groupby(groupby_attrs)
        else:
            groups = self.df.groupby(self.df.columns.tolist())

        dsets = {}

        for compat_key, compatible_group in tqdm(groups, desc='Dataset(s)', leave=True):
            mi = compatible_group.set_index(agg_columns)
            nd = to_nested_dict(mi[path_column_name])
            if use_format_column:
                lookup = _create_asset_info_lookup(
                    compatible_group,
                    path_column_name,
                    variable_column_name,
                    format_column_name=format_column_name,
                )
            else:

                lookup = _create_asset_info_lookup(
                    compatible_group,
                    path_column_name,
                    variable_column_name,
                    data_format=self._col_data['assets']['format'],
                )

            ds = aggregate(
                aggregation_dict, agg_columns, n_agg, nd, lookup, self.zarr_kwargs, self.cdf_kwargs
            )
            group_id = '.'.join(compat_key)
            dsets[group_id] = _restore_non_dim_coords(ds)

        self._ds = dsets


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
            logger.info('Loading ESMCol from URL')
            resp = requests.get(input_path)
            data = resp.json()
        else:
            with open(input_path) as f:
                logger.info('Loading ESMCol from filesystem')
                data = json.load(f)

    except Exception as e:
        raise e

    return data
