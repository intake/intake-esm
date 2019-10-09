import json
import logging
from urllib.parse import urlparse

import fsspec
import intake
import intake_xarray
import numpy as np
import pandas as pd
import requests
import xarray as xr

logger = logging.getLogger(__name__)


class ESMMetadataStoreCollection(intake.catalog.Catalog):
    name = 'esm_metadatastore'

    def __init__(self, esmcol_path):
        super().__init__()
        self.esmcol_path = esmcol_path
        self._col_data = _fetch_and_parse_file(esmcol_path)
        self.df = pd.read_csv(self._col_data['catalog_file'])

    def search(self, **query):
        """ Search for entries in the collection
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
        """ Return dataset as an xarray dataset
        Additional keyword arguments are passed through to
        `xarray.open_dataset()`, xarray.open_zarr()` methods
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

    def _get_open_dset_settings(self):

        if 'format' in self._col_data['assets']:
            use_format_column = False
        else:
            use_format_column = True

        union = set()
        join_new = set()
        aggs = self._col_data['aggregations']
        for agg in aggs:
            if agg['type'] == 'union':
                union.add(agg['attribute_name'])
            elif agg['type'] == 'join_new':
                join_new.add(agg['attribute_name'])

        dset_groupby_column_names = (
            set([item['column_name'] for item in self._col_data['attributes']])
            .difference(join_new)
            .difference(union)
        )

        return (use_format_column, list(union), list(join_new), list(dset_groupby_column_names))

    def _open_dataset(self):

        path_column_name = self._col_data['assets']['column_name']
        use_format_column, union, join_new, dset_groupby_column_names = (
            self._get_open_dset_settings()
        )
        if use_format_column:
            format_column_name = self._col_data['assets']['format_column_name']

        if dset_groupby_column_names:
            groups = self.df.groupby(dset_groupby_column_names)
        else:
            groups = self.df.groupby(self.df.columns.tolist())

        dsets = {}

        for compat_key, compatible_group in groups:
            if join_new:
                join_new_groups = compatible_group.groupby(join_new)
            else:
                join_new_groups = compatible_group.groupby(compatible_group.columns.tolist())

            datasets = []
            for join_new_key, join_new_group in join_new_groups:
                temp_ds = []
                for _, row in join_new_group.iterrows():
                    if use_format_column:
                        data_format = row[format_column_name]
                    else:
                        data_format = self._col_data['assets']['format']
                    if join_new:
                        if isinstance(join_new_key, str):
                            join_new_key = [join_new_key]
                        expand_dims = dict(zip(join_new, join_new_key))

                        expand_dims = {
                            dim_name: [dim_value] for dim_name, dim_value in expand_dims.items()
                        }
                    varname = [row[union]]
                    temp_ds.append(
                        _open_dataset(
                            row,
                            path_column_name,
                            varname,
                            data_format,
                            expand_dims=expand_dims,
                            zarr_kwargs=self.zarr_kwargs,
                            cdf_kwargs=self.cdf_kwargs,
                        )
                    )
                datasets.extend(temp_ds)
            attrs = dict_union(*[ds.attrs for ds in datasets])
            dset = xr.combine_by_coords(datasets)
            dset = _restore_non_dim_coords(dset)
            dset.attrs = attrs
            group_id = '.'.join(compat_key)
            dsets[group_id] = dset

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


def _open_store(path, varname, zarr_kwargs):
    """ Open zarr store """
    mapper = fsspec.get_mapper(path)
    ds = xr.open_zarr(mapper, **zarr_kwargs)
    return _set_coords(ds, varname)


def _open_cdf_dataset(path, varname, cdf_kwargs):
    """ Open netcdf file """
    ds = xr.open_dataset(path, **cdf_kwargs)
    return _set_coords(ds, varname)


def _open_dataset(
    row, path_column_name, varname, data_format, expand_dims={}, zarr_kwargs={}, cdf_kwargs={}
):
    path = row[path_column_name]
    if data_format == 'zarr':
        ds = _open_store(path, varname, zarr_kwargs)
    else:
        ds = _open_cdf_dataset(path, varname, cdf_kwargs)

    if expand_dims:
        return ds.expand_dims(expand_dims)
    else:
        return ds


def _restore_non_dim_coords(ds):
    """restore non_dim_coords to variables"""
    non_dim_coords_reset = set(ds.coords) - set(ds.dims)
    ds = ds.reset_coords(non_dim_coords_reset)
    return ds


def _set_coords(ds, varname):
    """Set all variables except varname to be coords."""
    if isinstance(varname, str):
        varname = [varname]
    coord_vars = set(ds.data_vars) - set(varname)
    return ds.set_coords(coord_vars)


def dict_union(*dicts, merge_keys=['history', 'tracking_id'], drop_keys=[]):
    """Return the union of two or more dictionaries."""
    from functools import reduce

    if len(dicts) > 2:
        return reduce(dict_union, dicts)
    elif len(dicts) == 2:
        d1, d2 = dicts
        d = type(d1)()
        # union
        all_keys = set(d1) | set(d2)
        for k in all_keys:
            v1 = d1.get(k)
            v2 = d2.get(k)
            if (v1 is None and v2 is None) or k in drop_keys:
                pass
            elif v1 is None:
                d[k] = v2
            elif v2 is None:
                d[k] = v1
            elif v1 == v2:
                d[k] = v1
            elif k in merge_keys:
                d[k] = '\n'.join([v1, v2])
        return d
    elif len(dicts) == 1:
        return dicts[0]
