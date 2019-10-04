from functools import reduce

import fsspec
import intake
import intake_xarray
import numpy as np
import pandas as pd
import xarray as xr
from tqdm import tqdm


class ESMMetadataStoreCollection(intake.catalog.Catalog):
    name = 'esm_metadatastore'

    def __init__(self, path, collection_options={}):
        super().__init__()
        self._path = path
        self.df = pd.read_csv(path)
        self.collection_options = collection_options

    def search(self, **query):
        """ Search for entries in the collection
        """
        import uuid

        args = {'path': self._path, 'query': query}
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

    def __init__(self, path, query, **kwargs):
        super().__init__(metadata={})
        self.path = path
        self.df = self._get_subset(**query)
        self.urlpath = ''
        self._ds = None

    def _get_subset(self, **query):
        df = pd.read_csv(self.path)
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

    def to_xarray(self, **kwargs):
        """ Return dataset as an xarray dataset
        Additional keyword arguments are passed through to
        `xarray.open_dataset()`, xarray.open_zarr()` methods
        """
        return self.to_dask()

    def _get_schema(self):
        from intake.source.base import Schema

        self._open_dataset()
        self._schema = Schema(
            datashape=None, dtype=None, shape=None, npartitions=None, extra_metadata={}
        )
        return self._schema

    def _open_dataset(self):
        dataset_fields = [
            'activity_id',
            'institution_id',
            'source_id',
            'experiment_id',
            'table_id',
            'grid_label',
        ]
        groups = self.df.groupby(dataset_fields)
        dsets = {}
        for group, group_dsets in tqdm(groups, desc='Datasets'):
            member_dsets = group_dsets.groupby('member_id')
            datasets = []
            for m_id, m_dset in member_dsets:
                temp_ds = []
                for _, row in m_dset.iterrows():
                    temp_ds.append(_open_dataset(row, expand_dims={'member_id': [m_id]}))
                datasets.extend(temp_ds)
            attrs = dict_union(*[ds.attrs for ds in datasets])
            dset = xr.combine_by_coords(datasets)
            dset = _restore_non_dim_coords(dset)
            dset.attrs = attrs
            group_id = '.'.join(group)
            dsets[group_id] = dset

        self._ds = dsets


def _open_store(path, varname):
    """ Open zarr store """
    mapper = fsspec.get_mapper(path)
    ds = xr.open_zarr(mapper)
    return _set_coords(ds, varname)


def _open_cdf_dataset(path, varname):
    """ Open netcdf file """
    ds = xr.open_dataset(path)
    return _set_coords(ds, varname)


def _open_dataset(row, expand_dims={}):
    path = row['path']
    variable = row['variable_id']
    data_format = row['data_format']
    if data_format == 'zarr':
        ds = _open_store(path, variable)
    else:
        ds = _open_cdf_dataset(path, variable)

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
