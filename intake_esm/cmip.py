import logging
import os
import re
from collections import OrderedDict

import dask.dataframe as dd
import numpy as np
import pandas as pd
import xarray as xr
from dask import delayed
from tqdm.autonotebook import tqdm

from . import aggregate, config
from ._version import get_versions
from .common import BaseSource, Collection, StorageResource, _open_collection, get_subset

__version__ = get_versions()['version']
del get_versions

logger = logging.getLogger(__name__)
logger.setLevel(level=logging.WARNING)


class CMIPCollection(Collection):
    """ Defines a CMIP collection

       Parameters
       ----------
       collection_spec : dict

       See Also
       --------
       intake_esm.core.ESMMetadataStoreCatalog
       intake_esm.cesm.CESMCollection
       """

    def __init__(self, collection_spec):
        super(CMIPCollection, self).__init__(collection_spec)
        self.df = pd.DataFrame()
        self.root_dir = self.collection_spec['data_sources']['root_dir']['urlpath']

    def build(self):
        """ Build collection and return a pandas Dataframe"""
        self._validate()
        if not os.path.exists(self.root_dir):
            raise NotADirectoryError(f'{self.root_dir} does not exist')

        dirs = _parse_dirs(self.root_dir)
        dfs = [_parse_directory(directory, self.columns) for directory in dirs]
        df = dd.from_delayed(dfs).compute()

        # NOTE: This is not a robust solution in case the root dir does not match the pattern
        vYYYYMMDD = r'v\d{4}\d{2}\d{2}'
        vN = r'v\d{1}'
        v = re.compile('|'.join([vYYYYMMDD, vN]))  # Combine both regex into one
        df['version'] = df['files_dirname'].str.findall(v)
        df['version'] = df['version'].apply(lambda x: x[0] if x else 'v0')
        sorted_df = (
            df.sort_values('version')
            .drop_duplicates(subset='file_basename', keep='last')
            .reset_index(drop=True)
        )
        self.df = sorted_df.copy()
        logger.warning(self.df.info())
        self.persist_db_file()
        return self.df


def _parse_dirs(root_dir):
    institution_dirs = [
        os.path.join(root_dir, activity, institution)
        for activity in os.listdir(root_dir)
        for institution in os.listdir(os.path.join(root_dir, activity))
        if os.path.isdir(os.path.join(root_dir, activity, institution))
    ]

    model_dirs = [
        os.path.join(institution_dir, model)
        for institution_dir in institution_dirs
        for model in os.listdir(institution_dir)
        if os.path.isdir(os.path.join(institution_dir, model))
    ]

    experiment_dirs = [
        os.path.join(model_dir, exp)
        for model_dir in model_dirs
        for exp in os.listdir(model_dir)
        if os.path.isdir(os.path.join(model_dir, exp))
    ]

    freq_dirs = [
        os.path.join(experiment_dir, freq)
        for experiment_dir in experiment_dirs
        for freq in os.listdir(experiment_dir)
        if os.path.isdir(os.path.join(experiment_dir, freq))
    ]

    realm_dirs = [
        os.path.join(freq_dir, realm)
        for freq_dir in freq_dirs
        for realm in os.listdir(freq_dir)
        if os.path.isdir(os.path.join(freq_dir, realm))
    ]

    return realm_dirs


def _get_entry(directory):
    dir_split = directory.split('/')
    entry = {}
    entry['realm'] = dir_split[-1]
    entry['frequency'] = dir_split[-2]
    entry['experiment'] = dir_split[-3]
    entry['model'] = dir_split[-4]
    entry['institution'] = dir_split[-5]
    return entry


@delayed
def _parse_directory(directory, columns):
    exclude = set(['files', 'latests'])  # directories to exclude

    df = pd.DataFrame(columns=columns)

    entry = _get_entry(directory)

    for root, dirs, files in os.walk(directory):
        # print(root)
        dirs[:] = [d for d in dirs if d not in exclude]
        if not files:
            continue
        sfiles = sorted([f for f in files if os.path.splitext(f)[1] == '.nc'])
        if not sfiles:
            continue

        fs = []
        for f in sfiles:
            try:
                f_split = f.split('_')
                entry['variable'] = f_split[0]
                entry['ensemble'] = f_split[-2]
                entry['files_dirname'] = root
                entry['file_basename'] = f
                entry['file_fullpath'] = os.path.join(root, f)
                fs.append(entry)
            except BaseException:
                continue
        if fs:
            temp_df = pd.DataFrame(fs, columns=columns)

        else:
            temp_df = pd.DataFrame(columns=columns)
        df = pd.concat([temp_df, df], ignore_index=True, sort=False)
    return df


class CMIPSource(BaseSource):
    """ Read CMIP collection datasets into an xarray dataset

    Parameters
    ----------

    collection_name : str
          Name of the collection to use.

    collection_type : str
          Type of the collection to load. Accepted values are:

          - `cesm`
          - `cmip`

    query : dict
         A query to execute against the specified collection

    chunks : int or dict, optional
        Chunks is used to load the new dataset into dask
        arrays. ``chunks={}`` loads the dataset with dask using a single
        chunk for all arrays.

    concat_dim : str, optional
        Name of dimension along which to concatenate the files. Can
        be new or pre-existing. Default is 'concat_dim'.

    kwargs :
        Further parameters are passed to xr.open_mfdataset
    """

    name = 'cmip'
    partition_access = True
    version = __version__

    def __init__(self, collection_name, collection_type, query={}, **kwargs):

        super(CMIPSource, self).__init__(collection_name, collection_type, query, **kwargs)
        self.urlpath = ''
        self.query_results = get_subset(self.collection_name, self.collection_type, self.query)
        if self.metadata is None:
            self.metadata = {}

    @property
    def results(self):
        """ Return collection entries matching query"""
        if self.query_results is not None:
            return self.query_results

        else:
            self.query_results = get_subset(self.collection_name, self.collection_type, self.query)
            return self.query_results

    def to_xarray(self, **kwargs):
        """Return dataset as an xarray instance"""
        _kwargs = self.kwargs.copy()
        _kwargs.update(kwargs)
        self.kwargs = _kwargs
        return self.to_dask()

    def _open_dataset(self):

        kwargs = self._validate_kwargs(self.kwargs)
        query = dict(self.query)
        # Check that the same variable is not in multiple realms
        realm_list = self.query_results['realm'].unique()
        frequency_list = self.query_results['frequency'].unique()
        if len(realm_list) > 1:
            raise ValueError(
                f'Found multiple realms: {realm_list} in query results. Please specify the realm to use'
            )

        if len(frequency_list) > 1:
            raise ValueError(
                f'Found multiple data frequencies: {frequency_list} in query results. Please specify the frequency to use'
            )

        _ds_dict = {}
        grouped = get_subset(self.collection_name, self.collection_type, query).groupby(
            'institution'
        )
        for name, group in tqdm(grouped, desc='institution'):
            ensembles = group['ensemble'].unique()
            ds_ens_list = []
            for _, group_ens in tqdm(group.groupby('ensemble'), desc='ensemble'):
                ds_var_list = []
                for var_i, group_var in tqdm(group_ens.groupby('variable'), desc='variable'):
                    urlpath_ei_vi = group_var['file_fullpath'].tolist()
                    dsets = [
                        aggregate.open_dataset_delayed(
                            url, data_vars=[var_i], decode_times=kwargs['decode_times']
                        )
                        for url in urlpath_ei_vi
                    ]
                    ds_var_i = aggregate.concat_time_levels(dsets, kwargs['time_coord_name'])
                    ds_var_list.append(ds_var_i)
                ds_ens_i = aggregate.merge(dsets=ds_var_list)
                ds_ens_list.append(ds_ens_i)
            _ds = aggregate.concat_ensembles(
                ds_ens_list, member_ids=ensembles, join=kwargs['join'], chunks=kwargs['chunks']
            )
            _ds_dict[name] = _ds
        keys = list(_ds_dict.keys())
        if len(keys) == 1:
            self._ds = _ds_dict[keys[0]]
        else:
            self._ds = _ds_dict
