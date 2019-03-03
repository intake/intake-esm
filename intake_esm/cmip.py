import logging
import os
import re
from collections import OrderedDict

import dask.dataframe as dd
import numpy as np
import pandas as pd
import xarray as xr
from dask import delayed

from ._version import get_versions
from .common import BaseSource, Collection, StorageResource, _open_collection, get_subset
from .config import INTAKE_ESM_CONFIG_FILE, SETTINGS

__version__ = get_versions()['version']
del get_versions

logger = logging.getLogger(__name__)
logger.setLevel(level=logging.WARNING)


class CMIPCollection(Collection):
    """ Defines a CMIP collection

       Parameters
       ----------
       collection_spec : dict

       """

    def __init__(self, collection_spec):
        super(CMIPCollection, self).__init__(collection_spec)
        self.df = pd.DataFrame()
        self.root_dir = self.collection_spec['data_sources']['root_dir']['urlpath']

    def _validate(self):
        for req_col in ['realm', 'frequency', 'ensemble', 'experiment', 'file_fullpath']:
            if req_col not in self.columns:
                raise ValueError(
                    f"Missing required column: {req_col} for {self.collection_spec['collection_type']} in {INTAKE_ESM_CONFIG_FILE}"
                )

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
        logger.warning(f"Persisting {self.collection_spec['name']} at : {self.collection_db_file}")
        self.df.to_csv(self.collection_db_file, index=True)
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
            temp_df = pd.DataFrame()
            temp_df.columns = df.columns
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

    def __init__(
        self,
        collection_name,
        collection_type,
        query={},
        chunks={'time': 1},
        concat_dim='time',
        **kwargs,
    ):

        super(CMIPSource, self).__init__(
            collection_name, collection_type, query, chunks, concat_dim, **kwargs
        )
        self.urlpath = get_subset(self.collection_name, self.collection_type, self.query)[
            'file_fullpath'
        ].tolist()
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

    def _open_dataset(self):

        kwargs = self._kwargs
        if 'concat_dim' not in kwargs.keys():
            kwargs.update(concat_dim=self.concat_dim)
        if self.pattern:
            kwargs.update(preprocess=self._add_path_to_ds)

        # Check that the same variable is not in multiple realms
        realm_list = self.query_results['realm'].unique()
        if len(realm_list) != 1:
            raise ValueError(
                f"Found in multiple realms:\n \
                          '\t{realm_list}. Please specify the realm to use"
            )

        ds_dict = OrderedDict()
        for ens in self.query_results['ensemble'].unique():
            ens_match = self.query_results['ensemble'] == ens
            paths = self.query_results.loc[ens_match]['file_fullpath'].tolist()
            ds_dict[ens] = paths

        ds_list = [
            xr.open_mfdataset(paths_, chunks=self.chunks, **kwargs) for paths_ in ds_dict.values()
        ]
        ens_list = list(ds_dict.keys())
        self._ds = xr.concat(ds_list, dim='ensemble')
        self._ds['ensemble'] = ens_list
