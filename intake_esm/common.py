import fnmatch
import logging
import os
import re
import shutil
from abc import ABC, abstractclassmethod
from glob import glob
from subprocess import PIPE, Popen

import intake_xarray
import numpy as np
import pandas as pd
import xarray as xr

from . import config

logger = logging.getLogger(__name__)
logger.setLevel(level=logging.WARNING)


class Collection(ABC):
    def __init__(self, collection_spec):

        self.collection_spec = collection_spec
        self.collection_definition = config.get('collections').get(
            collection_spec['collection_type'], None
        )
        self.df = pd.DataFrame()
        if self.collection_definition is None:
            raise ValueError(
                f"*** {collection_spec['collection_type']} *** is not a defined collection type in {config.PATH}"
            )

        self.columns = self.collection_definition.get(
            config.normalize_key('collection_columns'), None
        )
        if self.columns is None:
            raise ValueError(
                f"Unable to locate collection columns for {collection_spec['collection_type']} collection type in {config.PATH}"
            )
        self.data_cache_dir = config.get('data-cache-directory', None)
        self.database_base_dir = config.get('database-directory', None)

        if self.database_base_dir:
            self.database_dir = f"{self.database_base_dir}/{collection_spec['collection_type']}"
            self.collection_db_file = f"{self.database_dir}/{collection_spec['name']}.csv"
            os.makedirs(self.database_dir, exist_ok=True)

    @abstractclassmethod
    def build(self):
        pass

    def _validate(self):
        for req_col in config.get('collections')[self.collection_spec['collection_type']][
            'required-columns'
        ]:
            if req_col not in self.columns:
                raise ValueError(
                    f"Missing required column: {req_col} for {self.collection_spec['collection_type']} in {config.PATH}"
                )

    def persist_db_file(self):
        if not self.df.empty:
            logger.warning(
                f"Persisting {self.collection_spec['name']} at : {self.collection_db_file}"
            )
            self.df.to_csv(self.collection_db_file, index=True)


class BaseSource(intake_xarray.base.DataSourceMixin):
    def __init__(self, collection_name, collection_type, query={}, **kwargs):
        self.collection_name = collection_name
        self.collection_type = collection_type
        self.query = query
        self.query_results = None
        self._ds = None
        self.kwargs = {'decode_times': False, 'chunks': {'time': 1}}
        super(BaseSource, self).__init__(**kwargs)

    @property
    def results(self):
        """Return collection entries matching query"""
        raise NotImplementedError()

    def _validate_kwargs(self, kwargs):

        _kwargs = kwargs.copy()
        if self.query_results.empty:
            raise ValueError(f'Query={self.query} returned empty results')
        if 'decode_times' not in _kwargs.keys():
            _kwargs.update(decode_times=False)
        if 'time_coord_name' not in _kwargs.keys():
            _kwargs.update(time_coord_name='time')
        if 'ensemble_dim_name' not in _kwargs.keys():
            _kwargs.update(ensemble_dim_name='member_id')
        if 'chunks' not in _kwargs.keys():
            _kwargs.update(chunks=None)
        if 'join' not in _kwargs.keys():
            _kwargs.update(join='outer')
        return _kwargs

    @abstractclassmethod
    def _open_dataset(self):
        pass

    def to_xarray(self, **kwargs):
        """Return dataset as an xarray instance"""
        raise NotImplementedError()

    def _get_schema(self):
        """Make schema object, which embeds xarray object and some details"""
        from intake.source.base import Schema

        self.urlpath = self._get_cache(self.urlpath)[0]

        if self._ds is None:
            self._open_dataset()

            if isinstance(self._ds, xr.Dataset):
                metadata = {
                    'dims': dict(self._ds.dims),
                    'data_vars': {k: list(self._ds[k].coords) for k in self._ds.data_vars.keys()},
                    'coords': tuple(self._ds.coords.keys()),
                }
                metadata.update(self._ds.attrs)

            else:
                metadata = {}

            self._schema = Schema(
                datashape=None, dtype=None, shape=None, npartitions=None, extra_metadata=metadata
            )

        return self._schema


class StorageResource(object):
    """ Defines a storage resource object"""

    def __init__(self, urlpath, loc_type, exclude_dirs, file_extension='.nc'):
        """

        Parameters
        -----------

        urlpath : str
              Path to storage resource
        loc_type : str
              Type of storage resource. Supported resources include: posix, hsi (tape)
        exclude_dirs : str, list
               Directories to exclude during catalog generation
        file_extension : str, default `.nc`
              File extension

        """

        self.urlpath = urlpath
        self.type = loc_type
        self.file_extension = file_extension
        self.exclude_dirs = exclude_dirs
        self.filelist = self._list_files()

    def _list_files(self):
        if self.type == 'posix':
            filelist = self._list_files_posix()

        elif self.type == 'hsi':
            filelist = self._list_files_hsi()

        elif self.type == 'input-file':
            filelist = self._list_files_input_file()

        else:
            raise ValueError(f'unknown resource type: {self.type}')

        return filter(self._filter_func, filelist)

    def _filter_func(self, path):
        return not any(fnmatch.fnmatch(path, pat=exclude_dir) for exclude_dir in self.exclude_dirs)

    def _list_files_posix(self):
        """Get a list of files"""
        w = os.walk(self.urlpath)

        filelist = []

        for root, dirs, files in w:
            filelist.extend(
                [os.path.join(root, f) for f in files if f.endswith(self.file_extension)]
            )

        return filelist

    def _list_files_hsi(self):
        """Get a list of files from HPSS"""
        if shutil.which('hsi') is None:
            logger.warning(f'no hsi; cannot access [HSI]{self.urlpath}')
            return []

        p = Popen(
            [
                'hsi',
                'find {urlpath} -name "*{file_extension}"'.format(
                    urlpath=self.urlpath, file_extension=self.file_extension
                ),
            ],
            stdout=PIPE,
            stderr=PIPE,
        )

        stdout, stderr = p.communicate()
        lines = stderr.decode('UTF-8').strip().split('\n')[1:]

        filelist = []
        i = 0
        while i < len(lines):
            if '***' in lines[i]:
                i += 2
                continue
            else:
                filelist.append(lines[i])
                i += 1

        return filelist

    def _list_files_input_file(self):
        """return a list of files from a file containing a list of files"""
        with open(self.urlpath, 'r') as fid:
            return fid.read().splitlines()


def _get_built_collections():
    """Loads built collections in a dictionary with key=collection_name, value=collection_db_file_path"""
    try:
        cc = [
            y
            for x in os.walk(config.get('database-directory'))
            for y in glob(os.path.join(x[0], '*.csv'))
        ]
        collections = {os.path.splitext(os.path.basename(x))[0]: x for x in cc}
    except Exception:
        collections = {}

    return collections


def _open_collection(collection_name, collection_type):
    """ Open an ESM collection"""

    collection_types = config.get('sources').keys()
    collections = _get_built_collections()
    if (collection_type in collection_types) and collections:
        try:
            df = pd.read_csv(collections[collection_name], index_col=0)
            return df, collection_name, collection_type
        except Exception as err:
            raise err

    else:
        raise ValueError("Couldn't open specified collection")


def get_subset(collection_name, collection_type, query, order_by=None):
    """ Get a subset of collection entries that match a query """
    df, _, _ = _open_collection(collection_name, collection_type)

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

    if order_by and isinstance(order_by, list):
        query_results = query_results.sort_values(by=order_by, ascending=True)

    return query_results
