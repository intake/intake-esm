import fnmatch
import logging
import os
import re
import shutil
from abc import ABC, abstractclassmethod
from glob import glob
from subprocess import PIPE, Popen

import numpy as np
import pandas as pd

from .config import INTAKE_ESM_CONFIG_FILE, SETTINGS

logger = logging.getLogger(__name__)
logger.setLevel(level=logging.WARNING)


class Collection(ABC):
    def __init__(self, collection_spec):
        self.collection_spec = collection_spec
        self.collection_definition = SETTINGS['collections'].get(
            collection_spec['collection_type'], None
        )
        if self.collection_definition is None:
            raise ValueError(
                f"*** {collection_spec['collection_type']} *** is not a defined collection type in {INTAKE_ESM_CONFIG_FILE}"
            )

        self.columns = self.collection_definition.get('collection_columns', None)
        if self.columns is None:
            raise ValueError(
                f"Unable to locate collection columns for {collection_spec['collection_type']} collection type in {INTAKE_ESM_CONFIG_FILE}"
            )
        self.data_cache_dir = SETTINGS.get('data_cache_directory', None)
        self.database_base_dir = SETTINGS.get('database_directory', None)

        if self.database_base_dir:
            self.database_dir = f"{self.database_base_dir}/{collection_spec['collection_type']}"
            self.collection_db_file = f"{self.database_dir}/{collection_spec['name']}.csv"
            os.makedirs(self.database_dir, exist_ok=True)

    @abstractclassmethod
    def build(self):
        pass


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
            for x in os.walk(SETTINGS['database_directory'])
            for y in glob(os.path.join(x[0], '*.csv'))
        ]
        collections = {os.path.splitext(os.path.basename(x))[0]: x for x in cc}
    except Exception:
        collections = {}

    return collections


def _open_collection(collection_name, collection_type):
    """ Open an ESM collection"""
    collection_types = {'cesm', 'cmip'}
    collections = _get_built_collections()
    try:
        if (collection_type in collection_types) and collections:
            df = pd.read_csv(collections[collection_name], index_col=0)
            return df, collection_name, collection_type
        else:
            raise ValueError(f'****** The specified collection type is not valid. ******')

    except Exception as err:
        print('****** The specified collection does not exit. ******')
        raise err


def get_subset(collection_name, collection_type, query):
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

    query_results = df.loc[condition].sort_values(by=['sequence_order', 'files'], ascending=True)

    return query_results
