import fnmatch
import logging
import os
import re
import shutil
from abc import ABC, abstractclassmethod
from subprocess import PIPE, Popen

import numpy as np
import pandas as pd
import yaml
from intake.catalog import Catalog
from tqdm import tqdm

from .config import INTAKE_ESM_CONFIG_FILE, SETTINGS

logger = logging.getLogger(__name__)
logger.setLevel(level=logging.DEBUG)


class StorageResource(object):
    """ Defines a storage resource object"""

    def __init__(self, urlpath, type, exclude_dirs, file_extension=".nc"):
        """

        Parameters
        -----------

        urlpath : str
              Path to storage resource
        type : str
              Type of storage resource. Supported resources include: posix, hsi (tape)
        file_extension : str, default `.nc`
              File extension

        """

        self.urlpath = urlpath
        self.type = type
        self.file_extension = file_extension
        self.exclude_dirs = exclude_dirs
        self.filelist = self._list_files()

    def _list_files(self):
        if self.type == "posix":
            filelist = self._list_files_posix()

        elif self.type == "hsi":
            filelist = self._list_files_hsi()

        elif self.type == "input-file":
            filelist = self._list_files_input_file()

        else:
            raise ValueError(f"unknown resource type: {self.type}")

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
        if shutil.which("hsi") is None:
            logger.warning(f"no hsi; cannot access [HSI]{self.urlpath}")
            return []

        p = Popen(
            [
                "hsi",
                'find {urlpath} -name "*{file_extension}"'.format(
                    urlpath=self.urlpath, file_extension=self.file_extension
                ),
            ],
            stdout=PIPE,
            stderr=PIPE,
        )

        stdout, stderr = p.communicate()
        lines = stderr.decode("UTF-8").strip().split("\n")[1:]

        filelist = []
        i = 0
        while i < len(lines):
            if "***" in lines[i]:
                i += 2
                continue
            else:
                filelist.append(lines[i])
                i += 1

        return filelist

    def _list_files_input_file(self):
        """return a list of files from a file containing a list of files"""
        with open(self.urlpath, "r") as fid:
            return fid.read().splitlines()


class Collection(ABC):
    def __init__(self, collection_name, collection_type, collection_vals):
        self.collection_name = collection_name
        self.collection_vals = collection_vals
        self.collection_type = collection_type
        self.collection_definition = SETTINGS["collections"].get(collection_type, None)
        self.db_dir = SETTINGS.get("database_directory", None)
        self.data_cache_dir = SETTINGS.get("data_cache_directory", None)
        if not self.collection_definition:
            raise ValueError(
                f"*** {collection_type} *** is not a defined collection type in {INTAKE_ESM_CONFIG_FILE}"
            )

        self.columns = self.collection_definition.get("collection_columns", None)
        if not self.columns:
            raise ValueError(
                f"Unable to locate collection columns for {collection_type} collection type in {INTAKE_ESM_CONFIG_FILE}"
            )
        print(collection_name, collection_type)

    @abstractclassmethod
    def _validate(self):
        pass


def open_collection(collection):
    """ Open a CESM collection and return a Pandas dataframe """
    try:
        db_dir = SETTINGS["database_directory"]
        collection_path = os.path.join(db_dir, f"{collection}.csv")
        df = pd.read_csv(collection_path, index_col=0)
        return df

    except (FileNotFoundError) as err:
        print("****** The specified collection does not exit. ******")
        raise err


def get_subset(collection, query):
    """ Get a subset of collection entries that match a query """
    df = open_collection(collection)

    condition = np.ones(len(df), dtype=bool)

    for key, val in query.items():

        if isinstance(val, list):
            condition_i = np.zeros(len(df), dtype=bool)
            for val_i in val:
                condition_i = condition_i | (df[key] == val_i)
            condition = condition & condition_i

        elif val is not None:
            condition = condition & (df[key] == val)

    query_results = df.loc[condition].sort_values(by=["sequence_order", "files"], ascending=True)

    return query_results


def get_collection_def(collection):
    """Return list of columns defining a collection.
    """
    return open_collection(collection).columns.tolist()
