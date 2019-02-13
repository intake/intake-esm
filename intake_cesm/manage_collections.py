import fnmatch
import logging
import os
import re
import shutil
from subprocess import PIPE, Popen

import numpy as np
import pandas as pd
import yaml
from intake.catalog import Catalog
from tqdm import tqdm

from .config import SETTINGS

logger = logging.getLogger(__name__)
logger.setLevel(level=logging.DEBUG)

class StorageResource(object):
    """ Defines a storage resource object"""

    def __init__(self, urlpath, type, file_extension=".nc"):
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

        self.filelist = self._list_files()

    def _list_files(self):
        if self.type == "posix":
            return self._list_files_posix()

        if self.type == "hsi":
            return self._list_files_hsi()

        if self.type == "input-file":
            return self._list_files_input_file()

        raise ValueError(f"unknown resource type: {self.type}")

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


class CESMCollections(object):
    """CESM collections builder"""

    def __init__(
        self,
        collection_input_file,
        collection_type_def_file,
        overwrite_existing=False,
        include_cache_dir=False,
    ):
        """

        Parameters
        ----------

        collection_input_file : str, Path, file
                        Path to a YAML file containing collection metadata
        collection_type_def_file : str, Path, file
                        Path to a YAML file containing collection type definition info
        overwrite_existing : bool, default `False`
                        Whether to overwrite existing collection database
        include_cache_dir : bool, default `False`
                        Whether to include a cache directory for the content of the generated collection
        """

        self.db_dir = SETTINGS["database_directory"]
        self.cache_dir = SETTINGS["cache_directory"]

        with open(collection_input_file) as f:
            self.collections = yaml.load(f)
        with open(collection_type_def_file) as f:
            self.collection_definition = yaml.load(f)

        self.columns = None
        self.active_collection = None
        self.active_db = None
        self.component_streams = None
        self.columns = None
        self.replacements = {}
        self.df = None
        self.include_cache_dir = include_cache_dir

        self._build_collections(overwrite_existing)
        super(CESMCollections, self).__init__()

    def _validate(self, collection_definition):
        self.columns = collection_definition["collection_columns"]
        for req_col in ["files", "sequence_order"]:
            if req_col not in self.columns:
                raise ValueError(f"missign required column:{req_col}")

    def _set_active_collection(self, name):
        self.active_collection = name
        os.makedirs(self.db_dir, exist_ok=True)
        self.active_db = f"{self.db_dir}/{name}.csv"

    def _extract_cesm_date_str(self, filename):
        """Extract a date string from file name."""
        try:
            b = filename.split(".")[-2]
            return b
        except Exception:
            logger.warning(f"Cannot extract date string from : {filename}")
            return

    def _cesm_filename_parts(self, filename, component_streams):
        """Extract each part of case.stream.variable.datestr.nc file pattern."""

        # define lists of stream strings
        datestr = self._extract_cesm_date_str(filename)

        if datestr:

            for component, streams in component_streams.items():
                # loop over stream strings (order matters!)
                for stream in sorted(streams, key=lambda s: len(s), reverse=True):

                    # search for case.stream part of filename
                    s = filename.find(stream)

                    if s >= 0:  # got a match
                        # get varname.datestr.nc part of filename
                        case = filename[0 : s - 1]
                        idx = len(stream)
                        variable_datestr_nc = filename[s + idx + 1 :]
                        variable = variable_datestr_nc[: variable_datestr_nc.find(".")]

                        # assert expected pattern
                        datestr_nc = variable_datestr_nc[
                            variable_datestr_nc.find(f".{variable}.") + len(variable) + 2 :
                        ]

                        # ensure that file name conforms to expectation
                        if datestr_nc != f"{datestr}.nc":
                            logger.warning(
                                f"Filename: {filename} does" " not conform to expected" " pattern"
                            )
                            return

                        return {
                            "case": case,
                            "component": component,
                            "stream": stream,
                            "variable": variable,
                            "datestr": datestr,
                        }

            logger.warning(f"could not identify CESM fileparts: {filename}")
            return

        else:
            return

    def _build_cesm_collection_df_files(self, resource_key, resource_type, direct_access, filelist, exclude_dirs):

        entries = {
            key: []
            for key in [
                "resource",
                "resource_type",
                "direct_access",
                "case",
                "component",
                "stream",
                "variable",
                "date_range",
                "files_basename",
                "files_dirname",
                "files",
            ]
        }

        if not filelist:
            return pd.DataFrame(entries)

        logger.info(f"building file database: {resource_key}")

        for f in tqdm(filelist):
            fileparts = self._cesm_filename_parts(os.path.basename(f), self.component_streams)

            if fileparts is None:
                continue

            entries["resource"].append(resource_key)
            entries["resource_type"].append(resource_type)
            entries["direct_access"].append(direct_access)

            entries["case"].append(fileparts["case"])
            entries["component"].append(fileparts["component"])
            entries["stream"].append(fileparts["stream"])
            entries["variable"].append(fileparts["variable"])
            entries["date_range"].append(fileparts["datestr"])
            entries["files_basename"].append(os.path.basename(f))
            entries["files_dirname"].append(os.path.dirname(f) + "/")
            entries["files"].append(f)

        df = pd.DataFrame(entries)
        condition = np.ones(len(df), dtype=bool)
        for exclude_dir in exclude_dirs:
            condition_exclude_dir = ~df["files_dirname"].apply(
                fnmatch.fnmatch, pat=exclude_dir).to_numpy()
            logger.debug(f"excluding {np.sum(condition_exclude_dir)} files from {exclude_dir}")
            condition = condition & condition_exclude_dir

        return df.loc[condition]

    def _build_cesm_collection(self, collection_attrs):

        # -- loop over experiments
        for experiment, experiment_attrs in collection_attrs["data_sources"].items():
            logger.info(f"working on experiment: {experiment}")

            component_attrs = experiment_attrs["component_attrs"]
            ensembles = experiment_attrs["case_members"]

            # -- loop over "locations" and assemble filelist databases
            df_files = {}
            for location in experiment_attrs["locations"]:
                res_key = ":".join([location["name"], location["type"], location["urlpath"]])

                if res_key not in df_files:
                    logger.info("getting file listing: %s", res_key)
                    resource = StorageResource(urlpath=location["urlpath"], type=location["type"])

                    exclude_dirs = []
                    if "exclude_dirs" in location:
                        exclude_dirs = location["exclude_dirs"]

                    df_files[res_key] = self._build_cesm_collection_df_files(
                        resource_key=res_key,
                        resource_type=location["type"],
                        direct_access=location["direct_access"],
                        filelist=resource.filelist,
                        exclude_dirs=exclude_dirs,
                    )

            if self.include_cache_dir:
                res_key = ":".join(["CACHE", "posix", self.cache_dir])
                if res_key not in df_files:
                    logger.info("getting file listing: %s", res_key)
                    resource = StorageResource(urlpath=self.cache_dir, type="posix")

                    df_files[res_key] = self._build_cesm_collection_df_files(
                        resource_key=res_key,
                        resource_type="posix",
                        direct_access=True,
                        filelist=resource.filelist,
                    )

            # -- loop over ensemble members
            for ensemble, ensemble_attrs in tqdm(enumerate(ensembles)):

                input_attrs_base = {"experiment": experiment}

                # -- get attributes from ensemble_attrs
                case = ensemble_attrs["case"]

                if "ensemble" not in ensemble_attrs:
                    input_attrs_base.update({"ensemble": ensemble})

                if "sequence_order" not in ensemble_attrs:
                    input_attrs_base.update({"sequence_order": 0})

                for res_key, df_f in df_files.items():
                    # build query to find entries relevant to *this*
                    # ensemble memeber:
                    # - "case" matches
                    condition = (df_f["case"] == case)

                    # if there are any matching files, append to self.df
                    if any(condition):
                        input_attrs = dict(input_attrs_base)

                        input_attrs.update(
                            {
                                key: val
                                for key, val in ensemble_attrs.items()
                                if key in self.columns and key not in df_f.columns
                            }
                        )

                        # relevant files
                        temp_df = pd.DataFrame(df_f.loc[condition])

                        # append data coming from input file (input_attrs)
                        for col, val in input_attrs.items():
                            temp_df.insert(loc=0, column=col, value=val)

                        # add data from "component_attrs" to appropriate column
                        for component in temp_df.component.unique():
                            if component not in component_attrs:
                                continue
                            for key, val in component_attrs[component].items():
                                if key in self.columns:
                                    loc = temp_df["component"] == component
                                    temp_df.loc[loc, key] = val

                        # append
                        self.df = pd.concat([temp_df, self.df], ignore_index=True, sort=False)

        # make replacements
        self.df.replace(self.replacements, inplace=True)

        # reorder columns
        self.df = self.df[self.columns]

        # remove duplicates
        self.df = self.df.drop_duplicates(subset=["resource", "files"], keep="last").reset_index(
            drop=True
        )

        # write data to csv
        self.df.to_csv(self.active_db, index=True)

    def _build_collections(self, overwrite_existing):
        """ Build CESM collection

        Parameters
        ----------

        overwrite_existing : bool
              Whether to overwrite existing collection database
        """

        for collection_name, collection_attrs in self.collections.items():
            self._validate(self.collection_definition)
            self._set_active_collection(collection_name)
            logger.info(f"Active collection : {self.active_collection}")
            logger.info(f"Active database: {self.active_db}")

            if collection_attrs["type"].lower() == "cesm":
                logger.info("calling build")

                self.component_streams = self.collection_definition["component_streams"]
                if "replacements" in self.collection_definition:
                    self.replacements = self.collection_definition["replacements"]

                if os.path.exists(self.active_db) and not overwrite_existing:
                    self.df = pd.read_csv(self.active_db, index_col=0)

                else:
                    self.df = pd.DataFrame(columns=self.columns)

                    self._build_cesm_collection(collection_attrs)

    def get_built_collection(self):
        """ Returns built collection database

        Returns
        -------
        pd.DataFrame
        """
        return self.df
