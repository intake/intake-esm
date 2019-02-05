import fnmatch
import logging
import os
import re
import shutil
from subprocess import PIPE, Popen

import pandas as pd
import yaml
from tqdm import tqdm

from .config import SETTINGS

logging.basicConfig(level=logging.DEBUG)


class StorageResource(object):
    def __init__(self, urlpath, type, file_extension=".nc"):
        self.urlpath = urlpath
        self.type = type
        self.file_extension = file_extension

        self.filelist = self._list_files()

    def _list_files(self):
        if self.type == "posix":
            return self._list_files_posix()

        if self.type == "hsi":
            return self._list_files_hsi()

    def _list_files_posix(self):
        """Get a list of files"""
        w = os.walk(self.urlpath)

        filelist = []
        for root, dirs, files in w:
            filelist.extend(
                [
                    os.path.join(root, f)
                    for f in files
                    if f.endswith(self.file_extension)
                ]
            )

        return filelist

    def _list_files_hsi(self):

        if shutil.which("hsi") is None:
            logging.warning(f"no hsi; cannot access [HSI]{self.urlpath}")
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


class CESMCollections(object):
    db_dir = SETTINGS["database_directory"]

    def __init__(self, collection_input_file, collection_type_def_file):
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

        self.build_collections()

    def _validate(self, collection_definition):
        self.columns = collection_definition["collection_columns"]
        for req_col in ["files", "sequence_order"]:
            if req_col not in self.columns:
                raise ValueError(f"missign required column:{req_col}")

    def _set_active_collection(self, name):
        self.active_collection = name
        os.makedirs(CESMCollections.db_dir, exist_ok=True)
        self.active_db = f"{CESMCollections.db_dir}/{name}.csv"

    def _extract_cesm_date_str(self, filename):
        """Extract a datastr from file name."""
        b = filename.split(".")[-2]
        return b

    def _cesm_filename_parts(self, filename, component_streams):
        """Extract each part of case.stream.variable.datestr.nc file pattern."""

        # define lists of stream strings
        datestr = self._extract_cesm_date_str(filename)

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
                        logging.warning(
                            f"Filename: {filename} does"
                            " not conform to expected"
                            " pattern"
                        )
                        return

                    return {
                        "case": case,
                        "component": component,
                        "stream": stream,
                        "variable": variable,
                        "datestr": datestr,
                    }

        logging.warning(f"could not identify CESM fileparts: {filename}")
        return

    def _build_cesm_collection_df_files(self, resource_key, filelist):

        entries = {
            key: []
            for key in [
                "resource",
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

        logging.info(f"building file database: {resource_key}")

        for f in tqdm(filelist):
            fileparts = self._cesm_filename_parts(
                os.path.basename(f), self.component_streams
            )

            if fileparts is None:
                continue

            entries["resource"].append(resource_key)
            entries["case"].append(fileparts["case"])
            entries["component"].append(fileparts["component"])
            entries["stream"].append(fileparts["stream"])
            entries["variable"].append(fileparts["variable"])
            entries["date_range"].append(fileparts["datestr"])
            entries["files_basename"].append(os.path.basename(f))
            entries["files_dirname"].append(os.path.dirname(f) + "/")
            entries["files"].append(f)

        return pd.DataFrame(entries)

    def _build_cesm_collection(self, collection_attrs):

        # -- loop over experiments
        df_files = {}
        for experiment, ensembles in collection_attrs["data_sources"].items():
            logging.info(f"working on experiment: {experiment}")
            input_attrs = {"experiment": experiment}

            # -- loop over ensemble members
            for ensemble, ensemble_attrs in tqdm(enumerate(ensembles)):

                # -- get attributes from ensemble_attrs
                case = ensemble_attrs["case"]

                component_attrs = ensemble_attrs["component_attrs"]

                exclude_dirs = []
                if "exclude_dirs" in ensemble_attrs:
                    exclude_dirs = ensemble_attrs["exclude_dirs"]

                if "ensemble" not in ensemble_attrs:
                    input_attrs.update({"ensemble": ensemble})

                if "sequence_order" not in ensemble_attrs:
                    input_attrs.update({"sequence_order": 0})

                # -- loop over "locations" and assemble filelist databases
                for location in ensemble_attrs["locations"]:
                    res_key = ":".join(
                        [location["name"], location["type"], location["urlpath"]]
                    )

                    if res_key not in df_files:
                        logging.info("getting file listing: %s", res_key)
                        resource = StorageResource(
                            urlpath=location["urlpath"], type=location["type"]
                        )

                        df_files[res_key] = self._build_cesm_collection_df_files(
                            resource_key=res_key, filelist=resource.filelist
                        )

                    input_attrs.update(
                        {
                            key: val
                            for key, val in ensemble_attrs.items()
                            if key in self.columns
                            and key not in df_files[res_key].columns
                        }
                    )

                    # build query to find entries relevant to *this*
                    # ensemble memeber:
                    # - "case" matches
                    # - "files_dirname" not in exclude_dirs
                    condition = df_files[res_key]["case"] == case
                    for exclude_dir in exclude_dirs:
                        condition = condition & (
                            ~df_files[res_key]["files_dirname"].apply(
                                fnmatch.fnmatch, pat=exclude_dir
                            )
                        )

                    # if there are any matching files, append to self.df
                    if any(condition):
                        # relevant files
                        temp_df = pd.DataFrame(df_files[res_key].loc[condition])

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
                        self.df = pd.concat(
                            [temp_df, self.df], ignore_index=True, sort=False
                        )

        # make replacements
        self.df.replace(self.replacements, inplace=True)

        # reorder columns
        self.df = self.df[self.columns]

        # write data to csv
        self.df = self.df.drop_duplicates(subset="files", keep="last").reset_index(
            drop=True
        )
        self.df.to_csv(self.active_db, index=False)

    def build_collections(self):
        for collection_name, collection_attrs in self.collections.items():
            self._validate(self.collection_definition)
            self._set_active_collection(collection_name)
            logging.info(f"Active collection : {self.active_collection}")
            logging.info(f"Active database: {self.active_db}")

            if collection_attrs["type"].lower() == "cesm":
                logging.info("calling build")

                self.component_streams = self.collection_definition["component_streams"]
                if "replacements" in self.collection_definition:
                    self.replacements = self.collection_definition["replacements"]

                self.df = pd.DataFrame(columns=self.columns)

                self._build_cesm_collection(collection_attrs)
