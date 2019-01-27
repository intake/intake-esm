import fnmatch
import logging
import os
import re

import pandas as pd
import yaml
from tqdm import tqdm


class CesmCollections(object):
    db_dir = "./collections"

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

    def _validate(self, collection_definition):
        self.columns = collection_definition["collection_columns"]
        for req_col in ["files", "sequence_order"]:
            if req_col not in self.columns:
                raise ValueError(f"missign required column:{req_col}")

    def _set_active_collection(self, name):
        self.active_collection = name
        os.makedirs(CesmCollections.db_dir, exist_ok=True)
        self.active_db = f"{CesmCollections.db_dir}/{name}.csv"

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

        raise ValueError(f"could not identify CESM fileparts: {filename}")

    def _build_cesm_collection(self, collection_attrs):
        for experiment, ensembles in collection_attrs["data_sources"].items():
            entry = {"experiment": experiment}

            for ensemble, ensemble_attrs in tqdm(enumerate(ensembles)):
                root_dir = ensemble_attrs["root_dir"]
                case = ensemble_attrs["case"]
                component_attrs = ensemble_attrs["component_attrs"]

                exclude_dirs = []
                if "exclude_dirs" in ensemble_attrs:
                    exclude_dirs = ensemble_attrs["exclude_dirs"]

                entry.update(
                    {
                        key: val
                        for key, val in ensemble_attrs.items()
                        if key in self.columns
                    }
                )

                if "ensemble" not in ensemble_attrs:
                    entry.update({"ensemble": ensemble})

                if "sequence_order" not in ensemble_attrs:
                    entry.update({"sequence_order": 0})

                w = os.walk(os.path.join(root_dir))

                for root, dirs, files in w:
                    if not files:
                        continue

                    sfiles = sorted([f for f in files if f.endswith(".nc")])
                    if not sfiles:
                        continue

                    # skip directories specified in `exclude_dirs`
                    local_root = root.replace(root_dir + "/", "")
                    if any(
                        fnmatch.fnmatch(local_root, exclude_dir)
                        for exclude_dir in exclude_dirs
                    ):
                        logging.warning(f"skipping {root}")
                        continue

                    fs = []
                    for f in sfiles:
                        fileparts = self._cesm_filename_parts(f, self.component_streams)

                        if fileparts is None:
                            continue

                        if fileparts["case"] != case:
                            continue

                        component = fileparts["component"]
                        if component in component_attrs:
                            entry.update(
                                {
                                    key: val
                                    for key, val in component_attrs[component].items()
                                    if key in self.columns
                                }
                            )

                        entry.update(
                            {
                                "variable": fileparts["variable"],
                                "component": component,
                                "stream": fileparts["stream"],
                                "date_range": fileparts["datestr"].split("-"),
                                "file_basename": f,
                                "files": os.path.join(root, f),
                            }
                        )

                        completed_entry = dict(entry)
                        for key, replist in self.replacements.items():
                            if key in completed_entry:
                                for old_new in replist:
                                    if completed_entry[key] == old_new[0]:
                                        completed_entry[key] = old_new[1]

                        fs.append(completed_entry)

                        if fs:
                            temp_df = pd.DataFrame(fs)
                        else:
                            temp_df = pd.DataFrame(columns=self.df.columns)

                        self.df = pd.concat(
                            [temp_df, self.df], ignore_index=True, sort=False
                        )

        self.df = self.df.drop_duplicates(subset="files", keep="last").reset_index(
            drop=True
        )
        self.df.to_csv(self.active_db, index=False)

    def build_collections(self):
        for collection_name, collection_attrs in self.collections.items():
            self._validate(self.collection_definition)
            self._set_active_collection(collection_name)
            print(f"Active collection : {self.active_collection}")
            print(f"Active database: {self.active_db}")

            if collection_attrs["type"].lower() == "cesm":
                print("calling build")
                self.component_streams = self.collection_definition["component_streams"]
                if "replacements" in self.collection_definition:
                    self.replacements = self.collection_definition["replacements"]

                self.df = pd.DataFrame(columns=self.columns)

                self._build_cesm_collection(collection_attrs)
