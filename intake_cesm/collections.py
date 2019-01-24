import os
import re

import pandas as pd
import yaml


class CesmCollections(object):
    def __init__(self, collection_input_file, clobber=False):
        self.collection_input_file = collection_input_file
        self.clobber = clobber
        self.collections = {}

    def _validate(self):
        with open(self.collection_input_file) as f:
            self.collections = yaml.load(f)

        for collection, values in self.collections.items():
            collection_columns = values["collection_columns"]
            for req_col in ["files", "sequence_order"]:
                if req_col not in collection_columns:
                    raise ValueError(f"missing required column: {req_col}")

    def _extract_cesm_date_str(self, filename):
        """Extract a datastr from file name."""

        # must be in order of longer to shorter strings
        # TODO: make this function return a date object as well as string
        # should it also return a freq?
        datestrs = [
            r"\d{12}Z-\d{12}Z",
            r"\d{10}Z-\d{10}Z",
            r"\d{8}-\d{8}",
            r"\d{6}-\d{6}",
            r"\d{4}-\d{4}",
        ]

        for datestr in datestrs:
            match = re.compile(datestr).findall(filename)
            if match:
                return match[0]

            raise ValueError(f"unable to match date string: {filename}")

    def _cesm_filename_parts(self, filename, component_streams):
        """Extract each part of case.stream.variable.datestr.nc file pattern."""

        # define lists of stream strings
        # datestr = self._extract_cesm_date_str(filename)

        return filename

    def _parse_dir(
        self,
        ensemble,
        ensemble_attrs,
        entry,
        df,
        columns,
        replacements,
        component_streams,
    ):
        root_dir = ensemble_attrs["root_dir"]
        case = ensemble_attrs["case"]
        component_attrs = ensemble_attrs["component_attrs"]

        exclude_dirs = []
        if "exclude_dirs" in ensemble_attrs:
            exclude_dirs = ensemble_attrs["exclude_dirs"]

        entry.update(
            {key: val for key, val in ensemble_attrs.items() if key in columns}
        )

        if "ensemble" not in ensemble_attrs:
            entry.update({"ensemble": ensemble})

        if "sequence_order" not in ensemble_attrs:
            entry.update({"sequence_order": 0})

        w = os.walk(os.path.join(root_dir))

        for root, dirs, files in w:
            dirs[:] = [d for d in dirs if d not in exclude_dirs]
            if not files:
                continue

            sfiles = sorted([f for f in files if f.endswith(".nc")])
            if not sfiles:
                continue

            fs = []
            for f in sfiles:
                filepaths = self._cesm_filename_parts(f, component_streams)
                print(filepaths, fs, case, component_attrs)

    def _build_cesm_collections(self, collection, collection_values):
        component_streams = collection_values["component_streams"]
        replacements = {}

        if "replacements" in collection_values:
            replacements = collection_values["replacements"]

        columns = collection_values["collection_columns"]
        df = pd.DataFrame(columns=columns)
        dirs_to_parse = []
        for experiment, ensembles in collection_values["data_sources"].items():
            entry = {"experiment": experiment}
            for ensemble, ensemble_attrs in enumerate(ensembles):
                dirs_to_parse.append(
                    self._parse_dir(
                        ensemble,
                        ensemble_attrs,
                        entry,
                        df,
                        columns,
                        replacements,
                        component_streams,
                    )
                )
        return dirs_to_parse

    def build(self):

        self._validate()
        dfs = []
        for collection, collection_values in self.collections.items():

            if collection_values["type"].lower() == "cesm":
                dfs.append(self._build_cesm_collections(collection, collection_values))
        return dfs
