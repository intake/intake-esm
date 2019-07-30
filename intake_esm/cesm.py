#!/usr/bin/env python
""" Implementation for NCAR's Community Earth System Model (CESM) data holdings """
import os
import re

import numpy as np
import pandas as pd
import xarray as xr
from tqdm.autonotebook import tqdm

from . import aggregate, config
from .collection import Collection, docstrings
from .source import BaseSource


class CESMCollection(Collection):

    __doc__ = docstrings.with_indents(
        """ Builds a collection for data produced by
        NCAR's Community Earth System Model (CESM).
    %(Collection.parameters)s
    """
    )

    def __init__(self, collection_spec, fs):
        super(CESMCollection, self).__init__(collection_spec, fs)
        self.component_streams = self.collection_definition.get(
            config.normalize_key('component_streams'), None
        )
        self.replacements = self.collection_definition.get('replacements', {})

    def _get_file_attrs(self, filepath):
        """ Extract each part of case.stream.variable.datestr.nc file pattern. """
        file_basename = os.path.basename(filepath)
        keys = list(set(self.columns) - set(['resource', 'resource_type', 'direct_access']))
        fileparts = {key: None for key in keys}
        fileparts['file_basename'] = file_basename
        fileparts['file_dirname'] = os.path.dirname(filepath) + '/'
        fileparts['file_fullpath'] = filepath

        date_str_regex = r'\d{4}\-\d{4}|\d{6}\-\d{6}|\d{8}\-\d{8}|\d{10}Z\-\d{10}Z|\d{12}Z\-\d{12}Z'
        datestr = CESMCollection._extract_attr_with_regex(file_basename, regex=date_str_regex)

        if datestr:
            fileparts['date_range'] = datestr

            for component, streams in self.component_streams.items():
                # Loop over stream strings
                # NOTE: The order matters here!
                for stream in sorted(streams, key=lambda s: len(s), reverse=True):

                    # Search for case.stream part of filename
                    s = file_basename.find(stream)

                    if s >= 0:  # Got a match
                        # Get varname.datestr.nc part of filename
                        case = file_basename[0 : s - 1]
                        idx = len(stream)
                        variable_datestr_nc = file_basename[s + idx + 1 :]
                        variable = variable_datestr_nc[: variable_datestr_nc.find('.')]

                        # Assert expected pattern
                        datestr_nc = variable_datestr_nc[
                            variable_datestr_nc.find(f'.{variable}.') + len(variable) + 2 :
                        ]

                        # Ensure that filename conforms to expected pattern
                        if datestr_nc != f'{datestr}.nc':
                            print(
                                f'Filename : {file_basename} does not conform to expected pattern'
                            )

                        else:
                            if component == 'ice':
                                if variable.endswith('_nh') or variable.endswith('_sh'):
                                    v = variable.split('_')
                                    if stream != 'cice.h1':
                                        variable = v[0]
                                    else:
                                        variable = '_'.join(v[0:2])

                                    component = f'ice_{v[-1]}'

                            fileparts['case'] = case
                            fileparts['component'] = component
                            fileparts['stream'] = stream
                            fileparts['variable'] = variable
                            break

        return fileparts

    def _add_extra_attributes(self, data_source, df, extra_attrs):

        res_df = pd.DataFrame(columns=self.columns)
        case_members = extra_attrs['case_members']
        component_attrs = extra_attrs['component_attrs']

        for member_id, member_attrs in enumerate(case_members):
            input_attrs_base = {'experiment': data_source}
            case = member_attrs['case']

            if 'member_id' not in member_attrs:
                input_attrs_base.update({'member_id': member_id})

            if 'sequence_order' not in member_attrs:
                input_attrs_base.update({'sequence_order': 0})

            if 'has_ocean_bgc' not in member_attrs:
                input_attrs_base.update({'has_ocean_bgc': False})

            # Find entries relevant to *this* member_id:
            # "case" matches
            condition = df['case'] == case

            # If there are any matching files, append to self.df
            if any(condition):
                input_attrs = dict(input_attrs_base)

                input_attrs.update(
                    {key: val for key, val in member_attrs.items() if key in self.columns}
                )

                # Relevant files
                temp_df = pd.DataFrame(df.loc[condition])

                # Append data coming from input file (input_attrs)
                for col, val in input_attrs.items():
                    temp_df[col] = val

                # Add data from "component_attrs" to appropriate column
                for component in temp_df['component'].unique():
                    if component not in component_attrs:
                        continue

                    for key, val in component_attrs[component].items():
                        if key in self.columns:
                            loc = temp_df['component'] == component
                            temp_df.loc[loc, key] = val

                res_df = pd.concat([temp_df, res_df], ignore_index=True, sort=False)

        res_df.replace(self.replacements, inplace=True)
        return res_df


class CESMSource(BaseSource):

    name = 'cesm'
    partition_access = True

    def _open_dataset(self):
        # fields which define a single dataset
        dataset_fields = ['stream', 'component']

        self._open_dataset_groups(
            dataset_fields=dataset_fields,
            member_column_name='member_id',
            variable_column_name='variable',
            file_fullpath_column_name='file_fullpath',
        )
