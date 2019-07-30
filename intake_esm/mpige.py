#!/usr/bin/env python
""" Implementation for The Max Planck Institute Grand Ensemble (MPI-GE) data holdings """
import os
import re
from collections import OrderedDict
from warnings import warn

import pandas as pd
import xarray as xr
from tqdm.autonotebook import tqdm

from . import aggregate, config
from .collection import Collection, docstrings, get_subset
from .source import BaseSource


class MPIGECollection(Collection):

    __doc__ = docstrings.with_indents(
        """ Builds a collection for The Max Planck Institute Grand Ensemble (MPI-GE)
        data holdings.
    %(Collection.parameters)s
    """
    )

    def __init__(self, collection_spec, fs):
        super(MPIGECollection, self).__init__(collection_spec, fs)
        self.component_streams = self.collection_definition.get(
            config.normalize_key('component_streams'), None
        )

    def _get_file_attrs(self, filepath):
        """ Extract each part of case.stream.variable.datestr.nc file pattern. """
        file_basename = os.path.basename(filepath)
        keys = list(set(self.columns) - set(['resource', 'resource_type', 'direct_access']))
        fileparts = {key: None for key in keys}
        fileparts['file_basename'] = file_basename
        fileparts['file_dirname'] = os.path.dirname(filepath) + '/'
        fileparts['file_fullpath'] = filepath

        date_str_regex = r'\d{4}\_\d{4}|\d{6}\_\d{6}|\d{8}\_\d{8}|\d{10}\_\d{10}|\d{12}\_\d{12}'
        datestr = MPIGECollection._extract_attr_with_regex(file_basename, regex=date_str_regex)

        if datestr:
            fileparts['date_range'] = datestr
            s = file_basename.split(datestr)[0].rstrip('_').split('_')
            case = s[0]
            component = s[1]
            stream = '_'.join(s[2:])
            fileparts['case'] = case
            fileparts['component'] = component
            fileparts['stream'] = stream
            fileparts['date_range'] = datestr.replace('_', '-')

        return fileparts

    def _add_extra_attributes(self, data_source, df, extra_attrs):
        res_df = pd.DataFrame(columns=self.columns)
        ensembles = extra_attrs['case_members']
        component_attrs = extra_attrs['component_attrs']

        for ensemble, ensemble_attrs in enumerate(ensembles):
            input_attrs_base = {'experiment': data_source}
            case = ensemble_attrs['case']

            if 'ensemble' not in ensemble_attrs:
                input_attrs_base.update({'ensemble': ensemble})

            if 'sequence_order' not in ensemble_attrs:
                input_attrs_base.update({'sequence_order': 0})
            # Find entries relevant to *this* ensemble:
            # "case" matches
            condition = df['case'] == case

            # If there are any matching files, append to self.df
            if any(condition):
                input_attrs = dict(input_attrs_base)

                input_attrs.update(
                    {key: val for key, val in ensemble_attrs.items() if key in self.columns}
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

        return res_df


class MPIGESource(BaseSource):

    name = 'mpige'
    partition_access = True

    def _open_dataset(self):
        # fields which define a single (unique) dataset
        dataset_fields = ['experiment']
        self._open_dataset_groups(
            dataset_fields=dataset_fields,
            member_column_name='ensemble',
            file_fullpath_column_name='file_fullpath',
        )

    def _open_dataset_groups(self, dataset_fields, member_column_name, file_fullpath_column_name):
        kwargs = self._validate_kwargs(self.kwargs)
        grouped = get_subset(self.collection_name, self.query).groupby(dataset_fields)
        all_dsets = OrderedDict()
        for dset_keys, dset_files in tqdm(grouped, desc='experiment'):
            dset_id = dset_keys
            comp_dsets = []
            for comp_id, comp_files in dset_files.groupby('component'):
                member_ids = []
                member_dsets = []
                for m_id, m_files in comp_files.groupby(member_column_name):
                    files = m_files[file_fullpath_column_name]
                    if kwargs['preprocess'] is not None:
                        ds = xr.open_mfdataset(
                            files,
                            preprocess=kwargs['preprocess'],
                            concat_dim=kwargs['time_coord_name'],
                            chunks=kwargs['chunks'],
                        )
                    else:
                        ds = xr.open_mfdataset(files, chunks=kwargs['chunks'])

                    member_dsets.append(ds)
                    member_ids.append(m_id)
                _ds = xr.concat(member_dsets, kwargs['ensemble_dim_name'])
                _ds[kwargs['ensemble_dim_name']] = member_ids
                comp_dsets.append(_ds)
            all_dsets[dset_id] = xr.merge(comp_dsets)
        if kwargs['merge_exp']:
            # when only streams are different
            try:
                self._ds = xr.merge(list(all_dsets.values()))
            except Exception:
                warn('Could not merge datasets. Returning non-merged datasets')
                self._ds = all_dsets

        else:
            # when for example, experiments = ['rcp26','rcp45','rcp85']
            try:
                self._ds = xr.concat(list(all_dsets.values()), 'experiment_id')
                self._ds['experiment_id'] = list(all_dsets.keys())
            except Exception:
                warn(
                    f'Could not concatenate datasets for {self.query["experiment"]}. Returning non-concatenated datasets'
                )
                self._ds = all_dsets
