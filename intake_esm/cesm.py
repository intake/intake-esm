#!/usr/bin/env python
""" Implementation for NCAR's Community Earth System Model (CESM) data holdings """

import logging
import os

import numpy as np
import pandas as pd
import xarray as xr

from . import aggregate, config
from .common import BaseSource, Collection, StorageResource, get_subset

logger = logging.getLogger(__name__)
logger.setLevel(level=logging.WARNING)


class CESMCollection(Collection):
    """ Defines a CESM collection

    Parameters
    ----------
    collection_spec : dict


    See Also
    --------
    intake_esm.core.ESMMetadataStoreCatalog
    intake_esm.cmip.CMIP5Collection
    intake_esm.cmip.CMIP6Collection
    """

    def __init__(self, collection_spec):
        super(CESMCollection, self).__init__(collection_spec)
        self.component_streams = self.collection_definition.get(
            config.normalize_key('component_streams'), None
        )
        self.replacements = self.collection_definition.get('replacements', {})
        self.include_cache_dir = self.collection_spec.get('include_cache_dir', False)
        self.df = pd.DataFrame(columns=self.columns)

    def build(self):
        self._validate()
        # Loop over data sources/experiments
        for experiment, experiment_attrs in self.collection_spec['data_sources'].items():
            logger.warning(f'Working on experiment: {experiment}')

            component_attrs = experiment_attrs['component_attrs']
            ensembles = experiment_attrs['case_members']
            self.assemble_file_list(experiment, experiment_attrs, component_attrs, ensembles)
        logger.warning(self.df.info())
        self.persist_db_file()
        return self.df

    def assemble_file_list(self, experiment, experiment_attrs, component_attrs, ensembles):
        df_files = {}
        for location in experiment_attrs['locations']:
            res_key = ':'.join([location['name'], location['loc_type'], location['urlpath']])
            if res_key not in df_files:
                logger.warning(f'Getting file listing : {res_key}')

                if 'exclude_dirs' not in location:
                    location['exclude_dirs'] = []

                resource = StorageResource(
                    urlpath=location['urlpath'],
                    loc_type=location['loc_type'],
                    exclude_dirs=location['exclude_dirs'],
                )

                df_files[res_key] = self._assemble_collection_df_files(
                    resource_key=res_key,
                    resource_type=location['loc_type'],
                    direct_access=location['direct_access'],
                    filelist=resource.filelist,
                )

        # Include user defined data cache directories
        if self.include_cache_dir:
            res_key = ':'.join(['CACHE', 'posix', self.data_cache_dir])
            if res_key not in df_files:
                logger.warning(f'Getting file listing : {res_key}')
                resource = StorageResource(
                    urlpath=self.data_cache_dir, loc_type='posix', exclude_dirs=[]
                )

                df_files[res_key] = self._assemble_collection_df_files(
                    resource_key=res_key,
                    resource_type='posix',
                    direct_access=True,
                    filelist=resource.filelist,
                )

        # Loop over ensemble members
        for ensemble, ensemble_attrs in enumerate(ensembles):
            input_attrs_base = {'experiment': experiment}

            # Get attributes from ensemble_attrs
            case = ensemble_attrs['case']

            if 'ensemble' not in ensemble_attrs:
                input_attrs_base.update({'ensemble': ensemble})

            if 'sequence_order' not in ensemble_attrs:
                input_attrs_base.update({'sequence_order': 0})

            if 'has_ocean_bgc' not in ensemble_attrs:
                input_attrs_base.update({'has_ocean_bgc': False})

            if 'ctrl_branch_year' not in ensemble_attrs:
                input_attrs_base.update({'ctrl_branch_year': np.datetime64('NaT')})

            for res_key, df_f in df_files.items():
                # Find entries relevant to *this* ensemble:
                # "case" matches
                condition = df_f['case'] == case

                # If there are any matching files, append to self.df
                if any(condition):
                    input_attrs = dict(input_attrs_base)

                    input_attrs.update(
                        {
                            key: val
                            for key, val in ensemble_attrs.items()
                            if key in self.columns and key not in df_f.columns
                        }
                    )

                    # Relevant files
                    temp_df = pd.DataFrame(df_f.loc[condition])

                    # Append data coming from input file (input_attrs)
                    for col, val in input_attrs.items():
                        temp_df.insert(loc=0, column=col, value=val)

                    # Add data from "component_attrs" to appropriate column
                    for component in temp_df.component.unique():
                        if component not in component_attrs:
                            continue

                        for key, val in component_attrs[component].items():
                            if key in self.columns:
                                loc = temp_df['component'] == component
                                temp_df.loc[loc, key] = val

                    # Append
                    self.df = pd.concat([temp_df, self.df], ignore_index=True, sort=False)

        # Make replacements
        self.df.replace(self.replacements, inplace=True)

        # Reorder columns
        self.df = self.df[self.columns]

        # Remove duplicates
        self.df = self.df.drop_duplicates(
            subset=['resource', 'file_fullpath'], keep='last'
        ).reset_index(drop=True)

    def _assemble_collection_df_files(self, resource_key, resource_type, direct_access, filelist):
        entries = {
            key: []
            for key in [
                'resource',
                'resource_type',
                'direct_access',
                'case',
                'component',
                'stream',
                'variable',
                'date_range',
                'file_basename',
                'file_dirname',
                'file_fullpath',
            ]
        }

        # If there are no files, return empty dataframe
        if not filelist:
            return pd.DataFrame(entries)

        logger.warning(f'Building file database : {resource_key}')
        for f in filelist:
            fileparts = self._get_filename_parts(os.path.basename(f), self.component_streams)

            if fileparts is None or len(fileparts) == 0:
                continue

            entries['resource'].append(resource_key)
            entries['resource_type'].append(resource_type)
            entries['direct_access'].append(direct_access)

            entries['case'].append(fileparts['case'])
            entries['component'].append(fileparts['component'])
            entries['stream'].append(fileparts['stream'])
            entries['variable'].append(fileparts['variable'])
            entries['date_range'].append(fileparts['datestr'])

            entries['file_basename'].append(os.path.basename(f))
            entries['file_dirname'].append(os.path.dirname(f) + '/')
            entries['file_fullpath'].append(f)

        return pd.DataFrame(entries)

    def _get_filename_parts(self, filename, component_streams):
        """ Extract each part of case.stream.variable.datestr.nc file pattern. """

        # Get Date string
        datestr = CESMCollection._extract_date_str(filename)

        if datestr:
            for component, streams in component_streams.items():
                # Loop over stream strings
                # NOTE: The order matters here!
                for stream in sorted(streams, key=lambda s: len(s), reverse=True):

                    # Search for case.stream part of filename
                    s = filename.find(stream)

                    if s >= 0:  # Got a match
                        # Get varname.datestr.nc part of filename
                        case = filename[0 : s - 1]
                        idx = len(stream)
                        variable_datestr_nc = filename[s + idx + 1 :]
                        variable = variable_datestr_nc[: variable_datestr_nc.find('.')]

                        # Assert expected pattern
                        datestr_nc = variable_datestr_nc[
                            variable_datestr_nc.find(f'.{variable}.') + len(variable) + 2 :
                        ]

                        # Ensure that filename conforms to expected pattern
                        if datestr_nc != f'{datestr}.nc':
                            logger.warning(
                                f'Filename : {filename} does not conform to expected pattern'
                            )
                            return

                        return {
                            'case': case,
                            'component': component,
                            'stream': stream,
                            'variable': variable,
                            'datestr': datestr,
                        }

            logger.warning(f'Could not identify CESM fileparts for : {filename}')
            return
        else:
            return

    @staticmethod
    def _extract_date_str(filename):
        """ Extract a date string from a file name"""
        try:
            b = filename.split('.')[-2]
            return b
        except Exception:
            logger.warning(f'Could not extract date string from : {filename}')
            return


class CESMSource(BaseSource):

    name = 'cesm'
    partition_access = True

    def _open_dataset(self):
        # fields which define a single dataset
        dataset_fields = ['stream', 'component']

        self._open_dataset_groups(
            dataset_fields=dataset_fields,
            member_column_name='ensemble',
            variable_column_name='variable',
            file_fullpath_column_name='file_fullpath',
        )
