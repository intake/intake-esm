#!/usr/bin/env python
""" Implementation for The Max Planck Institute Grand Ensemble (MPI-GE) data holdings """
import logging
import os
import re
from collections import OrderedDict
from warnings import warn

import numpy as np
import pandas as pd
import xarray as xr
from tqdm.autonotebook import tqdm

from . import aggregate, config
from .cesm import CESMCollection
from .common import BaseSource, Collection, StorageResource, get_subset

logger = logging.getLogger(__name__)
logger.setLevel(level=logging.WARNING)


class MPIGECollection(Collection):
    """ Defines an MPIGE collection

    Parameters
    ----------
    collection_spec : dict


    See Also
    --------
    intake_esm.core.ESMMetadataStoreCatalog
    intake_esm.cmip.CMIP5Collection
    intake_esm.cmip.CMIP6Collection
    intake_esm.cesm.CESMCollection
    """

    def __init__(self, collection_spec):
        super(MPIGECollection, self).__init__(collection_spec)
        self.component_streams = self.collection_definition.get(
            config.normalize_key('component_streams'), None
        )
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

        # Loop over ensemble members
        for ensemble, ensemble_attrs in enumerate(ensembles):
            input_attrs_base = {'experiment': experiment}

            # Get attributes from ensemble_attrs
            case = ensemble_attrs['case']

            if 'ensemble' not in ensemble_attrs:
                input_attrs_base.update({'ensemble': ensemble})

            if 'sequence_order' not in ensemble_attrs:
                input_attrs_base.update({'sequence_order': 0})

            if 'ctrl_branch_year' not in ensemble_attrs:
                input_attrs_base.update({'ctrl_branch_year': None})

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

        # Reorder columns
        self.df = self.df[self.columns]

        # Remove duplicates
        self.df = self.df.drop_duplicates(
            subset=['resource', 'file_fullpath'], keep='last'
        ).reset_index(drop=True)

    def _assemble_collection_df_files(self, resource_key, resource_type, direct_access, filelist):
        """ Assemble file listing into a Pandas DataFrame."""
        entries = {
            key: []
            for key in [
                'resource',
                'resource_type',
                'direct_access',
                'case',
                'component',
                'stream',
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
            entries['date_range'].append(fileparts['datestr'])
            entries['file_basename'].append(os.path.basename(f))
            entries['file_dirname'].append(os.path.dirname(f) + '/')
            entries['file_fullpath'].append(f)

        return pd.DataFrame(entries)

    def _get_filename_parts(self, filename, component_streams):
        """ Get file attributes from filename """
        datestr = MPIGECollection._extract_date_str(filename)

        if datestr != '00000000_00000000':
            s = filename.split(datestr)[0].rstrip('_').split('_')
            case = s[0]
            component = s[1]
            stream = '_'.join(s[2:])
            return {
                'case': case,
                'stream': stream,
                'component': component,
                'datestr': datestr.replace('_', '-'),
            }

        else:
            logger.warning(f'Could not identify MPI-GE fileparts for : {filename}')
            return None

    @staticmethod
    def _extract_date_str(filename):
        date_range = r'\d{8}\_\d{8}'
        pattern = re.compile(date_range)
        datestr = re.search(pattern, filename)
        if datestr:
            datestr = datestr.group()
            return datestr
        else:
            logger.warning(f'Could not extract date string from : {filename}')
            return '00000000_00000000'


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
