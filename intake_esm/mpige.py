import logging
import os
import re

import numpy as np
import pandas as pd
import xarray as xr
from tqdm.autonotebook import tqdm

from . import aggregate, config
from .cesm import CESMCollection
from .common import BaseSource, Collection, StorageResource, get_subset

logger = logging.getLogger(__name__)
logger.setLevel(level=logging.WARNING)


class MPIGECollection(CESMCollection):
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

        # # Loop over ensemble members
        # for ensemble, ensemble_attrs in enumerate(ensembles):
        #     input_attrs_base = {'experiment': experiment}

        #     # Get attributes from ensemble_attrs
        #      #case = ensemble_attrs['case']

        #     if 'ensemble' not in ensemble_attrs:
        #         input_attrs_base.update({'ensemble': ensemble})

        #     if 'sequence_order' not in ensemble_attrs:
        #         input_attrs_base.update({'sequence_order': 0})

        #     if 'has_ocean_bgc' not in ensemble_attrs:
        #         input_attrs_base.update({'has_ocean_bgc': False})

        #     if 'ctrl_branch_year' not in ensemble_attrs:
        #         input_attrs_base.update({'ctrl_branch_year': np.datetime64('NaT')})

        self.df = pd.concat(df_files.values())
        # Reorder columns
        # self.df = self.df[self.columns]

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
            return

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
