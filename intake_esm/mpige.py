import logging
import os
import re

import pandas as pd

from .cesm import CESMCollection

logger = logging.getLogger(__name__)
logger.setLevel(level=logging.WARNING)


class MPIGECollection(CESMCollection):
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
