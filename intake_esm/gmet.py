""" Implementation for The Gridded Meteorological Ensemble Tool (GMET) data holdings """
import logging
import os
import re

import numpy as np
import pandas as pd
import xarray as xr
from dask import delayed
from tqdm.autonotebook import tqdm

from . import aggregate, config
from .common import BaseSource, Collection, StorageResource, get_subset

logger = logging.getLogger(__name__)
logger.setLevel(level=logging.WARNING)


class GMETCollection(Collection):
    """ Defines a GMET (Gridded Meteorological Ensemble Tool) collection

    Parameters
    ----------
    collection_spec : dict


    See Also
    --------
    intake_esm.core.ESMMetadataStoreCatalog
    intake_esm.cmip.CMIP5Collection
    intake_esm.cmip.CMIP6Collection
    intake_esm.cesm.CESMCollection
    intake_esm.mpige.MPIGECollection
    """

    def __init__(self, collection_spec):

        super(GMETCollection, self).__init__(collection_spec)
        self.include_cache_dir = self.collection_spec.get('include_cache_dir', False)
        self.df = pd.DataFrame(columns=self.columns)

    def build(self):
        self._validate()
        for version, version_attrs in self.collection_spec['data_sources'].items():
            logger.warning(f'Working on version: {version}')
            self.assemble_file_list(version, version_attrs)

        logger.warning(self.df.info())
        self.persist_db_file()
        return self.df

    def assemble_file_list(self, version, version_attrs):
        df_files = {}
        for location in version_attrs['locations']:
            res_key = ':'.join([location['name'], location['loc_type'], location['urlpath']])
            if res_key not in df_files:
                logger.warning(f'Getting file listing : {res_key}')

                if 'exclude_dirs' not in location:
                    location['exclude_dirs'] = []

                resource = StorageResource(
                    urlpath=location['urlpath'],
                    loc_type=location['loc_type'],
                    exclude_dirs=location['exclude_dirs'],
                    file_extension='.nc4',
                )

                df_files[res_key] = self._assemble_collection_df_files(
                    resource_key=res_key,
                    resource_type=location['loc_type'],
                    direct_access=location['direct_access'],
                    filelist=resource.filelist,
                )

        for res_key, df_f in df_files.items():
            df_f.insert(loc=0, column='version', value=version)
            self.df = pd.concat([df_f, self.df], ignore_index=True, sort=False)

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
                'frequency',
                'member_id',
                'resolution',
                'time_range',
                'file_basename',
                'file_dirname',
                'file_fullpath',
            ]
        }

        if not filelist:
            return pd.DataFrame(entries)

        logger.warning(f'Building file database : {resource_key}')
        for f in filelist:
            fileparts = fileparts = self._get_filename_parts(os.path.basename(f))
            if fileparts is None or len(fileparts) == 0:
                continue

            entries['resource'].append(resource_key)
            entries['resource_type'].append(resource_type)
            entries['direct_access'].append(direct_access)
            entries['time_range'].append(fileparts['datestr'])
            entries['member_id'].append(fileparts['member_id'])
            entries['frequency'].append(fileparts['frequency'])
            entries['resolution'].append(fileparts['resolution'])
            entries['file_basename'].append(os.path.basename(f))
            entries['file_dirname'].append(os.path.dirname(f) + '/')
            entries['file_fullpath'].append(f)

        return pd.DataFrame(entries)

    def _get_filename_parts(self, filename):
        """ Get file attributes from filename """
        datestr = GMETCollection._extract_date_str(filename)

        if datestr != '00000000_00000000':
            s = filename.split(datestr)
            part_1 = s[0].rstrip('_').split('_')
            part_2 = s[1].lstrip('_').split('.')
            return {
                'frequency': part_1[-2],
                'resolution': part_1[-1],
                'member_id': part_2[0],
                'datestr': datestr.replace('_', '-'),
            }

        else:
            logger.warning(f'Could not identify GMET fileparts for : {filename}')
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


class GMETSource(BaseSource):

    name = 'gmet'
    partition_access = True

    def _open_dataset(self):
        kwargs = self._validate_kwargs(self.kwargs)

        dataset_fields = ['member_id']
        grouped = get_subset(self.collection_name, self.query).groupby(dataset_fields)
        member_ids = []
        member_dsets = []
        for m_id, m_files in tqdm(grouped, desc='member'):
            files = m_files['file_fullpath'].tolist()
            dsets = [
                aggregate.open_dataset_delayed(
                    url,
                    data_vars=None,
                    chunks=kwargs['chunks'],
                    decode_times=kwargs['decode_times'],
                )
                for url in files
            ]

            member_dset = aggregate.concat_time_levels(dsets, kwargs['time_coord_name'])
            member_dsets.append(member_dset)
            member_ids.append(m_id)

        self._ds = aggregate.concat_ensembles(
            member_dsets, member_ids=member_ids, join=kwargs['join']
        )
