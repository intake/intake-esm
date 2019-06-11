""" Implementation for The Gridded Meteorological Ensemble Tool (GMET) data holdings """
import os
import re

import numpy as np
import pandas as pd
import xarray as xr
from tqdm.autonotebook import tqdm

from . import aggregate, config
from .collection import Collection, docstrings, get_subset
from .source import BaseSource


class GMETCollection(Collection):
    __doc__ = docstrings.with_indents(
        """ Builds a GMET (Gridded Meteorological Ensemble Tool) collection

    %(Collection.parameters)s
    """
    )

    def _add_extra_attributes(self, df, extra_attrs={}):
        df['version'] = extra_attrs['version']
        return df

    def _get_file_attrs(self, filepath):
        file_basename = os.path.basename(filepath)
        datestr = datestr = GMETCollection._extract_date_str(filepath)

        if datestr != '00000000_00000000':
            s = file_basename.split(datestr)
            part_1 = s[0].rstrip('_').split('_')
            part_2 = s[1].lstrip('_').split('.')
            return {
                'frequency': part_1[-2],
                'resolution': part_1[-1],
                'member_id': part_2[0],
                'time_range': datestr.replace('_', '-'),
                'file_basename': file_basename,
                'file_dirname': os.path.dirname(filepath) + '/',
                'file_fullpath': filepath,
            }

        else:
            print(f'Could not identify GMET fileparts for : {filepath}')
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
            print(f'Could not extract date string from : {filename}')
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
