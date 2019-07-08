""" Implementation for The Gridded Meteorological Ensemble Tool (GMET) data holdings """
import os
import re

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

    def _get_file_attrs(self, filepath):
        file_basename = os.path.basename(filepath)
        keys = list(set(self.columns) - set(['resource', 'resource_type', 'direct_access']))

        fileparts = {key: None for key in keys}
        fileparts['file_basename'] = file_basename
        fileparts['file_dirname'] = os.path.dirname(filepath) + '/'
        fileparts['file_fullpath'] = filepath

        date_str_regex = r'\d{8}\_\d{8}'
        datestr = GMETCollection._extract_attr_with_regex(filepath, regex=date_str_regex)

        if datestr:
            s = file_basename.split(datestr)
            part_1 = s[0].rstrip('_').split('_')
            part_2 = s[1].lstrip('_').split('.')

            fileparts['frequency'] = part_1[-2]
            fileparts['resolution'] = part_1[-1]
            fileparts['member_id'] = part_2[0]
            fileparts['time_range'] = datestr.replace('_', '-')

        return fileparts


class GMETSource(BaseSource):

    name = 'gmet'
    partition_access = True

    def _open_dataset(self):
        kwargs = self._validate_kwargs(self.kwargs)
        data_vars = ['pcp', 't_mean', 't_range']
        dataset_fields = ['member_id']
        grouped = get_subset(self.collection_name, self.query).groupby(dataset_fields)
        member_ids = []
        member_dsets = []
        for m_id, m_files in tqdm(grouped, desc='member'):
            files = m_files['file_fullpath'].tolist()
            dsets = [
                aggregate.open_dataset_delayed(
                    url,
                    data_vars=data_vars,
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
