import os
import re

import numpy as np
import pandas as pd
import xarray as xr
from tqdm.autonotebook import tqdm

from intake_esm import aggregate, config
from intake_esm.collection import Collection, docstrings, get_subset
from intake_esm.source import BaseSource


class CMIP5Collection(Collection):

    __doc__ = docstrings.with_indents(
        """ Builds a collection for CMIP5 data holdings.
    %(Collection.parameters)s
    """
    )

    def _get_file_attrs(self, filepath):
        """ Extract attributes of a file using information from CMIP5 DRS.

        Notes
        -----
        Reference: CMIP5 DRS: https://cmip.llnl.gov/cmip5/docs/cmip5_data_reference_syntax_v1-00_clean.pdf

        """
        file_basename = os.path.basename(filepath)
        keys = list(set(self.columns) - set(['resource', 'resource_type', 'direct_access']))

        freq_regex = r'/3hr/|/6hr/|/day/|/fx/|/mon/|/monClim/|/subhr/|/yr/'
        temporal_subset_regex = (
            r'\d{4}\-\d{4}|\d{6}\-\d{6}|\d{8}\-\d{8}|\d{10}\-\d{10}|\d{12}\-\d{12}'
        )
        realm_regex = r'aerosol|atmos|land|landIce|ocean|ocnBgchem|seaIce'
        version_regex = r'v\d{4}\d{2}\d{2}'

        fileparts = {key: None for key in keys}
        fileparts['file_basename'] = file_basename
        fileparts['file_dirname'] = os.path.dirname(filepath) + '/'
        fileparts['file_fullpath'] = filepath

        f_split = file_basename.split('_')
        fileparts['variable'] = f_split[0]
        fileparts['mip_table'] = f_split[1]
        fileparts['model'] = f_split[2]
        fileparts['experiment'] = f_split[3]
        fileparts['ensemble_member'] = f_split[-2]

        frequency = CMIP5Collection._extract_attr_with_regex(
            filepath, regex=freq_regex, strip_chars='/'
        )
        temporal_subset = CMIP5Collection._extract_attr_with_regex(
            filepath, regex=temporal_subset_regex
        )
        realm = CMIP5Collection._extract_attr_with_regex(filepath, regex=realm_regex)
        version = CMIP5Collection._extract_attr_with_regex(filepath, regex=version_regex) or 'v0'
        fileparts['frequency'] = frequency
        fileparts['temporal_subset'] = temporal_subset
        fileparts['modeling_realm'] = realm
        fileparts['version'] = version

        return fileparts

    def _add_extra_attributes(self, data_source, df, extra_attrs):
        for key, value in extra_attrs.items():
            df[key] = value
        return df

    @staticmethod
    def _extract_attr_with_regex(input_str, regex, strip_chars=None):
        pattern = re.compile(regex)
        match = re.search(pattern, input_str)
        if match:
            match = match.group()
            if strip_chars:
                match = match.strip(strip_chars)

            else:
                match = match.strip()

            return match

        else:
            return None


class CMIP5Source(BaseSource):
    name = 'cmip5'
    partition_access = True

    def _open_dataset(self):
        dataset_fields = ['institute', 'model', 'experiment', 'frequency', 'modeling_realm']
        self._open_dataset_groups(
            dataset_fields=dataset_fields,
            member_column_name='ensemble_member',
            variable_column_name='variable',
            file_fullpath_column_name='file_fullpath',
        )


# class CMIP6Collection(Collection):
#     """ Defines a CMIP6 collection

#     Parameters
#     ----------
#     collection_spec : dict

#     See Also
#     --------
#     intake_esm.core.ESMMetadataStoreCatalog
#     intake_esm.cesm.CESMCollection
#     intake_esm.cmip.CMIP5Collection
#     """

#     def __init__(self, collection_spec):
#         super(CMIP6Collection, self).__init__(collection_spec)
#         self.root_dir = self.collection_spec['data_sources']['root_dir']['urlpath']

#     def build(self):
#         """
#         Builds CMIP6 collection and returns a pandas Dataframe

#         Notes
#         -----
#         Directory structure = <mip_era>/
#                                 <activity_id>/
#                                     <institution_id>/
#                                         <source_id>/
#                                             <experiment_id>/
#                                                 <member_id>/
#                                                     <table_id>/
#                                                         <variable_id>/
#                                                             <grid_label>/
#                                                                 <version>
#         With ``depth=9``, we retrieve all directories up to ``grid_label`` level
#         Reference: CMIP6 DRS: http://goo.gl/v1drZl
#         """
#         self.build_cmip(depth=9)

#     def _get_entry(self, directory):
#         try:
#             entry = {}
#             dir_split = directory.split('/')
#             entry['mip_era'] = 'CMIP6'
#             entry['activity_id'] = dir_split[-8]
#             entry['institution_id'] = dir_split[-7]
#             entry['source_id'] = dir_split[-6]
#             entry['experiment_id'] = dir_split[-5]
#             entry['member_id'] = dir_split[-4]
#             entry['table_id'] = dir_split[-3]
#             entry['variable_id'] = dir_split[-2]
#             entry['grid_label'] = dir_split[-1]
#             return entry

#         except Exception:
#             return {}

#     @delayed
#     def _parse_directory(self, directory, columns):
#         exclude = set(self.exclude_dirs)
#         time_range = r'\d{6}-\d{6}'
#         time_range_regex = re.compile(time_range)
#         df = pd.DataFrame(columns=columns)
#         entry = self._get_entry(directory)
#         if not entry:
#             return df
#         for root, dirs, files in os.walk(directory):
#             dirs[:] = [d for d in dirs if d not in exclude]
#             if not files:
#                 continue
#             sfiles = sorted([f for f in files if os.path.splitext(f)[1] == '.nc'])
#             if not sfiles:
#                 continue

#             fs = []
#             for f in sfiles:
#                 entry_ = entry.copy()
#                 try:
#                     temporal_subset = f.split('_')[-1].split('.')[0]
#                     match = time_range_regex.match(temporal_subset)
#                     entry_['time_range'] = match.group() if match else 'fixed'
#                     entry_['file_dirname'] = root
#                     entry_['file_basename'] = f
#                     entry_['file_fullpath'] = os.path.join(root, f)
#                     fs.append(entry_)

#                 except Exception:
#                     print(f'Could not parse metadata info for file: {f}')
#                     continue

#             temp_df = pd.DataFrame(fs, columns=columns)
#             df = pd.concat([temp_df, df], ignore_index=True, sort=False)

#         return df


# class CMIP5Source(BaseSource):
#     name = 'cmip5'
#     partition_access = True

#     def _open_dataset(self):
#         dataset_fields = ['institute', 'model', 'experiment', 'frequency', 'modeling_realm']
#         self._open_dataset_groups(
#             dataset_fields=dataset_fields,
#             member_column_name='ensemble_member',
#             variable_column_name='variable',
#             file_fullpath_column_name='file_fullpath',
#         )


# class CMIP6Source(BaseSource):
#     name = 'cmip6'
#     partition_access = True

#     def _open_dataset(self):
#         # fields which define a single dataset
#         dataset_fields = ['institution_id', 'source_id', 'experiment_id', 'table_id', 'grid_label']
#         self._open_dataset_groups(
#             dataset_fields=dataset_fields,
#             member_column_name='member_id',
#             variable_column_name='variable_id',
#             file_fullpath_column_name='file_fullpath',
#         )
