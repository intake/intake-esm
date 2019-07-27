import os
import re

import pandas as pd
import xarray as xr
from intake.source.utils import reverse_format
from tqdm.autonotebook import tqdm

from intake_esm import aggregate, config
from intake_esm.collection import Collection, docstrings
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


class CMIP6Collection(Collection):

    __doc__ = docstrings.with_indents(
        """ Builds a collection for CMIP6 data holdings.
    %(Collection.parameters)s
    """
    )

    def _get_file_attrs(self, filepath, urlpath):
        """ Extract attributes of a file using information from CMI6 DRS.

        Notes
        -----
        References
         1. CMIP6 DRS: http://goo.gl/v1drZl
         2. Controlled Vocabularies (CVs) for use in CMIP6:
            https://github.com/WCRP-CMIP/CMIP6_CVs
        """
        keys = list(set(self.columns) - set(['resource', 'resource_type', 'direct_access']))
        fileparts = {key: None for key in keys}

        file_basename = os.path.basename(filepath)
        fileparts['file_basename'] = file_basename
        fileparts['file_dirname'] = os.path.dirname(filepath) + '/'
        fileparts['file_fullpath'] = filepath

        filename_template = '{variable_id}_{table_id}_{source_id}_{experiment_id}_{member_id}_{grid_label}_{time_range}.nc'
        filename_template_invariant = (
            '{variable_id}_{table_id}_{source_id}_{experiment_id}_{member_id}_{grid_label}.nc'
        )

        def reverse_string(
            file_basename,
            filename_template=filename_template,
            filename_template_invariant=filename_template_invariant,
        ):
            try:
                return reverse_format(filename_template, file_basename)
            except ValueError:
                try:
                    return reverse_format(filename_template_invariant, file_basename)
                except:
                    print(
                        f'Failed to parse file: {file_basename} using patterns: {filename_template} and {filename_template_invariant}'
                    )
                    return {}

        f = reverse_string(file_basename)
        fileparts.update(f)
        version_regex = r'v\d{4}\d{2}\d{2}|v\d{1}'
        version = CMIP6Collection._extract_attr_with_regex(filepath, regex=version_regex) or 'v0'
        fileparts['version'] = version

        return fileparts


class CMIP6Source(BaseSource):
    name = 'cmip6'
    partition_access = True

    def _open_dataset(self):
        # fields which define a single dataset
        dataset_fields = ['institution_id', 'source_id', 'experiment_id', 'table_id', 'grid_label']
        self._open_dataset_groups(
            dataset_fields=dataset_fields,
            member_column_name='member_id',
            variable_column_name='variable_id',
            file_fullpath_column_name='file_fullpath',
        )
