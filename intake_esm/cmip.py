import os
import re

import numpy as np
import pandas as pd
import xarray as xr
from dask import delayed
from tqdm.autonotebook import tqdm

from . import aggregate, config
from .common import BaseSource, Collection, StorageResource


class CMIP5Collection(Collection):
    """ Defines a CMIP5 collection

    Parameters
    ----------
    collection_spec : dict

    See Also
    --------
    intake_esm.core.ESMMetadataStoreCatalog
    intake_esm.cesm.CESMCollection
    intake_esm.cmip.CMIP6Collection
    """

    def __init__(self, collection_spec):
        super(CMIP5Collection, self).__init__(collection_spec)
        self.root_dir = self.collection_spec['data_sources']['root_dir']['urlpath']

    def build(self):
        """ Builds CMIP5 collection and returns a pandas Dataframe

        Notes
        ------
        Directory structure = <activity>/
                                <product>/
                                    <institute>/
                                        <model>/
                                            <experiment>/
                                                <frequency>/
                                                   <modeling_realm>/
                                                     <MIP table>/
                                                        <ensemble member>/
                                                            <version number>/
                                                                <variable name>
        With ``depth=7``, we retrieve all directories up to ``modeling_realm`` level
        Reference: CMIP5 DRS: https://cmip.llnl.gov/cmip5/docs/cmip5_data_reference_syntax_v1-00_clean.pdf
        """
        self.build_cmip(depth=7, exclude_dirs=['files', 'latest'])

    def _get_entry(self, directory):

        try:
            entry = {}
            dir_split = directory.split('/')
            entry['activity'] = 'CMIP5'
            entry['product'] = dir_split[-6]
            entry['institute'] = dir_split[-5]
            entry['model'] = dir_split[-4]
            entry['experiment'] = dir_split[-3]
            entry['frequency'] = dir_split[-2]
            entry['modeling_realm'] = dir_split[-1]
            return entry

        except Exception:
            return {}

    @delayed
    def _parse_directory(self, directory, columns, exclude_dirs=[]):
        exclude = set(exclude_dirs)  # directories to exclude

        df = pd.DataFrame(columns=columns)

        entry = self._get_entry(directory)
        if not entry:
            return df
        for root, dirs, files in os.walk(directory):
            dirs[:] = [d for d in dirs if d not in exclude]
            if not files:
                continue
            sfiles = sorted([f for f in files if os.path.splitext(f)[1] == '.nc'])
            if not sfiles:
                continue

            fs = []
            for f in sfiles:
                entry_ = entry.copy()
                try:
                    f_split = f.split('_')
                    entry_['variable'] = f_split[0]
                    entry_['mip_table'] = f_split[1]
                    entry_['ensemble_member'] = f_split[-2]
                    entry_['temporal_subset'] = (
                        'fixed' if entry['frequency'] == 'fx' else f_split[-1].split('.')[0]
                    )
                    entry_['file_dirname'] = root
                    entry_['file_basename'] = f
                    entry_['file_fullpath'] = os.path.join(root, f)
                    fs.append(entry_)
                except Exception:
                    continue

            temp_df = pd.DataFrame(fs, columns=columns)
            df = pd.concat([temp_df, df], ignore_index=True, sort=False)
        return df


class CMIP6Collection(Collection):
    """ Defines a CMIP6 collection

    Parameters
    ----------
    collection_spec : dict

    See Also
    --------
    intake_esm.core.ESMMetadataStoreCatalog
    intake_esm.cesm.CESMCollection
    intake_esm.cmip.CMIP5Collection
    """

    def __init__(self, collection_spec):
        super(CMIP6Collection, self).__init__(collection_spec)
        self.root_dir = self.collection_spec['data_sources']['root_dir']['urlpath']

    def build(self):
        """
        Builds CMIP6 collection and returns a pandas Dataframe

        Notes
        -----
        Directory structure = <mip_era>/
                                <activity_id>/
                                    <institution_id>/
                                        <source_id>/
                                            <experiment_id>/
                                                <member_id>/
                                                    <table_id>/
                                                        <variable_id>/
                                                            <grid_label>/
                                                                <version>
        With ``depth=9``, we retrieve all directories up to ``grid_label`` level
        Reference: CMIP6 DRS: http://goo.gl/v1drZl
        """
        self.build_cmip(depth=9, exclude_dirs=[])

    def _get_entry(self, directory):
        try:
            entry = {}
            dir_split = directory.split('/')
            entry['mip_era'] = 'CMIP6'
            entry['activity_id'] = dir_split[-8]
            entry['institution_id'] = dir_split[-7]
            entry['source_id'] = dir_split[-6]
            entry['experiment_id'] = dir_split[-5]
            entry['member_id'] = dir_split[-4]
            entry['table_id'] = dir_split[-3]
            entry['variable_id'] = dir_split[-2]
            entry['grid_label'] = dir_split[-1]
            return entry

        except Exception:
            return {}

    @delayed
    def _parse_directory(self, directory, columns, exclude_dirs=[]):
        exclude = set(exclude_dirs)
        time_range = r'\d{6}-\d{6}'
        time_range_regex = re.compile(time_range)
        df = pd.DataFrame(columns=columns)
        entry = self._get_entry(directory)
        if not entry:
            return df
        for root, dirs, files in os.walk(directory):
            dirs[:] = [d for d in dirs if d not in exclude]
            if not files:
                continue
            sfiles = sorted([f for f in files if os.path.splitext(f)[1] == '.nc'])
            if not sfiles:
                continue

            fs = []
            for f in sfiles:
                entry_ = entry.copy()
                try:
                    temporal_subset = f.split('_')[-1].split('.')[0]
                    match = time_range_regex.match(temporal_subset)
                    entry_['time_range'] = match.group() if match else 'fixed'
                    entry_['file_dirname'] = root
                    entry_['file_basename'] = f
                    entry_['file_fullpath'] = os.path.join(root, f)
                    fs.append(entry_)

                except Exception:
                    print(f'Could not parse metadata info for file: {f}')
                    continue

            temp_df = pd.DataFrame(fs, columns=columns)
            df = pd.concat([temp_df, df], ignore_index=True, sort=False)

        return df


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
