import os
import re

import numpy as np
import pandas as pd
import xarray as xr
from dask import delayed
from tqdm.autonotebook import tqdm

from . import aggregate, config
from ._version import get_versions
from .common import BaseSource, Collection, StorageResource, _open_collection, get_subset

__version__ = get_versions()['version']
del get_versions


class CMIP5Collection(Collection):
    """ Defines a CMIP5 collection

    Parameters
    ----------
    collection_spec : dict

    See Also
    --------
    intake_esm.core.ESMMetadataStoreCatalog
    intake_esm.cesm.CESMCollection
    """

    def __init__(self, collection_spec):
        super(CMIP5Collection, self).__init__(collection_spec)
        self.root_dir = self.collection_spec['data_sources']['root_dir']['urlpath']

    def build(self):
        """ Build collection and return a pandas Dataframe
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
        with ``depth=7``, we retrieve all directories up to ``realm`` level
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
                try:
                    f_split = f.split('_')
                    entry['variable'] = f_split[0]
                    entry['mip_table'] = f_split[1]
                    entry['ensemble_member'] = f_split[-2]
                    entry['temporal_subset'] = (
                        'fixed' if entry['frequency'] == 'fx' else f_split[-1].split('.')[0]
                    )
                    entry['file_dirname'] = root
                    entry['file_basename'] = f
                    entry['file_fullpath'] = os.path.join(root, f)
                    fs.append(entry)
                except Exception:
                    continue

            temp_df = pd.DataFrame(fs, columns=columns)
            df = pd.concat([temp_df, df], ignore_index=True, sort=False)
        return df


class CMIP6Collection(Collection):
    def __init__(self, collection_spec):
        super(CMIP6Collection, self).__init__(collection_spec)
        self.root_dir = self.collection_spec['data_sources']['root_dir']['urlpath']

    def build(self):
        """
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

        with ``depth=9``, we retrieve all directories up to ``grid_label`` level
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
                try:
                    temporal_subset = f.split('_')[-1].split('.')[0]
                    match = time_range_regex.match(temporal_subset)
                    entry['time_range'] = match.group() if match else 'fixed'
                    entry['file_dirname'] = root
                    entry['file_basename'] = f
                    entry['file_fullpath'] = os.path.join(root, f)
                    fs.append(entry)

                except Exception:
                    print(f'Could not parse metadata info for file: {f}')
                    continue

            temp_df = pd.DataFrame(fs, columns=columns)
            df = pd.concat([temp_df, df], ignore_index=True, sort=False)

        return df


class CMIP5Source(BaseSource):

    """ Read CMIP collection datasets into an xarray dataset

    Parameters
    ----------

    collection_name : str
          Name of the collection to use.

    collection_type : str
          Type of the collection to load. Accepted values are:

          - `cesm`
          - `cmip`

    query : dict
         A query to execute against the specified collection

    chunks : int or dict, optional
        Chunks is used to load the new dataset into dask
        arrays. ``chunks={}`` loads the dataset with dask using a single
        chunk for all arrays.

    concat_dim : str, optional
        Name of dimension along which to concatenate the files. Can
        be new or pre-existing. Default is 'concat_dim'.

    kwargs :
        Further parameters are passed to xr.open_mfdataset
    """

    name = 'cmip5'
    partition_access = True
    version = __version__

    def __init__(self, collection_name, collection_type, query={}, **kwargs):

        super(CMIP5Source, self).__init__(collection_name, collection_type, query, **kwargs)
        self.urlpath = ''
        self.query_results = self.get_results()
        if self.metadata is None:
            self.metadata = {}

    def _open_dataset(self):

        kwargs = self._validate_kwargs(self.kwargs)
        query = dict(self.query)
        # Check that the same variable is not in multiple realms
        modeling_realm_list = self.query_results['modeling_realm'].unique()
        frequency_list = self.query_results['frequency'].unique()
        if len(modeling_realm_list) > 1:
            raise ValueError(
                f'Found multiple modeling_realms: {modeling_realm_list} in query results. Please specify the modeling_realm to use'
            )

        if len(frequency_list) > 1:
            raise ValueError(
                f'Found multiple data frequencies: {frequency_list} in query results. Please specify the frequency to use'
            )

        _ds_dict = {}
        grouped = get_subset(self.collection_name, self.collection_type, query).groupby('institute')
        for name, group in tqdm(grouped, desc='institute'):
            ensembles = group['ensemble_member'].unique()
            ds_ens_list = []
            for _, group_ens in tqdm(group.groupby('ensemble_member'), desc='ensemble_member'):
                ds_var_list = []
                for var_i, group_var in tqdm(group_ens.groupby('variable'), desc='variable'):
                    urlpath_ei_vi = group_var['file_fullpath'].tolist()
                    dsets = [
                        aggregate.open_dataset_delayed(
                            url, data_vars=[var_i], decode_times=kwargs['decode_times']
                        )
                        for url in urlpath_ei_vi
                    ]
                    ds_var_i = aggregate.concat_time_levels(dsets, kwargs['time_coord_name'])
                    ds_var_list.append(ds_var_i)
                ds_ens_i = aggregate.merge(dsets=ds_var_list)
                ds_ens_list.append(ds_ens_i)
            _ds = aggregate.concat_ensembles(
                ds_ens_list, member_ids=ensembles, join=kwargs['join'], chunks=kwargs['chunks']
            )
            _ds_dict[name] = _ds
        keys = list(_ds_dict.keys())
        if len(keys) == 1:
            self._ds = _ds_dict[keys[0]]
        else:
            self._ds = _ds_dict
