import os
import re
from collections import OrderedDict

import dask.dataframe as dd
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
        """
        self._validate()
        if not os.path.exists(self.root_dir):
            raise NotADirectoryError(f'{os.path.abspath(self.root_dir)} does not exist')
        dirs = self.get_directories(
            root_dir=self.root_dir, depth=7, exclude_dirs=['files', 'latest']
        )
        dfs = [self._parse_directory(directory, self.columns) for directory in dirs]
        df = dd.from_delayed(dfs).compute()

        vYYYYMMDD = r'v\d{4}\d{2}\d{2}'
        vN = r'v\d{1}'
        v = re.compile('|'.join([vYYYYMMDD, vN]))  # Combine both regex into one
        df['version'] = df['file_dirname'].str.findall(v)
        df['version'] = df['version'].apply(lambda x: x[0] if x else 'v0')
        sorted_df = (
            df.sort_values('version')
            .drop_duplicates(subset='file_basename', keep='last')
            .reset_index(drop=True)
        )
        self.df = sorted_df.copy()
        print(self.df.info())
        self.persist_db_file()
        return self.df

    def _get_entry(self, directory):

        try:
            entry = {}
            dir_split = directory.split('/')
            entry['activity'] = 'CMIP5'
            entry['modeling_realm'] = dir_split[-1]
            entry['frequency'] = dir_split[-2]
            entry['experiment'] = dir_split[-3]
            entry['model'] = dir_split[-4]
            entry['institute'] = dir_split[-5]
            entry['product'] = dir_split[-6]
            return entry

        except Exception:
            return {}

    @delayed
    def _parse_directory(self, directory, columns):
        exclude = set(['files', 'latest'])  # directories to exclude

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
        """
        self._validate()
        if not os.path.exists(self.root_dir):
            raise NotADirectoryError(f'{os.path.abspath(self.root_dir)} does not exist')
        dirs = self.get_directories(root_dir=self.root_dir, depth=9, exclude_dirs=[])
        print(dirs)


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
