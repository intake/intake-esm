#!/usr/bin/env python
""" Implementation for NCAR's Community Earth System Model (CESM)-LENS AWS data holdings """
import os

import pandas as pd
import xarray as xr
from tqdm.autonotebook import tqdm

from . import aggregate
from .collection import Collection, docstrings, get_subset
from .source import BaseSource


class CESMAWSCollection(Collection):

    __doc__ = docstrings.with_indents(
        """ Builds a collection for CESM-LENS data hosted on AWS.
    %(Collection.parameters)s
    """
    )

    def _get_file_attrs(self, storepath):
        """ Extract each part of cesmLE-experiment-component-frequency-variable.zarr store pattern. """
        keys = list(set(self.columns) - set(['resource', 'resource_type', 'direct_access']))
        storeparts = {key: None for key in keys}
        store_meta = storepath.split('/')
        storeparts['store_bucketname'] = 's3://' + store_meta[0]
        storeparts['store_fullpath'] = 's3://' + storepath
        storeparts['component'] = store_meta[1]
        storeparts['frequency'] = store_meta[2]

        x = store_meta[-1].split('-')
        storeparts['experiment'] = x[1]
        storeparts['variable'] = x[-1].split('.')[0]

        return storeparts

    def _finalize_build(self, df_files):
        """ This method is used to finalize the build process by:

            - Removing duplicates
            - Adding extra metadata

        Parameters
        ----------
        df_files : dict
             Dictionary containing Pandas dataframes for different data sources


        Returns
        --------
        df : pandas.DataFrame
            Cleaned pandas dataframe containing all entries

        """

        df = pd.concat(list(df_files.values()), ignore_index=True, sort=False)
        # Reorder columns
        df = df[self.columns]

        # Remove duplicates
        df = df.drop_duplicates(subset=['resource', 'store_fullpath'], keep='last').reset_index(
            drop=True
        )
        return df


class CESMAWSSource(BaseSource):

    name = 'cesm-aws'
    partition_access = True

    @staticmethod
    def _validate_zarr_kwargs(kwargs):
        _kwargs = {}
        _kwargs['group'] = kwargs.get('group', None)
        _kwargs['synchronizer'] = kwargs.get('synchronizer', None)
        _kwargs['auto_chunk'] = kwargs.get('auto_chunk', True)
        _kwargs['decode_cf'] = kwargs.get('decode_cf', True)
        _kwargs['decode_times'] = kwargs.get('decode_times', True)
        _kwargs['decode_coords'] = kwargs.get('decode_coords', True)
        _kwargs['mask_and_scale'] = kwargs.get('mask_and_scale', True)
        _kwargs['concat_characters'] = kwargs.get('concat_characters', True)
        _kwargs['drop_variables'] = kwargs.get('drop_variables', None)
        _kwargs['consolidated'] = kwargs.get('consolidated', True)
        return _kwargs

    def _open_dataset(self):
        # fields which define a single dataset
        dataset_fields = ['component', 'frequency']
        kwargs = self._validate_kwargs(self.kwargs)
        zarr_kwargs = CESMAWSSource._validate_zarr_kwargs(kwargs)

        query_results = get_subset(self.collection_name, self.query)
        grouped = query_results.groupby(dataset_fields)
        all_dsets = {}
        for dset_keys, dset_stores in tqdm(grouped, desc='dataset(s)'):
            dset_id = '.'.join(dset_keys)
            grouped_exp = dset_stores.groupby('experiment')
            dsets = []
            for exp_id, exp_stores in grouped_exp:
                exp_dsets = []
                for v_id, v_stores in tqdm(exp_stores.groupby('variable'), desc='variable(s)'):
                    urlpath_ei_vi = v_stores['store_fullpath'].tolist()
                    v_dsets = [
                        aggregate.open_store(
                            url,
                            data_vars=[v_id],
                            storage_options=self.storage_options,
                            **zarr_kwargs,
                        )
                        for url in urlpath_ei_vi
                    ]
                    exp_dsets.extend(v_dsets)
                exp_dset = aggregate.merge(exp_dsets)
                dsets.append(exp_dset)

            dset = aggregate.concat_time_levels(
                dsets, kwargs['time_coord_name'], restore_non_dim_coords=True
            )
            all_dsets[dset_id] = dset
        self._ds = all_dsets
