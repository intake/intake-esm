""" Implementation for The ECMWF ERA5 Reanalyses data holdings """
import os
import re

import pandas as pd
import xarray as xr
from tqdm.autonotebook import tqdm

from . import aggregate, config
from .collection import Collection, docstrings, get_subset
from .source import BaseSource


class ERA5Collection(Collection):

    __doc__ = docstrings.with_indents(
        """ Builds an ECWMF ERA5 Reanalysis collection for data
        stored on NCAR's GLADE in ``/glade/collections/rda/data/ds630.0``.
    %(Collection.parameters)s
    """
    )

    def _get_file_attrs(self, filepath):
        file_basename = os.path.basename(filepath)
        fs = file_basename.split('.')

        keys = list(set(self.columns) - set(['resource', 'resource_type', 'direct_access']))

        fileparts = {key: None for key in keys}
        fileparts['file_basename'] = file_basename
        fileparts['file_dirname'] = os.path.dirname(filepath) + '/'
        fileparts['file_fullpath'] = filepath

        fileparts['stream'] = fs[1]
        if fs[2] == 'an':
            fileparts['product_type'] = 'reanalysis'

        elif fs[2] == 'fc':
            fileparts['product_type'] = 'forecast'

        else:
            fileparts['product_type'] = fs[2]

        if fileparts['product_type'] == 'invariant':
            fileparts['level_type'] = None

        else:
            fileparts['level_type'] = fs[3]

        if fileparts['product_type'] == 'reanalysis':
            fileparts['variable_type'] = 'instan'

        elif fileparts['product_type'] == 'forecast':
            fileparts['variable_type'] = fs[4]

        else:
            fileparts['variable_type'] = None

        ecmwf_params = fs[-4].split('_')
        if len(ecmwf_params) == 3:
            fileparts['local_table'] = ecmwf_params[0]
            fileparts['variable_id'] = ecmwf_params[1]
            fileparts['variable_short_name'] = ecmwf_params[2]

        fileparts['grid'] = fs[-3]
        time_ranges = fs[-2].replace('_', '-').split('-')
        start_time = time_ranges[0]
        if len(start_time) == 10:
            start_year = str(start_time[0:4])
            start_month = str(start_time[4:6])
            start_day = str(start_time[6:8])
            start_hour = str(start_time[8:]) + ':00'
            start_date = '-'.join([start_year, start_month, start_day])
            if fileparts['product_type'] == 'reanalysis':
                fileparts['reanalysis_day'] = start_day
                fileparts['reanalysis_month'] = start_month
                fileparts['reanalysis_year'] = start_year

            elif fileparts['product_type'] == 'forecast':
                fileparts['forecast_initial_date'] = start_date
                fileparts['forecast_initial_hour'] = start_hour

            else:
                pass

        return fileparts


class ERA5Source(BaseSource):
    name = 'era5'
    partition_access = True

    def _open_dataset(self):
        """
        Notes
        -----
        - netCDF variables names are uppercase in netCDF files.
        - netCDF variables names that could conceivably begin with digit, say '10U',
          the actual netCDF variable name will be VAR_10U.
          In ECMWF reanalysis, there about a half dozen such names beginning with digits.
          '10fg', '10v', '2d', '2t', '10u', '100u', '100v'

        """

        kwargs = self._validate_kwargs(self.kwargs)
        dataset_fields = ['product_type']
        variable_column_name = 'variable_short_name'
        file_fullpath_column_name = 'file_fullpath'

        invariant_files = get_subset(self.collection_name, {'product_type': 'invariant'})[
            file_fullpath_column_name
        ].tolist()
        invariants_dset = (
            xr.open_mfdataset(invariant_files, drop_variables=['time']).squeeze().load()
        )

        grouped = get_subset(self.collection_name, self.query).groupby(dataset_fields)
        product_dsets = {}
        for p_id, p_files in tqdm(grouped, desc='product'):
            new_time_coord_name = 'forecast_initial_time' if p_id == 'forecast' else 'time'
            chunks = kwargs['chunks']
            chunks[new_time_coord_name] = chunks.pop(kwargs['time_coord_name'])
            var_dsets = []
            for v_id, v_files in tqdm(p_files.groupby(variable_column_name), desc='variable'):
                urlpath_ei_vi = v_files[file_fullpath_column_name].tolist()

                if v_id[0].isdigit():
                    v_id = 'var_' + v_id

                dsets = [
                    aggregate.open_dataset_delayed(
                        url,
                        data_vars=[v_id.upper()],
                        chunks=chunks,
                        decode_times=kwargs['decode_times'],
                    )
                    for url in urlpath_ei_vi
                ]
                var_dset_i = aggregate.concat_time_levels(dsets, new_time_coord_name)
                var_dsets.append(var_dset_i)

            var_dsets.append(invariants_dset)
            product_dsets[p_id] = aggregate.merge(dsets=var_dsets).set_coords(
                invariants_dset.data_vars
            )
        self._ds = product_dsets
