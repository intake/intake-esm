""" Implementation for The ECMWF ERA5 Reanalyses data holdings """
import os
import warnings

import pandas as pd
import xarray as xr
from tqdm.autonotebook import tqdm

from . import aggregate
from .common import BaseSource, Collection, StorageResource, get_subset

warnings.simplefilter('ignore')


class ERA5Collection(Collection):

    """ Defines ERA5 dataset collection

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
    intake_esm.gmet.GMETCollection
    """

    def __init__(self, collection_spec):
        super(ERA5Collection, self).__init__(collection_spec)
        self.df = pd.DataFrame(columns=self.columns)

    def build(self):
        self._validate()
        for data_source, data_source_attrs in self.collection_spec['data_sources'].items():
            print(f'Working on data source: {data_source}')
            self.assemble_file_list(data_source, data_source_attrs)

        print(self.df.info())
        self.persist_db_file()
        return self.df

    def assemble_file_list(self, data_source, data_source_attrs):
        df_files = {}
        for location in data_source_attrs['locations']:
            res_key = ':'.join([location['name'], location['loc_type'], location['urlpath']])
            if res_key not in df_files:
                print(f'Getting file listing : {res_key}')

                exclude_dirs = location.get('exclude_dirs', [])
                file_extension = location.get('file_extension', '.nc')
                required_keys = ['urlpath', 'loc_type', 'direct_access']
                for key in required_keys:
                    if key not in location.keys():
                        raise ValueError(f'{key} must be specified in {self.collection_spec}')

                resource = resource = StorageResource(
                    urlpath=location['urlpath'],
                    loc_type=location['loc_type'],
                    exclude_dirs=exclude_dirs,
                    file_extension=file_extension,
                )

                df_files[res_key] = self._assemble_collection_df_files(
                    resource_key=res_key,
                    resource_type=location['loc_type'],
                    direct_access=location['direct_access'],
                    filelist=resource.filelist,
                )

        for res_key, df_f in df_files.items():
            self.df = pd.concat([df_f, self.df], ignore_index=True, sort=False)

        # Reorder columns
        self.df = self.df[self.columns]

        # Remove inconsistent rows and duplicates
        self.df = self.df[~self.df['variable_id'].isna()]
        self.df = self.df.drop_duplicates(
            subset=['resource', 'file_fullpath'], keep='last'
        ).reset_index(drop=True)

    def _assemble_collection_df_files(self, resource_key, resource_type, direct_access, filelist):
        """ Assemble file listing into a Pandas DataFrame."""
        entries = {key: [] for key in self.columns}

        if not filelist:
            return pd.DataFrame(entries)

        print(f'Building file database : {resource_key}')
        for f in filelist:
            try:
                basename = os.path.basename(f)
                fileparts = self._get_filename_parts(basename)
            except Exception:
                continue

            entries['resource'].append(resource_key)
            entries['resource_type'].append(resource_type)
            entries['direct_access'].append(direct_access)
            entries['local_table'].append(fileparts['local_table'])
            entries['stream'].append(fileparts['stream'])
            entries['level_type'].append(fileparts['level_type'])
            entries['product_type'].append(fileparts['product_type'])
            entries['variable_id'].append(fileparts['variable_id'])
            entries['variable_type'].append(fileparts['variable_type'])
            entries['variable_short_name'].append(fileparts['variable_short_name'])
            entries['forecast_initial_date'].append(fileparts['forecast_initial_date'])
            entries['forecast_initial_hour'].append(fileparts['forecast_initial_hour'])
            entries['reanalysis_month'].append(fileparts['reanalysis_month'])
            entries['reanalysis_year'].append(fileparts['reanalysis_year'])
            entries['reanalysis_day'].append(fileparts['reanalysis_day'])
            entries['grid'].append(fileparts['grid'])
            entries['file_basename'].append(basename)
            entries['file_dirname'].append(os.path.dirname(f) + '/')
            entries['file_fullpath'].append(f)

        return pd.DataFrame(entries)

    def _get_filename_parts(self, filename):
        """ Get file attributes from filename """
        fs = filename.split('.')

        keys = [
            'forecast_initial_date',
            'forecast_initial_hour',
            'grid',
            'level_type',
            'local_table',
            'product_type',
            'reanalysis_day',
            'reanalysis_month',
            'reanalysis_year',
            'stream',
            'variable_id',
            'variable_short_name',
            'variable_type',
        ]

        fileparts = {key: None for key in keys}

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
