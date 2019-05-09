""" Implementation for The ECMWF ERA5 Reanalyses data holdings """
import os

import pandas as pd

from .common import BaseSource, Collection, StorageResource, get_subset


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
        self.df = self.df[~self.df['parameter_id'].isna()]
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
            entries['start_date'].append(fileparts['start_date'])
            entries['end_date'].append(fileparts['end_date'])
            entries['start_year'].append(fileparts['start_year'])
            entries['start_month'].append(fileparts['start_month'])
            entries['end_year'].append(fileparts['end_year'])
            entries['end_month'].append(fileparts['end_month'])
            entries['start_day'].append(fileparts['start_day'])
            entries['start_hour'].append(fileparts['start_hour'])
            entries['end_day'].append(fileparts['end_day'])
            entries['end_hour'].append(fileparts['end_hour'])
            entries['local_table'].append(fileparts['local_table'])
            entries['stream'].append(fileparts['stream'])
            entries['level_type'].append(fileparts['level_type'])
            entries['data_type'].append(fileparts['data_type'])
            entries['parameter_id'].append(fileparts['parameter_id'])
            entries['parameter_type'].append(fileparts['parameter_type'])
            entries['parameter_short_name'].append(fileparts['parameter_short_name'])
            entries['grid'].append(fileparts['grid'])
            entries['file_basename'].append(basename)
            entries['file_dirname'].append(os.path.dirname(f) + '/')
            entries['file_fullpath'].append(f)

        return pd.DataFrame(entries)

    def _get_filename_parts(self, filename):
        """ Get file attributes from filename """
        fs = filename.split('.')

        keys = [
            'stream',
            'data_type',
            'level_type',
            'parameter_type',
            'parameter_id',
            'parameter_short_name',
            'local_table',
            'grid',
            'start_date',
            'end_date',
            'start_year',
            'end_year',
            'start_month',
            'end_month' 'start_day',
            'start_hour',
            'end_day',
            'end_hour',
            'grid',
        ]

        fileparts = {key: None for key in keys}

        fileparts['stream'] = fs[1]
        fileparts['data_type'] = fs[2]
        if fileparts['data_type'] == 'invariant':
            fileparts['level_type'] = None
        else:
            fileparts['level_type'] = fs[3]

        if fileparts['data_type'] == 'an':
            fileparts['parameter_type'] = 'instan'

        elif fileparts['data_type'] == 'fc':
            fileparts['parameter_type'] = fs[4]

        else:
            fileparts['parameter_type'] = None

        ecmwf_params = fs[-4].split('_')
        if len(ecmwf_params) == 3:
            fileparts['local_table'] = ecmwf_params[0]
            fileparts['parameter_id'] = ecmwf_params[1]
            fileparts['parameter_short_name'] = ecmwf_params[2]

        fileparts['grid'] = fs[-3]
        time_range = fs[-2].replace('_', '-')
        time_ranges = time_range.split('-')
        start_time = time_ranges[0]
        end_time = time_ranges[1]
        if len(start_time) == 10:
            start_year = start_time[0:4]
            start_month = start_time[4:6]
            start_day = start_time[6:8]
            start_hour = start_time[8:]
            fileparts['start_date'] = '-'.join([start_year, start_month, start_day])
            fileparts['start_year'] = int(start_year)
            fileparts['start_month'] = int(start_month)
            fileparts['start_day'] = int(start_day)
            fileparts['start_hour'] = int(start_hour)

        if len(end_time) == 10:
            end_year = end_time[0:4]
            end_month = end_time[4:6]
            end_day = end_time[6:8]
            end_hour = end_time[8:]
            fileparts['end_date'] = '-'.join([end_year, end_month, end_day])
            fileparts['end_year'] = int(end_year)
            fileparts['end_month'] = int(end_month)
            fileparts['end_day'] = int(end_day)
            fileparts['end_hour'] = int(end_hour)

        return fileparts


class ERA5Source(BaseSource):
    name = 'era5'
    partition_access = True
