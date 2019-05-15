""" Implementation for The CSIRO DCFP CAFE Reanalyses data holdings """
import os
import warnings

import pandas as pd
import xarray as xr
from tqdm.autonotebook import tqdm

from . import aggregate
from .common import BaseSource, Collection, StorageResource, get_subset

warnings.simplefilter('ignore')


class CAFECollection(Collection):

    """ Defines CAFE dataset collection

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
    intake_esm.era5.ERA5Collection
    """

    def __init__(self, collection_spec):
        super(CAFECollection, self).__init__(collection_spec)
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
        self.df = self.df.drop_duplicates(
            subset=['product_type', 'file_fullpath'], keep='last'
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

            entries['variable_short_name'].append(fileparts['variable_short_name'])
            entries['realm'].append(fileparts['realm'])
            entries['frequency'].append(fileparts['frequency'])
            entries['product_type'].append(fileparts['product_type'])
            entries['start_date'].append(fileparts['start_date'])
            entries['end_date'].append(fileparts['end_date'])
            entries['file_basename'].append(basename)
            entries['file_dirname'].append(os.path.dirname(f) + '/')
            entries['file_fullpath'].append(f)

        return pd.DataFrame(entries)

    def _get_filename_parts(self, filename):
        """ Get file attributes from filename """
        fs = filename.split('.')

        keys = [
            'variable_short_name',
            'realm',
            'frequency',
            'product_type',
            'start_date',
            'end_date',
        ]

        fileparts = {key: None for key in keys}

        fileparts['variable_short_name'] = fs[0]
        fileparts['realm'] = fs[1]
        fileparts['frequency'] = fs[2]
        fileparts['product_type'] = fs[3]

        # fs[4] is YYYYMMDD-YYYYMMDD
        s, e = fs[4].split('-')
        fileparts['start_date'] = f'{s[:4]}-{s[4:6]}-{s[6:8]}'
        fileparts['end_date'] = f'{e[:4]}-{e[4:6]}-{e[6:8]}'

        return fileparts


class CAFESource(BaseSource):
    name = 'cafe'
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
