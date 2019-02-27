import logging
import os

import numpy as np
import pandas as pd
import xarray as xr
from intake_xarray.netcdf import NetCDFSource

from ._version import get_versions
from .common import Collection, StorageResource, _open_collection, get_subset
from .config import INTAKE_ESM_CONFIG_FILE, SETTINGS

__version__ = get_versions()['version']
del get_versions


logger = logging.getLogger(__name__)
logger.setLevel(level=logging.WARNING)


class CESMCollection(Collection):
    def __init__(self, collection_spec):
        super(CESMCollection, self).__init__(collection_spec)
        self.component_streams = self.collection_definition.get('component_streams', None)
        self.replacements = self.collection_definition.get('replacements', {})
        self.overwrite_existing = self.collection_spec.get('overwriting_existing', True)
        self.include_cache_dir = self.collection_spec.get('include_cache_dir', False)
        self.df = pd.DataFrame(columns=self.columns)

    def _validate(self):
        for req_col in ['files', 'sequence_order']:
            if req_col not in self.columns:
                raise ValueError(
                    f"Missing required column: {req_col} for {self.collection_spec['collection_type']} in {INTAKE_ESM_CONFIG_FILE}"
                )

    def build(self):
        self._validate()
        # Loop over data sources/experiments
        for experiment, experiment_attrs in self.collection_spec['data_sources'].items():
            logger.warning(f'Working on experiment: {experiment}')

            component_attrs = experiment_attrs['component_attrs']
            ensembles = experiment_attrs['case_members']
            self.assemble_file_list(experiment, experiment_attrs, component_attrs, ensembles)
        logger.warning(self.df.info())
        logger.warning(f"Persisting {self.collection_spec['name']} at : {self.collection_db_file}")
        self.df.to_csv(self.collection_db_file, index=True)
        return self.df

    def assemble_file_list(self, experiment, experiment_attrs, component_attrs, ensembles):
        df_files = {}
        for location in experiment_attrs['locations']:
            res_key = ':'.join([location['name'], location['loc_type'], location['urlpath']])
            if res_key not in df_files:
                logger.warning(f'Getting file listing : {res_key}')

                if 'exclude_dirs' not in location:
                    location['exclude_dirs'] = []

                resource = StorageResource(
                    urlpath=location['urlpath'],
                    loc_type=location['loc_type'],
                    exclude_dirs=location['exclude_dirs'],
                )

                df_files[res_key] = self._assemble_collection_df_files(
                    resource_key=res_key,
                    resource_type=location['loc_type'],
                    direct_access=location['direct_access'],
                    filelist=resource.filelist,
                )

        # Include user defined data cache directories
        if self.include_cache_dir:
            res_key = ':'.join(['CACHE', 'posix', self.data_cache_dir])
            if res_key not in df_files:
                logger.warning(f'Getting file listing : {res_key}')
                resource = StorageResource(
                    urlpath=self.data_cache_dir, loc_type='posix', exclude_dirs=[]
                )

                df_files[res_key] = self._assemble_collection_df_files(
                    resource_key=res_key,
                    resource_type='posix',
                    direct_access=True,
                    filelist=resource.filelist,
                )

        # Loop over ensemble members
        for ensemble, ensemble_attrs in enumerate(ensembles):
            input_attrs_base = {'experiment': experiment}

            # Get attributes from ensemble_attrs
            case = ensemble_attrs['case']

            if 'ensemble' not in ensemble_attrs:
                input_attrs_base.update({'ensemble': ensemble})

            if 'sequence_order' not in ensemble_attrs:
                input_attrs_base.update({'sequence_order': 0})

            if 'has_ocean_bgc' not in ensemble_attrs:
                input_attrs_base.update({'has_ocean_bgc': False})

            if 'ctrl_branch_year' not in ensemble_attrs:
                input_attrs_base.update({'ctrl_branch_year': np.datetime64('NaT')})

            for res_key, df_f in df_files.items():
                # Find entries relevant to *this* ensemble:
                # "case" matches
                condition = df_f['case'] == case

                # If there are any matching files, append to self.df
                if any(condition):
                    input_attrs = dict(input_attrs_base)

                    input_attrs.update(
                        {
                            key: val
                            for key, val in ensemble_attrs.items()
                            if key in self.columns and key not in df_f.columns
                        }
                    )

                    # Relevant files
                    temp_df = pd.DataFrame(df_f.loc[condition])

                    # Append data coming from input file (input_attrs)
                    for col, val in input_attrs.items():
                        temp_df.insert(loc=0, column=col, value=val)

                    # Add data from "component_attrs" to appropriate column
                    for component in temp_df.component.unique():
                        if component not in component_attrs:
                            continue

                        for key, val in component_attrs[component].items():
                            if key in self.columns:
                                loc = temp_df['component'] == component
                                temp_df.loc[loc, key] = val

                    # Append
                    self.df = pd.concat([temp_df, self.df], ignore_index=True, sort=False)

        # Make replacements
        self.df.replace(self.replacements, inplace=True)

        # Reorder columns
        self.df = self.df[self.columns]

        # Remove duplicates
        self.df = self.df.drop_duplicates(subset=['resource', 'files'], keep='last').reset_index(
            drop=True
        )

    def _assemble_collection_df_files(self, resource_key, resource_type, direct_access, filelist):
        entries = {
            key: []
            for key in [
                'resource',
                'resource_type',
                'direct_access',
                'case',
                'component',
                'stream',
                'variable',
                'date_range',
                'files_basename',
                'files_dirname',
                'files',
            ]
        }

        # If there are no files, return empty dataframe
        if not filelist:
            return pd.DataFrame(entries)

        logger.warning(f'Building file database : {resource_key}')
        for f in filelist:
            fileparts = self._get_filename_parts(os.path.basename(f), self.component_streams)

            if fileparts is None or len(fileparts) == 0:
                continue

            entries['resource'].append(resource_key)
            entries['resource_type'].append(resource_type)
            entries['direct_access'].append(direct_access)

            entries['case'].append(fileparts['case'])
            entries['component'].append(fileparts['component'])
            entries['stream'].append(fileparts['stream'])
            entries['variable'].append(fileparts['variable'])
            entries['date_range'].append(fileparts['datestr'])

            entries['files_basename'].append(os.path.basename(f))
            entries['files_dirname'].append(os.path.dirname(f) + '/')
            entries['files'].append(f)

        return pd.DataFrame(entries)

    def _get_filename_parts(self, filename, component_streams):
        """ Extract each part of case.stream.variable.datestr.nc file pattern. """

        # Get Date string
        datestr = self._extract_date_str(filename)

        if datestr:
            for component, streams in component_streams.items():
                # Loop over stream strings
                # NOTE: The order matters here!
                for stream in sorted(streams, key=lambda s: len(s), reverse=True):

                    # Search for case.stream part of filename
                    s = filename.find(stream)

                    if s >= 0:  # Got a match
                        # Get varname.datestr.nc part of filename
                        case = filename[0 : s - 1]
                        idx = len(stream)
                        variable_datestr_nc = filename[s + idx + 1 :]
                        variable = variable_datestr_nc[: variable_datestr_nc.find('.')]

                        # Assert expected pattern
                        datestr_nc = variable_datestr_nc[
                            variable_datestr_nc.find(f'.{variable}.') + len(variable) + 2 :
                        ]

                        # Ensure that filename conforms to expected pattern
                        if datestr_nc != f'{datestr}.nc':
                            logger.warning(
                                f'Filename : {filename} does not conform to expected pattern'
                            )
                            return

                        return {
                            'case': case,
                            'component': component,
                            'stream': stream,
                            'variable': variable,
                            'datestr': datestr,
                        }

            logger.warning(f'Could not identify CESM fileparts for : {filename}')
            return
        else:
            return

    def _extract_date_str(self, filename):
        """ Extract a date string from a file name"""
        try:
            b = filename.split('.')[-2]
            return b
        except Exception:
            logger.warning(f'Could not extract date string from : {filename}')
            return


class CESMSource(NetCDFSource):
    """ Read CESM data sets into xarray datasets
    """

    name = 'cesm'
    partition_access = True
    version = __version__

    def __init__(
        self,
        collection_name,
        collection_type,
        query={},
        chunks={'time': 1},
        concat_dim='time',
        **kwargs,
    ):
        self.collection_name = collection_name
        self.collection_type = collection_type
        self.query = query
        self.query_results = get_subset(self.collection_name, self.collection_type, self.query)
        self._ds = None
        urlpath = get_subset(self.collection_name, self.collection_type, self.query).files.tolist()
        super(CESMSource, self).__init__(
            urlpath, chunks, concat_dim=concat_dim, path_as_pattern=False, **kwargs
        )
        if self.metadata is None:
            self.metadata = {}

    @property
    def results(self):
        """ Return collection entries matching query"""
        if self.query_results is not None:
            return self.query_results

        else:
            self.query_results = get_subset(self.collection_name, self.collection_type, self.query)
            return self.query_results

    def _open_dataset(self):
        url = self.urlpath
        kwargs = self._kwargs

        query = dict(self.query)
        if '*' in url or isinstance(url, list):
            if 'concat_dim' not in kwargs.keys():
                kwargs.update(concat_dim=self.concat_dim)
            if self.pattern:
                kwargs.update(preprocess=self._add_path_to_ds)

            ensembles = self.query_results.ensemble.unique()
            variables = self.query_results.variable.unique()

            ds_ens_list = []
            for ens_i in ensembles:
                query['ensemble'] = ens_i

                dsi = xr.Dataset()
                for var_i in variables:

                    query['variable'] = var_i
                    urlpath_ei_vi = get_subset(
                        self.collection_name, self.collection_type, query
                    ).files.tolist()
                    dsi = xr.merge(
                        (
                            dsi,
                            xr.open_mfdataset(
                                urlpath_ei_vi, data_vars=[var_i], chunks=self.chunks, **kwargs
                            ),
                        )
                    )

                    ds_ens_list.append(dsi)

            self._ds = xr.concat(ds_ens_list, dim='ens', data_vars=variables)
        else:
            self._ds = xr.open_dataset(url, chunks=self.chunks, **kwargs)

    def to_xarray(self, dask=True):
        """Return dataset as an xarray instance"""
        if dask:
            return self.to_dask()
        return self.read()
