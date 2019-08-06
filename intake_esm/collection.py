import datetime
import os
import re
from abc import ABC, abstractclassmethod
from glob import glob
from pathlib import Path

import docrep
import pandas as pd
import pkg_resources
import xarray as xr
from intake.source.utils import reverse_format
from tqdm.autonotebook import tqdm

from . import config
from .storage import StorageResource

docstrings = docrep.DocstringProcessor()


@docstrings.get_sectionsf('Collection')
class Collection(ABC):
    """ Base class to build collections.

    This class should not be used directly, use inherited class approriate for
    individual collection (e.g. CMIP5Collection, CESMCollection)

    Parameters
    ----------

    collection_spec : dict


    See Also
    --------

    CMIP5Collection
    CMIP6Collection
    CESMCollection
    MPIGECollection
    GMETCollection

    """

    def __init__(self, collection_spec, fs=None):
        self.fs = fs
        self.collection_spec = collection_spec
        self.collection_definition = config.get('collections').get(
            collection_spec['collection_type'], None
        )
        self.columns = self.collection_definition.get(
            config.normalize_key('collection_columns'), None
        )
        if not self.columns:
            raise ValueError(
                f"Unable to locate collection columns for {collection_spec['collection_type']} collection type in {config.PATH}"
            )
        self.df = pd.DataFrame(columns=self.columns)
        self._ds = xr.Dataset()
        self.exclude_patterns = self._get_exclude_patterns()
        self.database_dir = config.get('database-directory', None)
        self.order_by_columns = self.collection_definition.get('order-by-columns')

        self._validate()

        if self.database_dir:
            self.collection_db_file = f"{self.database_dir}/{collection_spec['name']}.nc"
            os.makedirs(self.database_dir, exist_ok=True)

    def build(self):
        """ Main method for looping through data sources and building
            a collection catalog.
        """
        dfs = {}
        data_sources = self.collection_spec['data_sources'].items()
        for data_source, data_source_attrs in data_sources:
            df_i = self.assemble_file_list(data_source, data_source_attrs, self.exclude_patterns)
            dfs.update(df_i)

        self.df = self._finalize_build(dfs)
        self._ds = self.df.reset_index(drop=True).to_xarray()
        attrs = make_attrs(
            attrs={
                'collection_spec': self.collection_spec,
                'name': self.collection_spec['name'],
                'collection_type': self.collection_spec['collection_type'],
            }
        )
        self._ds.attrs = attrs
        print(self._ds)
        self.persist_db_file()

    def assemble_file_list(self, data_source, data_source_attrs, exclude_patterns=[]):
        """ Assemble file listing for data sources into Pandas dataframes.
        """
        df_files = {}
        for location in data_source_attrs['locations']:
            res_key = ':'.join(
                [data_source, location['name'], location['loc_type'], location['urlpath']]
            )
            if res_key not in df_files:
                print(f'Getting file listing: {res_key}')

                resource = StorageResource(
                    urlpath=location['urlpath'],
                    loc_type=location['loc_type'],
                    exclude_patterns=exclude_patterns,
                    file_extension=location.get('file_extension', '.nc'),
                    fs=self.fs,
                )

                df_files[res_key] = self._assemble_collection_df_files(
                    resource_key=res_key,
                    resource_type=location['loc_type'],
                    direct_access=location['direct_access'],
                    filelist=resource.filelist,
                    urlpath=location['urlpath'],
                )
                df_files[res_key] = self._add_extra_attributes(
                    data_source,
                    df_files[res_key],
                    extra_attrs=data_source_attrs.get('extra_attributes', {}),
                )

        return df_files

    def _add_extra_attributes(self, data_source, df, extra_attrs):
        """ Add extra attributes to individual data sources.

        Subclasses can override this method with a custom implementation.

        """

        if extra_attrs:
            for key, value in extra_attrs.items():
                df[key] = value
        return df

    @staticmethod
    def _extract_attr_with_regex(input_str, regex, strip_chars=None):
        pattern = re.compile(regex)
        match = re.findall(pattern, input_str)
        if match:
            match = max(match, key=len)
            if strip_chars:
                match = match.strip(strip_chars)

            else:
                match = match.strip()

            return match

        else:
            return None

    @staticmethod
    def _reverse_filename_format(file_basename, filename_template=None, gridspec_template=None):
        """
        Uses intake's ``reverse_format`` utility to reverse the string method format.

        Given format_string and resolved_string, find arguments
        that would give format_string.format(arguments) == resolved_string
        """
        try:
            return reverse_format(filename_template, file_basename)
        except ValueError:
            try:
                return reverse_format(gridspec_template, file_basename)
            except:
                print(
                    f'Failed to parse file: {file_basename} using patterns: {filename_template} and {gridspec_template}'
                )
                return {}

    def _assemble_collection_df_files(
        self, resource_key, resource_type, direct_access, filelist, urlpath=None
    ):
        entries = {key: [] for key in self.columns}
        if not filelist:
            return pd.DataFrame(entries)

        # Check parameters of _get_file_attrs for presence of urlpath for backwards compatibility
        from inspect import signature

        sig = signature(self._get_file_attrs)
        if 'urlpath' in sig.parameters:
            pass_urlpath = True
        else:
            pass_urlpath = False

        for f in tqdm(filelist, desc='file listing'):
            if pass_urlpath:
                file_attrs = self._get_file_attrs(f, urlpath)
            else:
                file_attrs = self._get_file_attrs(f)

            if not file_attrs:
                continue

            file_attrs['resource'] = resource_key
            file_attrs['resource_type'] = resource_type
            file_attrs['direct_access'] = direct_access

            for col in self.columns:
                entries[col].append(file_attrs.get(col, None))

        return pd.DataFrame(entries)

    @abstractclassmethod
    def _get_file_attrs(self, filepath):
        """Extract attributes from file path

        """
        pass

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

        Notes
        -----

        Subclasses can implement custom version.
        """

        df = pd.concat(list(df_files.values()), ignore_index=True, sort=False)
        # Reorder columns
        df = df[self.columns]

        # Remove duplicates
        df = df.drop_duplicates(subset=['resource', 'file_fullpath'], keep='last').reset_index(
            drop=True
        )
        df = df.sort_values(self.order_by_columns)

        return df

    def _get_exclude_patterns(self):
        """Get patterns of files and directories to exclude from
           the collection
        """
        collection_spec = self.collection_spec
        exclude_patterns = []
        data_sources = collection_spec['data_sources']
        for data_source, data_source_attrs in data_sources.items():
            locations = data_source_attrs['locations']
            for loc in locations:
                exclude = loc.get('exclude_patterns', None) or loc.get('exclude_dirs', None)
                if exclude:
                    exclude_patterns.extend(exclude)

        return exclude_patterns

    def _validate(self):
        """Checks that collection columns are properly defined in `confi.yaml` file.
        """
        for req_col in config.get('collections')[self.collection_spec['collection_type']][
            'required-columns'
        ]:
            if req_col not in self.columns:
                raise ValueError(
                    f"Missing required column: {req_col} for {self.collection_spec['collection_type']} in {config.PATH}"
                )

    def persist_db_file(self):
        """ Persist built collection database to disk.
        """
        if not self.df.empty:
            print(
                f"Persisting {self.collection_spec['name']} at : {os.path.abspath(self.collection_db_file)}"
            )

            if os.path.exists(self.collection_db_file):
                os.remove(self.collection_db_file)
            # specify encoding to avoid: ValueError: unsupported dtype for netCDF4 variable: bool
            self._ds.to_netcdf(
                self.collection_db_file,
                mode='w',
                engine='netcdf4',
                encoding={'direct_access': {'dtype': 'bool'}},
            )

        else:
            print(f"{self.df} is an empty dataframe. It won't be persisted to disk.")


def _get_built_collections():
    """Loads built collections in a dictionary with key=collection_name, value=collection_db_file_path"""
    try:
        db_dir = Path(config.get('database-directory'))
        cc = db_dir.glob('*.nc')
        collections = {}
        for f in cc:
            name = f.stem
            fullpath = f.absolute()
            collections[name] = fullpath
        return collections
    except Exception as e:
        raise e


def _open_collection(collection_name):
    """ Open an ESM collection"""
    collections = _get_built_collections()

    ds = xr.open_dataset(collections[collection_name], engine='netcdf4')
    # ds['direct_access'] = ds['direct_access'].astype(bool)
    collection_type = ds.attrs['collection_type']
    collection_name = ds.attrs['name']
    return ds.to_dataframe(), collection_name, collection_type


def get_subset(collection_name, query, order_by=None):
    """ Get a subset of collection entries that match a query """
    import numpy as np

    df, _, collection_type = _open_collection(collection_name)

    condition = np.ones(len(df), dtype=bool)

    for key, val in query.items():
        if isinstance(val, list):
            condition_i = np.zeros(len(df), dtype=bool)
            for val_i in val:
                condition_i = condition_i | (df[key] == val_i)
            condition = condition & condition_i

        elif val is not None:
            condition = condition & (df[key] == val)
    query_results = df.loc[condition]

    if order_by is None:
        order_by = config.get('collections')[collection_type]['order-by-columns']

    query_results = query_results.sort_values(by=order_by, ascending=True)

    return query_results


def make_attrs(attrs=None):
    """Make standard attributes to attach to xarray datasets (collections).
    Parameters
    ----------
    attrs : dict (optional)
        Additional attributes to add or overwrite
    Returns
    -------
    dict
        attrs
    """

    import intake_xarray
    import intake
    import json

    default_attrs = {
        'created_at': datetime.datetime.utcnow().isoformat(),
        'intake_esm_version': pkg_resources.get_distribution('intake_esm').version,
    }

    upstream_deps = [intake, intake_xarray]
    for dep in upstream_deps:
        dep_name = dep.__name__
        try:
            version = pkg_resources.get_distribution(dep_name).version
            default_attrs[f'{dep_name}_version'] = version
        except pkg_resources.DistributionNotFound:
            if hasattr(dep, '__version__'):
                version = dep.__version__
                default_attrs[f'{dep_name}_version'] = version
    if attrs is not None:
        if 'collection_spec' in attrs:
            attrs['collection_spec'] = json.dumps(attrs['collection_spec'])
        default_attrs.update(attrs)
    return default_attrs
