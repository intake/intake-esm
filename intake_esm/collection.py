import os
import re
from abc import ABC, abstractclassmethod
from glob import glob

import docrep
import numpy as np
import pandas as pd
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
        self.exclude_patterns = self._get_exclude_patterns()
        self.database_base_dir = config.get('database-directory', None)
        self.order_by_columns = self.collection_definition.get('order-by-columns')

        self._validate()

        if self.database_base_dir:
            self.database_dir = f"{self.database_base_dir}/{collection_spec['collection_type']}"
            self.collection_db_file = f"{self.database_dir}/{collection_spec['name']}.{collection_spec['collection_type']}.csv"
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
        print(self.df.info())
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
            self.df.to_csv(self.collection_db_file, index=True)

        else:
            print(f"{self.df} is an empty dataframe. It won't be persisted to disk.")


def _get_built_collections():
    """Loads built collections in a dictionary with key=collection_name, value=collection_db_file_path"""
    try:
        db_dir = config.get('database-directory')
        cc = [y for x in os.walk(db_dir) for y in glob(os.path.join(x[0], '*.csv'))]
        collections = {}
        for collection in cc:
            name, meta = _decipher_collection_name(collection)
            collections[name] = meta
        return collections
    except Exception as e:
        raise e


def _decipher_collection_name(collection_path):
    c_ = os.path.basename(collection_path).split('.')
    collection_meta = {}
    collection_name = c_[0]
    collection_meta['collection_type'] = c_[1]
    collection_meta['path'] = collection_path
    return collection_name, collection_meta


def _open_collection(collection_name):
    """ Open an ESM collection"""

    collection_types = config.get('sources').keys()
    collections = _get_built_collections()

    collection_type = collections[collection_name]['collection_type']
    path = collections[collection_name]['path']
    if (collection_type in collection_types) and collections:
        try:
            df = pd.read_csv(path, index_col=0, dtype={'direct_access': bool})
            return df, collection_name, collection_type
        except Exception as err:
            raise err

    else:
        raise ValueError("Couldn't open specified collection")


def _test_str_pattern(ser, pat, case=False, regex=True):
    """Test if pattern or regex is contained within a string of a Series or Index.

    Parameters
    ----------

    ser: pandas.Series

    pat: str
        Character sequence or regular expression.

    case: bool, default True
         If True, case sensitive.

    regex: bool, default True

        If True, assumes the pat is a regular expression.
        If False, treats the pat as a literal string.

    Returns
    -------

    Index of boolean values
       Index of boolean values indicating whether the given pattern
       is contained within the string of each element of the Series or Index.
    """

    return ser.str.contains(pat, case=case, regex=regex)


def get_subset(collection_name, query, order_by=None):
    """ Get a subset of collection entries that match a query """
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
