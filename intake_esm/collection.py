from abc import ABC, abstractclassmethod
from pathlib import Path

import docrep
import pandas as pd
import xarray as xr
from tqdm.auto import tqdm

from . import config
from .bld_collection_utils import make_attrs
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
        self.database_dir = Path(config.get('database-directory')).absolute()
        self.order_by_columns = self.collection_definition.get('order-by-columns')

        self._validate()

        if self.database_dir:
            self.collection_db_file = Path(
                f"{self.database_dir}/{collection_spec['name']}.nc"
            ).absolute()
            self.database_dir.mkdir(parents=True, exist_ok=True)

    def build(self):
        """ Main method for looping through data sources and building
            a collection catalog.
        """
        dfs = {}
        data_sources = self.collection_spec['data_sources'].items()
        for data_source, data_source_attrs in data_sources:
            df_i = self.assemble_file_list(data_source, data_source_attrs, self.exclude_patterns)
            dfs.update(df_i)

        self._ds = self._finalize_build(dfs).reset_index(drop=True).to_xarray()

        attrs = make_attrs(
            attrs={
                'collection_spec': self.collection_spec,
                'name': self.collection_spec['name'],
                'collection_type': self.collection_spec['collection_type'],
            }
        )
        self._ds.attrs = attrs
        self._persist_db_file()

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

        for f in tqdm(filelist, desc='file listing', disable=not config.get('progress-bar')):
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
            Cleaned dataframe containing all entries

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
        """Checks that collection columns are properly defined in `config.yaml` file.
        """
        for req_col in config.get('collections')[self.collection_spec['collection_type']][
            'required-columns'
        ]:
            if req_col not in self.columns:
                raise ValueError(
                    f"Missing required column: {req_col} for {self.collection_spec['collection_type']} in {config.PATH}"
                )

    def _persist_db_file(self):
        """ Persist built collection database to disk.
        """
        if len(self._ds.index) > 0:
            print(f"Persisting {self.collection_spec['name']} at : {self.collection_db_file}")

            if self.collection_db_file.exists():
                self.collection_db_file.unlink()

            # specify encoding to avoid: ValueError: unsupported dtype for netCDF4 variable: bool
            self._ds.to_netcdf(
                self.collection_db_file,
                mode='w',
                engine='netcdf4',
                encoding={'direct_access': {'dtype': 'bool'}},
            )

        else:
            print(f"{self._ds} is an empty dataset. It won't be persisted to disk.")
