import datetime
import os
import uuid

import numpy as np
import s3fs
from cached_property import cached_property
from intake.catalog import Catalog
from intake.catalog.local import LocalCatalogEntry
from intake.utils import yaml_load

from . import config as config
from .bld_collection_utils import (
    FILE_ALIAS_DICT,
    _get_built_collections,
    _open_collection,
    load_collection_input_file,
)
from .cesm import CESMCollection
from .cesm_aws import CESMAWSCollection
from .cmip import CMIP5Collection, CMIP6Collection
from .cordex import CORDEXCollection
from .era5 import ERA5Collection
from .gmet import GMETCollection
from .mpige import MPIGECollection


class ESMMetadataStoreCatalog(Catalog):
    """ESM collection Metadata store. This class acts as an entry point for ``intake_esm``.

    Parameters
    ----------

    collection_input_definition : str, dict, filepath, default (None)

                If None, prints out list of collection definitions supported
                out of the box and raise `ValueError`.

                If str, this should be a valid collection name among the ones
                supported out of the box

                If dict, or filepath, this should be a path to a YAML file
                containing collection definition or a dictionary containing
                nested dictionaries of entries.

    collection_name : str
                Name of the collection to use. This name should refer to
                a collection catalog that is already built and persisted on disk.

    overwrite_existing : bool,
            Whether to overwrite existing built collection catalog.

    storage_options : dict
            Parameters to pass to requests when issuing http commands to remote
            backend file-systems such as s3.

    kwargs : dict, optional
        Keyword arguments passed to ``intake_esm.bld_collection_utils.load_collection_input_file`` function

    """

    name = 'esm_metadatastore'
    collection_types = {
        'cesm': CESMCollection,
        'cesm-aws': CESMAWSCollection,
        'cmip5': CMIP5Collection,
        'cmip6': CMIP6Collection,
        'mpige': MPIGECollection,
        'gmet': GMETCollection,
        'era5': ERA5Collection,
        'cordex': CORDEXCollection,
    }

    def __init__(
        self,
        collection_input_definition=None,
        collection_name=None,
        overwrite_existing=True,
        storage_options=None,
        **kwargs,
    ):

        super().__init__(**kwargs)
        self.storage_options = storage_options or {}
        self.collection_type = None
        self.fs = None
        self.ds = None
        self.collections = _get_built_collections()

        if (
            collection_name
            and (collection_name not in self.collections)
            and (collection_name in FILE_ALIAS_DICT)
        ):
            self.input_collection = self._validate_collection_definition(collection_name, **kwargs)
            self._build_collection(overwrite_existing)

        elif collection_name and collection_input_definition is None:
            self.open_collection(collection_name)

        elif collection_input_definition and (collection_name is None):
            self.input_collection = self._validate_collection_definition(
                collection_input_definition, **kwargs
            )
            self._build_collection(overwrite_existing)

        else:

            if self.collections:
                print(
                    '\n******************************************************\n'
                    '* Collections with following names are built already *\n'
                    '******************************************************\n\n'
                    f'{list(self.collections.keys())}\n\n'
                )

            load_collection_input_file()
            raise ValueError(
                'Cannot instantiate class with provided arguments. Please provide either: \n'
                '\t1) collection_input_definition: to build a collection or\n'
                '\t2) collection_name: to open a collection.'
            )

        self._entries = {}

    @cached_property
    def df(self):
        return self.ds.to_dataframe()

    def nunique(self):
        """Count distinct observations across dataframe columns"""
        return self.df.nunique()

    def unique(self, columns=None):
        """ Return unique values for given columns"""
        if isinstance(columns, str):
            columns = [columns]
        if not columns:
            columns = self.df.columns

        info = {}
        for col in columns:
            uniques = self.df[col].unique().tolist()
            info[col] = {'count': len(uniques), 'values': uniques}
        return info

    def __repr__(self):
        """Making string representation of object."""
        info = self.nunique().to_dict()
        output = []
        for key, values in info.items():
            output.append(f'{values} {key}(s)\n')
        output = '\n\t> '.join(output)
        items = len(self.ds.index)
        return f'{self.collection_name.upper()} collection catalogue with {items} entries:\n\t> {output}'

    def _validate_collection_definition(self, definition, **kwargs):

        if isinstance(definition, str) and definition in FILE_ALIAS_DICT:
            input_collection = load_collection_input_file(definition, **kwargs)

        elif isinstance(definition, dict):
            input_collection = definition.copy()

        else:
            try:
                with open(os.path.abspath(definition)) as f:
                    input_collection = yaml_load(f)
            except Exception as exc:
                raise exc

        name = input_collection.get('name', None)
        self.collection_type = input_collection.get('collection_type', None)
        if self.collection_type == 'cesm-aws':
            self._get_s3_connection_info()
        if name is None or self.collection_type is None:
            raise ValueError(f'name and/or collection_type keys are missing from {definition}')
        else:
            return input_collection

    def _build_collection(self, overwrite_existing):
        """ Build a collection defined in a YAML input file or a dictionary of nested dictionaries"""
        name = self.input_collection['name']
        collection_type = self.input_collection['collection_type']
        if name not in self.collections or overwrite_existing:
            cc = ESMMetadataStoreCatalog.collection_types[collection_type]
            cc = cc(self.input_collection, fs=self.fs)
            cc.build()
            self.collections = _get_built_collections()
        self.open_collection(name)

    def _get_s3_connection_info(self):
        try:
            if 'requester_pays' not in self.storage_options:
                self.storage_options['requester_pays'] = True
            self.fs = s3fs.S3FileSystem(**self.storage_options)
        except Exception as exc:
            raise exc

    def open_collection(self, collection_name):
        """ Open an ESM collection """
        self.ds = _open_collection(collection_name)
        self.collection_name = self.ds.attrs['name']
        self.collection_type = self.ds.attrs['collection_type']

    def search(self, **query):
        """ Search for entries in the collection catalog
        """
        collection_columns = list(self.ds.data_vars)
        for key in query.keys():
            if key not in collection_columns:
                raise ValueError(f'{key} is not in {self.collection_name}')
        for key in collection_columns:
            if key not in query:
                query[key] = None
        name = self.collection_name + '_' + str(uuid.uuid4())
        args = {
            'collection_name': self.collection_name,
            'query': query,
            'storage_options': self.storage_options,
        }
        driver = config.get('sources')[self.collection_type]
        description = f'Catalog entry generated from {self.collection_name} collection'
        keys = ['created_at', 'intake_esm_version', 'intake_version', 'intake_xarray_version']
        metadata = {k: self.ds.attrs[k] for k in keys}
        metadata['catalog_entry_generated_at'] = datetime.datetime.utcnow().isoformat()

        cat = LocalCatalogEntry(
            name=name,
            description=description,
            driver=driver,
            direct_access=True,
            args=args,
            cache={},
            parameters={},
            metadata=metadata,
            catalog_dir='',
            getenv=False,
            getshell=False,
        )
        self._entries[name] = cat
        return cat
