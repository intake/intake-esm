import fnmatch
import logging
import os
import uuid
from glob import glob

import numpy as np
import pandas as pd
import xarray as xr
import yaml
from intake.catalog import Catalog
from intake.catalog.local import LocalCatalogEntry
from intake_xarray.netcdf import NetCDFSource

from ._version import get_versions
from .cesm import CESMCollection, CESMSource
from .cmip import CMIPCollection, CMIPSource
from .common import _get_built_collections, _open_collection
from .config import INTAKE_ESM_CONFIG_FILE, SETTINGS, SOURCES

__version__ = get_versions()['version']
del get_versions


logger = logging.getLogger(__name__)
logger.setLevel(level=logging.DEBUG)


class ESMMetadataStoreCatalog(Catalog):
    """ESM collection Metadata store. This class acts as an entry point for `intake_esm`. """

    name = 'esm_metadatastore'
    version = __version__
    collection_types = {'cesm': CESMCollection, 'cmip': CMIPCollection}

    def __init__(
        self, collection_input_file=None, collection_name=None, collection_type=None, metadata=None
    ):
        """
        Parameters
        ----------

        collection_input_file : str,  Path, file
                    Path to a YAML file containing collection metadata
        collection_name : str
                 Collection name

        collection_type : str,
                 Collection type. Accepted values include:

                 - `cesm`
                 - `cmip`

        metadata : dict
               Arbitrary information to carry along with the data collection source specs.


        """
        self.collections = {}
        self.get_built_collections()

        if (collection_name and collection_type) and collection_input_file is None:
            self.open_collection(collection_name, collection_type)

        elif collection_input_file and (collection_name is None or collection_type is None):
            self.input_collection = self._validate_collection_input_file(collection_input_file)
            self.build_collection()

        else:
            raise ValueError(
                "Cannot instantiate class with provided arguments. Please provide either 'collection_input_file' \
                  \n\t\tor 'collection_name' and 'collection_type' "
            )
        super(ESMMetadataStoreCatalog, self).__init__(metadata)
        if self.metadata is None:
            self.metadata = {}

        self._entries = {}

    def _validate_collection_input_file(self, filepath):
        if os.path.exists(filepath):
            with open(filepath) as f:
                input_collection = yaml.safe_load(f)
                name = input_collection.get('name', None)
                collection_type = input_collection.get('collection_type', None)
                if name is None or collection_type is None:
                    raise ValueError(
                        f'name and/or collection_type keys are missing from {filepath} '
                    )
                else:
                    return input_collection

        else:
            raise FileNotFoundError(f'Specified collection input file: {filepath} doesn’t exist.')

    def build_collection(self):
        ctype = self.input_collection['collection_type']
        cc = ESMMetadataStoreCatalog.collection_types[ctype]
        cc = cc(self.input_collection)
        cc.build()
        self.get_built_collections()
        self.open_collection(
            self.input_collection['name'], self.input_collection['collection_type']
        )

    def get_built_collections(self):
        """Loads built collections in a dictionary with key=collection_name, value=collection_db_file_path"""
        self.collections = _get_built_collections()

    def open_collection(self, collection_name, collection_type):
        """ Open an ESM collection"""
        self.df, self.collection_name, self.collection_type = _open_collection(
            collection_name, collection_type
        )

    def search(self, **query):
        collection_columns = self.df.columns.tolist()
        for key in query.keys():
            if key not in collection_columns:
                raise ValueError(f'{key} is not in {self.collection_name}')
        for key in collection_columns:
            if key not in query:
                query[key] = None
        name = self.collection_name + '_' + str(uuid.uuid4())
        args = {
            'collection_name': self.collection_name,
            'collection_type': self.collection_type,
            'query': query,
            'chunks': {'time': 1},
            'engine': 'netcdf4',
            'decode_times': False,
            'decode_coords': False,
            'concat_dim': 'time',
        }
        driver = SOURCES[self.collection_type]
        description = f'Catalog entry from {self.collection_name} collection'
        cat = LocalCatalogEntry(
            name=name,
            description=description,
            driver=driver,
            direct_access=True,
            args=args,
            cache={},
            parameters={},
            metadata=self.metadata.copy(),
            catalog_dir='',
            getenv=False,
            getshell=False,
        )
        self._entries[name] = cat
        return cat
