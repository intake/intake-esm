import os
import uuid

import yaml
from intake.catalog import Catalog
from intake.catalog.local import LocalCatalogEntry

from . import config as config
from .cesm import CESMCollection
from .cmip import CMIP5Collection, CMIP6Collection
from .common import _get_built_collections, _open_collection
from .era5 import ERA5Collection
from .gmet import GMETCollection
from .mpige import MPIGECollection


class ESMMetadataStoreCatalog(Catalog):
    """ESM collection Metadata store. This class acts as an entry point for `intake_esm`.

    Parameters
    ----------

    collection_input_definition : Path, file_object or dict
                Path to a YAML file containing collection definition or
                a dictionary containing nested dictionaries of entries.

    collection_name : str
                name of the collection to use

    overwrite_existing : bool,
            Whether to overwrite existing built collection catalog

    metadata : dict
            Arbitrary information to carry along with the data collection source specs.


    """

    name = 'esm_metadatastore'
    collection_types = {
        'cesm': CESMCollection,
        'cmip5': CMIP5Collection,
        'cmip6': CMIP6Collection,
        'mpige': MPIGECollection,
        'gmet': GMETCollection,
        'era5': ERA5Collection,
    }

    def __init__(
        self,
        collection_input_definition=None,
        collection_name=None,
        overwrite_existing=True,
        metadata={},
    ):

        self.metadata = metadata
        self.collections = {}
        self.get_built_collections()

        if collection_name and collection_input_definition is None:
            self.open_collection(collection_name)

        elif collection_input_definition and (collection_name is None):
            self.input_collection = self._validate_collection_definition(
                collection_input_definition
            )
            self.build_collection(overwrite_existing)

        else:
            raise ValueError(
                "Cannot instantiate class with provided arguments. Please provide either 'collection_input_definition' \
                  \n\t\tor 'collection_name' "
            )

        self._entries = {}

    def _validate_collection_definition(self, definition):

        if isinstance(definition, dict):
            input_collection = definition.copy()

        elif os.path.exists(definition):
            with open(os.path.abspath(definition)) as f:
                input_collection = yaml.safe_load(f)

        name = input_collection.get('name', None)
        collection_type = input_collection.get('collection_type', None)
        if name is None or collection_type is None:
            raise ValueError(f'name and/or collection_type keys are missing from {definition}')
        else:
            return input_collection

    def build_collection(self, overwrite_existing):
        """ Build a collection defined in a YAML input file or a dictionary of nested dictionaries"""
        name = self.input_collection['name']
        if name not in self.collections or overwrite_existing:
            ctype = self.input_collection['collection_type']
            cc = ESMMetadataStoreCatalog.collection_types[ctype]
            cc = cc(self.input_collection)
            cc.build()
            self.get_built_collections()
        self.open_collection(name)

    def get_built_collections(self):
        """ Loads built collections in a dictionary with ``key=collection_name``,
        ``value=collection_db_file_path`` """
        self.collections = _get_built_collections()

    def open_collection(self, collection_name):
        """ Open an ESM collection """
        self.df, self.collection_name, self.collection_type = _open_collection(collection_name)

    def search(self, **query):
        """ Search for entries in the collection catalog
        """
        collection_columns = self.df.columns.tolist()
        for key in query.keys():
            if key not in collection_columns:
                raise ValueError(f'{key} is not in {self.collection_name}')
        for key in collection_columns:
            if key not in query:
                query[key] = None
        name = self.collection_name + '_' + str(uuid.uuid4())
        args = {'collection_name': self.collection_name, 'query': query}
        driver = config.get('sources')[self.collection_type]
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
