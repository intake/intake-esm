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

from intake_esm import __version__

from ._version import get_versions
from .cesm import CESMCollection, CESMSource
from .cmip import CMIPCollection, CMIPSource
from .common import _get_built_collections, _open_collection
from .config import INTAKE_ESM_CONFIG_FILE, SETTINGS

logger = logging.getLogger(__name__)
logger.setLevel(level=logging.DEBUG)


class ESMMetadataStoreCatalog(Catalog):
    """ESM collection Metadata store. This class servers as an entry point for `intake_esm`. """

    name = "esm_metadatastore"
    version = __version__
    collection_types = {"cesm": CESMCollection, "cmip": CMIPCollection}
    SOURCES = {"cesm": "intake_esm.cesm.CESMSource", "cmip": "intake_esm.esm.CMIPSource"}

    def __init__(
        self, collection_input_file=None, collection_name=None, collection_type=None, **kwargs
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

        **kwargs

        """
        self.collections = {}
        self.get_built_collections()

        if collection_name and collection_type:
            self.open_collection(collection_name, collection_type)

        elif collection_input_file:
            with open(collection_input_file) as f:
                self.input_collections = yaml.load(f)

        else:
            raise ValueError(
                "Cannot instantiate class with empty arguments. Please provide either 'collection_input_file' \
                  \n\t\tor 'collection_name' and `collection_type`"
            )
        super(ESMMetadataStoreCatalog, self).__init__(**kwargs)
        if self.metadata is None:
            self.metadata = {}

        self._entries = {}

    def build_collections(self):
        """ Builds collections defined in a collection input YAML file"""
        for collection_name, collection_vals in self.input_collections.items():
            logger.info(f"Calling build_collection on {collection_name}")
            collection_type = collection_vals["collection_type"]
            cc = ESMMetadataStoreCatalog.collection_types[collection_type]
            cc = cc(collection_name, collection_type, collection_vals)

            # If collection exists in database_directory, continue
            if cc.overwrite_existing:
                cc.build()
            else:
                continue

        self.get_built_collections()
        return self

    def get_built_collections(self):
        """Loads built collections in a dictionary with key=collection_name, value=collection_db_file_path"""
        self.collections = _get_built_collections()

    def open_collection(self, collection_name, collection_type):
        """ Open an ESM collection"""
        self.df, self.collection_name, self.collection_type = _open_collection(
            collection_name, collection_type
        )
        return self

    def search(self, **query):
        collection_columns = self.df.columns.tolist()
        for key in query.keys():
            if key not in collection_columns:
                raise ValueError(f"{key} is not in {self.collection_name}")
        for key in collection_columns:
            if key not in query:
                query[key] = None
        name = self.collection_name + "_" + str(uuid.uuid4())
        args = {
            "collection_name": self.collection_name,
            "collection_type": self.collection_type,
            "query": query,
            "chunks": {"time": 1},
            "engine": "netcdf4",
            "decode_times": False,
            "decode_coords": False,
            "concat_dim": "time",
        }
        driver = ESMMetadataStoreCatalog.SOURCES[self.collection_type]
        description = f"Catalog entry from {self.collection_name} collection"
        cat = LocalCatalogEntry(
            name=name,
            description=description,
            driver=driver,
            direct_access=True,
            args=args,
            cache={},
            parameters={},
            metadata=self.metadata.copy(),
            catalog_dir="",
            getenv=False,
            getshell=False,
        )
        self._entries[name] = cat
        return cat
