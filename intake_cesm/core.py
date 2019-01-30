import inspect
import uuid

import numpy as np
import pandas as pd
import xarray as xr
from intake.catalog import Catalog
from intake.catalog.local import LocalCatalogEntry
from intake_xarray.netcdf import NetCDFSource

from ._version import get_versions
from .common import get_subset, open_collection

__version__ = get_versions()["version"]
del get_versions


class CesmMetadataStoreCatalog(Catalog):
    name = "cesm_metadatastore"
    version = __version__

    def __init__(self, collection, **kwargs):
        """ CESM collection Metadata store

        Parameters
        ----------
        collection : string
                   CESM collection to use. Accepted values are `cesm1_le`, `cesm2_runs`, `cesm_dple`.

        """
        self.collection = collection
        self.df = open_collection(collection)
        print(f"Active collection: {collection}")
        kwargs.setdefault("name", collection)
        super(CesmMetadataStoreCatalog, self).__init__(**kwargs)
        if self.metadata is None:
            self.metadata = {}
        self._entries = {}

    def set_collection(self, collection):
        """Set the active collection"""
        self.__init__(collection)

    def search(
        self,
        case=None,
        component=None,
        date_range=None,
        ensemble=None,
        experiment=None,
        stream=None,
        variable=None,
        ctrl_branch_year=None,
        has_ocean_bgc=None,
    ):
        """ Search for entries matching query

        Parameters
        ----------

        case : string or list of strings
        component : string or list of strings
        date_range : string
        ensemble : int or list of integers
        stream : string
        variable : string
        ctrl_branch_year : string
        has_ocean_bgc : bool


        Returns:
            intake.Catalog -- An intake catalog entry
        """

        # Capture parameter names and values in a dictionary to be use as a query
        frame = inspect.currentframe()
        _, _, _, query = inspect.getargvalues(frame)
        query.pop("self", None)
        query.pop("frame", None)
        name = self.collection + "-" + str(uuid.uuid4())
        args = {
            "collection": self.collection,
            "query": query,
            "chunks": {"time": 1},
            "engine": "netcdf4",
            "decode_times": False,
            "decode_coords": False,
            "concat_dim": "time",
        }

        description = f"Catalog from {self.collection} collection"
        cat = LocalCatalogEntry(
            name=name,
            description=description,
            driver="intake_cesm.core.CesmSource",
            direct_access=True,
            args=args,
            cache={},
            parameters={},
            metadata=(self.metadata or {}).copy(),
            catalog_dir="",
            getenv=False,
            getshell=False,
        )
        self._entries[name] = cat
        return cat


class CesmSource(NetCDFSource):
    """ Read CESM data sets into xarray datasets
    """

    name = "cesm"
    partition_access = True
    version = __version__

    def __init__(self, collection=None, query={}, chunks={"time": 1}, concat_dim="time", **kwargs):
        self.collection = collection
        self.query = query
        self.query_results = get_subset(self.collection, self.query)
        self._ds = None
        urlpath = get_subset(self.collection, self.query).files.tolist()
        super(CesmSource, self).__init__(
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
            self.query_results = get_subset(self.collection, self.query)
            return self.query_results

    def _open_dataset(self):
        url = self.urlpath
        kwargs = self._kwargs
        if "*" in url or isinstance(url, list):
            _open_dataset = xr.open_mfdataset
            if "concat_dim" not in kwargs.keys():
                kwargs.update(concat_dim=self.concat_dim)
            if self.pattern:
                kwargs.update(preprocess=self._add_path_to_ds)
        else:
            _open_dataset = xr.open_dataset

        self._ds = _open_dataset(url, chunks=self.chunks, **kwargs)

    def to_xarray(self, dask=True):
        """Return dataset as an xarray instance"""
        if dask:
            return self.to_dask()
        return self.read()
