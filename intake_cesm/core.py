import inspect
import uuid

import numpy as np
import pandas as pd
import xarray as xr
from intake.catalog import Catalog
from intake.catalog.local import LocalCatalogEntry
from intake_xarray.netcdf import NetCDFSource

from ._version import get_versions
from .common import get_collection_def, get_subset, open_collection
from .manage_collections import CESMCollections

__version__ = get_versions()["version"]
del get_versions


class CesmMetadataStoreCatalog(Catalog):
    """ CESM collection Metadata store """

    name = "cesm_metadatastore"
    version = __version__

    def __init__(self, collection, build_args={}, **kwargs):
        """
        Parameters
        ----------
        collection : string
                   CESM collection name
        build_args : dict
                    A dictionary containing arguments to enable and trigger a collection build.
                    This dictionary should contain the following keys:

                    - `collection_input_file`
                    - `collection_type_def_file`
                    - `overwriting_existing`
                    - `include_cache_dir`
                    For more info about these arguments, check :class:`~intake_cesm._manage_collections.CESMCollections`

        """
        self.collection = collection
        self.build_args = build_args
        if self.build_args:
            self.df = self.build_collections()
        else:
            self.df = open_collection(collection)

        kwargs.setdefault("name", collection)
        super(CesmMetadataStoreCatalog, self).__init__(**kwargs)
        if self.metadata is None:
            self.metadata = {}
        self._entries = {}

    def build_collections(self):
        """ Build CESM collection
        """

        if "collection_input_file" in self.build_args:
            collection_input_file = self.build_args.get("collection_input_file")

        if "collection_type_def_file" in self.build_args:
            collection_type_def_file = self.build_args.get("collection_type_def_file")

        overwrite_existing = self.build_args.get("overwrite_existing", False)
        include_cache_dir = self.build_args.get("include_cache_dir", False)

        cc = CESMCollections(
            collection_input_file, collection_type_def_file, overwrite_existing, include_cache_dir
        )
        return cc.get_built_collection()

    def search(self, **query):
        """ Search for entries matching query

        Parameters
        ----------

        query : keyword arguments of a catalog query

        Returns:
            intake.Catalog -- An intake catalog entry
        """

        collection_columns = get_collection_def(self.collection)
        for key in query.keys():
            if key not in collection_columns:
                raise ValueError(f"{key} is not in {self.collection}")

        for key in collection_columns:
            if key not in query:
                query[key] = None

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

        query = dict(self.query)
        if "*" in url or isinstance(url, list):
            if "concat_dim" not in kwargs.keys():
                kwargs.update(concat_dim=self.concat_dim)
            if self.pattern:
                kwargs.update(preprocess=self._add_path_to_ds)

            ensembles = self.query_results.ensemble.unique()
            variables = self.query_results.variable.unique()

            ds_ens_list = []
            for ens_i in ensembles:
                query["ensemble"] = ens_i

                dsi = xr.Dataset()
                for var_i in variables:

                    query["variable"] = var_i
                    urlpath_ei_vi = get_subset(self.collection, query).files.tolist()
                    dsi = xr.merge(
                        (
                            dsi,
                            xr.open_mfdataset(
                                urlpath_ei_vi, data_vars=[var_i], chunks=self.chunks, **kwargs
                            ),
                        )
                    )

                    ds_ens_list.append(dsi)

            self._ds = xr.concat(ds_ens_list, dim="ens", data_vars=variables)
        else:
            self._ds = xr.open_dataset(url, chunks=self.chunks, **kwargs)

    def to_xarray(self, dask=True):
        """Return dataset as an xarray instance"""
        if dask:
            return self.to_dask()
        return self.read()
