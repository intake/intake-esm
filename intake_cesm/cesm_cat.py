import inspect
import uuid

import numpy as np
import pandas as pd
from intake.catalog import Catalog
from intake.catalog.local import LocalCatalogEntry

from ._version import get_versions
from .config import collections

__version__ = get_versions()["version"]
del get_versions


class CesmCatalog(Catalog):
    name = "cesm_cat"
    version = __version__

    def __init__(self, collection, **kwargs):
        self.collection = collection
        self.df = self._open_collection(collection)
        kwargs.setdefault("name", collection)
        super(CesmCatalog, self).__init__(**kwargs)
        if self.metadata is None:
            self.metadata = {}
        self._entries = {}

    def _open_collection(self, collection):
        try:
            df = pd.read_csv(collections[collection], index_col=0)
            print(f"Active collection: {collection}")
            return df

        except (KeyError, FileNotFoundError) as err:
            print("****** The specified collection does not exit. ******")
            raise err

    def set_collection(self, collection):
        """Set the active collection"""
        self.__init__(collection)

    def _get_subset(self, query):
        condition = np.ones(len(self.df), dtype=bool)

        for key, val in query.items():
            if not isinstance(val, list) and val is not None:
                val = [val]
                condition = condition & (self.df[key].isin(val))

        query_results = (
            self.df.loc[condition]
            .sort_values(by=["sequence_order", "files"], ascending=True)
            .files.tolist()
        )
        return query_results

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

        # Capture parameter names and values in a dictionary to be use as a query
        frame = inspect.currentframe()
        _, _, _, query = inspect.getargvalues(frame)
        query.pop("self", None)
        query.pop("frame", None)
        name = self.collection + "-" + str(uuid.uuid4())
        files = self._get_subset(query)
        args = {
            "urlpath": files,
            "chunks": {"time": 1},
            "decode_times": False,
            "decode_coords": False,
        }
        description = f"Catalog from {self.collection} collection"
        cat = LocalCatalogEntry(
            name=name,
            description=description,
            driver="netcdf",
            direct_access=False,
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
