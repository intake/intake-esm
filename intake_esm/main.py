import functools
import os
import typing

import fsspec
import intake
import intake.catalog
import pandas as pd
import pydantic

from ._types import ESMCatalogModel


class esm_datastore_v2(intake.catalog.Catalog):
    """in-memory representation for the Earth System Model (ESM) catalog."""

    name = 'esm_datastore'

    def __init__(
        self,
        path: typing.Union[pydantic.FilePath, pydantic.AnyUrl],
        *,
        sep: str = '.',
        read_csv_kwargs: typing.Dict[str, typing.Any] = None,
        storage_options: typing.Dict = None,
        intake_kwargs: typing.Dict = None,
    ):
        """Intake Catalog representing an ESM Collection.
        Parameters
        ----------
        path: str
            path to the catalog file
        sep: str
            Delimiter to use when constructing a key for a query, by default '.'
        read_csv_kwargs: dict, optional
            Additional keyword arguments passed through to the :py:func:`~pandas.read_csv` function.
        storage_options : dict, optional
            Parameters passed to the backend file-system such as Google Cloud Storage,
            Amazon Web Service S3.
        intake_kwargs: dict, optional
            Additional keyword arguments are passed through to the :py:class:`~intake.catalog.Catalog` base class.
        """

        intake_kwargs = intake_kwargs or {}
        super(esm_datastore_v2, self).__init__(**intake_kwargs)
        self.storage_options = storage_options or {}
        self.read_csv_kwargs = read_csv_kwargs or {}
        self.sep = sep
        self._mapper = fsspec.get_mapper(path, **self.storage_options)
        self.esmcat = ESMCatalogModel.load_json_file(
            self._mapper.root, storage_options=self.storage_options
        )
        self._entries = {}

    def _load_dataframe(self) -> pd.DataFrame:
        """Load the catalog into a dataframe."""

        if self.esmcat.catalog_file:
            if self._mapper.fs.exists(self.esmcat.catalog_file):
                csv_path = self.esmcat.catalog_file
            else:
                csv_path = f'{os.path.dirname(self._mapper.root)}/{self.esmcat.catalog_file}'
            self.esmcat.catalog_file = csv_path
            return pd.read_csv(
                self.esmcat.catalog_file,
                storage_options=self.storage_options,
                **self.read_csv_kwargs,
            )

        return pd.DataFrame(self.esmcat.catalog_dict)

    @functools.cached_property
    def df(self):
        return self._load_dataframe()
