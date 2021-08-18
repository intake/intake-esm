import os
import typing

import fsspec
import intake
import intake.catalog
import pandas as pd
import pydantic
import toolz

from ._types import ESMCatalogModel

# from .source import ESMDataSource, ESMGroupDataSource


class esm_datastore_v2(intake.catalog.Catalog):
    """in-memory representation for the Earth System Model (ESM) catalog."""

    name = 'esm_datastore_v2'

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
        self._df = self._load_dataframe()
        self._cast_agg_columns_with_iterables()

    def _cast_agg_columns_with_iterables(self) -> None:
        """Cast all agg_columns with iterables to tuple values so as
        to avoid hashing issues (e.g. TypeError: unhashable type: 'list')
        """
        columns = list(
            self._columns_with_iterables.intersection(
                set(
                    map(
                        lambda agg: agg.attribute_name, self.esmcat.aggregation_control.aggregations
                    )
                )
            )
        )
        if columns:
            self._df[columns] = self._df[columns].apply(tuple)

    @toolz.memoize
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

    @property
    def df(self) -> pd.DataFrame:
        return self._df

    @property
    @toolz.memoize
    def _columns_with_iterables(self) -> typing.Set[str]:
        """Return a list of columns that have iterables."""
        if self.df.empty:
            return set()
        has_iterables = (
            self.df.sample(20, replace=True).applymap(type).isin([list, tuple, set]).any().to_dict()
        )
        return {column for column, check in has_iterables.items() if check}

    @property
    def _multiple_variable_assets(self) -> bool:
        return self.esmcat.aggregation_control.variable_column_name in self._columns_with_iterables

    @property
    def _grouped(self) -> typing.Union[pd.core.groupby.DataFrameGroupBy, pd.DataFrame]:
        if self.esmcat.aggregation_control.groupby_attrs and set(
            self.esmcat.aggregation_control.groupby_attrs
        ) != set(self.df.columns):
            return self.df.groupby(self.esmcat.aggregation_control.groupby_attrs)
        return self.df

    def _construct_groups_and_keys(self) -> typing.List[str]:
        if isinstance(self._grouped, pd.core.groupby.generic.DataFrameGroupBy):
            internal_keys = self._grouped.groups.keys()
            public_keys = map(
                lambda key: key
                if isinstance(key, str)
                else self.sep.join(str(value) for value in key),
                internal_keys,
            )

        else:
            internal_keys = self._grouped.index
            public_keys = (
                self.df[self.df.columns.tolist()]
                .apply(lambda row: self.sep.join(str(v) for v in row), axis=1)
                .tolist()
            )

        return dict(zip(public_keys, internal_keys))

    def keys(self) -> typing.List[str]:
        return list(self._keys_dict.keys())

    @property
    def _internal_keys(self) -> typing.List[str]:
        return list(self._keys_dict.values())

    @property
    def _keys_dict(self) -> typing.Dict[str, typing.Tuple[str]]:
        return self._construct_groups_and_keys()

    @property
    def key_template(self) -> str:
        """Return string template used to create catalog entry keys."""
        if self.esmcat.aggregation_control.groupby_attrs:
            return self.sep.join(self.esmcat.aggregation_control.groupby_attrs)
        else:
            return self.sep.join(self.df.columns)

    def _unique(self) -> typing.Dict:
        def _find_unique(series):
            values = series.dropna()
            if series.name in self._columns_with_iterables:
                values = toolz.concat(values)
            return list(toolz.unique(values))

        return self.df[self.df.columns].apply(_find_unique, result_type='reduce').to_dict()

    def unique(self) -> pd.Series:
        return pd.Series(self._unique())

    def nunique(self) -> pd.Series:
        return pd.Series(toolz.valmap(len, self._unique()))

    def __len__(self):
        return len(self.keys())

    def __dir__(self):
        values = ['df', 'keys', 'unique', 'nunique', 'key_template']
        return sorted(list(self.__dict__.keys()) + values)

    def _get_entries(self):
        # Due to just-in-time entry creation, we may not have all entries loaded
        # We need to make sure to create entries missing from self._entries
        missing = set(self.keys()) - set(self._entries.keys())
        for key in missing:
            _ = self[key]
        return self._entries

    # def __getitem__(self, key:str) -> typing.Union[ESMDataSource, ESMGroupDataSource]:
    #     """Get an entry from the catalog.

    #     Parameters
    #     ----------
    #     key : str
    #         The key to get from the catalog.

    #     Returns
    #     -------
    #     ESMDataSource or ESMGroupDataSource
    #         The data source for the entry.
    #     """
    #     try:
    #         return self._entries[key]
    #     except KeyError:
    #         if key in self.keys():
    #             internal_key = self._keys_dict[key]
    #             if isinstance(self._grouped, pd.DataFrame):
    #                 df = self._grouped.loc[internal_key]
    #                 entry = intake.catalog.local.LocalCatalogEntry(name=key, description='', driver=ESMDataSource, args={'df': df, 'aggregation_dict'}, metadata={}).get()
