from __future__ import annotations

import datetime
import enum
import functools
import json
import os
import typing

import fsspec
import pandas as pd
import polars as pl
import pydantic
import tlz
from pydantic import ConfigDict
from typing_extensions import Self

from ._search import search, search_apply_require_all_on


def _allnan_or_nonan(df, column: str) -> bool:
    """Check if all values in a column are NaN or not NaN

    Returns
    -------
    bool
        Whether the dataframe column has all NaNs or no NaN valles

    Raises
    ------
    ValueError
        When the column has a mix of NaNs non NaN values
    """
    if df[column].isnull().all():
        return False
    if df[column].isnull().any():
        raise ValueError(
            f'The data in the {column} column should either be all NaN or there should be no NaNs'
        )
    return True


class AggregationType(str, enum.Enum):
    join_new = 'join_new'
    join_existing = 'join_existing'
    union = 'union'

    model_config = ConfigDict(validate_assignment=True)


class DataFormat(str, enum.Enum):
    netcdf = 'netcdf'
    zarr = 'zarr'
    reference = 'reference'
    opendap = 'opendap'

    model_config = ConfigDict(validate_assignment=True)


class Attribute(pydantic.BaseModel):
    column_name: pydantic.StrictStr
    vocabulary: pydantic.StrictStr = ''

    model_config = ConfigDict(validate_assignment=True)


class Assets(pydantic.BaseModel):
    column_name: pydantic.StrictStr
    format: DataFormat | None = None
    format_column_name: pydantic.StrictStr | None = None

    model_config = ConfigDict(validate_assignment=True)

    @pydantic.model_validator(mode='after')
    def _validate_data_format(cls, model):
        data_format, format_column_name = model.format, model.format_column_name
        if data_format is not None and format_column_name is not None:
            raise ValueError('Cannot set both format and format_column_name')
        elif data_format is None and format_column_name is None:
            raise ValueError('Must set one of format or format_column_name')
        return model


class Aggregation(pydantic.BaseModel):
    type: AggregationType
    attribute_name: pydantic.StrictStr
    options: dict = {}

    model_config = ConfigDict(validate_assignment=True)


class AggregationControl(pydantic.BaseModel):
    variable_column_name: pydantic.StrictStr
    groupby_attrs: list[pydantic.StrictStr]
    aggregations: list[Aggregation] = []

    model_config = ConfigDict(validate_default=True, validate_assignment=True)


class ESMCatalogModel(pydantic.BaseModel):
    """
    Pydantic model for the ESM data catalog defined in https://git.io/JBWoW
    """

    esmcat_version: pydantic.StrictStr
    attributes: list[Attribute]
    assets: Assets
    aggregation_control: AggregationControl | None = None
    id: str = ''
    catalog_dict: list[dict] | None = None
    catalog_file: pydantic.StrictStr | None = None
    description: pydantic.StrictStr | None = None
    title: pydantic.StrictStr | None = None
    last_updated: datetime.datetime | datetime.date | None = None
    _df: pd.DataFrame | None = pydantic.PrivateAttr()
    _frames: FramesModel | None = pydantic.PrivateAttr()

    model_config = ConfigDict(arbitrary_types_allowed=True, validate_assignment=True)

    @pydantic.model_validator(mode='after')
    def validate_catalog(cls, model):
        catalog_dict, catalog_file = model.catalog_dict, model.catalog_file
        if catalog_dict is not None and catalog_file is not None:
            raise ValueError('catalog_dict and catalog_file cannot be set at the same time')

        return model

    def __setattr__(self, name, value):
        """If we manually set _df, we need to propagate the change to _frames"""
        if name == '_df':
            self._frames = FramesModel(df=value)
        return super().__setattr__(name, value)

    @classmethod
    def from_dict(cls, data: dict) -> ESMCatalogModel:
        esmcat = data['esmcat']
        df = data['df']
        if 'last_updated' not in esmcat:
            esmcat['last_updated'] = None
        cat = cls.model_validate(esmcat)
        cat._df = df
        cat._frames = FramesModel(df=df)
        return cat

    def save(
        self,
        name: str,
        *,
        directory: str | None = None,
        catalog_type: str = 'dict',
        to_csv_kwargs: dict | None = None,
        json_dump_kwargs: dict | None = None,
        storage_options: dict[str, typing.Any] | None = None,
    ) -> None:
        """
        Save the catalog to a file.

        Parameters
        -----------
        name: str
            The name of the file to save the catalog to.
        directory: str
            The directory or cloud storage bucket to save the catalog to.
            If None, use the current directory.
        catalog_type: str
            The type of catalog to save. Whether to save the catalog table as a dictionary
            in the JSON file or as a separate CSV file. Valid options are 'dict' and 'file'.
        to_csv_kwargs : dict, optional
            Additional keyword arguments passed through to the :py:meth:`~pandas.DataFrame.to_csv` method.
        json_dump_kwargs : dict, optional
            Additional keyword arguments passed through to the :py:func:`~json.dump` function.
        storage_options: dict
            fsspec parameters passed to the backend file-system such as Google Cloud Storage,
            Amazon Web Service S3.

        Notes
        -----
        Large catalogs can result in large JSON files. To keep the JSON file size manageable, call with
        `catalog_type='file'` to save catalog as a separate CSV file.

        """

        if catalog_type not in {'file', 'dict'}:
            raise ValueError(
                f'catalog_type must be either "dict" or "file". Received catalog_type={catalog_type}'
            )

        # Check if the directory is None, and if it is, set it to the current directory
        if directory is None:
            directory = os.getcwd()

        # Configure the fsspec mapper and associated filenames
        storage_options = storage_options if storage_options is not None else {}
        mapper = fsspec.get_mapper(f'{directory}', **storage_options)
        fs = mapper.fs
        csv_file_name = fs.unstrip_protocol(f'{mapper.root}/{name}.csv')
        json_file_name = fs.unstrip_protocol(f'{mapper.root}/{name}.json')

        data = self.model_dump().copy()
        for key in {'catalog_dict', 'catalog_file'}:
            data.pop(key, None)
        data['id'] = name
        data['last_updated'] = datetime.datetime.now().utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')

        if catalog_type == 'file':
            csv_kwargs: dict[str, typing.Any] = {'index': False}
            csv_kwargs |= to_csv_kwargs or {}
            compression = csv_kwargs.get('compression', '')
            extensions = {'gzip': '.gz', 'bz2': '.bz2', 'zip': '.zip', 'xz': '.xz'}
            csv_file_name = f'{csv_file_name}{extensions.get(compression, "")}'
            data['catalog_file'] = str(csv_file_name)

            with fs.open(csv_file_name, 'wb') as csv_outfile:
                self.df.to_csv(csv_outfile, **csv_kwargs)
        else:
            data['catalog_dict'] = self.df.to_dict(orient='records')

        with fs.open(json_file_name, 'w') as outfile:
            json_kwargs = {'indent': 2}
            json_kwargs |= json_dump_kwargs or {}
            json.dump(data, outfile, **json_kwargs)  # type: ignore[arg-type]

        print(f'Successfully wrote ESM catalog json file to: {json_file_name}')

    @classmethod
    def load(
        cls,
        json_file: str | pydantic.FilePath | pydantic.AnyUrl,
        storage_options: dict[str, typing.Any] | None = None,
        read_csv_kwargs: dict[str, typing.Any] | None = None,
    ) -> ESMCatalogModel:
        """
        Loads the catalog from a file

        Parameters
        -----------
        json_file: str or pathlib.Path
            The path to the json file containing the catalog
        storage_options: dict
            fsspec parameters passed to the backend file-system such as Google Cloud Storage,
            Amazon Web Service S3.
        read_csv_kwargs: dict
            Additional keyword arguments passed through to the :py:func:`~pandas.read_csv` function.

        """
        storage_options = storage_options if storage_options is not None else {}
        read_csv_kwargs = read_csv_kwargs or {}
        json_file = str(json_file)  # We accept Path, but fsspec doesn't.
        _mapper = fsspec.get_mapper(json_file, **storage_options)

        with fsspec.open(json_file, **storage_options) as fobj:
            data = json.loads(fobj.read())
            if 'last_updated' not in data:
                data['last_updated'] = None
            cat = cls.model_validate(data)
            if cat.catalog_file:
                cat._frames = cat._df_from_file(cat, _mapper, storage_options, read_csv_kwargs)
            else:
                cat._frames = FramesModel(
                    lf=pl.LazyFrame(cat.catalog_dict),
                    pl_df=pl.DataFrame(cat.catalog_dict),
                    df=pl.DataFrame(cat.catalog_dict).to_pandas(),
                )

            cat._cast_agg_columns_with_iterables()
            return cat

    def _df_from_file(
        self,
        cat: ESMCatalogModel,
        _mapper: fsspec.FSMap,
        storage_options: dict[str, typing.Any],
        read_csv_kwargs: dict[str, typing.Any],
    ) -> FramesModel:
        """
        Read the catalog file from disk, falling back to pandas for bz2 files which
        polars can't read.

        Returns a FramesModel, which contains at least one of:
        - a polars LazyFrame
        - a polars DataFrame
        - a pandas DataFrame

        , as well as handling dataframe related methods, eg. columns_with_iterables.

        Parameters
        ----------
        cat: ESMCatalogModel
            The catalog model
        _mapper: fsspec mapper
            A fsspec mapper object
        storage_options: dict
            fsspec parameters passed to the backend file-system such as Google Cloud Storage,
            Amazon Web Service S3.
        read_csv_kwargs: dict
            Additional keyword arguments passed through to the :py:func:`~pandas.read_csv` function.

        Returns
        -------
        FramesModel:
            A pydantic model containing at least one of a pandas/polars dataframe
            and a polars lazyframe
        """
        if _mapper.fs.exists(cat.catalog_file):
            csv_path = cat.catalog_file
        else:
            csv_path = f'{os.path.dirname(_mapper.root)}/{cat.catalog_file}'
        cat.catalog_file = csv_path
        if not cat.catalog_file.endswith('.csv.bz2'):  # type: ignore[union-attr]
            converters = read_csv_kwargs.pop('converters', {})  # Hack
            # See https://github.com/pola-rs/polars/issues/13040 - can't use read_csv.
            lf = pl.scan_csv(
                cat.catalog_file,  # type: ignore[arg-type]
                storage_options=storage_options,
                infer_schema=False,
                **read_csv_kwargs,
            ).with_columns(
                [
                    pl.col(colname)
                    .str.replace('^.', '[')  # Replace first/last chars with [ or ].
                    .str.replace('.$', ']')  # set/tuple => list
                    .str.replace_all("'", '"')
                    .str.json_decode()  # This is to do with the way polars reads json - single versus double quotes
                    for colname in converters.keys()
                ]
            )
            return FramesModel(lf=lf)
        else:
            df = pd.read_csv(
                cat.catalog_file,
                storage_options=storage_options,
                **read_csv_kwargs,
            )
            return FramesModel(df=df)

    @property
    def lf(self) -> pl.LazyFrame:
        """Return a `pl.LazyFrame` containing the catalog, creating it if necessary"""
        return self._frames.lazy  # type: ignore[union-attr]

    @property
    def pl_df(self) -> pl.DataFrame:
        """Return a `pl.DataFrame` containing the catalog, creating it if necessary"""
        return self._frames.polars  # type: ignore[union-attr]

    @property
    def df(self) -> pd.DataFrame:
        """Return the `pd.DataFrame` containing the catalog, creating it if necessary"""
        return self._frames.pandas  # type: ignore[union-attr]

    @property
    def columns_with_iterables(self) -> set[str]:
        """Return a set of columns that have iterables."""
        return self._frames.columns_with_iterables  # type: ignore[union-attr]

    @property
    def has_multiple_variable_assets(self) -> bool:
        """Return True if the catalog has multiple variable assets."""
        if self.aggregation_control:
            return self.aggregation_control.variable_column_name in self.columns_with_iterables
        return False

    def _cast_agg_columns_with_iterables(self) -> None:
        """Cast all agg_columns with iterables to tuple values so as
        to avoid hashing issues (e.g. TypeError: unhashable type: 'list')
        """
        if self.aggregation_control:
            if columns := list(
                self.columns_with_iterables.intersection(
                    set(
                        map(
                            lambda agg: agg.attribute_name,
                            self.aggregation_control.aggregations,
                        )
                    )
                )
            ):
                self.df[columns] = self.df[columns].apply(tuple)

    @property
    def grouped(self) -> pd.core.groupby.DataFrameGroupBy | pd.DataFrame:
        if self.aggregation_control:
            if self.aggregation_control.groupby_attrs:
                self.aggregation_control.groupby_attrs = list(
                    filter(
                        functools.partial(_allnan_or_nonan, self.df),
                        self.aggregation_control.groupby_attrs,
                    )
                )

            if self.aggregation_control.groupby_attrs and set(
                self.aggregation_control.groupby_attrs
            ) != set(self.df.columns):
                return self.df.groupby(self.aggregation_control.groupby_attrs)
        cols = list(
            filter(
                functools.partial(_allnan_or_nonan, self.df),
                self.df.columns,
            )
        )
        return self.df.groupby(cols)

    def _construct_group_keys(self, sep: str = '.') -> dict[str, str | tuple[str]]:
        internal_keys = self.grouped.groups.keys()
        public_keys = map(
            lambda key: key if isinstance(key, str) else sep.join(str(value) for value in key),
            internal_keys,
        )

        return dict(zip(public_keys, internal_keys))

    def _unique(self) -> dict:
        def _find_unique(series):
            values = series.dropna()
            if series.name in self.columns_with_iterables:
                values = tlz.concat(values)
            return list(tlz.unique(values))

        data = self.df[self.df.columns]
        if data.empty:
            return {col: [] for col in self.df.columns}
        else:
            return data.apply(_find_unique, result_type='reduce').to_dict()

    def unique(self) -> pd.Series:
        """Return a series of unique values for each column in the catalog."""
        return pd.Series(self._unique())

    def nunique(self) -> pd.Series:
        """Return a series of the number of unique values for each column in the catalog."""

        return self._frames.nunique()  # type: ignore[union-attr]

    def search(
        self,
        *,
        query: QueryModel | dict[str, typing.Any],
        require_all_on: str | list[str] | None = None,
    ) -> pd.DataFrame:
        """
        Search for entries in the catalog.

        Parameters
        ----------
        query: dict, optional
            A dictionary of query parameters to execute against the dataframe.
        require_all_on : list, str, optional
            A dataframe column or a list of dataframe columns across
            which all entries must satisfy the query criteria.
            If None, return entries that fulfill any of the criteria specified
            in the query, by default None.

        Returns
        -------
        catalog: ESMCatalogModel
            A new catalog with the entries satisfying the query criteria.

        """

        _query = (
            query
            if isinstance(query, QueryModel)
            else QueryModel(
                query=query, require_all_on=require_all_on, columns=self.df.columns.tolist()
            )
        )

        results = search(
            df=self.df, query=_query.query, columns_with_iterables=self.columns_with_iterables
        )
        if _query.require_all_on is not None and not results.empty:
            results = search_apply_require_all_on(
                df=results,
                query=_query.query,
                require_all_on=_query.require_all_on,
                columns_with_iterables=self.columns_with_iterables,
            )
        return results


class QueryModel(pydantic.BaseModel):
    """A Pydantic model to represent a query to be executed against a catalog."""

    query: dict[pydantic.StrictStr, typing.Any | list[typing.Any]]
    columns: list[str]
    require_all_on: str | list[typing.Any] | None = None

    # TODO: Seem to be unable to modify fields in model_validator with
    # validate_assignment=True since it leads to recursion
    model_config = ConfigDict(validate_assignment=False)

    @pydantic.model_validator(mode='after')
    def validate_query(cls, model):
        query = model.query
        columns = model.columns
        require_all_on = model.require_all_on

        if query:
            for key in query:
                if key not in columns:
                    raise ValueError(f'Column {key} not in columns {columns}')
        if isinstance(require_all_on, str):
            model.require_all_on = [require_all_on]
        if require_all_on is not None:
            for key in model.require_all_on:
                if key not in columns:
                    raise ValueError(f'Column {key} not in columns {columns}')
        _query = query.copy()
        for key, value in _query.items():
            if isinstance(value, str | int | float | bool) or value is None or value is pd.NA:
                _query[key] = [value]

        model.query = _query
        return model


class FramesModel(pydantic.BaseModel):
    """A Pydantic model to represent our collection of dataframes - pandas, polars,
    and lazyframe."""

    df: pd.DataFrame | None = None
    pl_df: pl.DataFrame | None = None
    lf: pl.LazyFrame | None = None

    model_config = ConfigDict(validate_assignment=True, arbitrary_types_allowed=True)

    @pydantic.model_validator(mode='after')
    def ensure_some(self) -> Self:
        """
        Make sure that at least one of the dataframes is not `None` when the model is
        instantiated.
        """
        if self.df is None and self.pl_df is None and self.lf is None:
            raise AssertionError('At least one of df, pl_df, or lf must be set')
        return self

    @property
    def pandas(self) -> pd.DataFrame:
        """Return the pandas DataFrame, instantiating it if necessary."""
        if self.df is not None:
            return self.df

        if self.pl_df is not None:
            self.df = self.pl_df.to_pandas(use_pyarrow_extension_array=True)
            return self.df

        self.pl_df = self.lf.collect()  # type: ignore[union-attr]
        self.df = self.pl_df.to_pandas(use_pyarrow_extension_array=True)
        return self.df

    @property
    def polars(self) -> pl.DataFrame:
        """Return the polars DataFrame, instantiating it if necessary."""
        if self.pl_df is not None:
            return self.pl_df

        if self.lf is not None:
            self.pl_df = self.lf.collect()
            return self.pl_df

        self.pl_df = pl.from_pandas(self.df)
        self.lf = self.pl_df.lazy()

        return self.pl_df

    @property
    def lazy(self) -> pl.LazyFrame:
        """Return the polars LazyFrame, instantiating it if necessary."""
        if self.lf is not None:
            return self.lf

        # Otherwise, it must be none - so lets create the lazyframe now. We use the
        # self.polars property, so we can cascade to creating it from the pandas dataframe
        # if necessary.
        self.lf = self.polars.lazy()
        return self.lf

    @property
    def columns_with_iterables(self) -> set[str]:
        """Return a set of columns that have iterables, preferentially using
        `self.lazy` > `self.polars` > `self.pandas` to minimise overhead."""
        if (trunc_df := self.lazy.head(1).collect()).is_empty():
            return set()
        if self.df is not None and self.df.empty:
            return set()

        colnames, dtypes = trunc_df.columns, trunc_df.dtypes
        return {colname for colname, dtype in zip(colnames, dtypes) if dtype == pl.List}

    def nunique(self) -> pd.Series:
        """Return a series of the number of unique values for each column in the catalog."""
        return pd.Series(
            {
                colname: self.polars.get_column(colname).explode().n_unique()
                if self.polars.schema[colname] == pl.List
                else self.polars.get_column(colname).n_unique()
                for colname in self.polars.columns
            }
        )
