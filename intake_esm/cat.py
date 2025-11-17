from __future__ import annotations

import datetime
import enum
import functools
import json
import os
import typing
import warnings
from pathlib import Path

import fsspec
import packaging.version
import pandas as pd
import polars as pl
import pydantic
import tlz
from pydantic import ConfigDict

from ._search import search, search_apply_require_all_on
from .iodrivers import (
    CatalogFileReader,
    CatalogFileWriter,
    FramesModel,
    PandasCsvReader,
    PandasCsvWriter,
    PandasParquetWriter,
    PolarsCsvReader,
    PolarsParquetReader,
)


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
    zarr2 = 'zarr2'
    zarr3 = 'zarr3'
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
    def _validate_data_format(self) -> typing.Self:
        data_format, format_column_name = self.format, self.format_column_name
        if data_format is not None and format_column_name is not None:
            raise ValueError('Cannot set both format and format_column_name')
        elif data_format is None and format_column_name is None:
            raise ValueError('Must set one of format or format_column_name')
        return self


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
    _driver: CatalogFileReader | None = pydantic.PrivateAttr(default=None)
    _iterable_dtype_map: dict[str, str] = pydantic.PrivateAttr(default_factory=dict)

    model_config = ConfigDict(arbitrary_types_allowed=True, validate_assignment=True)

    @pydantic.model_validator(mode='after')
    def validate_catalog(self) -> typing.Self:
        catalog_dict, catalog_file = self.catalog_dict, self.catalog_file
        if catalog_dict is not None and catalog_file is not None:
            raise ValueError('catalog_dict and catalog_file cannot be set at the same time')

        return self

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
        file_format: str = 'csv',
        write_kwargs: dict | None = None,
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
        file_format: str
            The file format to use when saving the catalog table. Either 'csv' or 'parquet'.
            If catalog_type is 'dict', this parameter is ignored.
        to_csv_kwargs : dict, optional
            Deprecated alias for ``write_kwargs``.
        write_kwargs : dict, optional
            Additional keyword arguments passed through to pandas CSV or Parquet writing.
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

        if to_csv_kwargs is not None:
            warnings.warn(
                'to_csv_kwargs is deprecated and will be removed in a future version. '
                'Please use write_kwargs instead.',
                DeprecationWarning,
                stacklevel=2,
            )
            if write_kwargs is not None:
                raise ValueError(
                    'Cannot provide both `to_csv_kwargs` and `write_kwargs`. '
                    'Please use `write_kwargs`.'
                )
            write_kwargs = to_csv_kwargs

        write_kwargs = write_kwargs or {}

        if catalog_type not in {'file', 'dict'}:
            raise ValueError(
                f'catalog_type must be either "dict" or "file". Received catalog_type={catalog_type}'
            )

        if file_format not in {'csv', 'parquet'}:
            raise ValueError(
                f'file_format must be either "csv" or "parquet". Received file_format={file_format}'
            )

        file_writer_kwargs = {
            'data': self.model_dump().copy(),
            'df': self.df,
            'dtype_map': self._iterable_dtype_map,
            'name': name,
            'directory': directory,
            'catalog_type': catalog_type,
            'file_format': file_format,
            'write_kwargs': write_kwargs,
            'json_dump_kwargs': json_dump_kwargs,
            'storage_options': storage_options,
        }

        if catalog_type == 'dict':
            writer = CatalogFileWriter
        elif file_format == 'csv':
            writer = PandasCsvWriter
        elif file_format == 'parquet':
            writer = PandasParquetWriter
        else:
            # This branch shouldn't be reached because of the earlier validation.
            # Left in for safety/completeness.
            raise NotImplementedError(
                f'Writer for catalog_type={catalog_type} and file_format={file_format} not implemented'
            )  # pragma: no cover

        return writer.write(**file_writer_kwargs)

    @classmethod
    def load(
        cls,
        json_file: str | pydantic.FilePath | pydantic.AnyUrl,
        storage_options: dict[str, typing.Any] | None = None,
        read_kwargs: dict[str, typing.Any] | None = None,
        df_reader=typing.Literal['polars', 'pandas', 'infer'],
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
        read_kwargs : dict, optional
            Additional keyword arguments passed through to the :py:func:`~pandas.read_csv` function, if the
            datastore is saved in csv format, or :py:func:`~pandas.read_parquet` if the datastore is saved in
            parquet format.

        """
        storage_options = storage_options if storage_options is not None else {}
        read_kwargs = read_kwargs or {}
        json_file = str(json_file)  # We accept Path, but fsspec doesn't.
        _mapper = fsspec.get_mapper(json_file, **storage_options)

        with fsspec.open(json_file, **storage_options) as fobj:
            data = json.loads(fobj.read())
            if 'last_updated' not in data:
                data['last_updated'] = None
            cat = cls.model_validate(data)
            if cat.catalog_file:
                cat._frames = cat._df_from_file(
                    cat, _mapper, storage_options, read_kwargs, df_reader
                )
            else:
                cat._frames = FramesModel(
                    lf=pl.LazyFrame(cat.catalog_dict),
                    pl_df=pl.DataFrame(cat.catalog_dict),
                    df=pl.DataFrame(cat.catalog_dict).to_pandas(),
                )

            return cat

    def _df_from_file(
        self,
        cat: ESMCatalogModel,
        _mapper: fsspec.FSMap,
        storage_options: dict[str, typing.Any],
        read_kwargs: dict[str, typing.Any],
        df_reader: typing.Literal['polars', 'pandas', 'infer'],
    ) -> FramesModel:
        """
        Read the catalog file from disk. If a reader is specified, that will be
        used. Else, the reader will be inferred from the file extension.

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
        read_kwargs: dict
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

        if cat.catalog_file is None:
            raise AssertionError(
                'catalog_file cannot be None here. Mostly for mypy..'
            )  # pragma: no cover

        if _is_parquet(cat.catalog_file):  # Parquet files only have polars support for now
            if df_reader == 'pandas':
                warnings.warn(
                    'Pandas parquet reader is not implemented yet. Falling back to polars parquet reader.',
                    UserWarning,
                    stacklevel=2,
                )
            self._driver = PolarsParquetReader(cat.catalog_file, storage_options, **read_kwargs)
        elif _pd_only_compression(cat.catalog_file):
            if df_reader == 'polars':
                warnings.warn(
                    f'Polars does not support reading {Path(cat.catalog_file).suffix} CSV files. Falling back to pandas CSV reader.',
                    UserWarning,
                    stacklevel=2,
                )
            self._driver = PandasCsvReader(cat.catalog_file, storage_options, **read_kwargs)
        elif df_reader == 'pandas':
            self._driver = PandasCsvReader(cat.catalog_file, storage_options, **read_kwargs)
        elif df_reader == 'polars':
            self._driver = PolarsCsvReader(cat.catalog_file, storage_options, **read_kwargs)
        else:
            self._driver = PandasCsvReader(cat.catalog_file, storage_options, **read_kwargs)

        self._iterable_dtype_map = self._driver.dtype_map
        return self._driver.frames

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

    @property
    def grouped(self) -> pd.core.groupby.DataFrameGroupBy | pd.DataFrame:
        cols = None
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
                cols = self.aggregation_control.groupby_attrs
        if cols is None:
            cols = list(
                filter(
                    functools.partial(_allnan_or_nonan, self.df),
                    self.df.columns,
                )
            )
        if len(cols) == 1:
            cols = cols[0]
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
    def validate_query(self) -> Self:
        query = self.query
        columns = self.columns
        require_all_on = self.require_all_on

        if query:
            for key in query:
                if key not in columns:
                    raise ValueError(f'Column {key} not in columns {columns}')
        if isinstance(require_all_on, str):
            self.require_all_on = [require_all_on]
        if require_all_on is not None:
            for key in self.require_all_on:
                if key not in columns:
                    raise ValueError(f'Column {key} not in columns {columns}')
        _query = query.copy()
        for key, value in _query.items():
            if isinstance(value, str | int | float | bool) or value is None or value is pd.NA:
                _query[key] = [value]

        self.query = _query
        return self


def _is_parquet(file_path: str) -> bool:
    return file_path.lower().endswith('.parquet')


def _pd_only_compression(file_path: str) -> bool:
    pd_only_compressions = ['.zip', '.xz', '.bz2', '.zst']
    return any(file_path.lower().endswith(ext) for ext in pd_only_compressions)
