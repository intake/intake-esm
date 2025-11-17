from __future__ import annotations

import builtins
import datetime
import json
import os
import typing
from abc import ABC, abstractmethod

import fsspec
import pandas as pd
import polars as pl
import pydantic
from pydantic import ConfigDict

__filetypes__ = ['csv', 'csv.bz2', 'csv.gz', 'csv.zip', 'csv.xz', 'parquet']


class FramesModel(pydantic.BaseModel):
    """A Pydantic model to represent our collection of dataframes - pandas, polars,
    and lazyframe."""

    df: pd.DataFrame | None = None
    pl_df: pl.DataFrame | None = None
    lf: pl.LazyFrame | None = None

    model_config = ConfigDict(validate_assignment=True, arbitrary_types_allowed=True)

    @pydantic.model_validator(mode='after')
    def ensure_some(self) -> typing.Self:
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
            self.df = self.pl_df.to_pandas(use_pyarrow_extension_array=False)
            self.df[list(self.columns_with_iterables)] = self.df[
                list(self.columns_with_iterables)
            ].map(tuple)
            return self.df

        self.pl_df = self.lf.collect()  # type: ignore[union-attr]
        self.df = self.pl_df.to_pandas(use_pyarrow_extension_array=False)
        self.df[list(self.columns_with_iterables)] = self.df[list(self.columns_with_iterables)].map(
            tuple
        )
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


class CatalogFileReader(ABC):
    """Abstracts away some of the complexity related to reading dataframes"""

    def __init__(
        self,
        catalog_file: pydantic.StrictStr | None,
        storage_options: dict[str, typing.Any],
        **read_kwargs,
    ):
        self.catalog_file = catalog_file
        self.storage_options = storage_options
        self.read_kwargs = read_kwargs

        if self.catalog_file is None:
            raise AssertionError('catalog_file must be set to a valid file path or URL')

        self._dtype_map: dict[str, str] = {}
        self._frames: FramesModel | None = None

    @abstractmethod
    def read(self) -> None: ...

    @property
    def dtype_map(self) -> dict[str, str]:
        """Return a map of column names to their dtypes for columns with iterables."""
        if self._frames is None:
            self.read()
        return self._dtype_map

    @property
    def frames(self) -> FramesModel:
        """Return the FramesModel containing the dataframes read from the catalog file."""
        if self._frames is None:
            self.read()
        return self._frames  # type: ignore[return-value]


class PolarsCsvReader(CatalogFileReader):
    """A driver to read catalog files from csv using polars"""

    def __init__(
        self,
        catalog_file: pydantic.StrictStr | None,
        storage_options: dict[str, typing.Any],
        **read_kwargs,
    ):
        super().__init__(catalog_file, storage_options, **read_kwargs)

    def read(self) -> None:
        """Read a catalog file stored as a csv using polars"""
        converters = self.read_kwargs.pop(
            'converters', {}
        )  # Hack. N.B polars doesn't have a converters arg so this is safe to do.
        # See https://github.com/pola-rs/polars/issues/13040 - can't use read_csv.
        lf = pl.scan_csv(
            self.catalog_file,  # type: ignore[arg-type]
            storage_options=self.storage_options,
            infer_schema=False,
            **self.read_kwargs,
        )

        if dtype_map := (
            lf.head(1)
            .select([colname for colname in converters.keys()])
            .with_columns(
                [
                    pl.col(colname)
                    .str.head(1)
                    .str.replace_many(
                        ['[', '(', '{'],
                        ['list', 'tuple', 'set'],
                    )
                    for colname in converters.keys()
                ]
            )
            .collect()
            .to_dicts()
        ):  # Returns an empty list if no rows - hence walrus
            self._dtype_map = dtype_map[0]

        lf = lf.with_columns(
            [
                pl.col(colname)
                .str.replace('^.', '[')  # Replace first/last chars with [ or ].
                .str.replace('.$', ']')  # set/tuple => list
                .str.replace(',]$', ']')  # Remove trailing commas
                .str.replace_all(
                    "'", '"'
                )  # This is to do with the JSON spec - single versus double quotes
                .str.json_decode(dtype=pl.List(pl.Utf8))
                for colname in converters.keys()
            ]
        )
        self._frames = FramesModel(lf=lf)


class PolarsParquetReader(CatalogFileReader):
    """A driver to read catalog files from parquet using polars"""

    def __init__(
        self,
        catalog_file: pydantic.StrictStr | None,
        storage_options: dict[str, typing.Any],
        **read_kwargs,
    ):
        if read_kwargs.get('converters') is not None:
            # Pop them out - they're being used to read iterable columns, but parquet
            # supports that out of the box.
            read_kwargs.pop('converters')
        super().__init__(catalog_file, storage_options, **read_kwargs)

    def read(self) -> None:
        """Read a catalog file stored as a parquet using polars"""
        lf = pl.scan_parquet(
            self.catalog_file,  # type: ignore[arg-type]
            storage_options=self.storage_options,
            **self.read_kwargs,
        )
        self._frames = FramesModel(lf=lf)
        self._dtype_map = {}


class PandasCsvReader(CatalogFileReader):
    """A driver to read catalog files from csv using pandas"""

    def __init__(
        self,
        catalog_file: pydantic.StrictStr | None,
        storage_options: dict[str, typing.Any],
        **read_kwargs,
    ):
        super().__init__(catalog_file, storage_options, **read_kwargs)

    def read(self) -> None:
        """Read a catalog file stored as a csv using pandas, casting all iterable
        columns to tuples"""
        df = pd.read_csv(
            self.catalog_file,
            storage_options=self.storage_options,
            **self.read_kwargs,
        )
        self._dtype_map = {
            colname: df.head(1)[colname]
            .astype(str)
            .str[0]
            .map(
                {'[': 'list', '{': 'set', '(': 'tuple'},
            )
            .iloc[0]
            for colname in self.read_kwargs.get('converters', {}).keys()
        }
        df[list(self._dtype_map.keys())] = df[list(self._dtype_map.keys())].map(tuple)
        self._frames = FramesModel(df=df)


class PandasParquetReader(CatalogFileReader):
    """A driver to read catalog files from csv using pandas"""

    def __init__(
        self,
        catalog_file: pydantic.StrictStr | None,
        storage_options: dict[str, typing.Any],
        **read_kwargs,
    ):
        super().__init__(catalog_file, storage_options, **read_kwargs)

    def read(self) -> None:
        raise NotImplementedError('PandasDriver does not currently support reading parquet files')


class CatalogFileWriter:
    """Abstracts away some of the complexity related to writing dataframes.
    Should only be used with catalog_type='dict' to write catalogs with the
    data embedded in the json file.
    """

    mapper: fsspec.FSMap = None

    @classmethod
    def write(
        cls,
        data: dict,
        df: pd.DataFrame,
        dtype_map: dict[str, str],
        name: str,
        *,
        write_kwargs: dict,
        directory: str | None = None,
        catalog_type: str = 'dict',
        file_format: str = 'csv',
        json_dump_kwargs: dict | None = None,
        storage_options: dict[str, typing.Any] | None = None,
    ) -> None:
        data, fs, json_file_name = cls._common(name, directory, storage_options, data)

        _tmp_df = df.copy(deep=True)

        for colname, dtype in dtype_map.items():
            _tmp_df[colname] = _tmp_df[colname].apply(getattr(builtins, dtype))

        data['catalog_dict'] = _tmp_df.to_dict(orient='records')

        with fs.open(json_file_name, 'w') as outfile:
            json_kwargs = {'indent': 2}
            json_kwargs |= json_dump_kwargs or {}
            json.dump(data, outfile, **json_kwargs)  # type: ignore[arg-type]

        print(f'Successfully wrote ESM catalog json file to: {json_file_name}')

    @classmethod
    def _common(
        cls,
        name: str,
        directory: str | None,
        storage_options: dict[str, typing.Any] | None,
        data: dict,
    ) -> tuple[dict, typing.Any, str]:
        """
        Common functionality for writing catalog files.
        """

        # Check if the directory is None, and if it is, set it to the current directory
        if directory is None:
            directory = os.getcwd()

        # Configure the fsspec mapper and associated filenames
        storage_options = storage_options if storage_options is not None else {}
        cls.mapper = fsspec.get_mapper(f'{directory}', **storage_options)
        fs = cls.mapper.fs
        json_file_name = fs.unstrip_protocol(f'{cls.mapper.root}/{name}.json')

        for key in {'catalog_dict', 'catalog_file'}:
            data.pop(key, None)

        data['id'] = name
        data['last_updated'] = datetime.datetime.now(datetime.timezone.utc).strftime(
            '%Y-%m-%dT%H:%M:%SZ'
        )

        return data, fs, json_file_name


class PandasCsvWriter(CatalogFileWriter):
    """A driver to write catalog files to csv using pandas"""

    @classmethod
    def write(
        cls,
        data: dict,
        df: pd.DataFrame,
        dtype_map: dict[str, str],
        name: str,
        *,
        write_kwargs: dict,
        directory: str | None = None,
        catalog_type: str = 'file',
        file_format: str = 'csv',
        json_dump_kwargs: dict | None = None,
        storage_options: dict[str, typing.Any] | None = None,
    ) -> None:
        data, fs, json_file_name = cls._common(name, directory, storage_options, data)
        csv_file_name = fs.unstrip_protocol(f'{cls.mapper.root}/{name}.csv')

        _tmp_df = df.copy(deep=True)

        for colname, dtype in dtype_map.items():
            _tmp_df[colname] = _tmp_df[colname].apply(getattr(builtins, dtype))

        csv_kwargs: dict[str, typing.Any] = {'index': False}
        csv_kwargs |= write_kwargs or {}
        compression = csv_kwargs.get('compression', '')
        extensions = {'gzip': '.gz', 'bz2': '.bz2', 'zip': '.zip', 'xz': '.xz'}

        csv_file_name = f'{csv_file_name}{extensions.get(compression, "")}'
        data['catalog_file'] = str(csv_file_name)
        with fs.open(csv_file_name, 'wb') as csv_outfile:
            _tmp_df.to_csv(csv_outfile, **csv_kwargs)

        with fs.open(json_file_name, 'w') as outfile:
            json_kwargs = {'indent': 2}
            json_kwargs |= json_dump_kwargs or {}
            json.dump(data, outfile, **json_kwargs)  # type: ignore[arg-type]

        print(f'Successfully wrote ESM catalog json file to: {json_file_name}')


class PandasParquetWriter(CatalogFileWriter):
    """A driver to write catalog files to parquet using pandas"""

    @classmethod
    def write(
        cls,
        data: dict,
        df: pd.DataFrame,
        dtype_map: dict[str, str],
        name: str,
        *,
        directory: str | None = None,
        catalog_type: str = 'file',
        file_format: str = 'parquet',
        write_kwargs: dict,
        json_dump_kwargs: dict | None = None,
        storage_options: dict[str, typing.Any] | None = None,
    ) -> None:
        data, fs, json_file_name = cls._common(name, directory, storage_options, data)
        pq_file_name = fs.unstrip_protocol(f'{cls.mapper.root}/{name}.parquet')

        write_kwargs |= {'index': False}

        data['catalog_file'] = str(pq_file_name)
        with fs.open(pq_file_name, 'wb') as pq_outfile:
            df.to_parquet(pq_outfile, **write_kwargs)

        with fs.open(json_file_name, 'w') as outfile:
            json_kwargs = {'indent': 2}
            json_kwargs |= json_dump_kwargs or {}
            json.dump(data, outfile, **json_kwargs)  # type: ignore[arg-type]

        print(f'Successfully wrote ESM catalog json file to: {json_file_name}')


class PolarsCsvWriter(CatalogFileWriter):
    """A driver to write catalog files to csv using polars"""

    @classmethod
    def write(
        cls,
        data: dict,
        df: pd.DataFrame,
        dtype_map: dict[str, str],
        name: str,
        *,
        write_kwargs: dict,
        directory: str | None = None,
        catalog_type: str = 'dict',
        file_format: str = 'csv',
        json_dump_kwargs: dict | None = None,
        storage_options: dict[str, typing.Any] | None = None,
    ) -> None:
        raise NotImplementedError('TODO')


class PolarsParquetWriter(CatalogFileWriter):
    """A driver to write catalog files to parquet using polars"""

    @classmethod
    def write(
        cls,
        data: dict,
        df: pd.DataFrame,
        dtype_map: dict[str, str],
        name: str,
        *,
        write_kwargs: dict,
        directory: str | None = None,
        catalog_type: str = 'dict',
        file_format: str = 'csv',
        json_dump_kwargs: dict | None = None,
        storage_options: dict[str, typing.Any] | None = None,
    ) -> None:
        raise NotImplementedError('TODO')
