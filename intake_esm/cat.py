import datetime
import enum
import functools
import json
import os
import typing

import fsspec
import pandas as pd
import pydantic
import tlz

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

    class Config:
        validate_all = True
        validate_assignment = True


class DataFormat(str, enum.Enum):
    netcdf = 'netcdf'
    zarr = 'zarr'
    reference = 'reference'

    class Config:
        validate_all = True
        validate_assignment = True


class Attribute(pydantic.BaseModel):
    column_name: pydantic.StrictStr
    vocabulary: pydantic.StrictStr = ''

    class Config:
        validate_all = True
        validate_assignment = True


class Assets(pydantic.BaseModel):
    column_name: pydantic.StrictStr
    format: typing.Optional[DataFormat]
    format_column_name: typing.Optional[pydantic.StrictStr]

    class Config:
        validate_all = True
        validate_assignment = True

    @pydantic.root_validator
    def _validate_data_format(cls, values):
        data_format, format_column_name = values.get('format'), values.get('format_column_name')
        if data_format is not None and format_column_name is not None:
            raise ValueError('Cannot set both format and format_column_name')
        elif data_format is None and format_column_name is None:
            raise ValueError('Must set one of format or format_column_name')
        return values


class Aggregation(pydantic.BaseModel):
    type: AggregationType
    attribute_name: pydantic.StrictStr
    options: typing.Optional[typing.Dict] = {}

    class Config:
        validate_all = True
        validate_assignment = True


class AggregationControl(pydantic.BaseModel):
    variable_column_name: pydantic.StrictStr
    groupby_attrs: typing.List[pydantic.StrictStr]
    aggregations: typing.List[Aggregation] = []

    class Config:
        validate_all = True
        validate_assignment = True


class ESMCatalogModel(pydantic.BaseModel):
    """
    Pydantic model for the ESM data catalog defined in https://git.io/JBWoW
    """

    esmcat_version: pydantic.StrictStr
    attributes: typing.List[Attribute]
    assets: Assets
    aggregation_control: AggregationControl
    id: typing.Optional[str] = ''
    catalog_dict: typing.Optional[typing.List[typing.Dict]] = None
    catalog_file: pydantic.StrictStr = None
    description: pydantic.StrictStr = None
    title: pydantic.StrictStr = None
    last_updated: typing.Optional[typing.Union[datetime.datetime, datetime.date]] = None
    _df: typing.Optional[pd.DataFrame] = pydantic.PrivateAttr()

    class Config:
        arbitrary_types_allowed = True
        underscore_attrs_are_private = True
        validate_all = True
        validate_assignment = True

    @pydantic.root_validator
    def validate_catalog(cls, values):
        catalog_dict, catalog_file = values.get('catalog_dict'), values.get('catalog_file')
        if catalog_dict is not None and catalog_file is not None:
            raise ValueError('catalog_dict and catalog_file cannot be set at the same time')

        return values

    @classmethod
    def from_dict(cls, data: typing.Dict) -> 'ESMCatalogModel':
        esmcat = data['esmcat']
        df = data['df']
        if 'last_updated' not in esmcat:
            esmcat['last_updated'] = None
        cat = cls.parse_obj(esmcat)
        cat._df = df
        return cat

    def save(
        self,
        name: str,
        *,
        directory: str = None,
        catalog_type: str = 'dict',
        to_csv_kwargs: dict = None,
        json_dump_kwargs: dict = None,
        storage_options: typing.Dict[str, typing.Any] = None,
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
        mapper = fsspec.get_mapper(f'{directory}', storage_options=storage_options)
        fs = mapper.fs
        csv_file_name = f'{mapper.fs.protocol}://{mapper.root}/{name}.csv'
        json_file_name = f'{mapper.fs.protocol}://{mapper.root}/{name}.json'

        data = self.dict().copy()
        for key in {'catalog_dict', 'catalog_file'}:
            data.pop(key, None)
        data['id'] = name
        data['last_updated'] = datetime.datetime.now().utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')

        if catalog_type == 'file':
            csv_kwargs = {'index': False}
            csv_kwargs.update(to_csv_kwargs or {})
            compression = csv_kwargs.get('compression')
            extensions = {'gzip': '.gz', 'bz2': '.bz2', 'zip': '.zip', 'xz': '.xz', None: ''}
            csv_file_name = f'{csv_file_name}{extensions[compression]}'
            data['catalog_file'] = str(csv_file_name)

            with fs.open(csv_file_name, 'wb') as csv_outfile:
                self.df.to_csv(csv_outfile, **csv_kwargs)
        else:
            data['catalog_dict'] = self.df.to_dict(orient='records')

        with fs.open(json_file_name, 'w') as outfile:
            json_kwargs = {'indent': 2}
            json_kwargs.update(json_dump_kwargs or {})
            json.dump(data, outfile, **json_kwargs)

        print(f'Successfully wrote ESM catalog json file to: {json_file_name}')

    @classmethod
    def load(
        cls,
        json_file: typing.Union[str, pydantic.FilePath, pydantic.AnyUrl],
        storage_options: typing.Dict[str, typing.Any] = None,
        read_csv_kwargs: typing.Dict[str, typing.Any] = None,
    ) -> 'ESMCatalogModel':
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
        _mapper = fsspec.get_mapper(json_file, **storage_options)

        with fsspec.open(json_file, **storage_options) as fobj:
            data = json.loads(fobj.read())
            if 'last_updated' not in data:
                data['last_updated'] = None
            cat = cls.parse_obj(data)
            if cat.catalog_file:
                if _mapper.fs.exists(cat.catalog_file):
                    csv_path = cat.catalog_file
                else:
                    csv_path = f'{os.path.dirname(_mapper.root)}/{cat.catalog_file}'
                cat.catalog_file = csv_path
                df = pd.read_csv(
                    cat.catalog_file,
                    storage_options=storage_options,
                    **read_csv_kwargs,
                )
            else:
                df = pd.DataFrame(cat.catalog_dict)

            cat._df = df
            cat._cast_agg_columns_with_iterables()
            return cat

    @property
    def columns_with_iterables(self) -> typing.Set[str]:
        """Return a set of columns that have iterables."""
        if self._df.empty:
            return set()
        has_iterables = (
            self._df.sample(20, replace=True)
            .applymap(type)
            .isin([list, tuple, set])
            .any()
            .to_dict()
        )
        return {column for column, check in has_iterables.items() if check}

    @property
    def df(self) -> pd.DataFrame:
        """Return the dataframe."""
        return self._df

    @property
    def has_multiple_variable_assets(self) -> bool:
        """Return True if the catalog has multiple variable assets."""
        return self.aggregation_control.variable_column_name in self.columns_with_iterables

    def _cast_agg_columns_with_iterables(self) -> None:
        """Cast all agg_columns with iterables to tuple values so as
        to avoid hashing issues (e.g. TypeError: unhashable type: 'list')
        """
        columns = list(
            self.columns_with_iterables.intersection(
                set(map(lambda agg: agg.attribute_name, self.aggregation_control.aggregations))
            )
        )
        if columns:
            self._df[columns] = self._df[columns].apply(tuple)

    @property
    def grouped(self) -> typing.Union[pd.core.groupby.DataFrameGroupBy, pd.DataFrame]:

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
        return self.df

    def _construct_group_keys(
        self, sep: str = '.'
    ) -> typing.Dict[str, typing.Union[str, typing.Tuple[str]]]:
        grouped = self.grouped
        if isinstance(grouped, pd.core.groupby.generic.DataFrameGroupBy):
            internal_keys = grouped.groups.keys()
            public_keys = map(
                lambda key: key if isinstance(key, str) else sep.join(str(value) for value in key),
                internal_keys,
            )

        else:
            internal_keys = grouped.index
            public_keys = (
                grouped[grouped.columns.tolist()]
                .apply(lambda row: sep.join(str(v) for v in row), axis=1)
                .tolist()
            )

        return dict(zip(public_keys, internal_keys))

    def _unique(self) -> typing.Dict:
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
        return pd.Series(tlz.valmap(len, self._unique()))

    def search(
        self,
        *,
        query: typing.Union['QueryModel', typing.Dict[str, typing.Any]],
        require_all_on: typing.Union[str, typing.List[str]] = None,
    ) -> 'ESMCatalogModel':
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

    query: typing.Dict[pydantic.StrictStr, typing.Union[typing.Any, typing.List[typing.Any]]]
    columns: typing.List[str]
    require_all_on: typing.Union[str, typing.List[typing.Any]] = None

    class Config:
        validate_all = True
        validate_assignment = True

    @pydantic.root_validator(pre=False)
    def validate_query(cls, values):
        query = values.get('query', {})
        columns = values.get('columns')
        require_all_on = values.get('require_all_on', [])

        if query:
            for key in query:
                if key not in columns:
                    raise ValueError(f'Column {key} not in columns {columns}')
        if isinstance(require_all_on, str):
            values['require_all_on'] = [require_all_on]
        if require_all_on is not None:
            for key in values['require_all_on']:
                if key not in columns:
                    raise ValueError(f'Column {key} not in columns {columns}')
        _query = query.copy()
        for key, value in _query.items():
            if isinstance(value, (str, int, float, bool)):
                _query[key] = [value]

        values['query'] = _query
        return values
