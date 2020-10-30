import copy

import dask
import pandas as pd
from intake.source.base import DataSource, Schema

from .merge_util import _aggregate, _open_asset, _path_to_mapper, _to_nested_dict

_DATA_FORMAT_KEY = '_data_format_'


class ESMDataSource(DataSource):
    version = '1.0'
    container = 'xarray'
    name = 'esm_single_source'
    partition_access = True

    def __init__(
        self,
        key,
        row,
        path_column,
        data_format=None,
        format_column=None,
        cdf_kwargs=None,
        zarr_kwargs=None,
        storage_options=None,
        preprocess=None,
        requested_variables=None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.key = key
        self.cdf_kwargs = cdf_kwargs or {'chunks': {}}
        self.zarr_kwargs = zarr_kwargs or {}
        self.storage_options = storage_options or {}
        self.preprocess = preprocess
        self.requested_variables = requested_variables
        if not isinstance(row, pd.Series) or row.empty:
            raise ValueError('`row` must be a non-empty pandas.Series')
        self.row = row.copy()
        self.path_column = path_column
        self._ds = None
        if format_column is not None:
            self.data_format = self.row[format_column]
        elif data_format:
            self.data_format = data_format
        else:
            raise ValueError('Please specify either `data_format` or `format_column`')

    def __repr__(self):
        return f'<name: {self.key}, asset: 1'

    def _get_schema(self):

        if self._ds is None:
            self._open_dataset()

            metadata = {
                'dims': dict(self._ds.dims),
                'data_vars': {k: list(self._ds[k].coords) for k in self._ds.data_vars.keys()},
                'coords': tuple(self._ds.coords.keys()),
            }
            self._schema = Schema(
                datashape=None,
                dtype=None,
                shape=None,
                npartitions=None,
                extra_metadata=metadata,
            )
        return self._schema

    def _open_dataset(self):
        mapper = _path_to_mapper(self.row[self.path_column], self.storage_options, self.data_format)
        ds = _open_asset(
            mapper,
            data_format=self.data_format,
            zarr_kwargs=self.zarr_kwargs,
            cdf_kwargs=self.cdf_kwargs,
            preprocess=self.preprocess,
            requested_variables=self.requested_variables,
        )
        ds.attrs['intake_esm_dataset_key'] = self.key
        self._ds = ds
        return ds

    def to_dask(self):
        """Return xarray object (which will have chunks)"""
        self._load_metadata()
        return self._ds

    def close(self):
        """Delete open files from memory"""
        self._ds = None
        self._schema = None


class ESMGroupDataSource(DataSource):
    version = '1.0'
    container = 'xarray'
    name = 'esm_group'
    partition_access = True

    def __init__(
        self,
        key,
        df,
        aggregation_dict,
        path_column,
        variable_column,
        data_format=None,
        format_column=None,
        cdf_kwargs=None,
        zarr_kwargs=None,
        storage_options=None,
        preprocess=None,
        requested_variables=None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.key = key
        self.cdf_kwargs = cdf_kwargs or {'chunks': {}}
        self.zarr_kwargs = zarr_kwargs or {}
        self.storage_options = storage_options or {}
        self.preprocess = preprocess
        self.requested_variables = requested_variables
        self._ds = None
        if not isinstance(df, pd.DataFrame) or df.empty:
            raise ValueError('`df` must be a non-empty pandas.DataFrame')
        self.df = df.copy()
        self.aggregation_columns, self.aggregation_dict = _sanitize_aggregations(
            df, aggregation_dict
        )
        self.path_column = path_column
        self.variable_column = variable_column
        if format_column is not None:
            self.df[_DATA_FORMAT_KEY] = df[format_column]
        else:
            if data_format is None:
                raise ValueError('Please specify either `data_format` or `format_column`')
            self.df[_DATA_FORMAT_KEY] = [data_format] * len(df)

        self.data_format = data_format
        self.format_column = format_column

    def __repr__(self):
        """Make string representation of object."""
        contents = f'<name: {self.name}, assets: {len(self.df)}'
        return contents

    def _ipython_display_(self):
        """
        Display the entry as a rich object in an IPython session
        """
        from IPython.display import HTML, display

        columns = list(set([self.path_column, self.variable_column] + self.aggregation_columns))
        text = self.df[columns].to_html()
        contents = f"""
        <p>
            <ul>
                <li><strong>Name</strong>                 : {self.name}</li>
                <li><strong>Num of xarray.Dataset</strong>: 1</li>
                <li><strong>Num of assets</strong>        : {len(self.df)}</li>
                <li><strong>Aggregation columns</strong>  : {str(self.aggregation_columns)}</li>
            </ul>
        </p>
        {text}
        """
        display(HTML(contents))

    def _get_schema(self):

        if self._ds is None:
            self._open_dataset()

            metadata = {
                'dims': dict(self._ds.dims),
                'data_vars': {k: list(self._ds[k].coords) for k in self._ds.data_vars.keys()},
                'coords': tuple(self._ds.coords.keys()),
            }
            self._schema = Schema(
                datashape=None,
                dtype=None,
                shape=None,
                npartitions=None,
                extra_metadata=metadata,
            )
        return self._schema

    def _open_dataset(self):
        @dask.delayed
        def read_dataset(
            path,
            data_format,
            storage_options,
            cdf_kwargs,
            zarr_kwargs,
            preprocess=None,
            varname=None,
        ):
            # replace path column with mapper (dependent on filesystem type)
            mapper = _path_to_mapper(path, storage_options, data_format)
            ds = _open_asset(
                mapper,
                data_format=data_format,
                zarr_kwargs=zarr_kwargs,
                cdf_kwargs=cdf_kwargs,
                preprocess=preprocess,
                varname=varname,
                requested_variables=self.requested_variables,
            )
            return (path, ds)

        datasets = [
            read_dataset(
                row[self.path_column],
                row[_DATA_FORMAT_KEY],
                self.storage_options,
                self.cdf_kwargs,
                self.zarr_kwargs,
                self.preprocess,
                row[self.variable_column],
            )
            for _, row in self.df.iterrows()
        ]
        datasets = dask.compute(*datasets)
        mapper_dict = dict(datasets)
        nd = create_nested_dict(self.df, self.path_column, self.aggregation_columns)
        n_agg = len(self.aggregation_columns)

        ds = _aggregate(
            self.aggregation_dict,
            self.aggregation_columns,
            n_agg,
            nd,
            mapper_dict,
            self.key,
        )
        ds.attrs['intake_esm_dataset_key'] = self.key
        self._ds = ds
        return ds

    def to_dask(self):
        """Return xarray object (which will have chunks)"""
        self._load_metadata()
        return self._ds

    def close(self):
        """Delete open files from memory"""
        self._ds = None
        self._schema = None


def create_nested_dict(df, path_column, aggregation_columns):
    mi = df.set_index(aggregation_columns)
    nd = _to_nested_dict(mi[path_column])
    return nd


def _sanitize_aggregations(df, aggregation_dict):
    _aggregation_dict = copy.deepcopy(aggregation_dict)
    agg_columns = list(_aggregation_dict.keys())
    drop_cols = []
    for col in agg_columns:
        if df[col].isnull().all():
            drop_cols.append(col)
            del _aggregation_dict[col]
        elif df[col].isnull().any():
            raise ValueError(
                f'The data in the {col} column should either be all NaN or there should be no NaNs'
            )

    aggregation_columns = list(filter(lambda x: x not in drop_cols, agg_columns))

    return aggregation_columns, _aggregation_dict
