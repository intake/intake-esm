import typing

import dask
import fsspec
import pandas as pd
import pydantic
import xarray as xr
from intake.source.base import DataSource, Schema

from ._types import ESMGroupedDataSourceModel, ESMSingleDataSourceModel
from .merge_util import _aggregate


def _get_xarray_open_kwargs(data_format, xarray_open_kwargs=None):
    xarray_open_kwargs = (xarray_open_kwargs or {}).copy()
    _default_open_kwargs = {
        'engine': 'zarr' if data_format == 'zarr' else 'netcdf4',
        'chunks': {},
        'backend_kwargs': {},
    }
    if not xarray_open_kwargs:
        xarray_open_kwargs = _default_open_kwargs
    else:
        xarray_open_kwargs = {**_default_open_kwargs, **xarray_open_kwargs}
    if (
        xarray_open_kwargs['engine'] == 'zarr'
        and 'storage_options' not in xarray_open_kwargs['backend_kwargs']
    ):
        xarray_open_kwargs['backend_kwargs']['storage_options'] = {}
    return xarray_open_kwargs


def _open_dataset(
    record, esmcat, *, xarray_open_kwargs=None, preprocess=None, requested_variables=None
):
    urlpath = record[esmcat.assets.column_name]
    varname = record[esmcat.aggregation_control.variable_column_name]

    _can_be_local = fsspec.utils.can_be_local(urlpath)
    storage_options = xarray_open_kwargs['backend_kwargs'].get('storage_options', {})
    if xarray_open_kwargs['engine'] == 'zarr':
        url = urlpath
    elif _can_be_local:
        url = fsspec.open_local(urlpath, **storage_options)
    else:
        url = fsspec.open(urlpath, **storage_options).open()

    ds = xr.open_dataset(url, **xarray_open_kwargs)

    if preprocess is not None:
        ds = preprocess(ds)
    if varname and isinstance(varname, str):
        varname = [varname]
    if requested_variables:
        if isinstance(requested_variables, str):
            requested_variables = [requested_variables]
        variable_intersection = set(requested_variables).intersection(set(varname))
        variables = [variable for variable in variable_intersection if variable in ds.data_vars]
        ds = ds[variables]
        ds.attrs['intake_esm_varname'] = variables
    else:
        ds.attrs['intake_esm_varname'] = varname
    return ds


class ESMSingleDataSource(DataSource):
    name = 'esm_single_data_source'
    version = '1.0'
    container = 'xarray'
    partition_access = True

    def __init__(
        self,
        model: ESMSingleDataSourceModel,
        xarray_open_kwargs: typing.Dict[str, typing.Any] = None,
        requested_variables: typing.List[pydantic.StrictStr] = None,
        preprocess: typing.Callable = None,
    ) -> 'ESMSingleDataSource':
        super().__init__(**model.kwargs)
        self.model = model
        self.df = pd.DataFrame.from_records([self.model.record])
        self.requested_variables = requested_variables or []
        self._ds = None
        self.xarray_open_kwargs = _get_xarray_open_kwargs(
            self.model.esmcat.assets.format.value, xarray_open_kwargs
        )

        self.preprocess = preprocess

    def __repr__(self) -> str:
        return f'<{type(self).__name__}  (name: {self.model.key}, asset: {len(self.df)}>)'

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
        self._ds = _open_dataset(
            self.model.record,
            self.model.esmcat,
            xarray_open_kwargs=self.xarray_open_kwargs,
            preprocess=self.preprocess,
            requested_variables=self.requested_variables,
        )
        return self._ds

    def to_dask(self):
        """Return xarray object (which will have chunks)"""
        self._load_metadata()
        return self._ds

    def close(self):
        """Delete open files from memory"""
        self._ds = None
        self._schema = None


class ESMGroupedDataSource(DataSource):
    name = 'esm_grouped_data_source'
    version = '1.0'
    container = 'xarray'
    partition_access = True

    def __init__(
        self,
        model: ESMGroupedDataSourceModel,
        xarray_open_kwargs: typing.Dict[str, typing.Any] = None,
        requested_variables: typing.List[pydantic.StrictStr] = None,
        preprocess: typing.Callable = None,
    ) -> 'ESMGroupedDataSource':
        super().__init__(**model.kwargs)
        self.model = model
        self.df = pd.DataFrame.from_records(self.model.records)
        self.requested_variables = requested_variables or []
        self._ds = None
        self.xarray_open_kwargs = _get_xarray_open_kwargs(
            self.model.esmcat.assets.format.value, xarray_open_kwargs
        )
        self.preprocess = preprocess
        self.aggregation_columns, self.aggregation_dict = self._construct_aggregations_info()

        def _to_nested_dict(df):
            """Converts a multiindex series to nested dict"""
            if hasattr(df.index, 'levels') and len(df.index.levels) > 1:
                return {k: _to_nested_dict(v.droplevel(0)) for k, v in df.groupby(level=0)}
            return df.to_dict()

        mi = self.df.set_index(self.aggregation_columns)
        self.nd = _to_nested_dict(mi[self.model.esmcat.assets.column_name])

    def __repr__(self) -> str:
        return f'<{type(self).__name__}  (name: {self.model.key}, asset(s): {len(self.df)})>'

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

    def _construct_aggregations_info(self):
        aggregation_dict = {}
        for aggregation in self.model.esmcat.aggregation_control.aggregations:
            rest = aggregation.dict().copy()
            del rest['attribute_name']
            rest['type'] = aggregation.type.value
            aggregation_dict[aggregation.attribute_name] = rest

        aggregation_columns = list(aggregation_dict.keys())
        drop_columns = []
        for column in aggregation_columns:
            if self.df[column].isnull().all():
                drop_columns.append(column)
                del aggregation_dict[column]
            elif self.df[column].isnull().any():
                raise ValueError(
                    f'The data in the {column} column should either be all NaN or there should be no NaNs'
                )
        aggregation_columns = list(
            filter(lambda item: item not in drop_columns, aggregation_columns)
        )
        return aggregation_columns, aggregation_dict

    def _open_dataset(self):
        _open_dataset_delayed = dask.delayed(_open_dataset)
        datasets = [
            (
                record[self.model.esmcat.assets.column_name],
                _open_dataset_delayed(
                    record,
                    self.model.esmcat,
                    xarray_open_kwargs=self.xarray_open_kwargs,
                    preprocess=self.preprocess,
                    requested_variables=self.requested_variables,
                ),
            )
            for record in self.model.records
        ]

        datasets = dask.compute(*datasets)
        mapper_dict = dict(datasets)
        ds = _aggregate(
            self.aggregation_dict,
            self.aggregation_columns,
            len(self.aggregation_columns),
            self.nd,
            mapper_dict,
            'test',
        )
        self._ds = ds

    def to_dask(self):
        """Return xarray object (which will have chunks)"""
        self._load_metadata()
        return self._ds

    def close(self):
        """Delete open files from memory"""
        self._ds = None
        self._schema = None
