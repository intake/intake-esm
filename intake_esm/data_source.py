import typing

import fsspec
import pandas as pd
import pydantic
import xarray as xr
from intake.source.base import DataSource, Schema

from ._types import ESMGroupedDataSourceModel, ESMSingleDataSourceModel


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


def _expand_dims(ds, aggregations, record):
    dims = {
        aggregation.attribute_name: [record[aggregation.attribute_name]]
        for aggregation in aggregations
        if aggregation.type.value == 'join_new'
    }
    return ds.expand_dims(**dims)


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

    for variable in ds.attrs['intake_esm_varname']:
        ds[variable] = _expand_dims(ds[variable], esmcat.aggregation_control.aggregations, record)

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

    def _open_dataset(self):
        datasets = [
            _open_dataset(
                record,
                self.model.esmcat,
                xarray_open_kwargs=self.xarray_open_kwargs,
                preprocess=self.preprocess,
                requested_variables=self.requested_variables,
            )
            for record in self.model.records
        ]

        self._ds = xr.merge(datasets, compat='override')
        return self._ds

    def to_dask(self):
        """Return xarray object (which will have chunks)"""
        self._load_metadata()
        return self._ds

    def close(self):
        """Delete open files from memory"""
        self._ds = None
        self._schema = None
