import typing

import dask
import fsspec
import pandas as pd
import pydantic
import xarray as xr
from intake.source.base import DataSource, Schema

from .cat import Aggregation, DataFormat
from .utils import INTAKE_ESM_ATTRS_PREFIX, INTAKE_ESM_DATASET_KEY, INTAKE_ESM_VARS_KEY


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


@dask.delayed
def _open_dataset(
    urlpath,
    varname,
    *,
    xarray_open_kwargs=None,
    preprocess=None,
    requested_variables=None,
    additional_attrs=None,
    expand_dims=None,
):

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
        ds.attrs[INTAKE_ESM_VARS_KEY] = variables
    else:
        ds.attrs[INTAKE_ESM_VARS_KEY] = varname

    ds = _expand_dims(expand_dims, ds)
    ds = _update_attrs(additional_attrs, ds)
    return ds


def _update_attrs(additional_attrs, ds):
    additional_attrs = additional_attrs or {}
    if additional_attrs:
        additional_attrs = {
            f'{INTAKE_ESM_ATTRS_PREFIX}/{key}': value for key, value in additional_attrs.items()
        }
    ds.attrs = {**ds.attrs, **additional_attrs}
    return ds


def _expand_dims(expand_dims, ds):
    if expand_dims:
        for variable in ds.attrs[INTAKE_ESM_VARS_KEY]:
            ds[variable] = ds[variable].expand_dims(**expand_dims)

    return ds


class ESMDataSource(DataSource):
    version = '1.0'
    container = 'xarray'
    name = 'esm_datasource'
    partition_access = True

    @pydantic.validate_arguments
    def __init__(
        self,
        key: pydantic.StrictStr,
        records: typing.List[typing.Dict[str, typing.Any]],
        variable_column_name: pydantic.StrictStr,
        path_column_name: pydantic.StrictStr,
        data_format: DataFormat,
        *,
        aggregations: typing.Optional[typing.List[Aggregation]] = None,
        requested_variables: typing.List[str] = None,
        preprocess: typing.Callable = None,
        storage_options: typing.Dict[str, typing.Any] = None,
        xarray_open_kwargs: typing.Dict[str, typing.Any] = None,
        xarray_combine_by_coords_kwargs: typing.Dict[str, typing.Any] = None,
        intake_kwargs: typing.Dict[str, typing.Any] = None,
    ):

        intake_kwargs = intake_kwargs or {}
        super().__init__(**intake_kwargs)
        self.key = key
        self.storage_options = storage_options or {}
        self.preprocess = preprocess
        self.requested_variables = requested_variables or []
        self.data_format = data_format.value
        self.path_column_name = path_column_name
        self.variable_column_name = variable_column_name
        self.aggregations = aggregations
        self.df = pd.DataFrame.from_records(records)
        self.xarray_open_kwargs = _get_xarray_open_kwargs(self.data_format, xarray_open_kwargs)
        self.xarray_combine_by_coords_kwargs = dict(combine_attrs='drop_conflicts')
        if xarray_combine_by_coords_kwargs is None:
            xarray_combine_by_coords_kwargs = {}
        self.xarray_combine_by_coords_kwargs = {
            **self.xarray_combine_by_coords_kwargs,
            **xarray_combine_by_coords_kwargs,
        }
        self._ds = None

    def __repr__(self) -> str:
        return f'<{type(self).__name__}  (name: {self.key}, asset(s): {len(self.df)})>'

    def _get_schema(self) -> Schema:

        if self._ds is None:
            self._open_dataset()
            metadata = {'dims': {}, 'data_vars': {}, 'coords': ()}
            self._schema = Schema(
                datashape=None,
                dtype=None,
                shape=None,
                npartitions=None,
                extra_metadata=metadata,
            )
        return self._schema

    def _open_dataset(self):
        """Open dataset with xarray"""

        datasets = [
            _open_dataset(
                record[self.path_column_name],
                record[self.variable_column_name],
                xarray_open_kwargs=self.xarray_open_kwargs,
                preprocess=self.preprocess,
                expand_dims={
                    agg.attribute_name: [record[agg.attribute_name]]
                    for agg in self.aggregations
                    if agg.type.value == 'join_new'
                },
                requested_variables=self.requested_variables,
                additional_attrs=record.to_dict(),
            )
            for _, record in self.df.iterrows()
        ]

        datasets = dask.compute(*datasets)
        if len(datasets) == 1:
            self._ds = datasets[0]
        else:
            datasets = sorted(
                datasets,
                key=lambda ds: tuple(
                    f'{INTAKE_ESM_ATTRS_PREFIX}/{agg.attribute_name}' for agg in self.aggregations
                ),
            )
            with dask.config.set(
                {'scheduler': 'single-threaded', 'array.slicing.split_large_chunks': True}
            ):  # Use single-threaded scheduler
                datasets = [
                    ds.set_coords(set(ds.variables) - set(ds.attrs[INTAKE_ESM_VARS_KEY]))
                    for ds in datasets
                ]
                self._ds = xr.combine_by_coords(datasets, **self.xarray_combine_by_coords_kwargs)

        self._ds.attrs[INTAKE_ESM_DATASET_KEY] = self.key

    def to_dask(self):
        """Return xarray object (which will have chunks)"""
        self._load_metadata()
        return self._ds

    def close(self):
        """Delete open files from memory"""
        self._ds = None
        self._schema = None
