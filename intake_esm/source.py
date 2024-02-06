import typing

import dask
import fsspec
import pandas as pd
import pydantic
import xarray as xr
from intake.source.base import DataSource, Schema

from .cat import Aggregation, DataFormat
from .utils import OPTIONS


class ESMDataSourceError(Exception):
    pass


def _get_xarray_open_kwargs(data_format, xarray_open_kwargs=None, storage_options=None):
    xarray_open_kwargs = (xarray_open_kwargs or {}).copy()
    _default_open_kwargs = {
        'engine': 'zarr' if data_format in {'zarr', 'reference'} else 'netcdf4',
        'chunks': {},
        'backend_kwargs': {},
    }
    xarray_open_kwargs = (
        {**_default_open_kwargs, **xarray_open_kwargs}
        if xarray_open_kwargs
        else _default_open_kwargs
    )

    if (
        xarray_open_kwargs['engine'] == 'zarr'
        and 'storage_options' not in xarray_open_kwargs['backend_kwargs']
    ):
        xarray_open_kwargs['backend_kwargs']['storage_options'] = {} or storage_options

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
    data_format=None,
    storage_options=None,
):
    storage_options = storage_options or xarray_open_kwargs.get('backend_kwargs', {}).get(
        'storage_options', {}
    )

    # Support kerchunk datasets, setting the file object (fo) and urlpath
    if data_format == 'reference':
        xarray_open_kwargs['backend_kwargs']['storage_options']['fo'] = urlpath
        xarray_open_kwargs['backend_kwargs']['consolidated'] = False
        urlpath = 'reference://'

    if xarray_open_kwargs['engine'] in 'zarr' or data_format == 'opendap':
        url = urlpath
    elif fsspec.utils.can_be_local(urlpath):
        url = fsspec.open_local(urlpath, **storage_options)
    else:
        url = fsspec.open(urlpath, **storage_options).open()

    # Handle multi-file datasets with `xr.open_mfdataset()`
    if (isinstance(url, str) and '*' in url) or isinstance(url, list):
        # How should we handle concat_dim, and other xr.open_mfdataset kwargs?
        xarray_open_kwargs.update(preprocess=preprocess)
        xarray_open_kwargs.update(parallel=True)
        ds = xr.open_mfdataset(url, **xarray_open_kwargs)
    else:
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
        scalar_variables = [v for v in ds.data_vars if len(ds[v].dims) == 0]
        ds = ds.set_coords(scalar_variables)
        ds = ds[variables]
        ds.attrs[OPTIONS['vars_key']] = variables
    elif varname:
        ds.attrs[OPTIONS['vars_key']] = varname

    ds = _expand_dims(expand_dims, ds)
    ds = _update_attrs(additional_attrs=additional_attrs, ds=ds)
    return ds


def _update_attrs(*, additional_attrs, ds):
    additional_attrs = additional_attrs or {}
    if additional_attrs:
        additional_attrs = {
            f"{OPTIONS['attrs_prefix']}:{key}": f'{value}'
            if isinstance(value, str) or not hasattr(value, '__iter__')
            else ','.join(value)
            for key, value in additional_attrs.items()
        }
    ds.attrs = {**ds.attrs, **additional_attrs}
    return ds


def _expand_dims(expand_dims, ds):
    if expand_dims:
        for variable in ds.attrs[OPTIONS['vars_key']]:
            ds[variable] = ds[variable].expand_dims(**expand_dims)

    return ds


class ESMDataSource(DataSource):
    version = '1.0'
    container = 'xarray'
    name = 'esm_datasource'
    partition_access = True

    @pydantic.validate_call
    def __init__(
        self,
        key: pydantic.StrictStr,
        records: list[dict[str, typing.Any]],
        path_column_name: pydantic.StrictStr,
        data_format: DataFormat | None,
        format_column_name: pydantic.StrictStr | None,
        *,
        variable_column_name: pydantic.StrictStr | None = None,
        aggregations: list[Aggregation] | None = None,
        requested_variables: list[str] | None = None,
        preprocess: typing.Callable | None = None,
        storage_options: dict[str, typing.Any] | None = None,
        xarray_open_kwargs: dict[str, typing.Any] | None = None,
        xarray_combine_by_coords_kwargs: dict[str, typing.Any] | None = None,
        intake_kwargs: dict[str, typing.Any] | None = None,
    ):
        """An intake compatible Data Source for ESM data.

        Parameters
        ----------
        key: str
            The key of the data source.
        records: list of dict
            A list of records, each of which is a dictionary
            mapping column names to values.
        path_column_name: str
            The column name of the path.
        data_format: DataFormat
            The data format of the data.
        variable_column_name: str, optional
            The column name of the variable name.
        aggregations: list of Aggregation, optional
            A list of aggregations to apply to the data.
        requested_variables: list of str, optional
            A list of variables to load.
        preprocess: callable, optional
            A preprocessing function to apply to the data.
        storage_options: dict, optional
            fsspec parameters passed to the backend file-system such as Google Cloud Storage,
            Amazon Web Service S3.
        xarray_open_kwargs: dict, optional
            Keyword arguments to pass to :py:func:`~xarray.open_dataset` function.
        xarray_combine_by_coords_kwargs: dict, optional
            Keyword arguments to pass to :py:func:`~xarray.combine_by_coords` function.
        intake_kwargs: dict, optional
            Additional keyword arguments are passed through to the :py:class:`~intake.source.base.DataSource` base class.
        """

        intake_kwargs = intake_kwargs or {}
        super().__init__(**intake_kwargs)
        self.key = key
        self.storage_options = storage_options or {}
        self.preprocess = preprocess
        self.requested_variables = requested_variables or []
        self.path_column_name = path_column_name
        self.variable_column_name = variable_column_name
        self.aggregations = aggregations
        self.df = pd.DataFrame.from_records(records)
        self.xarray_open_kwargs = xarray_open_kwargs
        self.xarray_combine_by_coords_kwargs = dict(combine_attrs='drop_conflicts')
        if xarray_combine_by_coords_kwargs is None:
            xarray_combine_by_coords_kwargs = {}
        self.xarray_combine_by_coords_kwargs = {
            **self.xarray_combine_by_coords_kwargs,
            **xarray_combine_by_coords_kwargs,
        }
        self._ds = None

        if data_format is not None:
            self.df['_data_format_'] = data_format.value
        else:
            self.df = self.df.rename(columns={format_column_name: '_data_format_'})

    def __repr__(self) -> str:
        return f'<{type(self).__name__}  (name: {self.key}, asset(s): {len(self.df)})>'

    def _get_schema(self) -> Schema:
        if self._ds is None:
            self._open_dataset()
            metadata: dict[str, typing.Any] = {'dims': {}, 'data_vars': {}, 'coords': ()}
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

        try:
            datasets = [
                _open_dataset(
                    record[self.path_column_name],
                    record[self.variable_column_name] if self.variable_column_name else None,
                    xarray_open_kwargs=_get_xarray_open_kwargs(
                        record['_data_format_'], self.xarray_open_kwargs, self.storage_options
                    ),
                    preprocess=self.preprocess,
                    expand_dims={
                        agg.attribute_name: [record[agg.attribute_name]]
                        for agg in self.aggregations
                        if agg.type.value == 'join_new'
                    },
                    requested_variables=self.requested_variables,
                    data_format=record['_data_format_'],
                    additional_attrs=record[~record.isnull()].to_dict(),
                    storage_options=self.storage_options,
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
                        f"{OPTIONS['attrs_prefix']}/{agg.attribute_name}"
                        for agg in self.aggregations
                    ),
                )
                datasets = [
                    ds.set_coords(set(ds.variables) - set(ds.attrs[OPTIONS['vars_key']]))
                    for ds in datasets
                ]
                self._ds = xr.combine_by_coords(datasets, **self.xarray_combine_by_coords_kwargs)

            self._ds.attrs[OPTIONS['dataset_key']] = self.key

        except Exception as exc:
            raise ESMDataSourceError(
                f"""Failed to load dataset with key='{self.key}'
                 You can use `cat['{self.key}'].df` to inspect the assets/files for this key.
                 """
            ) from exc

    def to_dask(self):
        """Return xarray object (which will have chunks)"""
        self._load_metadata()
        return self._ds

    def close(self):
        """Delete open files from memory"""
        self._ds = None
        self._schema = None
