""" Functions for aggregating multiple xarray datasets into a single xarray dataset"""
from typing import Any, Dict, List, Union

import fsspec
import xarray as xr


class AggregationError(Exception):
    pass


def _path_to_mapper(path, storage_options):
    """Convert path to mapper if necessary."""
    if fsspec.core.split_protocol(path)[0] is not None:
        return fsspec.get_mapper(path, **storage_options)
    return path


def join_new(
    dsets: List[xr.Dataset],
    dim_name: str,
    coord_value: Any,
    varname: Union[str, List],
    options: Dict[str, Any] = None,
    group_key: str = None,
) -> xr.Dataset:
    """
    Concatenate a list of datasets along a new dimension.

    Parameters
    ----------
    dsets : List[xr.Dataset]
        A list of xarray.Dataset(s) to concatenate along a new dimension
    dim_name : str
        Name of the new dimension
    coord_value : Any
        Value corresponding to the new dimension coordinate
    varname : Union[str, List]
        Name of data variables
    options : Dict, optional
        Additional keyword arguments passed through to
        :py:func:`~xarray.concat()`, by default None

    Returns
    -------
    xr.Dataset
        xarray Dataset
    """
    options = options or {}
    if isinstance(varname, str):
        varname = [varname]
    try:
        concat_dim = xr.DataArray(coord_value, dims=(dim_name), name=dim_name)
        return xr.concat(dsets, dim=concat_dim, data_vars=varname, **options)
    except Exception as exc:
        message = f"""
        Failed to join/concatenate datasets in group with key={group_key} along a new dimension `{dim_name}`.

        *** Arguments passed to xarray.concat() ***:

        - objs: a list of {len(dsets)} datasets
        - dim: {concat_dim}
        - data_vars: {varname}
        - and kwargs: {options}

        ********************************************
        """

        raise AggregationError(message) from exc


def join_existing(
    dsets: List[xr.Dataset], options: Dict[str, Any] = None, group_key: str = None
) -> xr.Dataset:
    """
    Concatenate a list of datasets along an existing dimension.

    Parameters
    ----------
    dsets : List[xr.Dataset]
        A list of xarray.Dataset(s) to concatenate along an existing dimension.
    options : Dict, optional
        Additional keyword arguments passed through to
        :py:func:`~xarray.concat()`, by default None

    Returns
    -------
    xr.Dataset
        xarray Dataset
    """

    options = options or {}
    try:
        return xr.concat(dsets, **options)
    except Exception as exc:
        message = f"""
        Failed to join/concatenate datasets in group with key={group_key} along an existing dimension.

        *** Arguments passed to xarray.concat() ***:

        - objs: a list of {len(dsets)} datasets
        - kwargs: {options}

        ********************************************
        """
        raise AggregationError(message) from exc


def union(
    dsets: List[xr.Dataset], options: Dict[str, Any] = None, group_key: str = None
) -> xr.Dataset:
    """
    Merge a list of datasets into a single dataset.

    Parameters
    ----------
    dsets : List[xr.Dataset]
        A list of xarray.Dataset(s) to merge.
    options : Dict, optional
        Additional keyword arguments passed through to
        :py:func:`~xarray.merge()`, by default None

    Returns
    -------
    xr.Dataset
        xarray Dataset
    """

    options = options or {}
    try:
        return xr.merge(dsets, **options)
    except Exception as exc:
        message = f"""
        Failed to merge multiple datasets in group with key={group_key} into a single xarray Dataset as variables.

        *** Arguments passed to xarray.merge() ***:

        - objs: a list of {len(dsets)} datasets
        - kwargs: {options}

        ********************************************
        """
        raise AggregationError(message) from exc


def _to_nested_dict(df):
    """Converts a multiindex series to nested dict"""
    if hasattr(df.index, 'levels') and len(df.index.levels) > 1:
        ret = {}
        for k, v in df.groupby(level=0):
            ret[k] = _to_nested_dict(v.droplevel(0))
        return ret
    return df.to_dict()


def _create_asset_info_lookup(
    df, path_column_name, variable_column_name=None, data_format=None, format_column_name=None,
):

    if format_column_name is not None:
        data_format_list = df[format_column_name]
    else:
        if data_format is None:
            raise ValueError('Please specify either `data_format` or `format_column_name`')
        data_format_list = [data_format] * len(df)
    if variable_column_name is None:
        varname_list = [None] * len(df)
    else:
        varname_list = df[variable_column_name]

    return dict(zip(df[path_column_name], tuple(zip(varname_list, data_format_list))))


def _aggregate(
    aggregation_dict,
    agg_columns,
    n_agg,
    v,
    lookup,
    mapper_dict,
    zarr_kwargs,
    cdf_kwargs,
    preprocess,
    group_key=None,
):
    def apply_aggregation(v, agg_column=None, key=None, level=0):
        """Recursively descend into nested dictionary and aggregate items.
        level tells how deep we are."""

        assert level <= n_agg

        if level == n_agg:
            # bottom of the hierarchy - should be an actual path at this point
            # return open_dataset(v)
            data_format = lookup[v][1]
            # Get varname in order to specify data_vars=[varname] during concatenation
            # See https://github.com/intake/intake-esm/issues/172#issuecomment-549001751
            varname = lookup[v][0]
            ds = _open_asset(
                mapper_dict[v],
                data_format=data_format,
                zarr_kwargs=zarr_kwargs,
                cdf_kwargs=cdf_kwargs,
                preprocess=preprocess,
                varname=varname,
            )
            return ds

        agg_column = agg_columns[level]
        agg_info = aggregation_dict[agg_column]
        agg_type = agg_info['type']

        if 'options' in agg_info:
            agg_options = agg_info['options']
        else:
            agg_options = {}

        dsets = [
            apply_aggregation(value, agg_column, key=key, level=level + 1)
            for key, value in v.items()
        ]
        keys = list(v.keys())
        attrs = dict_union(*[ds.attrs for ds in dsets])
        # copy encoding for each variable from first encounter
        variables = {v for ds in dsets for v in ds.variables}
        encoding = {}
        for ds in dsets:
            for v in variables:
                if v in ds.variables and v not in encoding:
                    if ds[v].encoding:
                        encoding[v] = ds[v].encoding
                        # get rid of the misleading file-specific attributes
                        # github.com/pydata/xarray/issues/2550
                        for enc_attrs in ['source', 'original_shape']:
                            if enc_attrs in encoding[v]:
                                del encoding[v][enc_attrs]

        if agg_type == 'join_new':
            varname = dsets[0].attrs['intake_esm_varname']
            ds = join_new(
                dsets,
                dim_name=agg_column,
                coord_value=keys,
                varname=varname,
                options=agg_options,
                group_key=group_key,
            )

        elif agg_type == 'join_existing':
            ds = join_existing(dsets, options=agg_options, group_key=group_key)

        elif agg_type == 'union':
            ds = union(dsets, options=agg_options, group_key=group_key)

        ds.attrs = attrs
        for v in ds.variables:
            if v in encoding and not ds[v].encoding:
                ds[v].encoding = encoding[v]
        return ds

    return apply_aggregation(v)


def _open_asset(
    path, data_format, zarr_kwargs=None, cdf_kwargs=None, preprocess=None, varname=None
):
    protocol = None
    root = path
    if isinstance(path, fsspec.mapping.FSMap):
        protocol = path.fs.protocol
        if isinstance(protocol, list):
            protocol = tuple(protocol)

        if protocol in {'http', 'https', 'file'} or protocol is None:
            path = path.root
            root = path

        else:
            root = path.root

    if data_format == 'zarr':
        try:
            ds = xr.open_zarr(path, **zarr_kwargs)
        except Exception as exc:
            message = f"""
            Failed to open zarr store.

            *** Arguments passed to xarray.open_zarr() ***:

            - store: {path}
            - kwargs: {zarr_kwargs}

            *** fsspec options used ***:

            - root: {root}
            - protocol: {protocol}

            ********************************************
            """

            raise IOError(message) from exc

    else:
        try:
            ds = xr.open_dataset(path, **cdf_kwargs)
        except Exception as exc:
            message = f"""
            Failed to open netCDF/HDF dataset.

            *** Arguments passed to xarray.open_dataset() ***:

            - filename_or_obj: {path}
            - kwargs: {cdf_kwargs}

            *** fsspec options used ***:

            - root: {root}
            - protocol: {protocol}

            ********************************************
            """
            raise IOError(message) from exc

    if varname:
        ds.attrs['intake_esm_varname'] = varname

    if preprocess is None:
        return ds
    try:
        return preprocess(ds)
    except Exception as exc:
        raise RuntimeError(
            'Failed to apply pre-processing function: {preprocess.__name__}'
        ) from exc


def dict_union(*dicts, merge_keys=None, drop_keys=None):
    """Return the union of two or more dictionaries."""
    from functools import reduce

    merge_keys = merge_keys or ['history', 'tracking_id']
    drop_keys = drop_keys or []

    if len(dicts) > 2:
        return reduce(dict_union, dicts)
    elif len(dicts) == 2:
        d1, d2 = dicts
        d = type(d1)()
        # union
        all_keys = set(d1) | set(d2)
        for k in all_keys:
            v1 = d1.get(k)
            v2 = d2.get(k)
            if (v1 is None and v2 is None) or k in drop_keys:
                pass
            elif v1 is None:
                d[k] = v2
            elif v2 is None:
                d[k] = v1
            elif v1 == v2:
                d[k] = v1
            elif k in merge_keys:
                d[k] = '\n'.join([v1, v2])
        return d
    elif len(dicts) == 1:
        return dicts[0]
