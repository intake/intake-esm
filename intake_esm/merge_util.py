import fsspec
import xarray as xr


def open_dataset(path, user_kwargs={'chunks': {'time': 36}}):
    return xr.open_dataset(path, **user_kwargs)


def join_new(dsets, dim_name, coord_value, options={}):
    concat_dim = xr.DataArray(coord_value, dims=(dim_name), name=dim_name)
    return xr.concat(dsets, dim=concat_dim, **options)


def join_existing(dsets, options={}):
    return xr.concat(dsets, dim='time')


def union(dsets, options={}):
    return xr.merge(dsets, **options)


def to_nested_dict(df):
    """Converts a multiindex series to nested dict"""
    if hasattr(df.index, 'levels') and len(df.index.levels) > 1:
        ret = {}
        for k, v in df.groupby(level=0):
            ret[k] = to_nested_dict(v.droplevel(0))
        return ret
    else:
        return df.to_dict()


def aggregate(aggregations, v):
    def apply_aggregation(v, agg_column=None, key=None, level=0):
        """Recursively descend into nested dictionary and aggregate items.
        level tells how deep we are."""

        assert level <= n_agg

        if level == n_agg:
            # bottom of the hierarchy - should be an actual path at this point
            return open_dataset(v)

        else:
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

            if agg_type == 'join_new':
                return join_new(dsets, dim_name=agg_column, coord_value=keys, options=agg_options)

            elif agg_type == 'join_existing':
                return join_existing(dsets, options=agg_options)

            elif agg_type == 'union':
                return union(dsets, options=agg_options)

    aggregation_dict = {}
    for agg in aggregations:
        key = agg['attribute_name']
        rest = agg.copy()
        del rest['attribute_name']
        aggregation_dict[key] = rest

    agg_columns = list(aggregation_dict.keys())

    # the number of aggregation columns determines the level of recursion
    n_agg = len(agg_columns)

    return apply_aggregation(v)


def _open_store(path, varname, zarr_kwargs):
    """ Open zarr store """
    mapper = fsspec.get_mapper(path)
    ds = xr.open_zarr(mapper, **zarr_kwargs)
    return _set_coords(ds, varname)


def _open_cdf_dataset(path, varname, cdf_kwargs):
    """ Open netcdf file """
    ds = xr.open_dataset(path, **cdf_kwargs)
    return _set_coords(ds, varname)


def _open_dataset(
    row, path_column_name, varname, data_format, expand_dims={}, zarr_kwargs={}, cdf_kwargs={}
):
    path = row[path_column_name]
    if data_format == 'zarr':
        ds = _open_store(path, varname, zarr_kwargs)
    else:
        ds = _open_cdf_dataset(path, varname, cdf_kwargs)

    if expand_dims:
        return ds.expand_dims(expand_dims)
    else:
        return ds


def _restore_non_dim_coords(ds):
    """restore non_dim_coords to variables"""
    non_dim_coords_reset = set(ds.coords) - set(ds.dims)
    ds = ds.reset_coords(non_dim_coords_reset)
    return ds


def _set_coords(ds, varname):
    """Set all variables except varname to be coords."""
    if isinstance(varname, str):
        varname = [varname]
    coord_vars = set(ds.data_vars) - set(varname)
    return ds.set_coords(coord_vars)


def dict_union(*dicts, merge_keys=['history', 'tracking_id'], drop_keys=[]):
    """Return the union of two or more dictionaries."""
    from functools import reduce

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
