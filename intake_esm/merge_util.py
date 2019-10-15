import xarray as xr


def join_new(dsets, dim_name, coord_value, options={}):
    concat_dim = xr.DataArray(coord_value, dims=(dim_name), name=dim_name)
    return xr.concat(dsets, dim=concat_dim, **options)


def join_existing(dsets, options={}):
    return xr.concat(dsets, **options)


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


def _create_asset_info_lookup(
    df, path_column_name, variable_column_name, data_format=None, format_column_name=None
):

    if data_format:
        return dict(
            zip(df[path_column_name], tuple(zip(df[variable_column_name], [data_format] * len(df))))
        )

    elif format_column_name is not None:
        return dict(
            zip(df[path_column_name], tuple(zip(df[variable_column_name], df[format_column_name])))
        )


def aggregate(
    aggregation_dict, agg_columns, n_agg, v, lookup, mapper_dict, zarr_kwargs, cdf_kwargs
):
    def apply_aggregation(v, agg_column=None, key=None, level=0):
        """Recursively descend into nested dictionary and aggregate items.
        level tells how deep we are."""

        assert level <= n_agg

        if level == n_agg:
            # bottom of the hierarchy - should be an actual path at this point
            # return open_dataset(v)
            varname = lookup[v][0]
            data_format = lookup[v][1]
            return open_dataset(
                mapper_dict[v],
                varname=[varname],
                data_format=data_format,
                zarr_kwargs=zarr_kwargs,
                cdf_kwargs=cdf_kwargs,
            )

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

            attrs = dict_union(*[ds.attrs for ds in dsets])

            if agg_type == 'join_new':
                ds = join_new(dsets, dim_name=agg_column, coord_value=keys, options=agg_options)

            elif agg_type == 'join_existing':

                ds = join_existing(dsets, options=agg_options)

            elif agg_type == 'union':
                ds = union(dsets, options=agg_options)

            ds.attrs = attrs
            return ds

    return apply_aggregation(v)


def open_dataset(path, varname, data_format, zarr_kwargs, cdf_kwargs):

    if data_format == 'zarr':
        ds = xr.open_zarr(path, **zarr_kwargs)
        return _set_coords(ds, varname)

    else:
        ds = xr.open_dataset(path, **cdf_kwargs)
        return _set_coords(ds, varname)


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
