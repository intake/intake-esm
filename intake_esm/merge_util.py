import logging

import fsspec
import xarray as xr

logger = logging.getLogger('intake-esm')


def _path_to_mapper(path, storage_options):
    """Convert path to mapper if necessary."""
    if fsspec.core.split_protocol(path)[0] is not None:
        return fsspec.get_mapper(path, **storage_options)
    else:
        return path


def join_new(dsets, dim_name, coord_value, varname, options={}):
    if isinstance(varname, str):
        varname = [varname]
    try:
        concat_dim = xr.DataArray(coord_value, dims=(dim_name), name=dim_name)
        return xr.concat(dsets, dim=concat_dim, data_vars=varname, **options)
    except Exception as e:
        logger.error(f'Failed to join datasets along new dimension.')
        raise e


def join_existing(dsets, options={}):
    try:
        return xr.concat(dsets, **options)
    except Exception as e:
        logger.error(f'Failed to join datasets along existing dimension.')
        raise e


def union(dsets, options={}):
    try:
        return xr.merge(dsets, **options)
    except Exception as e:
        logger.error(f'Failed to merge datasets.')
        raise e


def _to_nested_dict(df):
    """Converts a multiindex series to nested dict"""
    if hasattr(df.index, 'levels') and len(df.index.levels) > 1:
        ret = {}
        for k, v in df.groupby(level=0):
            ret[k] = _to_nested_dict(v.droplevel(0))
        return ret
    else:
        return df.to_dict()


def _create_asset_info_lookup(
    df, path_column_name, variable_column_name=None, data_format=None, format_column_name=None
):

    if format_column_name is not None:
        data_format_list = df[format_column_name]
    else:
        if data_format is None:
            raise ValueError(f'Please specify either `data_format` or `format_column_name`')
        else:
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
            # See https://github.com/NCAR/intake-esm/issues/172#issuecomment-549001751
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
            # copy encoding for each variable from first encounter
            variables = set([v for ds in dsets for v in ds.variables])
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
                logger.debug(
                    f'Joining {len(dsets)} dataset(s) along new {agg_column} dimension with options={agg_options}.\ndsets={dsets}'
                )
                varname = dsets[0].attrs['intake_esm_varname']
                ds = join_new(
                    dsets,
                    dim_name=agg_column,
                    coord_value=keys,
                    varname=varname,
                    options=agg_options,
                )

            elif agg_type == 'join_existing':
                logger.debug(
                    f'Joining {len(dsets)} dataset(s) along existing dimension with options={agg_options}.\ndsets={dsets}'
                )
                ds = join_existing(dsets, options=agg_options)

            elif agg_type == 'union':
                logger.debug(
                    f'Merging {len(dsets)} dataset(s) into a single Dataset with options={agg_options}.\ndsets={dsets}'
                )
                ds = union(dsets, options=agg_options)

            ds.attrs = attrs
            for v in ds.variables:
                if v in encoding and not ds[v].encoding:
                    ds[v].encoding = encoding[v]

            return ds

    return apply_aggregation(v)


def _open_asset(path, data_format, zarr_kwargs, cdf_kwargs, preprocess, varname):
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
        logger.debug(f'Opening zarr store: {root} - protocol: {protocol}')
        try:
            ds = xr.open_zarr(path, **zarr_kwargs)
        except Exception as e:
            logger.error(f'Failed to open zarr store with zarr_kwargs={zarr_kwargs}')
            raise e

    else:
        logger.debug(f'Opening netCDF/HDF dataset: {root} - protocol: {protocol}')
        try:
            ds = xr.open_dataset(path, **cdf_kwargs)
        except Exception as e:
            logger.error(f'Failed to open netCDF/HDF dataset with cdf_kwargs={cdf_kwargs}')
            raise e

    ds.attrs['intake_esm_varname'] = varname

    if preprocess is None:
        return ds
    else:
        logger.debug(f'Applying pre-processing with {preprocess.__name__} function')
        return preprocess(ds)


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
