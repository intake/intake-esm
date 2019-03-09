from functools import reduce
from datetime import datetime

import numpy as np
import dask
import xarray as xr

time_coord_name_default = 'time'
ensemble_dim_name = 'member_id'


def infer_time_coord_name(ds):
    if time_coord_name_default in ds.variables:
        return time_coord_name_default
    unlimited_dims = ds.encoding.get("unlimited_dims", None)
    if len(unlimited_dims) == 1:
        return list(unlimited_dims)[0]
    raise ValueError(
        "Cannot infer `time_coord_name` from multiple unlimited dimensions: %s \n\t\t ***** Please specify time_coord_name to use. *****"
        % unlimited_dims
    )

def set_time_bound_as_coord(ds, time_coord_name):
    attrs = ds[time_coord_name].attrs
    encoding = ds[time_coord_name].encoding

    bounds = None
    if "bounds" in attrs:
        bounds = attrs["bounds"]
    elif "bounds" in encoding:
        bounds = encoding["bounds"]

    if bounds is not None and bounds in ds.data_vars:
        ds = ds.set_coords(bounds)

    return ds


def dict_union(*dicts, merge_keys=['history'], drop_keys=[]):
    """Return the union of two or more dictionaries."""
    if len(dicts) > 2:
        return reduce(dict_union, dicts)
    elif len(dicts)==2:
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
            elif v1==v2:
                d[k] = v1
            elif k in merge_keys:
                d[k] = '\n'.join([v1, v2])
        return d
    elif len(dicts)==1:
        return dicts[0]

def get_static_vars(ds, time_coord_name=time_coord_name_default):
    return set([v for v in ds.variables if time_coord_name not in ds[v].dims])


def set_coords(ds, varname):
    """Set all variables except varname to be coords."""
    coord_vars = set(ds.data_vars) - set(varname)
    return ds.set_coords(coord_vars)


def merge_vars(ds1, ds2):
    """
    Merge two datasets at a time, dropping all "static" variables from
    second dataset that already exist in the first dataset.
    """

    # save attrs
    attrs = dict_union(ds1.attrs, ds2.attrs)

    # drop static_vars from ds2
    time_coord_name = attrs['time_coord_name']
    ds1_static_vars = get_static_vars(ds1, time_coord_name)
    ds2_dropvars = set(ds2.variables).intersection(ds1_static_vars)

    ds = xr.merge([ds1, ds2.drop(ds2_dropvars)])

    ds.attrs = attrs
    return ds

def merge(dsets):
    """Merge datasets."""

    if len(dsets)==1:
        return dsets[0]

    dsm = reduce(merge_vars, dsets)

    new_history = f'\n{datetime.now()} xarray.merge(<ALL_VARIABLES>)'
    if 'history' in dsm.attrs:
        dsm.attrs['history'] += new_history
    else:
        dsm.attrs['history'] = new_history

    # rechunk
    chunks = {'time': 'auto'}
    if ensemble_dim_name in dsm.dims:
        chunks.update({ensemble_dim_name: 1})

    return dsm.chunk(chunks)

def concat_time_levels(dsets):
    if len(dsets)==1:
        return dsets[0]

    attrs = dict_union(*[ds.attrs for ds in dsets])

    # get static vars from first dataset to simplify concatenation
    first = dsets[0]
    time_coord_name = attrs['time_coord_name']
    static_vars = get_static_vars(first, time_coord_name)
    time_vars = set(first.data_vars) - set(static_vars)
    #rest = [ds.drop(static_vars) for ds in dsets[1:]]
    #objs_to_concat = [first] + rest
    objs_to_concat = [ds.drop(static_vars) for ds in dsets]

    ds = xr.concat(objs_to_concat, dim=time_coord_name, coords='minimal')
    ds = xr.merge((ds, first.drop(time_vars)))

    new_history = f"\n{datetime.now()} xarray.concat(<ALL_TIMESTEPS>, dim='{time_coord_name}', coords='minimal')"
    if 'history' in ds.attrs:
        ds.attrs['history'] += new_history
    else:
        ds.attrs['history'] = new_history
    ds.attrs = attrs

    return ds

def concat_ensembles(dsets, member_ids=None, join='inner'):

    if member_ids is None:
        member_ids = np.arange(0, len(dsets))

    ensemble_dim = xr.DataArray(member_ids, dims=ensemble_dim_name, name=ensemble_dim_name)

    attrs = dict_union(*[ds.attrs for ds in dsets])

    # align first to deal with the fact that some ensemble members have different lengths
    # inner join keeps only overlapping segments of each ensemble
    # outer join gives us the longest possible record
    dsets_aligned = xr.align(*dsets, join=join)

    # use coords from first dataset to simplify concatenation
    first = dsets_aligned[0]
    #rest = [ds.reset_coords(drop=True) for ds in dsets[1:]]
    time_coord_name = attrs['time_coord_name']
    static_vars = get_static_vars(first, time_coord_name)
    time_vars = set(first.data_vars) - set(static_vars)

    objs_to_concat = [ds.drop(static_vars) for ds in dsets]

    ds = xr.concat(objs_to_concat, dim=ensemble_dim, coords='minimal')
    ds = xr.merge((ds, first.drop(time_vars)))
    ds.attrs = attrs

    return ds

def open_dataset(url, data_vars, default_chunk_size='12MiB'):
    # try to use smaller chunks
    with dask.config.set({'array.chunk-size': default_chunk_size}):
        ds = xr.open_dataset(url, chunks={'time': 'auto'}, decode_times=False)
    ds.attrs['history'] = f"{datetime.now()} xarray.open_dataset('{url}')"
    ds.attrs['time_coord_name'] = infer_time_coord_name(ds)
    #ds = set_coords(ds, data_vars)
    ds = set_time_bound_as_coord(ds, ds.attrs['time_coord_name'])
    return ds

open_dataset_delayed = dask.delayed(open_dataset)
