"""Tools to support merge/concat of datasets."""

from datetime import datetime
from functools import reduce

import dask
import numpy as np
import xarray as xr

from . import config


def ensure_time_coord_name(ds, time_coord_name_default):
    """Infer the name of the time coordinate in a dataset."""
    if time_coord_name_default in ds.variables:
        return time_coord_name_default
    unlimited_dims = ds.encoding.get('unlimited_dims', None)
    if len(unlimited_dims) == 1:
        return list(unlimited_dims)[0]
    raise ValueError(
        'Cannot infer `time_coord_name` from multiple unlimited dimensions: %s \n',
        '\t\t ***** Please specify time_coord_name to use. *****' % unlimited_dims,
    )


def dict_union(*dicts, merge_keys=['history'], drop_keys=[]):
    """Return the union of two or more dictionaries."""
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


def merge_vars_two_datasets(ds1, ds2):
    """
    Merge two datasets, dropping all variables from
    second dataset that already exist in the first dataset's coordinates.
    """

    # save attrs
    attrs = dict_union(ds1.attrs, ds2.attrs)

    # drop non-dimensional coords and merge
    ds1_ndcoords = set(ds1.coords) - set(ds1.dims)
    ds2_dropvars = set(ds2.variables).intersection(ds1_ndcoords)
    ds = xr.merge([ds1, ds2.drop(ds2_dropvars)])

    ds.attrs = attrs

    return ds


def merge(dsets):
    """Merge datasets."""

    if len(dsets) == 1:
        return dsets[0]

    dsm = reduce(merge_vars_two_datasets, dsets)

    new_history = f'\n{datetime.now()} xarray.merge(<ALL_VARIABLES>)'
    if 'history' in dsm.attrs:
        dsm.attrs['history'] += new_history
    else:
        dsm.attrs['history'] = new_history
    return dsm


def concat_time_levels(dsets, time_coord_name_default):
    """
    Concatenate datasets across "time" levels, taking time invariant variables
    from the first dataset.

    Parameters
    ----------
    dsets : list
        A list of datasets to concatenate.

    time_coord_name_default : string
        Default name of the time coordinate

    Returns
    -------
    dset : xarray.Dataset,
        The concatenated dataset.
    """
    dsets = dask.compute(*dsets)
    if len(dsets) == 1:
        return dsets[0]

    attrs = dict_union(*[ds.attrs for ds in dsets])

    # get static vars from first dataset
    first = dsets[0]
    time_coord_name = ensure_time_coord_name(first, time_coord_name_default)

    def drop_unnecessary_coords(ds):
        """Drop coordinates that do not correspond with dimensions."""
        non_dim_coords = set(ds.coords) - set(ds.dims)
        non_dim_coords_drop = [
            coord for coord in non_dim_coords if time_coord_name not in ds[coord].dims
        ]
        return ds.drop(non_dim_coords_drop)

    rest = [drop_unnecessary_coords(ds) for ds in dsets[1:]]
    objs_to_concat = [first] + rest

    ds = xr.concat(objs_to_concat, dim=time_coord_name, coords='minimal')

    new_history = f"\n{datetime.now()} xarray.concat(<ALL_TIMESTEPS>, dim='{time_coord_name}', coords='minimal')"
    if 'history' in attrs:
        attrs['history'] += new_history
    else:
        attrs['history'] = new_history
    ds.attrs = attrs

    return ds


def concat_ensembles(
    dsets,
    member_ids=None,
    join='inner',
    ensemble_dim_name='member_id',
    time_coord_name_default='time',
):
    """Concatenate datasets across an ensemble dimension, taking coordinates and
    time-invariant variables from the first ensemble member.
    """
    if len(dsets) == 1:
        return dsets[0]

    if member_ids is None:
        member_ids = np.arange(0, len(dsets))

    attrs = dict_union(*[ds.attrs for ds in dsets])

    # align first to deal with the fact that some ensemble members have different lengths
    # inner join keeps only overlapping segments of each ensemble
    # outer join gives us the longest possible record
    dsets_aligned = xr.align(*dsets, join=join)

    # use coords and static_vars from first dataset
    first = dsets_aligned[0]
    rest = [ds.reset_coords(drop=True) for ds in dsets_aligned[1:]]
    objs_to_concat = [first] + rest

    ensemble_dim = xr.DataArray(member_ids, dims=ensemble_dim_name, name=ensemble_dim_name)
    ds = xr.concat(objs_to_concat, dim=ensemble_dim, coords='minimal')

    # restore non_dim_coords to variables
    non_dim_coords_reset = set(ds.coords) - set(ds.dims)
    ds = ds.reset_coords(non_dim_coords_reset)

    new_history = (
        f"\n{datetime.now()} xarray.concat(<ALL_MEMBERS>, dim='member_id', coords='minimal')"
    )
    if 'history' in attrs:
        attrs['history'] += new_history
    else:
        attrs['history'] = new_history
    ds.attrs = attrs

    return ds


def set_coords(ds, varname):
    """Set all variables except varname to be coords."""
    coord_vars = set(ds.data_vars) - set(varname)
    return ds.set_coords(coord_vars)


def open_dataset(url, data_vars, **kwargs):
    """open dataset with chunks determined."""
    ds = xr.open_dataset(url, **kwargs)
    ds.attrs['history'] = f"{datetime.now()} xarray.open_dataset('{url}')"

    return set_coords(ds, data_vars)


open_dataset_delayed = dask.delayed(open_dataset)
