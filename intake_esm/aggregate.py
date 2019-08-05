"""Tools to support merge/concat of datasets."""

from datetime import datetime
from functools import reduce

import dask
import numpy as np
import xarray as xr
from fsspec import get_mapper

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


def _override_coords(dsets, time_coord_name):
    """
    Return a list of datasets where all coordinates associated with dimensions
    (except time) have been dropped from all but the first entry.
    """
    dim_coords_except_time = set(dsets[0].coords).intersection(set(dsets[0].dims)) - set(
        [time_coord_name]
    )
    dim_sizes_first = {name: da.shape for name, da in dsets[0].coords.items()}
    datasets = [dsets[0]]
    for ds in dsets[1:]:
        if not all(ds[name].shape == dim_sizes_first[name] for name in dim_coords_except_time):
            raise AssertionError(f'Dataset coordinates mismatch: {dsets[0].dims} != {ds.dims}')
        datasets.append(ds.drop(dim_coords_except_time))

    return datasets


def _drop_additional_dims(dsets):
    all_dims = [set(dset.dims) for dset in dsets]
    common_dims = all_dims[0].intersection(*all_dims[1:])
    ds = []
    for dset in dsets:
        dset_dims = set(dset.dims)
        keep_dims = dset_dims.intersection(common_dims)
        drop_dims = list(dset_dims - keep_dims)
        if drop_dims:
            ds.append(dset.drop_dims(drop_dims))
        else:
            ds.append(dset)

    return ds


def dict_union(*dicts, merge_keys=['history', 'tracking_id'], drop_keys=[]):
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


def _restore_non_dim_coords(ds):
    """restore non_dim_coords to variables"""
    non_dim_coords_reset = set(ds.coords) - set(ds.dims)
    ds = ds.reset_coords(non_dim_coords_reset)
    return ds


def concat_time_levels(
    dsets, time_coord_name_default, restore_non_dim_coords=False, override_coords=False
):
    """
    Concatenate datasets across "time" levels, taking time invariant variables
    from the first dataset.

    Parameters
    ----------
    dsets : list
        A list of datasets to concatenate.
    time_coord_name_default : str
        Default name of the time coordinate
    restore_non_coord_dim : bool, default (False)
        Whether or not to restore non coord dims
    override_coords: bool, default (False)
        Whether or not to drop all coordinates associated with dimensions
        (except time) from all but the first entry in dsets.

    Returns
    -------
    dset : xarray.Dataset,
        The concatenated dataset.
    """
    if not isinstance(dsets[0], xr.Dataset):
        dsets = dask.compute(*dsets)

    if len(dsets) == 1:
        if restore_non_dim_coords:
            return _restore_non_dim_coords(dsets[0])
        else:
            return dsets[0]

    attrs = dict_union(*[ds.attrs for ds in dsets])
    time_coord_name = ensure_time_coord_name(dsets[0], time_coord_name_default)

    # https://github.com/NCAR/intake-esm/issues/104#issuecomment-513404844
    if override_coords:
        dsets = _override_coords(dsets, time_coord_name)
    # get static vars from first dataset
    first = dsets[0]

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

    if restore_non_dim_coords:
        return _restore_non_dim_coords(ds)
    else:
        return ds


def concat_ensembles(
    dsets,
    member_ids=None,
    join='inner',
    ensemble_dim_name='member_id',
    time_coord_name_default='time',
    override_coords=False,
):
    """Concatenate datasets across an ensemble dimension, taking coordinates and
    time-invariant variables from the first ensemble member.

    Parameters
    ----------
    dsets : list
        A list of datasets to concatenate.

    member_ids : list, default (None)
         A list of ids for the ensemble members

    join : str
        Accepted values: ``inner``, ``outer``

    ensemble_dim_name : str, default(``member_id``)
       Ensemble dimension name to use.

    time_coord_name_default : str
        Default name of the time coordinate

    override_coords: bool, default (False)
        Whether or not to drop all coordinates associated with dimensions
        (except time) from all but the first entry in dsets.

    Returns
    -------
    dset : xarray.Dataset,
        The concatenated dataset.

    """
    if len(dsets) == 1:
        return _restore_non_dim_coords(dsets[0])

    if member_ids is None:
        member_ids = np.arange(0, len(dsets))

    time_coord_name = ensure_time_coord_name(dsets[0], time_coord_name_default)
    dsets = _drop_additional_dims(dsets)
    attrs = dict_union(*[ds.attrs for ds in dsets])

    # align first to deal with the fact that some ensemble members have different lengths
    # inner join keeps only overlapping segments of each ensemble
    # outer join gives us the longest possible record
    dim_coords_except_time = set(dsets[0].coords).intersection(set(dsets[0].dims)) - set(
        [time_coord_name]
    )

    # https://github.com/NCAR/intake-esm/issues/104#issuecomment-513404844
    dsets_aligned = xr.align(*dsets, join=join, exclude=dim_coords_except_time)
    if override_coords:
        dsets_aligned = _override_coords(dsets_aligned, time_coord_name)

    # use coords and static_vars from first dataset
    first = dsets_aligned[0]
    rest = [ds.reset_coords(drop=True) for ds in dsets_aligned[1:]]
    objs_to_concat = [first] + rest

    ensemble_dim = xr.DataArray(member_ids, dims=ensemble_dim_name, name=ensemble_dim_name)
    ds = xr.concat(objs_to_concat, dim=ensemble_dim, coords='minimal')

    ds = _restore_non_dim_coords(ds)

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
    if isinstance(varname, str):
        varname = [varname]
    coord_vars = set(ds.data_vars) - set(varname)
    return ds.set_coords(coord_vars)


def open_dataset(url, data_vars, **kwargs):
    """open dataset with chunks determined."""
    ds = xr.open_dataset(url, **kwargs)
    ds.attrs['history'] = f"{datetime.now()} xarray.open_dataset('{url}')"

    if data_vars:
        return set_coords(ds, data_vars)
    else:
        return ds


def open_store(url, data_vars, storage_options={}, **kwargs):
    """open zarr store."""
    mapper = get_mapper(url, **storage_options)
    ds = xr.open_zarr(mapper, **kwargs)

    if data_vars:
        return set_coords(ds, data_vars)
    else:
        return ds


open_dataset_delayed = dask.delayed(open_dataset)
