import datetime
import hashlib
import os
import re
import urllib
from pathlib import Path
from urllib.request import urlopen, urlretrieve

import numpy as np
import pandas as pd
import xarray as xr
from intake.source.utils import reverse_format
from intake.utils import yaml_load

from . import config
from .storage import _get_hsi_files, _posix_symlink

_default_cache_dir = config.get('database-directory')
_default_cache_dir = f'{_default_cache_dir}/bld-collection-input'

aliases = [
    'CESM1-LE',
    'GLADE-CMIP5',
    'GLADE-CMIP6',
    'GLADE-RDA-ERA5',
    'GLADE-GMET',
    'MPI-GE',
    'AWS-CESM1-LE',
    'GLADE-NA-CORDEX',
]

true_file_names = [
    'cesm1-le-collection',
    'glade-cmip5-collection',
    'glade-cmip6-collection',
    'glade-rda-era5-collection',
    'glade-gmet-collection',
    'mpige-collection',
    'aws-cesm1-le-collection',
    'glade-na-cordex-collection',
]


descriptions = [
    'Community Earth System Model Large Ensemble (CESM LENS) data holdings @ NCAR',
    'Coupled Model Intercomparison Project - Phase 5 data holdings on the CMIP Analysis Platform @ NCAR',
    'Coupled Model Intercomparison Project - Phase 6 data holdings on the CMIP Analysis Platform @ NCAR',
    'ECWMF ERA5 Reanalysis data holdings @ NCAR',
    'The Gridded Meteorological Ensemble Tool data holdings',
    'The Max Planck Institute for Meteorology (MPI-M) Grand Ensemble (MPI-GE) data holdings',
    'Community Earth System Model Large Ensemble (CESM LENS) data holdings publicly available on Amazon S3 (us-west-2 region)',
    'The North American CORDEX program data holdings @ NCAR',
]


FILE_ALIAS_DICT = dict(zip(aliases, true_file_names))
FILE_DESCRIPTIONS = dict(zip(aliases, descriptions))


def _file_md5_checksum(fname):
    hash_md5 = hashlib.md5()
    with open(fname, 'rb') as f:
        hash_md5.update(f.read())
    return hash_md5.hexdigest()


def _get_collection_input_files():
    """Prints out available collection definitions for the user to load if no args are
       given.
    """

    print(
        '*********************************************************************\n'
        '* The following collection inputs are supported out-of-the-box *\n'
        '*********************************************************************\n'
    )
    for key in FILE_DESCRIPTIONS.keys():
        print(f"'{key}': {FILE_DESCRIPTIONS[key]}")


def load_collection_input_file(
    name=None,
    cache=True,
    cache_dir=_default_cache_dir,
    github_url='https://github.com/NCAR/intake-esm-datastore',
    branch='master',
    extension='collection-input',
):
    """Load collection definition from an online repository.

    Parameters
    ----------

    name: str, default (None)
        Name of the yaml file containing collection definition, without the .yml extension.
        If None, this function prints out the available collection definitions to specify.

    cache: bool, optional
         If True, cache collection definition locally for use on later calls.

    cache_dir: str, optional
        The directory in which to search for and cache the downloaded file.

    github_url: str, optional
        Github repository where the collection definition is stored.

    branch: str, optional
         The git branch to download from.

    extension: str, optional Subfolder within the repository where the
        collection definition file is stored.

    Returns
    -------

    The desired collection definition dictionary
    """

    if name is None:
        return _get_collection_input_files()

    name, ext = os.path.splitext(name)
    if not ext.endswith('.yml'):
        ext += '.yml'

    if name in FILE_ALIAS_DICT.keys():
        name = FILE_ALIAS_DICT[name]

    longdir = os.path.expanduser(cache_dir)
    fullname = name + ext
    localfile = os.sep.join((longdir, fullname))
    md5name = name + '.md5'
    md5file = os.sep.join((longdir, md5name))

    if extension is not None:
        url = '/'.join((github_url, 'raw', branch, extension, fullname))
        url_md5 = '/'.join((github_url, 'raw', branch, extension, md5name))

    else:
        url = '/'.join((github_url, 'raw', branch, fullname))
        url_md5 = '/'.join((github_url, 'raw', branch, md5name))

    if not os.path.exists(localfile):
        os.makedirs(longdir, exist_ok=True)
        urlretrieve(url, localfile)
        urlretrieve(url_md5, md5file)

    with open(md5file, 'r') as f:
        localmd5 = f.read()

    with urlopen(url_md5) as f:
        remotemd5 = f.read().decode('utf-8')

    if localmd5 != remotemd5:
        os.remove(localfile)
        os.remove(md5file)
        msg = """
        Try downloading the file again. There was a confliction between
        your local .md5 file compared to the one in the remote repository,
        so the local copy has been removed to resolve the issue.
        """
        raise IOError(msg)

    with open(localfile) as f:
        d = yaml_load(f)

    if not cache:
        os.remove(localfile)

    return d


def _get_built_collections():
    """Loads built collections in a dictionary with key=collection_name, value=collection_db_file_path"""
    try:
        db_dir = Path(config.get('database-directory'))
        cc = db_dir.glob('*.nc')
        collections = {}
        for f in cc:
            name = f.stem
            fullpath = f.absolute()
            collections[name] = fullpath
        return collections
    except Exception as e:
        raise e


def _open_collection(collection_name):
    """ Open an ESM collection"""
    collections = _get_built_collections()

    ds = xr.open_dataset(collections[collection_name], engine='netcdf4')
    ds['direct_access'] = ds['direct_access'].astype(bool)
    return ds


def get_subset(collection_name, query, order_by=None):
    """ Get a subset of collection entries that match a query """

    ds = _open_collection(collection_name)
    collection_type = ds.attrs['collection_type']
    condition = np.ones(len(ds.index), dtype=bool)
    for key, val in query.items():
        if isinstance(val, list):
            condition_i = np.zeros(len(ds.index), dtype=bool)
            for val_i in val:
                condition_i = condition_i | (ds[key] == val_i)
            condition = condition & condition_i

        elif val is not None:
            condition = condition & (ds[key] == val)
    query_results = ds.where(condition, drop=True)

    if order_by is None:
        order_by = config.get('collections')[collection_type]['order-by-columns']

    query_results = query_results.sortby(order_by, ascending=True)
    return query_results


def make_attrs(attrs=None):
    """Make standard attributes to attach to xarray datasets (collections).
    Parameters
    ----------
    attrs : dict (optional)
        Additional attributes to add or overwrite
    Returns
    -------
    dict
        attrs
    """

    import intake_xarray
    import intake
    import json
    import pkg_resources

    default_attrs = {
        'created_at': datetime.datetime.utcnow().isoformat(),
        'intake_esm_version': pkg_resources.get_distribution('intake_esm').version,
    }

    upstream_deps = [intake, intake_xarray]
    for dep in upstream_deps:
        dep_name = dep.__name__
        try:
            version = pkg_resources.get_distribution(dep_name).version
            default_attrs[f'{dep_name}_version'] = version
        except pkg_resources.DistributionNotFound:
            if hasattr(dep, '__version__'):
                version = dep.__version__
                default_attrs[f'{dep_name}_version'] = version
    if attrs is not None:
        if 'collection_spec' in attrs:
            attrs['collection_spec'] = json.dumps(attrs['collection_spec'])
        default_attrs.update(attrs)
    return default_attrs


def _extract_attr_with_regex(input_str, regex, strip_chars=None):
    pattern = re.compile(regex, re.IGNORECASE)
    match = re.findall(pattern, input_str)
    if match:
        match = max(match, key=len)
        if strip_chars:
            match = match.strip(strip_chars)

        else:
            match = match.strip()

        return match

    else:
        return None


def _reverse_filename_format(file_basename, filename_template=None, gridspec_template=None):
    """
    Uses intake's ``reverse_format`` utility to reverse the string method format.

    Given format_string and resolved_string, find arguments
    that would give format_string.format(arguments) == resolved_string
    """
    try:
        return reverse_format(filename_template, file_basename)
    except ValueError:
        try:
            return reverse_format(gridspec_template, file_basename)
        except:
            print(
                f'Failed to parse file: {file_basename} using patterns: {filename_template} and {gridspec_template}'
            )
            return {}


def _filter_query_results(ds, file_basename_column_name):
    """Filter for entries where file_basename is the same and remove all
       but the first ``direct_access = True`` row."""

    groups = ds.groupby(file_basename_column_name)

    gps = []
    for _, group in groups:

        g = group.where(group['direct_access'], drop=True)
        # File does not exist on resource with high priority
        if len(g.index) == 0:
            gps.append(group)

        else:
            gps.append(g)

    ds = xr.concat(gps, dim='index')
    return ds


def _ensure_file_access(
    ds, file_fullpath_column_name='file_fullpath', file_basename_column_name='file_basename'
):
    """Ensure that requested files are available locally.

    Paramters
    ---------
    ds : `xarray.Dataset`
        Results of a query.

    Returns
    -------
    df : `pandas.DataFrame`
        Results of a query in form of a DataFrame with a modified List of urls to use when loading files.
    """

    resource_types = {'hsi': _get_hsi_files, 'copy-to-cache': _posix_symlink}

    data_cache_directory = config.get('data-cache-directory')

    os.makedirs(data_cache_directory, exist_ok=True)

    file_remote_local = {k: [] for k in resource_types.keys()}
    ds = _filter_query_results(ds, file_basename_column_name)
    df = ds.to_dataframe()
    local_urlpaths = []
    for idx, row in df.iterrows():
        if row.direct_access:
            local_urlpaths.append(row[file_fullpath_column_name])

        else:
            file_remote = row[file_fullpath_column_name]
            file_local = os.path.join(data_cache_directory, os.path.basename(file_remote))
            local_urlpaths.append(file_local)

            if not os.path.exists(file_local):
                if row.resource_type not in resource_types:
                    raise ValueError(f'unknown resource type: {row.resource_type}')

                file_remote_local[row.resource_type].append((file_remote, file_local))

    for res_type in resource_types:
        if file_remote_local[res_type]:
            print(f'transfering {len(file_remote_local[res_type])} files')
            resource_types[res_type](file_remote_local[res_type])

    df[file_fullpath_column_name] = local_urlpaths

    return df
