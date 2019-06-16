import hashlib
import os
import urllib
from urllib.request import urlretrieve

import yaml

from . import config

_default_cache_dir = config.get('database-directory')
_default_cache_dir = f'{_default_cache_dir}/collection-definitions'


aliases = ['CESM1-LE', 'GLADE-CMIP5', 'GLADE-CMIP6', 'GLADE-RDA-ERA5', 'GLADE-GMET', 'MPI-GE']

true_file_names = [
    'cesm1-le-collection',
    'glade-cmip5-collection',
    'glade-cmip6-collection',
    'glade-rda-era5-collection',
    'glade-gmet-collection',
    'mpige-collection',
]


descriptions = [
    'Community Earth System Model Large Ensemble (LENS) data holdings',
    'Coupled Model Intercomparison Project - Phase 5 data holdings on the CMIP Analysis Platform @ NCAR',
    'Coupled Model Intercomparison Project - Phase 6 data holdings on the CMIP Analysis Platform @ NCAR',
    'ECWMF ERA5 Reanalysis data holdings',
    'The Gridded Meteorological Ensemble Tool data holdings',
    'The Max Planck Institute for Meteorology (MPI-M) Grand Ensemble (MPI-GE) data holdings',
]


FILE_ALIAS_DICT = dict(zip(aliases, true_file_names))
FILE_DESCRIPTIONS = dict(zip(aliases, descriptions))


def _file_md5_checksum(fname):
    hash_md5 = hashlib.md5()
    with open(fname, 'rb') as f:
        hash_md5.update(f.read())
    return hash_md5.hexdigest()


def _get_collection_definitions():
    """Prints out available collection definitions for the user to load if no args are
       given.
    """

    print(
        '*********************************************************************\n'
        '* The following collection definitions are supported out-of-the-box *\n'
        '*********************************************************************\n'
    )
    for key in FILE_DESCRIPTIONS.keys():
        print(f"'{key}': {FILE_DESCRIPTIONS[key]}")


def load_collection_definition(
    name=None,
    cache=True,
    cache_dir=_default_cache_dir,
    github_url='https://github.com/NCAR/intake-esm-datastore',
    branch='master',
    extension='collection-definitions',
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
        return _get_collection_definitions()

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

    if not os.path.exists(localfile):
        os.makedirs(longdir, exist_ok=True)

        if extension is not None:
            url = '/'.join((github_url, 'raw', branch, extension, fullname))
            urlretrieve(url, localfile)
            url = '/'.join((github_url, 'raw', branch, extension, md5name))
            urlretrieve(url, md5file)

        else:
            url = '/'.join((github_url, 'raw', branch, fullname))
            urlretrieve(url, localfile)
            url = '/'.join((github_url, 'raw', branch, md5name))
            urlretrieve(url, md5file)

        localmd5 = _file_md5_checksum(localfile)
        with open(md5file, 'r') as f:
            remotemd5 = f.read()

        if localmd5 != remotemd5:
            os.remove(localfile)
            msg = """
            Try downloading the file again. There was a confliction between
            your local .md5 file compared to the one in the remote repository,
            so the local copy has been removed to resolve the issue.
            """
            raise IOError(msg)

    with open(localfile) as f:
        d = yaml.safe_load(f)

    if not cache:
        os.remove(localfile)

    return d
