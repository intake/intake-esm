"""
Useful for:
* users learning intake-esm
* building tutorials in the documentation.
"""
import os
import pathlib

import intake

_default_cache_dir_name = 'intake_esm_tutorial_data'
base_url = 'https://github.com/intake/intake-esm'
version = 'main'


def _construct_cache_dir(path):
    import pooch

    if isinstance(path, os.PathLike):
        path = os.fspath(path)
    elif path is None:
        path = pooch.os_cache(_default_cache_dir_name)

    return path


sample_catalogues = {
    'cesm1_le': 'tests/sample-collections/cesm1-lens-netcdf.csv/cesm1-lens-netcdf.json',
    'cmip5': 'tests/sample-collections/cmip5-netcdf.csv/cmip5-netcdf.json',
    'cmip6': 'tests/sample-collections/cmip6-netcdf-test.csv/cmip6-netcdf.json',
    'multi_variable_catalog': 'tests/sample-collections/multi-variable-catalog.csv/multi-variable-catalog.json',
    'tiny': 'tests/sample-collections/tiny.nc',
}

sample_data = {
    'cesm_le': 'tests/sample-data/cesm-le/*.nc',
    'cmip5': 'tests/sample-data/cmip/cmip5/*',
    'cmip6': 'tests/sample-data/cmip/CMIP6/*',
    'cesm_multi_variables': 'tests/sample-data/cesm-multi-variables/*.nc',
    'tiny': 'tests/sample-data/tiny.nc',
}


# idea borrowed from Seaborn and Xarray
def open_catalog(
    name,
    cache=True,
    cache_dir=None,
    *,
    engine=None,
    **kws,
):
    """
    Open a catalogue from the online repository (requires internet).
    If a local copy is found then always use that to avoid network traffic.
    Available catalogues:
    * ``""``:
    Parameters
    """
    try:
        import pooch
    except ImportError as e:
        raise ImportError(
            'tutorial.open_catalogue depends on pooch to download and manage catalogues.'
            ' To proceed please install pooch.'
        ) from e

    logger = pooch.get_logger()
    logger.setLevel('WARNING')

    cache_dir = _construct_cache_dir(cache_dir)
    cat_path = sample_catalogues[name]
    cat_url = f'{base_url}/raw/{version}/{cat_path}'

    # retrieve the file
    cat_filepath = pooch.retrieve(url=cat_url, known_hash=None, path=cache_dir)
    cat = intake.open_esm_datastore(cat_filepath, **kws)
    if not cache:
        cat = cat.load()
        pathlib.Path(cat_filepath).unlink()

    return cat


def load_catalog(*args, **kwargs):
    """
    Open, load into memory, and close a catalogue from the online repository
    (requires internet)
    """
    with open_catalog(*args, **kwargs) as cat:
        return cat.load()


def open_dataset(
    name,
    cache=True,
    cache_dir=None,
    *,
    engine=None,
    **kws,
):
    """
    Open a dataset from the online repository (requires internet).
    If a local copy is found then always use that to avoid network traffic.
    Available datasets:
    * ``""``:
    Parameters
    """
    try:
        import pooch
    except ImportError as e:
        raise ImportError(
            'tutorial.open_dataset depends on pooch to download and manage datasdts.'
            ' To proceed please install pooch.'
        ) from e

    logger = pooch.get_logger()
    logger.setLevel('WARNING')

    cache_dir = _construct_cache_dir(cache_dir)
    data_path = sample_data[name]
    data_url = f'{base_url}/raw/{version}/{data_path}'

    # retrieve the file
    data_filepath = pooch.retrieve(url=data_url, known_hash=None, path=cache_dir)
    data = intake.open_esm_datastore(data_filepath, **kws)
    if not cache:
        data = data.load()
        pathlib.Path(data_filepath).unlink()

    return data


def load_dataset(*args, **kwargs):
    """
    Open, load into memory, and close a catalogue from the online repository
    (requires internet)
    """
    with open_dataset(*args, **kwargs) as data:
        return data.load()
