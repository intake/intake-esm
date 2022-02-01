"""
Useful for:
* users learning intake-esm
* building tutorials in the documentation.
"""
import os
import pathlib

import numpy as np

_default_cache_dir_name = 'intake_esm_tutorial_data'
base_url = 'https://github.com/ncar/intake-esm'
version = 'main'


def _construct_cache_dir(path):
    import pooch

    if isinstance(path, os.PathLike):
        path = os.fspath(path)
    elif path is None:
        path = pooch.os_cache(_default_cache_dir_name)

    return path

sample_catalogues = {
    cesm1_lens_netcdf: "tests/sample-collections/cesm1-lens-netcdf.csv/cesm1-lens-netcdf.json",
    cmip5_netcdf: "tests/sample-collections/cmip5-netcdf.csv/cmip5-netcdf.json",
    cmip6_netcdf: "tests/sample-collections/cmip6-netcdf-test.csv/cmip6-netcdf.json",
    multi-variable-catalog: "tests/sample-collections/multi-variable-catalog.csv/multi-variable-catalog.json"
}

sample_data = {
    cesm_le: "tests/sample-data/cesm-le/*.nc",
    cmip5: "tests/sample-data/cmip/cmip5/*",
    cmip6: "tests/sample-data/cmip/CMIP6/*",
    cesm_multi_variables: "tests/sample-data/cesm-multi-variables/*.nc"
}

# idea borrowed from Seaborn and Xarray
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
    * ``"air_temperature"``: NCEP reanalysis subset
    * ``"rasm"``: Output of the Regional Arctic System Model (RASM)
    * ``"ROMS_example"``: Regional Ocean Model System (ROMS) output
    * ``"tiny"``: small synthetic dataset with a 1D data variable
    * ``"era5-2mt-2019-03-uk.grib"``: ERA5 temperature data over the UK
    * ``"eraint_uvz"``: data from ERA-Interim reanalysis, monthly averages of upper level data
    Parameters
    ----------
    name : str
        Name of the file containing the dataset.
        e.g. 'air_temperature'
    cache_dir : path-like, optional
        The directory in which to search for and write cached data.
    cache : bool, optional
        If True, then cache data locally for use on subsequent calls
    **kws : dict, optional
        Passed to xarray.open_dataset
    See Also
    --------
    xarray.open_dataset
    """
    try:
        import pooch
    except ImportError as e:
        raise ImportError(
            'tutorial.open_dataset depends on pooch to download and manage datasets.'
            ' To proceed please install pooch.'
        ) from e

    logger = pooch.get_logger()
    logger.setLevel('WARNING')

    cache_dir = _construct_cache_dir(cache_dir)
    if name in external_urls:
        url = external_urls[name]
    else:
        path = pathlib.Path(name)
        if not path.suffix:
            # process the name
            default_extension = '.nc'
            if engine is None:
                _check_netcdf_engine_installed(name)
            path = path.with_suffix(default_extension)
        elif path.suffix == '.grib':
            if engine is None:
                engine = 'cfgrib'

        url = f'{base_url}/raw/{version}/{path.name}'

    # retrieve the file
    filepath = pooch.retrieve(url=url, known_hash=None, path=cache_dir)
    ds = _open_dataset(filepath, engine=engine, **kws)
    if not cache:
        ds = ds.load()
        pathlib.Path(filepath).unlink()

    return ds


def open_rasterio(
    name,
    engine=None,
    cache=True,
    cache_dir=None,
    **kws,
):
    """
    Open a rasterio dataset from the online repository (requires internet).
    If a local copy is found then always use that to avoid network traffic.
    Available datasets:
    * ``"RGB.byte"``: TIFF file derived from USGS Landsat 7 ETM imagery.
    * ``"shade"``: TIFF file derived from from USGS SRTM 90 data
    ``RGB.byte`` and ``shade`` are downloaded from the ``rasterio`` repository [1]_.
    Parameters
    ----------
    name : str
        Name of the file containing the dataset.
        e.g. 'RGB.byte'
    cache_dir : path-like, optional
        The directory in which to search for and write cached data.
    cache : bool, optional
        If True, then cache data locally for use on subsequent calls
    **kws : dict, optional
        Passed to xarray.open_rasterio
    See Also
    --------
    xarray.open_rasterio
    References
    ----------
    .. [1] https://github.com/mapbox/rasterio
    """
    try:
        import pooch
    except ImportError as e:
        raise ImportError(
            'tutorial.open_rasterio depends on pooch to download and manage datasets.'
            ' To proceed please install pooch.'
        ) from e

    logger = pooch.get_logger()
    logger.setLevel('WARNING')

    cache_dir = _construct_cache_dir(cache_dir)
    url = external_rasterio_urls.get(name)
    if url is None:
        raise ValueError(f'unknown rasterio dataset: {name}')

    # retrieve the file
    filepath = pooch.retrieve(url=url, known_hash=None, path=cache_dir)
    arr = _open_rasterio(filepath, **kws)
    if not cache:
        arr = arr.load()
        pathlib.Path(filepath).unlink()

    return arr


def load_dataset(*args, **kwargs):
    """
    Open, load into memory, and close a dataset from the online repository
    (requires internet).
    See Also
    --------
    open_dataset
    """
    with open_dataset(*args, **kwargs) as ds:
        return ds.load()


def load_catalogue(*args, **kwargs):
    """
    Open, load into memory, and close a catalogue from the online repository
    (requires internet)
    """
