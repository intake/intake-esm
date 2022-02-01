""" Helper functions for fetching and loading catalog"""
import importlib
import sys

INTAKE_ESM_ATTRS_PREFIX = 'intake_esm_attrs'
INTAKE_ESM_DATASET_KEY = 'intake_esm_dataset_key'
INTAKE_ESM_VARS_KEY = 'intake_esm_vars'


def _allnan_or_nonan(df, column: str) -> bool:
    """Check if all values in a column are NaN or not NaN

    Returns
    -------
    bool
        Whether the dataframe column has all NaNs or no NaN valles

    Raises
    ------
    ValueError
        When the column has a mix of NaNs non NaN values
    """
    if df[column].isnull().all():
        return False
    if df[column].isnull().any():
        raise ValueError(
            f'The data in the {column} column should either be all NaN or there should be no NaNs'
        )
    return True


def show_versions(file=sys.stdout):  # pragma: no cover
    """print the versions of intake-esm and its dependencies.
       Adapted from xarray/util/print_versions.py

    Parameters
    ----------
    file : file-like, optional
        print to the given file-like object. Defaults to sys.stdout.
    """

    deps = [
        ('xarray', lambda mod: mod.__version__),
        ('pandas', lambda mod: mod.__version__),
        ('intake', lambda mod: mod.__version__),
        ('intake_esm', lambda mod: mod.__version__),
        ('fsspec', lambda mod: mod.__version__),
        ('s3fs', lambda mod: mod.__version__),
        ('gcsfs', lambda mod: mod.__version__),
        ('fastprogress', lambda mod: mod.__version__),
        ('dask', lambda mod: mod.__version__),
        ('zarr', lambda mod: mod.__version__),
        ('cftime', lambda mod: mod.__version__),
        ('netCDF4', lambda mod: mod.__version__),
        ('requests', lambda mod: mod.__version__),
    ]

    deps_blob = []
    for (modname, ver_f) in deps:
        try:
            if modname in sys.modules:
                mod = sys.modules[modname]
            else:
                mod = importlib.import_module(modname)
        except Exception:
            deps_blob.append((modname, None))
        else:
            try:
                ver = ver_f(mod)
                deps_blob.append((modname, ver))
            except Exception:
                deps_blob.append((modname, 'installed'))

    print('\nINSTALLED VERSIONS', file=file)
    print('------------------', file=file)

    print('', file=file)
    for k, stat in sorted(deps_blob):
        print(f'{k}: {stat}', file=file)
