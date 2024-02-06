""" Helper functions for fetching and loading catalog"""
import importlib
import sys


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
    for modname, ver_f in deps:
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


OPTIONS = {
    'attrs_prefix': 'intake_esm_attrs',
    'dataset_key': 'intake_esm_dataset_key',
    'vars_key': 'intake_esm_vars',
}


class set_options:
    """Set options for intake_esm in a controlled context.

    Currently-supported options:

    - ``attrs_prefix``:
      The prefix to use in the names of attributes constructed from the catalog's columns
      when returning xarray Datasets.
      Default: ``intake_esm_attrs``.
    - ``dataset_key``:
      Name of the global attribute where to store the dataset's key.
      Default: ``intake_esm_dataset_key``.
    - ``vars_key``:
      Name of the global attribute where to store the list of requested variables when
      opening a dataset. Default: ``intake_esm_vars``.

    Examples
    --------
    You can use ``set_options`` either as a context manager:

    >>> import intake
    >>> import intake_esm
    >>> cat = intake.open_esm_datastore('catalog.json')
    >>> with intake_esm.set_options(attrs_prefix='cat'):
    ...     out = cat.to_dataset_dict()

    Or to set global options:

    >>> intake_esm.set_options(attrs_prefix='cat', vars_key='cat_vars')
    """

    def __init__(self, **kwargs):
        self.old = {}
        for k, v in kwargs.items():
            if k not in OPTIONS:
                raise ValueError(
                    f'argument name {k} is not in the set of valid options {set(OPTIONS)}'
                )

            if not isinstance(v, str):
                raise ValueError(f'option {k} given an invalid value: {v}')

            self.old[k] = OPTIONS[k]

        self._update(kwargs)

    def __enter__(self):
        """Context management."""
        return

    def _update(self, kwargs):
        """Update values."""
        for k, v in kwargs.items():
            OPTIONS[k] = v

    def __exit__(self, type, value, traceback):
        """Context management."""
        self._update(self.old)
