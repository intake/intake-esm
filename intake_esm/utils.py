"""Helper functions for fetching and loading catalog"""

import importlib
import sys
from collections import defaultdict

import polars as pl
import zarr

__all__ = [
    'OPTIONS',
    'set_options',
    '_set_async_flag',
]


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


def _zarr_async() -> bool:
    """
    Zarr went all async in version 3.0.0. This sets the async flag based on
    the zarr version in storage options
    """

    return int(zarr.__version__.split('.')[0]) > 2


def _set_async_flag(data_format: str, xarray_open_kwargs: dict) -> dict:
    """
    If we have the data format set to either zarr2 or zarr3, the async flag in
    `xarray_open_kwargs['storage_options']['remote_opetions']` is constrained to
    be either False or True, respectively.

    Parameters
    ----------
    data_format : str

    xarray_open_kwargs : dict
        The xarray open kwargs dictionary that may contain storage options.
    Returns
    -------
    dict
        The updated xarray open kwargs with the async flag set appropriately.
    """
    if data_format not in {'zarr2', 'zarr3'}:
        return xarray_open_kwargs

    storage_opts_template = {
        'backend_kwargs': {'storage_options': {'remote_options': {'asynchronous': _zarr_async()}}}
    }
    if (
        xarray_open_kwargs.get('backend_kwargs', {})
        .get('storage_options', {})
        .get('remote_options', None)
        is not None
    ):
        xarray_open_kwargs['backend_kwargs']['storage_options']['remote_options'][
            'asynchronous'
        ] = _zarr_async()
    elif xarray_open_kwargs.get('backend_kwargs', {}).get('storage_options', None) is not None:
        xarray_open_kwargs['backend_kwargs']['storage_options'] = storage_opts_template[
            'backend_kwargs'
        ]['storage_options']
    elif xarray_open_kwargs.get('backend_kwargs', None) is not None:
        xarray_open_kwargs['backend_kwargs'] = storage_opts_template['backend_kwargs']
    else:
        xarray_open_kwargs = storage_opts_template

    return xarray_open_kwargs


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


class MinimalExploder:
    """
    A comprehensive class for analyzing and performing minimal explosions
    of DataFrames with multiple list columns.
    """

    def __init__(self, df: pl.DataFrame):
        self.df = df
        self._list_cols: list[str] | None = None
        self._length_patterns: dict[str, tuple[int, ...]] | None = None
        self._explodable_groups: list[list[str]] | None = None

    @property
    def list_columns(self) -> list[str]:
        """Get all list-type columns in the DataFrame."""
        if self._list_cols is None:
            self._list_cols = [col for col in self.df.columns if self.df[col].dtype == pl.List]
        return self._list_cols

    @property
    def length_patterns(self) -> dict[str, tuple[int, ...]]:
        """Get length patterns for all list columns.

        This is stored as a dictionary containing tuples of all list lengths, ie
        'a' : (1,3,2),
        'b' : (2,2,2),

        """
        if self._length_patterns is None:
            self._length_patterns = self._analyze_patterns()
        return self._length_patterns

    @property
    def explodable_groups(self) -> list[list[str]]:
        """Get groups of columns that can be exploded together."""
        if self._explodable_groups is None:
            self._explodable_groups = self._compute_groups()
        return self._explodable_groups

    def _analyze_patterns(self) -> dict[str, tuple[int, ...]]:
        """Analyze length patterns of all list columns. Returns a value
        rather than setting self._length_patterns to shut up mypy."""
        _length_patterns = {}

        for col in self.list_columns:
            lengths = self.df.select(pl.col(col).list.len()).to_series().to_list()
            _length_patterns[col] = tuple(lengths)

        return _length_patterns

    def _compute_groups(self):
        """Compute explodable groups based on length patterns. Returns a value
        rather than setting self._explodable_groups to shut up mypy."""
        pattern_groups = defaultdict(list)

        for col, pattern in self.length_patterns.items():
            pattern_groups[pattern].append(col)

        return list(pattern_groups.values())

    @property
    def summary(self) -> dict:
        """Get a summary of the explosion analysis."""
        return {
            'total_columns': len(self.df.columns),
            'list_columns': len(self.list_columns),
            'unique_patterns': len(set(self.length_patterns.values())),
            'explodable_groups': len(self.explodable_groups),
            'explosion_operations_needed': len(self.explodable_groups),
            'groups': self.explodable_groups,
        }

    def __call__(self) -> pl.DataFrame:
        """Perform the minimal explosion."""
        if not self.list_columns:
            return self.df

        result_df = self.df
        for group in self.explodable_groups:
            result_df = result_df.explode(*group)

        return result_df
