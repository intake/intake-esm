"""Regenerate the netcdf fixtures for the ``_expand_dims`` coordinate regression.

Each file has a ``time`` dimension (and ``lat``/``lon``) plus a ``tas`` data
variable, but no ``member`` dimension. The accompanying ``catalog.csv`` lists
``variable=time`` so that intake-esm tries to ``expand_dims`` a dimension
*coordinate*, which destroys the time index and breaks ``combine_by_coords`` on
unfixed code. See ``catalog.json``.

Run from anywhere::

    python tests/sample_data/expand-dims-coord/_generate.py
"""

import os

import numpy as np
import xarray as xr

HERE = os.path.dirname(os.path.abspath(__file__))


def main():
    rng = np.random.default_rng(0)
    for mem in ['r1', 'r2']:
        time = xr.date_range('2015-01-01', periods=3, freq='MS', use_cftime=True)
        ds = xr.Dataset(
            {'tas': (('time', 'lat', 'lon'), rng.random((3, 2, 2)))},
            coords={'time': time, 'lat': [0.0, 1.0], 'lon': [0.0, 1.0]},
        )
        ds.to_netcdf(os.path.join(HERE, f'regression_{mem}.nc'))


if __name__ == '__main__':
    main()
