===========
Intake-esm
===========

.. image:: https://img.shields.io/circleci/project/github/NCAR/intake-esm/master.svg?style=for-the-badge&logo=circleci
    :target: https://circleci.com/gh/NCAR/intake-esm/tree/master

.. image:: https://img.shields.io/codecov/c/github/NCAR/intake-esm.svg?style=for-the-badge
    :target: https://codecov.io/gh/NCAR/intake-esm


.. image:: https://img.shields.io/readthedocs/intake-esm/latest.svg?style=for-the-badge
    :target: https://intake-esm.readthedocs.io/en/latest/?badge=latest
    :alt: Documentation Status

.. image:: https://img.shields.io/pypi/v/intake-esm.svg?style=for-the-badge
    :target: https://pypi.org/project/intake-esm
    :alt: Python Package Index

.. image:: https://img.shields.io/conda/vn/conda-forge/intake-esm.svg?style=for-the-badge
    :target: https://anaconda.org/conda-forge/intake-esm
    :alt: Conda Version


Intake-esm provides a plugin for building and loading intake catalogs for earth system data sets holdings, such as CMIP and CESM Large Ensemble datasets.

Intake-esm supports data holdings from the following projects:

- CMIP: Coupled Model Intercomparison Project (phase 5 and phase 6)
- CESM: Community Earth System Model Large Ensemble (LENS), and Decadal Prediction Large Ensemble (DPLE)
- MPI-GE: The Max Planck Institute for Meteorology (MPI-M) Grand Ensemble (MPI-GE)
- GMET: The Gridded Meteorological Ensemble Tool data
- ERA5: ECWMF ERA5 Reanalysis dataset stored on NCAR's GLADE in ``/glade/collections/rda/data/ds630.0``

See documentation_ for more information.

.. _documentation: https://intake-esm.readthedocs.io/en/latest/


Installation
------------

Intake-esm can be installed from PyPI with pip:

.. code-block:: bash

    pip install intake-esm


It is also available from `conda-forge` for conda installations:

.. code-block:: bash

    conda install -c conda-forge intake-esm
