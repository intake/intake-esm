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


===========
Intake-esm
===========

`Intake-esm` provides an `intake`_ plugin for creating ``file-based Intake catalogs``
for climate data from project efforts such as the `Coupled Model Intercomparison Project (CMIP)`_
and the `Community Earth System Model (CESM) Large Ensemble Project`_.
These projects produce a huge of amount climate data persisted on tape, disk storage components
across multiple (in the order of ~ 300,000) netCDF files. Finding, investigating, loading these files into data array containers
such as `xarray` can be a daunting task due to the large number of files a user may be interested in.
``Intake-esm`` addresses this issue in three steps:

- `Datasets Collection Curation`_ in form of YAML files. These YAML files provide information about data locations, access     pattern,  directory structure, etc. ``intake-esm`` uses these YAML files in conjunction with file name templates
  to construct a local database. Each row in this database consists of a set of metadata such as ``experiment``,
  ``modeling realm``, ``frequency`` corresponding to data contained in one netCDF file.

  .. code-block:: python

        >>> import intake
        >>> col = intake.open_esm_metadatastore(collection_name="GLADE-CMIP5")


- Search and Discovery: once the database is built, ``intake-esm`` can be used for searching and discovering
  of climate datasets by eliminating the need for the user to know specific locations (file path) of
  their data set of interest:

  .. code-block:: python

        >>> cat = col.search(variable=['hfls'], frequency='mon',
        ...          modeling_realm='atmos',
        ...          institute=['CCCma', 'CNRM-CERFACS'])

- Access: when the user is satisfied with the results of their query, they can ask ``intake-esm``
  to load the actual netCDF files into xarray datasets:

  .. code-block:: python

        >>> dsets = cat.to_xarray(decode_times=True, chunks={'time': 50})


Intake-esm supports data holdings from the following projects:

- `CMIP`_: Coupled Model Intercomparison Project (phase 5 and phase 6)
- `CESM`_: Community Earth System Model Large Ensemble (LENS), and Decadal Prediction Large Ensemble (DPLE)
- `MPI-GE`_: The Max Planck Institute for Meteorology (MPI-M) Grand Ensemble (MPI-GE)
- `GMET`_: The Gridded Meteorological Ensemble Tool data
- `ERA5`_: ECWMF ERA5 Reanalysis dataset stored on NCAR's GLADE in ``/glade/collections/rda/data/ds630.0``
- `NA-CORDEX`_: The North American CORDEX program dataset residing on NCAR's GLADE in ``/glade/collections/cdg/data/cordex/data/``
- `CESM-LENS-AWS`_: Community Earth System Model Large Ensemble (CESM LENS) data holdings publicly available on Amazon S3 (us-west-2 region)

.. _CMIP: https://www.wcrp-climate.org/wgcm-cmip
.. _CESM: http://www.cesm.ucar.edu/projects/community-projects/LENS/
.. _ERA5: https://www.ecmwf.int/en/forecasts/datasets/reanalysis-datasets/era5
.. _GMET: https://ncar.github.io/hydrology/models/GMET
.. _MPI-GE: https://www.mpimet.mpg.de/en/grand-ensemble/
.. _NA-CORDEX: https://na-cordex.org/
.. _CESM-LENS-AWS: http://ncar-aws-www.s3-website-us-west-2.amazonaws.com/
.. _intake: https://github.com/intake/intake
.. _Datasets Collection Curation: https://github.com/NCAR/intake-esm-datastore
.. _Coupled Model Intercomparison Project (CMIP): https://www.wcrp-climate.org/wgcm-cmip
.. _Community Earth System Model (CESM) Large Ensemble Project: http://www.cesm.ucar.edu/projects/community-projects/LENS/



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
