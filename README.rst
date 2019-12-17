.. image:: https://img.shields.io/github/workflow/status/NCAR/intake-esm/code-style?label=Code%20Style&style=for-the-badge
    :target: https://github.com/NCAR/intake-esm/actions
    :alt: GitHub Workflow Status

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

.. image:: http://img.shields.io/badge/DOI-10.5281%20%2F%20zenodo.3491062-blue.svg?style=for-the-badge
    :target: https://doi.org/10.5281/zenodo.3491062
    :alt: Zenodo



===========
Intake-esm
===========

Motivation
----------

Project efforts such as the `Coupled Model Intercomparison Project (CMIP)`_
and the `Community Earth System Model (CESM) Large Ensemble Project`_
produce a huge of amount climate data persisted on tape, disk storage, object storage
components across multiple (in the order of ~ 300,000) data assets.
These data assets are stored in netCDF and more recently `Zarr`_ formats.
Finding, investigating, loading these assets into data array containers
such as xarray can be a daunting task due to the large number of files
a user may be interested in. Intake-esm aims to address these issues by
providing necessary functionality for searching, discovering, data access/loading.


Overview
--------

`intake-esm` is a data cataloging utility built on top of `intake`_, `pandas`_, and
`xarray`_, and it's pretty awesome!

- Opening an ESM collection definition file: An ESM (Earth System Model) collection file is a JSON file that conforms
  to the `ESM Collection Specification`_. When provided a link/path to an esm collection file, ``intake-esm`` establishes
  a link to a database (CSV file) that contains data assets locations and associated metadata
  (i.e., which experiement, model, the come from). The collection JSON file can be stored on a local filesystem
  or can be hosted on a remote server.

  .. code-block:: python

        >>> import intake
        >>> col_url = "https://raw.githubusercontent.com/NCAR/intake-esm-datastore/master/catalogs/pangeo-cmip6.json"
        >>> col = intake.open_esm_datastore(col_url)

- Search and Discovery: ``intake-esm`` provides functionality to execute queries against the database:

  .. code-block:: python

        >>> cat = col.search(experiment_id=['historical', 'ssp585'], table_id='Oyr',
        ...          variable_id='o2', grid_label='gn')

- Access: when the user is satisfied with the results of their query, they can ask ``intake-esm``
  to load data assets (netCDF/HDF files and/or Zarr stores) into xarray datasets:

  .. code-block:: python

        >>> dset_dict = cat.to_dataset_dict(zarr_kwargs={'consolidated': True, 'decode_times': False},
        ...                        cdf_kwargs={'chunks': {}, 'decode_times': False})


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
.. _Zarr: https://zarr.readthedocs.io/en/stable/
.. _pandas: https://pandas.pydata.org/
.. _xarray: https://xarray.pydata.org/en/stable/
.. _ESM Collection Specification: https://github.com/NCAR/esm-collection-spec


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
