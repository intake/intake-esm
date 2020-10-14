# Intake-esm

- [Intake-esm](#intake-esm)
  - [Badges](#badges)
  - [Motivation](#motivation)
  - [Overview](#overview)
  - [Installation](#installation)

## Badges

| CI           | [![GitHub Workflow Status](https://img.shields.io/github/workflow/status/intake/intake-esm/CI?label=CI&logo=github&style=for-the-badge)](https://github.com/intake/intake-esm/actions?query=workflow%3ACI) [![GitHub Workflow Status](https://img.shields.io/github/workflow/status/intake/intake-esm/linting?label=linting&style=for-the-badge)](https://github.com/intake/intake-esm/actions?query=workflow%3Alinting) [![Code Coverage Status](https://img.shields.io/codecov/c/github/intake/intake-esm.svg?style=for-the-badge)](https://codecov.io/gh/intake/intake-esm) |
| :----------- | :----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------: |
| **Docs**     |                                                                                                                                                                                                       [![Documentation Status](https://img.shields.io/readthedocs/intake-esm/latest.svg?style=for-the-badge)](https://intake-esm.readthedocs.io/en/latest/?badge=latest)                                                                                                                                                                                                       |
| **Package**  |                                                                                                                                                            [![Conda](https://img.shields.io/conda/vn/conda-forge/intake-esm.svg?style=for-the-badge)](https://anaconda.org/conda-forge/intake-esm) [![PyPI](https://img.shields.io/pypi/v/intake-esm.svg?style=for-the-badge)](https://pypi.org/project/intake-esm)                                                                                                                                                            |
| **License**  |                                                                                                                                                                                                                        [![License](https://img.shields.io/github/license/intake/intake-esm?style=for-the-badge)](https://github.com/intake/intake-esm)                                                                                                                                                                                                                         |
| **Citation** |                                                                                                                                                                                                               [![Zenodo](https://img.shields.io/badge/DOI-10.5281%20%2F%20zenodo.3491062-blue.svg?style=for-the-badge)](https://doi.org/10.5281/zenodo.3491062)                                                                                                                                                                                                                |

## Motivation

Computer simulations of the Earth’s climate and weather generate huge amounts of data.
These data are often persisted on HPC systems or in the cloud across multiple data
assets of a variety of formats (netCDF, [zarr](https://zarr.readthedocs.io/en/stable/), etc...). Finding, investigating,
loading these data assets into compute-ready data containers costs time and effort.
The data user needs to know what data sets are available, the attributes describing
each data set, before loading a specific data set and analyzing it.

Finding, investigating, loading these assets into data array containers
such as xarray can be a daunting task due to the large number of files
a user may be interested in. Intake-esm aims to address these issues by
providing necessary functionality for searching, discovering, data access/loading.

## Overview

`intake-esm` is a data cataloging utility built on top of [intake](https://github.com/intake/intake), [pandas](https://pandas.pydata.org/), and [xarray](https://xarray.pydata.org/en/stable/), and it's pretty awesome!

- Opening an ESM collection definition file: An ESM (Earth System Model) collection file is a JSON file that conforms
  to the [ESM Collection Specification](https://github.com/NCAR/esm-collection-spec). When provided a link/path to an esm collection file, `intake-esm` establishes
  a link to a database (CSV file) that contains data assets locations and associated metadata
  (i.e., which experiement, model, the come from). The collection JSON file can be stored on a local filesystem
  or can be hosted on a remote server.

  ```python

  In [1]: import intake

  In [2]: col_url = "https://raw.githubusercontent.com/NCAR/intake-esm-datastore/master/catalogs/pangeo-cmip6.json"

  In [3]: col = intake.open_esm_datastore(col_url)

  In [4]: col
  Out[4]: <pangeo-cmip6 catalog with 4287 dataset(s) from 282905 asset(s)>
  ```

- Search and Discovery: `intake-esm` provides functionality to execute queries against the database:

  ```python
  In [5]: col_subset = col.search(
     ...:     experiment_id=["historical", "ssp585"],
     ...:     table_id="Oyr",
     ...:     variable_id="o2",
     ...:     grid_label="gn",
     ...: )

  In [6]: col_subset
  Out[6]: <pangeo-cmip6 catalog with 18 dataset(s) from 138 asset(s)>
  ```

- Access: when the user is satisfied with the results of their query, they can ask `intake-esm`
  to load data assets (netCDF/HDF files and/or Zarr stores) into xarray datasets:

  ```python

    In [7]: dset_dict = col_subset.to_dataset_dict(zarr_kwargs={"consolidated": True})

    --> The keys in the returned dictionary of datasets are constructed as follows:
            'activity_id.institution_id.source_id.experiment_id.table_id.grid_label'
    |███████████████████████████████████████████████████████████████| 100.00% [18/18 00:10<00:00]
  ```

See [documentation](https://intake-esm.readthedocs.io/en/latest/) for more information.

## Installation

Intake-esm can be installed from PyPI with pip:

```bash
python -m pip install intake-esm
```

It is also available from `conda-forge` for conda installations:

```bash
conda install -c conda-forge intake-esm
```
