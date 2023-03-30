# Intake-esm

- [Intake-esm](#intake-esm)
  - [Badges](#badges)
  - [Motivation](#motivation)
  - [Overview](#overview)
  - [Installation](#installation)

## Badges

| CI           | [![GitHub Workflow Status][github-ci-badge]][github-ci-link] [![Code Coverage Status][codecov-badge]][codecov-link] [![pre-commit.ci status][pre-commit.ci-badge]][pre-commit.ci-link] |
| :----------- | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------: |
| **Docs**     |                                                                     [![Documentation Status][rtd-badge]][rtd-link]                                                                     |
| **Package**  |                                                          [![Conda][conda-badge]][conda-link] [![PyPI][pypi-badge]][pypi-link]                                                          |
| **License**  |                                                                         [![License][license-badge]][repo-link]                                                                         |
| **Citation** |                                                                         [![Zenodo][zenodo-badge]][zenodo-link]                                                                         |

## Motivation

Computer simulations of the Earth’s climate and weather generate huge amounts of data.
These data are often persisted on HPC systems or in the cloud across multiple data
assets of a variety of formats ([netCDF](https://www.unidata.ucar.edu/software/netcdf/), [zarr](https://zarr.readthedocs.io/en/stable/), etc...). Finding, investigating,
loading these data assets into compute-ready data containers costs time and effort.
The data user needs to know what data sets are available, the attributes describing
each data set, before loading a specific data set and analyzing it.

Finding, investigating, loading these assets into data array containers
such as xarray can be a daunting task due to the large number of files
a user may be interested in. Intake-esm aims to address these issues by
providing necessary functionality for searching, discovering, data access/loading.

## Overview

`intake-esm` is a data cataloging utility built on top of [intake](https://github.com/intake/intake), [pandas](https://pandas.pydata.org/), and [xarray](https://xarray.pydata.org/en/stable/), and it's pretty awesome!

- Opening an ESM catalog definition file: An Earth System Model (ESM) catalog file is a JSON file that conforms
  to the [ESM Collection Specification](./docs/source/reference/esm-catalog-spec.md). When provided a link/path to an esm catalog file, `intake-esm` establishes
  a link to a database (CSV file) that contains data assets locations and associated metadata
  (i.e., which experiment, model, the come from). The catalog JSON file can be stored on a local filesystem
  or can be hosted on a remote server.

  ```python

  In [1]: import intake

  In [2]: import intake_esm

  In [3]: cat_url = intake_esm.tutorial.get_url("google_cmip6")

  In [4]: cat = intake.open_esm_datastore(cat_url)

  In [5]: cat
  Out[5]: <GOOGLE-CMIP6 catalog with 4 dataset(s) from 261 asset(s>
  ```

- Search and Discovery: `intake-esm` provides functionality to execute queries against the catalog:

  ```python
  In [5]: cat_subset = cat.search(
     ...:     experiment_id=["historical", "ssp585"],
     ...:     table_id="Oyr",
     ...:     variable_id="o2",
     ...:     grid_label="gn",
     ...: )

  In [6]: cat_subset
  Out[6]: <GOOGLE-CMIP6 catalog with 4 dataset(s) from 261 asset(s)>
  ```

- Access: when the user is satisfied with the results of their query, they can load data assets (netCDF and/or Zarr stores) into xarray datasets:

  ```python

    In [7]: dset_dict = cat_subset.to_dataset_dict()

    --> The keys in the returned dictionary of datasets are constructed as follows:
            'activity_id.institution_id.source_id.experiment_id.table_id.grid_label'
    |███████████████████████████████████████████████████████████████| 100.00% [2/2 00:18<00:00]
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

[github-ci-badge]: https://github.com/intake/intake-esm/actions/workflows/ci.yaml/badge.svg
[github-ci-link]: https://github.com/intake/intake-esm/actions/workflows/ci.yaml
[codecov-badge]: https://img.shields.io/codecov/c/github/intake/intake-esm.svg?logo=codecov
[codecov-link]: https://codecov.io/gh/intake/intake-esm
[rtd-badge]: https://readthedocs.org/projects/intake-esm/badge/?version=latest
[rtd-link]: https://intake-esm.readthedocs.io/en/latest/?badge=latest
[pypi-badge]: https://img.shields.io/pypi/v/intake-esm?logo=pypi
[pypi-link]: https://pypi.org/project/intake-esm
[conda-badge]: https://img.shields.io/conda/vn/conda-forge/intake-esm?logo=anaconda
[conda-link]: https://anaconda.org/conda-forge/intake-esm
[zenodo-badge]: https://img.shields.io/badge/DOI-10.5281%20%2F%20zenodo.3491062-blue.svg
[zenodo-link]: https://doi.org/10.5281/zenodo.3491062
[license-badge]: https://img.shields.io/github/license/intake/intake-esm
[repo-link]: https://github.com/intake/intake-esm
[pre-commit.ci-badge]: https://results.pre-commit.ci/badge/github/intake/intake-esm/main.svg
[pre-commit.ci-link]: https://results.pre-commit.ci/latest/github/intake/intake-esm/main
