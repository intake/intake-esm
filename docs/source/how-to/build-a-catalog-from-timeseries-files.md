---
jupytext:
  text_representation:
    format_name: myst
kernelspec:
  display_name: Python 3
  name: python3
---

# Build an intake-ESM catalog from CESM timeseries files

In this example, we will cover how to build a data catalog from [Community Earth System Model (CESM)](https://www.cesm.ucar.edu/) output. One of the requirements for using intake-esm is having a catalog which is comprised of two pieces:

- A Comma Separated Value (CSV) file with relevant metadata (ex. file path, variable, stream, etc.)
- A JSON file describing the contents of the CSV file, including how to combine compatible datasets into a single dataset.

Typically, these pieces are constructed "manually" using information within the file path, on a very ad-hoc basis. Also, these catalogs are typically only created for "larger", community datasets, not neccessarily used within smaller model runs/daily workflows. A package (currently a prototype), called [ecgtools](https://ecgtools.readthedocs.io/en/latest/) works to solve the issues of generating these intake-esm catalogs. Ecgtools stands for Earth System Model (ESM) Catalog Generation tools. The current built-in catalog generation tools supported are:

- CMIP6 models
- CESM "history" files
- CESM "timeseries" files

This example provides an overview of using [ecgtools](https://ecgtools.readthedocs.io/en/latest/) for parsing CESM timeseries file model output, and reading in the data using [Intake-ESM](https://intake-esm.readthedocs.io/en/latest/). In this example, we use sample CESM data within the test directory for Intake-ESM.

## Installing ecgtools

You can install [ecgtools](https://github.com/NCAR/ecgtools) through [PyPI](https://pypi.org/project/docs/) or [conda-forge](https://conda-forge.org/docs/). Examples of the syntax are provided below:

```{eval-rst}

.. tab-set::

    .. tab-item:: pip

        Using the `pip <https://pypi.org/project/pip/>`__ package manager:

        .. code:: bash

            $ python -m pip install ecgtools

    .. tab-item:: conda

        Using the `conda <https://conda.io/>`__ package manager that comes with the
        Anaconda/Miniconda distribution:

        .. code:: bash

            $ conda install ecgtools --channel conda-forge


```

## Import packages

The only parts of ecgtools we need are the `Builder` object and the `parse_cesm_history` parser from the CESM parsers! We import `pathlib` to take a look at the files we are parsing.

```{code-cell} ipython3
import pathlib

import dask
import intake
from ecgtools import Builder
from ecgtools.parsers.cesm import parse_cesm_timeseries

```

## Understanding the directory structure

The first step to setting up the `Builder` object is determining where your files are stored. As mentioned previously, we have a sample dataset of CESM2 model output, which is stored in test directory `/tests/sample_data` directory of this repository.

Taking a look at that directory, we see that there is a single case `g.e11_LENS.GECOIAF.T62_g16.009`

```{code-cell} ipython3
root_path = pathlib.Path('../../../tests/sample_data/cesm/').absolute()

sorted(root_path.rglob('*'))
```

Now that we understand the directory structure, let's make the catalog.

## Build the catalog

Let's start by setting the builder object up.

```{code-cell} ipython3

cat_builder = Builder(
    # Directory with the output
    paths=['../../../tests/sample_data/cesm/'],
    # Depth of 1 since we are sending it to the case output directory
    depth=1,
    # Exclude the timeseries and restart directories
    exclude_patterns=["*/tseries/*", "*/rest/*"],
    # Number of jobs to execute - should be equal to # threads you are using
    joblib_parallel_kwargs={'n_jobs': -1},
)

cat_builder
```

We are good to go! Let's build the catalog by calling `.build()` on the object, passing in the `parse_cesm_history` parser, which is a built-in parser for CESM history files.

```{code-cell} ipython3
cat_builder = cat_builder.build(parsing_func=parse_cesm_timeseries)
```

## Inspect the built catalog

Now that the catalog is built, we can inspect the dataframe which is used to create the catalog by calling `.df` on the builder object:

```{code-cell} ipython3
cat_builder.df
```

The resultant dataframe includes the:

- Component
- Stream
- Case
- Date
- Frequency
- Variables
- Path

We can also check to see which files **_were not_** parsed by calling `.invalid_assets`

```{code-cell} ipython3
cat_builder.invalid_assets
```

This is empty, as expected!

## Save the catalog

Now that we have our data catalog, we can save it, by specifying the path to the comma separated values file (`csv`) or compressed csv (`csv.gz`).

```{code-cell} ipython3
cat_builder.save(
    name='cesm_sample_data',
    directory='/tmp',
    # Column name including filepath
    path_column_name='path',
    # Column name including variables
    variable_column_name='variable',
    # Data file format - could be netcdf or zarr (in this case, netcdf)
    data_format="netcdf",
    # Which attributes to groupby when reading in variables using intake-esm
    groupby_attrs=["component", "stream", "case"],
    # Aggregations which are fed into xarray when reading in data using intake
    aggregations=[
        {'type': 'union', 'attribute_name': 'variable'},
        {
            "type": "join_existing",
            "attribute_name": "time_range",
            "options": {"dim": "time", "coords": "minimal", "compat": "override"},
        },
    ],
)
```

## Use the catalog to read in data

```{code-cell} ipython3
data_catalog = intake.open_esm_datastore("/tmp/cesm_sample_data.json")
data_catalog
```

```{code-cell} ipython3
dsets = data_catalog.to_dataset_dict()
dsets
```

Let's plot a quick figure from the dataset!

```{code-cell} ipython3
dsets['ocn.pop.h.ecosys.nday1.g.e11_LENS.GECOIAF.T62_g16.009'].CaCO3_form_zint.isel(time=0).plot();
```

## Conclusion

Having the ability to easily create intake-esm catalogs from model outputs can be a powerful tool in your analysis toolkit. These data can be read in relatively quickly, easing the ability to quickly take a look at model output or even share your data with others! For more updates on [ecgtools](https://github.com/NCAR/ecgtools), be sure to follow [the ecgtools repository](https://github.com/NCAR/ecgtools) on Github! Have an idea for another helpful parser? Submit an issue!

```{code-cell} ipython3
---
tags: [hide-input, hide-output]
---
import intake_esm  # just to display version information
intake_esm.show_versions()
```
