---
jupytext:
  text_representation:
    format_name: myst
kernelspec:
  display_name: Python 3
  name: python3
---

# Use catalogs with assets containing multiple variables

By default, `intake-esm` assumes that the data assets (files) contain a single variable (e.g. `temperature`, `precipitation`, etc..). If you have multiple variables in your data files, intake-esm requires the following:

- the `variable_column` of the catalog must contain iterables (list, tuple, set) of values (e.g. `['temperature', 'precipitation']`).
- the user must provide converters with appropriate functions for parsing values in the `variable_column` (and/or any other column with iterables) into iterables when loading the catalog. There are two ways to do this with the `open_esm_datastore` function: either pass the converter functions directly through the `read_csv_kwargs` argument, or specify the columns in `columns_with_iterables` parameter. The latter is a shortcut for the former. Both are demonstrated below.

## Inspect the catalog

In the example below, we are are going to use the following catalog to
demonstrate how to work with multi-variable assets:

```{code-cell} ipython3
# Look at the catalog on disk
!cat multi-variable-catalog.csv
```

As you can see, the variable column contains a list of varibles, and this list
was serialized as a string:
`"['SHF', 'REGION_MASK', 'ANGLE', 'DXU', 'KMT', 'NO2', 'O2']"`.

## Load the catalog

```{code-cell} ipython3
import intake
import ast
import dask

# Make sure this is single-threaded
dask.config.set(scheduler='single-threaded')

cat = intake.open_esm_datastore(
    "multi-variable-catalog.json",
    read_csv_kwargs={"converters": {"variable": ast.literal_eval}},
)
cat
```

To confirm that intake-esm has loaded the catalog correctly, we can inspect the `.has_multiple_variable_assets` property:

```{code-cell} ipython3
cat.esmcat.has_multiple_variable_assets
```

Alternatively, we can specify the variable column name in the `columns_with_iterables` parameter:

```{code-cell} ipython3
cat = intake.open_esm_datastore(
    "multi-variable-catalog.json",
    columns_with_iterables=["variable"],
)
cat.esmcat.has_multiple_variable_assets
```

## Search for datasets

The search functionatilty works in the same way:

```{code-cell} ipython3
cat_subset =cat.search(variable=["O2", "SiO3"])
cat_subset.df
```

## Load assets into xarray datasets

When loading the data files into xarray datasets, `intake-esm` will load only **data variables** that were requested. For example, if a data file contains ten data variables and the user requests for two variables, intake-esm will load the two requested variables plus necessary coordinates information.

```{code-cell} ipython3
dsets = cat_subset.to_dataset_dict()
dsets
```

## Why does `intake.open_esm_datastore` need the `columns_with_iterables` parameter?

Why does intake `intake.open_esm_datastore` need the `columns_with_iterables` argument when we can achieve the same functionality with just `read_csv_kwargs`? Intake facilitates writing YAML descriptions of catalogs that can be opened with `intake.open_catalog`. These YAML descriptions include the information required to open the catalog: things like the catalog driver (`intake_esm.core.esm_datastore` in our case) and the arguments to pass to the driver to open the catalog. They can be included as entries in other catalogs enabling features like [catalog nesting](https://intake.readthedocs.io/en/latest/catalog.html#catalog-nesting). However, intake does not support Python function arguments like those we provided to `read_csv_kwargs` above so if we want a functional intake YAML description of an intake-esm catalog with multi-variable assets we need to use the `columns_with_iterables` argument instead. You can return an intake YAML description of an `esm_datastore` as follows:

```{code-cell} ipython3
cat.name = "my-esm-catalog"
print(cat.yaml())
```

```{code-cell} ipython3
---
tags: [hide-input, hide-output]
---
import intake_esm  # just to display version information
intake_esm.show_versions()
```
