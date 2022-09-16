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
- the user must provide a `converters` dictionary with appropriate functions for parsing values in the `variable_column` and/or any other column with iterables into iterables when loading the catalog. This is done via the `read_csv_kwargs` argument of the `open_esm_datastore` function.

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

```{code-cell} ipython3
---
tags: [hide-input, hide-output]
---
import intake_esm  # just to display version information
intake_esm.show_versions()
```
