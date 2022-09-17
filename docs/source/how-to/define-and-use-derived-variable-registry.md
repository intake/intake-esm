---
jupytext:
  text_representation:
    format_name: myst
kernelspec:
  display_name: Python 3
  name: python3
---

# Define and use derived variable registry

## What is a derived variable ?

A derived variable is a variable that is not present in the original dataset, but is computed from one or more variables in the dataset. For example, a derived variable could be temperature in degrees Fahrenheit. Often times, climate model models write temperature in Celsius or Kelvin, but the user may want degrees Fahrenheit!
This is a really simple example; derived variables could include more sophsticated diagnostic output like aggregations of terms in a tracer budget or gradients in a particular field.

```{note}
Currently, the derived variable implementation requires variables on the same grid, etc.; i.e., it assumes that all variables involved can be merged within **the same dataset**.
```

A traditional workflow for derived variables might consist of the following:

- Load the data
- Apply some function to the loaded datasets
- Plot the output

But what if we could couple those first two steps? What if we could have some set of **variable definitions**, consisting of variable requirements, such as `dependent variables`, and a function which derives the quantity. This is what the `derived_variable` funtionality offers in `intake-esm`! This enables users to share a "registry" of derived variables across catalogs!

Let's get started with an example!

```{code-cell} ipython3
import intake
from intake_esm import DerivedVariableRegistry
```

## How to define a derived variable

Let's compute a derived variable - wind speed! This can be derived from using the zonal (`U`) and meridional (`V`) components of the wind.

### Step 1: define a function to compute `wind speed`

```{code-cell} ipython3
import numpy as np

def calc_wind_speed(ds):
    ds['wind_speed'] = np.sqrt(ds.U ** 2 + ds.V ** 2)
    ds['wind_speed'].attrs = {'units': 'm/s',
                              'long_name': 'Wind Speed',
                              'derived_by': 'intake-esm'}
    return ds
```

### Step 2: create our derived variable registry

We need to instantiate our derived variable registry, which will store our derived variable information! We use the variable `dvr` for this (**D**erived**V**ariable**R**egistry).

```{code-cell} ipython3
dvr = DerivedVariableRegistry()
```

In order to add our derived variable to the registry, we need to add a [decorator](https://www.python.org/dev/peps/pep-0318/)to our function. This allows us to define our derived variable, dependent variables, and the function associated with the calculation.

```{note}

For more in-depth details about decorators, check this tutorial: [Primer on Python Decorators](https://realpython.com/primer-on-python-decorators/)

```

```{code-cell} ipython3
@dvr.register(variable='wind_speed', query={'variable': ['U', 'V']})
def calc_wind_speed(ds):
    ds['wind_speed'] = np.sqrt(ds.U ** 2 + ds.V ** 2)
    ds['wind_speed'].attrs = {'units': 'm/s',
                              'long_name': 'Wind Speed',
                              'derived_by': 'intake-esm'}
    return ds
```

The `register` function has two required arguments: `variable` and `query`. In this particular example, the derived variable `wind_speed` is derived from `U` and `V`. It is possible to specify additional, required metadata in the query , e.g. `U` and `V` from monthly control runs (e.g `query={'variable': ['U', 'V'], 'experiment': 'CTRL', 'frequency': 'monthl'}` in the case of CESM Large Ensemble).

You'll notice `dvr` now has a registered variable, `wind_speed`, which was defined in the cell above!

```{code-cell} ipython3
dvr
```

```{warning}
All fields (keys) specified in the query argument when registering a derived variable must be present in the catalog otherwise you will get a validation error when connecting a derived variable registry to an intake-esm catalog.
```

### Step 3: connect our derived variable registry to an intake-esm catalog

The derived variable registry is now ready to be used with an intake-esm catalog. To do this, we need to add the registry to the catalog. In this case, we will use data from the CESM Large Ensemble (LENS). This is a climate model ensemble, a subset of which is hosted on the AWS Cloud. If you are interested in learning more about this dataset, check out the [LENS on AWS documentation page](https://ncar.github.io/cesm-lens-aws/).

We connect our derived variable registry to a catalog by using the `registry` argument when instantiating the catalog:

```{code-cell} ipython3
data_catalog = intake.open_esm_datastore(
    'https://raw.githubusercontent.com/NCAR/cesm-lens-aws/master/intake-catalogs/aws-cesm1-le.json',
    registry=dvr,
)
```

You'll notice we have a new field - `derived_variable` which has 1 unique value. This is because we have only registered one derived variable, `wind_speed`.

```{code-cell} ipython3
data_catalog
```

Let's also subset for monthly frequency, as well as the 20th century (20C) and RCP 8.5 (RCP85) experiments.

```{code-cell} ipython3
catalog_subset = data_catalog.search(
    variable=['wind_speed'], frequency='monthly', experiment='RCP85'
)

catalog_subset
```

When loading in the data, `intake-esm` will lazily add our calculation for `wind_speed` to the appropriate datasets!

```{code-cell} ipython3
dsets = catalog_subset.to_dataset_dict(
    xarray_open_kwargs={'backend_kwargs': {'storage_options': {'anon': True}}}
)
dsets.keys()
```

Let's look at single dataset from this dictionary of datasets... using the key `atm.CTRL.monthly`. You'll notice upon reading in the dataset, we have three variables:

- `U`
- `V`
- `wind_speed`

```{code-cell} ipython3
ds = dsets['atm.RCP85.monthly']
ds
```

```{code-cell} ipython3
---
tags: [hide-input, hide-output]
---
import intake_esm  # just to display version information
intake_esm.show_versions()
```
