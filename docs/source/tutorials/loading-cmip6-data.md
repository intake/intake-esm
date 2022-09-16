---
jupytext:
  text_representation:
    format_name: myst
kernelspec:
  display_name: Python 3
  name: python3
---

# Accessing CMIP6 data with intake-esm

This notebook demonstrates how to access Google Cloud CMIP6 data using intake-esm.

Intake-esm is a data cataloging utility built on top of intake, pandas, and
xarray. Intake-esm aims to facilitate:

- the discovery of earth’s climate and weather datasets.
- the ingestion of these datasets into xarray dataset containers.

It's basic usage is shown below. To begin, let's import `intake`:

```{code-cell} ipython3
import intake
```

## Load the catalog

At import time, intake-esm plugin is available in intake’s registry as
`esm_datastore` and can be accessed with `intake.open_esm_datastore()` function.
Use the `intake_esm.tutorial.get_url()` method to access smaller subsetted catalogs for tutorial purposes.

```{code-cell} ipython3

import intake_esm
url = intake_esm.tutorial.get_url('google_cmip6')
print(url)
```

```{code-cell} ipython3

cat = intake.open_esm_datastore(url)
cat
```

The summary above tells us that this catalog contains 261 data assets.
We can get more information on the individual data assets contained in the
catalog by looking at the underlying dataframe created when we load the catalog:

```{code-cell} ipython3
cat.df.head()
```

The first data asset listed in the catalog contains:

- the Northward Wind (variable_id='va'), as a function of latitude, longitude, time,

- the latest version of the IPSL climate model (source_id='IPSL-CM6A-LR'),

- hindcasts initialized from observations with historical forcing (experiment_id='historical'),

- developed by theInstitut Pierre Simon Laplace (instution_id='IPSL'),

- run as part of the Coupled Model Intercomparison Project (activity_id='CMIP')

And is located in Google Cloud Storage at 'gs://cmip6/CMIP6/CMIP/IPSL/IPSL-CM6A-LR/historical/r2i1p1f1/Amon/va/gr/v20180803/'.

## Finding unique entries

To get unique values for given columns in the catalog, intake-esm provides a
{py:meth}`~intake_esm.core.esm_datastore.unique` method:

Let's query the data catalog to see what models(`source_id`), experiments
(`experiment_id`) and temporal frequencies (`table_id`) are available.

```{code-cell} ipython3
unique = cat.unique()
unique
```

```{code-cell} ipython3
unique['source_id']
```

```{code-cell} ipython3
unique['experiment_id']
```

```{code-cell} ipython3
unique['table_id']
```

## Search for specific datasets

The {py:meth}`~intake_esm.core.esm_datastore.search` method allows the user to
perform a query on a catalog using keyword arguments. The keyword argument names
must match column names in the catalog. The search method returns a
subset of the catalog with all the entries that match the provided query.

In the example below, we are are going to search for the following:

- variable_d: `o2` which stands for
  `mole_concentration_of_dissolved_molecular_oxygen_in_sea_water`
- experiments: ['historical', 'ssp585']:
  - historical: all forcing of the recent past.
  - ssp585: emission-driven RCP8.5 based on SSP5.
- table_id: `0yr` which stands for annual mean variables on the ocean grid.
- grid_label: `gn` which stands for data reported on a model's native grid.

```{note}
For more details on the CMIP6 vocabulary, please check this
[website](http://clipc-services.ceda.ac.uk/dreq/index.html), and
[Core Controlled Vocabularies (CVs) for use in CMIP6](https://github.com/WCRP-CMIP/CMIP6_CVs)
GitHub repository.
```

```{code-cell} ipython3
cat_subset = cat.search(
    experiment_id=["historical", "ssp585"],
    table_id="Oyr",
    variable_id="o2",
    grid_label="gn",
)

cat_subset
```

## Load datasets using `to_dataset_dict()`

Intake-esm implements convenience utilities for loading the query results into
higher level xarray datasets. The logic for merging/concatenating the query
results into higher level xarray datasets is provided in the input JSON file and
is available under `.aggregation_info` property of the catalog:

```{code-cell} ipython3
cat.esmcat.aggregation_control
```

To load data assets into xarray datasets, we need to use the
{py:meth}`~intake_esm.core.esm_datastore.to_dataset_dict` method. This method
returns a dictionary of aggregate xarray datasets as the name hints.

```{code-cell} ipython3
dset_dict = cat_subset.to_dataset_dict(
    xarray_open_kwargs={"consolidated": True, "decode_times": True, "use_cftime": True}
)
```

```{code-cell} ipython3
[key for key in dset_dict.keys()]
```

We can access a particular dataset as follows:

```{code-cell} ipython3
ds = dset_dict["CMIP.CCCma.CanESM5.historical.Oyr.gn"]
ds
```

Let’s create a quick plot for a slice of the data:

```{code-cell} ipython3
ds.o2.isel(time=0, lev=0, member_id=range(1, 24, 4)).plot(col="member_id", col_wrap=3, robust=True)
```

## Use custom preprocessing functions

When comparing many models it is often necessary to preprocess (e.g. rename
certain variables) them before running some analysis step. The `preprocess`
argument lets the user pass a function, which is executed for each loaded asset
before combining datasets.

```{code-cell} ipython3
cat_pp = cat.search(
    experiment_id=["historical"],
    table_id="Oyr",
    variable_id="o2",
    grid_label="gn",
    source_id=["IPSL-CM6A-LR", "CanESM5"],
    member_id="r10i1p1f1",
)
cat_pp.df
```

```{code-cell} ipython3
dset_dict_raw = cat_pp.to_dataset_dict(xarray_open_kwargs={"consolidated": True})

for k, ds in dset_dict_raw.items():
    print(f"dataset key={k}\n\tdimensions={sorted(list(ds.dims))}\n")
```

```{note}
Note that both models follow a different naming scheme. We can define a little
helper function and pass it to `.to_dataset_dict()` to fix this. For
demonstration purposes we will focus on the vertical level dimension which is
called `lev` in `CanESM5` and `olevel` in `IPSL-CM6A-LR`.
```

```{code-cell} ipython3
def helper_func(ds):
    """Rename `olevel` dim to `lev`"""
    ds = ds.copy()
    # a short example
    if "olevel" in ds.dims:
        ds = ds.rename({"olevel": "lev"})
    return ds
```

```{code-cell} ipython3
dset_dict_fixed = cat_pp.to_dataset_dict(xarray_open_kwargs={"consolidated": True}, preprocess=helper_func)

for k, ds in dset_dict_fixed.items():
    print(f"dataset key={k}\n\tdimensions={sorted(list(ds.dims))}\n")
```

This was just an example for one dimension.

```{note}
Check out [xmip package](https://github.com/jbusecke/xMIP)
for a full renaming function for all available CMIP6 models and some other
utilities.
```

## Load datasets into an xarray-datatree using `to_datatree()`

We can also load our data into an [xarray-datatree](https://xarray-datatree.readthedocs.io/en/latest/) object using the following:

```{code-cell} ipython3
tree = cat_pp.to_datatree(xarray_open_kwargs={"consolidated": True}, preprocess=helper_func)

print(tree)
```

```{code-cell} ipython3
---
tags: [hide-input, hide-output]
---
import intake_esm  # just to display version information
intake_esm.show_versions()
```
