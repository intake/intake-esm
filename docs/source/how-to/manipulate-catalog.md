---
jupytext:
  text_representation:
    format_name: myst
kernelspec:
  display_name: Python 3
  name: python3
---

# How to Manipulate Catalog's DataFrame

```{code-cell} ipython3
import intake
```

The in-memory representation of an Earth System Model (ESM) catalog is a pandas
dataframe, and is accessible via the `.df` property:

```{code-cell} ipython3
url = "https://gist.githubusercontent.com/andersy005/7f416e57acd8319b20fc2b88d129d2b8/raw/987b4b336d1a8a4f9abec95c23eed3bd7c63c80e/pangeo-gcp-subset.json"
cat = intake.open_esm_datastore(url)
cat.df.head()
```

In this notebook we will go through some examples showing how to manipulate this
dataframe outside of intake-esm.

## Use Case 1: Complex Search Queries

Let's say we are interested in datasets with the following attributes:

- `experiment_id=["historical"]`
- `table_id="Amon"`
- `variable_id="tas"`
- `source_id=['TaiESM1', 'AWI-CM-1-1-MR', 'AWI-ESM-1-1-LR', 'BCC-CSM2-MR', 'BCC-ESM1', 'CAMS-CSM1-0', 'CAS-ESM2-0', 'UKESM1-0-LL']`

In addition to these attributes, **we are interested in the first ensemble
member (member_id) of each model (source_id) only**.

This can be achieved in two steps:

### Step 1: Run a query against the catalog

We can run a query against the catalog:

```{code-cell} ipython3
cat_subset = cat.search(
    experiment_id=["historical"],
    table_id="Amon",
    variable_id="tas",
    source_id=[
        "TaiESM1",
        "AWI-CM-1-1-MR",
        "AWI-ESM-1-1-LR",
        "BCC-CSM2-MR",
        "BCC-ESM1",
        "CAMS-CSM1-0",
        "CAS-ESM2-0",
        "UKESM1-0-LL",
    ],
)
cat_subset
```

### Step 2: Select the first `member_id` for each `source_id`

The subsetted catalog contains `source_id` with the following number of
`member_id` per `source_id`:

```{code-cell} ipython3
cat_subset.df.groupby("source_id")["member_id"].nunique()
```

To get the first `member_id` for each `source_id`, we group the dataframe by
`source_id` and use the `.first()` function to retrieve the first `member_id`:

```{code-cell} ipython3
grouped = cat_subset.df.groupby(["source_id"])
df = grouped.first().reset_index()

# Confirm that we have one ensemble member per source_id

df.groupby("source_id")["member_id"].nunique()
```

### Step 3: Attach the new dataframe to our catalog object

```{code-cell} ipython3
cat_subset.esmcat.df = df
cat_subset
```

Let's load the subsetted catalog into a dictionary of datasets:

```{code-cell} ipython3
dsets = cat_subset.to_dataset_dict(xarray_open_kwargs={"consolidated": True})
[key for key in dsets]
```

```{code-cell} ipython3
dsets["CMIP.CAS.CAS-ESM2-0.historical.Amon.gn"]
```

```{code-cell} ipython3
---
tags: [hide-input, hide-output]
---
import intake_esm  # just to display version information
intake_esm.show_versions()
```
