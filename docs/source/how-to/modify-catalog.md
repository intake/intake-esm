---
jupytext:
  text_representation:
    format_name: myst
kernelspec:
  display_name: Python 3
  name: python3
---

# Modify catalog

```{code-cell} ipython3
import intake
```

The in-memory representation of an Earth System Model (ESM) catalog is a pandas
dataframe, and is accessible via the `.df` property:

```{code-cell} ipython3
url ="https://raw.githubusercontent.com/intake/intake-esm/main/tutorial-catalogs/GOOGLE-CMIP6.json"
cat = intake.open_esm_datastore(url)
cat.df.head()
```

In this notebook we will go through some examples showing how to modify this
dataframe and some of its behavior during data loading steps.

```{note}
Pandas is a powerful tool for data manipulation. If you are not familiar with it, we recommend you to read the [Pandas documentation](https://pandas.pydata.org/docs/user_guide/index.html).
```

## Use case 1: complex search queries

Let's say we are interested in datasets with the following attributes:

- `experiment_id=["historical"]`
- `table_id="Amon"`
- `variable_id="ua"`

In addition to these attributes, **we are interested in the first ensemble
member (member_id) of each model (source_id) only**.

This can be achieved in two steps:

### Step 1: run a query against the catalog

We can run a query against the catalog:

```{code-cell} ipython3
cat_subset = cat.search(
    experiment_id=["historical"],
    table_id="Amon",
    variable_id="ua",
)
cat_subset
```

### Step 2: select the first `member_id` for each `source_id`

The subsetted catalog contains `source_id` with the following number of
`member_id` per `source_id`:

```{code-cell} ipython3
cat_subset.df.groupby("source_id")["member_id"].nunique()
```

To get the first `member_id` for each `source_id`, we group the dataframe by
`source_id` and use the `.first()` method to retrieve the first `member_id`:

```{code-cell} ipython3
grouped = cat_subset.df.groupby(["source_id"])
df = grouped.first().reset_index()

# Confirm that we have one ensemble member per source_id

df.groupby("source_id")["member_id"].nunique()
```

```{code-cell} ipython3
df
```

### Step 3: attach the new dataframe to our catalog object

```{code-cell} ipython3
cat_subset.esmcat._df = df
cat_subset
```

Let's load the subsetted catalog into a dictionary of datasets:

```{code-cell} ipython3
dsets = cat_subset.to_dataset_dict()
[key for key in dsets]
```

```{code-cell} ipython3
dsets["CMIP.IPSL.IPSL-CM6A-LR.historical.Amon.gr"]
```

## Use case 2:

```{code-cell} ipython3
---
tags: [hide-input, hide-output]
---
import intake_esm
intake_esm.show_versions()
```
