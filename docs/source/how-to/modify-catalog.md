---
jupytext:
  text_representation:
    format_name: myst
kernelspec:
  display_name: Python 3
  name: python3
---

# Modify a catalog

```{code-cell} ipython3
import intake
```

The in-memory representation of an Earth System Model (ESM) catalog is a Pandas {py:class}`~pandas.DataFrame`, and is accessible via the `.df` property:

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

## Use case 2: save a catalog subset as a new catalog

Another use case is to save a subset of the catalog as a new catalog. This is highly useful when you want to share a subset of the catalog or preserve a copy of the catalog for future use.

```{tip}
We highly recommend that you save the subset of the catalog which you use in your analysis. Remote catalogs can change over time, and you may want to preserve a copy of the original catalog  to ensure reproducibility of your analysis.
```

To save a subset of the catalog as a new catalog, we can use the {py:meth}`~intake_esm.core.esm_datastore.serialize` method:

```{code-cell} ipython3
import tempfile
directory = tempfile.gettempdir()
cat_subset.serialize(directory=directory, name="my_catalog_subset")
```

By default, the {py:meth}`~intake_esm.core.esm_datastore.serialize` method will write a single `JSON` file containing the catalog subset.

```{code-cell} ipython3
!cat {directory}/my_catalog_subset.json
```

For large catalogs, we recommend that you write the catalog subset to its own `CSV` file. This can be achieved by setting `catalog_type` to `file`:

```{code-cell} ipython3
cat_subset.serialize(directory=directory, name="my_catalog_subset", catalog_type="file")
```

```{code-cell} ipython3
!cat {directory}/my_catalog_subset.json
!cat {directory}/my_catalog_subset.csv
```

## Conclusion

Intake-ESM provides a powerful search API, however, there are cases where you may want to modify the catalog by using [`pandas`](https://pandas.pydata.org/docs/) directly. In this notebook we showed how to do that and how to attach the modified dataframe to the catalog object and/or save the modified catalog as a new catalog.

```{code-cell} ipython3
---
tags: [hide-input, hide-output]
---
import intake_esm
intake_esm.show_versions()
```
