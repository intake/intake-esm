---
jupytext:
  text_representation:
    format_name: myst
kernelspec:
  display_name: Python 3
  name: python3
---

# Understanding intake-ESM keys and how to use them

Intake-ESM helps with aggregating your datasets using some `keys`. Here, we dig into what exactly these keys are, how they are constructed, and how you can change them. Understanding how this work will help you control how your datasets are merged together, and remove the mystery behind these strings of text.

## Import packages and spin up a Dask cluster

We start first with importing `intake` and a `Client` from `dask.distributed`

```{code-cell} ipython3
import intake
from distributed import Client

client = Client()
```

## Investigate a CMIP6 catalog

Let's start with a sample CMIP6 catalog! This is a fairly large dataset.

```{code-cell} ipython3
url ="https://raw.githubusercontent.com/intake/intake-esm/main/tutorial-catalogs/GOOGLE-CMIP6.json"
catalog = intake.open_esm_datastore(url)
catalog.df.head()
```

Typically, the next step would be to search and load your datasets using {py:meth}`~intake_esm.core.esm_datastore.to_dataset_dict` or {py:meth}`~intake_esm.core.esm_datastore.to_datatree`

```{code-cell} ipython3
catalog_subset = catalog.search(variable_id='ua')
dsets = catalog_subset.to_dataset_dict()
print(dsets)
```

### Investigating the keys

The keys for these datasets include some helpful information - but you might be wondering what this all means and where this text comes from...

```{code-cell} ipython3
print(list(dsets))
```

When `intake-esm` aggregates these datasets, it uses some pre-determined metadata, defined in the catalog file. We can look at which fields are used for aggregation, or merging of the datasets, using the following

```{code-cell} ipython3
print(catalog.esmcat.aggregation_control.groupby_attrs)
```

Let's go back to our data catalog... and find these fields. You'll notice they are all column labels! These are key components of the metadata.

```{code-cell} ipython3
catalog_subset.df
```

## Using keys_info()

These groupby attributes are columns in our catalog! This means that the datasets which will be aggregated using the hierarchy:

```
activity_id --> institution_id --> source_id --> experiment_id --> table_id --> grid_label
```

A more clear of taking a look at these aggregation variables using the `.keys_info()` method for the catalog:

```{code-cell} ipython3
catalog_subset.keys_info()
```

## Change our groupby/aggregation controls

If we wanted to instead aggregate our datasets at the member_id level, we can change that using the following method:

```{code-cell} ipython3
original_groupby_attributes = catalog.esmcat.aggregation_control.groupby_attrs
new_groupby_attributes = original_groupby_attributes + ["member_id"]
print(new_groupby_attributes)
```

Now that we have our new groupby attributes, we can assign these to our catalog subset.

```{code-cell} ipython3
catalog_subset.esmcat.aggregation_control.groupby_attrs = new_groupby_attributes
```

Let's check our new keys! You'll notice we now have 97 keys, aggregated on

```
activity_id --> institution_id --> source_id --> experiment_id --> table_id --> grid_label --> member_id
```

```{code-cell} ipython3
catalog_subset.keys_info()
```

### Load our datasets with the new keys

We can now load our new datasets to our dictionary of datasets using:

```{code-cell} ipython3
dsets = catalog_subset.to_dataset_dict()
```

And if we only wanted the first key, we could use the following to grab the first key in the list. Notice how we now have our member_id at the end!

```{code-cell} ipython3
first_key = catalog_subset.keys()[0]
first_key
```

And the .to_dask() method to load our dataset into our notebook.

```{code-cell} ipython3
ds = catalog_subset[first_key].to_dask()
ds
```

### Compare this dataset with the original catalog configuration

Compare this to our original catalog, which aggregated one level higher, placing all of the `member_id`s into the same dataset.

```{note}
Notice how our metadata now mentions there are 65 member_ids in this dataset, compared to 1 in the previous dataset
```

```{code-cell} ipython3
original_ds = catalog[catalog.keys()[0]].to_dask()
original_ds
```

## Conclusion

These `intake-esm` keys can be a bit abstract when first accessing your data, but understanding them is essential to understand **how** `intake-esm` aggregates your data, and how you can change these aggregation controls for your desired analysis! We hope this helped demystify these keys.

```{code-cell} ipython3
---
tags: [hide-input, hide-output]
---
import intake_esm  # just to display version information
intake_esm.show_versions()
```
