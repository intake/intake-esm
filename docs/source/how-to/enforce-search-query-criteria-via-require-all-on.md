---
jupytext:
  text_representation:
    format_name: myst
kernelspec:
  display_name: Python 3
  name: python3
---

# How to Enforce Search Query Criteria via `require_all_on` Argument

```{code-cell} ipython3
import intake

url = "https://gist.githubusercontent.com/andersy005/7f416e57acd8319b20fc2b88d129d2b8/raw/987b4b336d1a8a4f9abec95c23eed3bd7c63c80e/pangeo-gcp-subset.json"
cat = intake.open_esm_datastore(url)
cat
```

By default, intake-esm’s {py:meth}`~intake_esm.core.esm_datastore.search` method
returns entries that fulfill **any** of the criteria specified in the query.
Intake-esm can return entries that fulfill **all** query criteria when the user
supplies the `require_all_on` argument. The `require_all_on` parameter can be a
dataframe column or a list of dataframe columns across which all elements must
satisfy the query criteria. The `require_all_on` argument is best explained with
the following example.

Let’s define a query for our collection that requests multiple variable_ids and
multiple experiment_ids from the Omon table_id, all from 3 different source_ids:

```{code-cell} ipython3
# Define our query
query = dict(
    variable_id=["thetao", "o2"],
    experiment_id=["historical", "ssp245", "ssp585"],
    table_id=["Omon"],
    source_id=["ACCESS-ESM1-5", "AWI-CM-1-1-MR", "FGOALS-f3-L"],
)
```

Now, let’s use this query to search for all assets in the collection that
satisfy any combination of these requests (i.e., with `require_all_on=None`,
which is the default):

```{code-cell} ipython3
cat_subset = cat.search(**query)
cat_subset
```

Let's group by `source_id` and count unique values for a few columns:

```{code-cell} ipython3
cat_subset.df.groupby("source_id")[["experiment_id", "variable_id", "table_id"]].nunique()
```

As you can see, the search results above include source_ids for which we only
have one of the two variables, and one or two of the three experiments.

We can tell intake-esm to discard any source_id that doesn’t have both variables
`["thetao", "o2"]` and all three experiments
`["historical", "ssp245", "ssp585"]` by passing `require_all_on=["source_id"]`
to the search method:

```{code-cell} ipython3
cat_subset = cat.search(require_all_on=["source_id"], **query)
cat_subset
```

```{code-cell} ipython3
cat_subset.df.groupby("source_id")[["experiment_id", "variable_id", "table_id"]].nunique()
```

Notice that with the `require_all_on=["source_id"]` option, the only source_id
that was returned by our query was the source_id for which all of the variables
and experiments were found.

```{code-cell} ipython3
---
tags: [hide-input, hide-output]
---
import intake_esm  # just to display version information
intake_esm.show_versions()
```
