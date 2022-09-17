---
jupytext:
  text_representation:
    format_name: myst
kernelspec:
  display_name: Python 3
  name: python3
---

# Filter a catalog by substring and/or regular expression

## Exact match keywords

```{code-cell} ipython3
import intake

url = "https://ncar-cesm-lens.s3-us-west-2.amazonaws.com/catalogs/aws-cesm1-le.json"
cat = intake.open_esm_datastore(url)
cat
```

```{code-cell} ipython3
cat.df.head()
```

By default, the
{py:meth}`~intake_esm.core.esm_datastore.search` method looks for exact matches,
and is case sensitive:

```{code-cell} ipython3
cat.search(experiment="20C", long_name="wind")
```

As you can see, the example above returns an empty catalog.

## Substring matches

In some cases, you may not know the exact term to look for. For such cases,
inkake-esm supports searching for substring matches. With use of wildcards
and/or regular expressions, we can find all items with a particular substring in
a given column. Let's search for:

- entries from `experiment` = '20C'
- all entries whose variable long name **contains** `wind`

```{code-cell} ipython3
cat.search(experiment="20C", long_name="wind*")
```

Now, let's search for:

- entries from `experiment` = '20C'
- all entries whose variable long name **starts** with `wind`

```{code-cell} ipython3
cat_subset = cat.search(experiment="20C", long_name="^wind")
cat_subset
```

```{code-cell} ipython3
cat_subset.df
```

```{code-cell} ipython3
---
tags: [hide-input, hide-output]
---
import intake_esm  # just to display version information
intake_esm.show_versions()
```
