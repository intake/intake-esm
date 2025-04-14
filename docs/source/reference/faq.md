# Frequently Asked Questions

## How do I create my own catalog?

To create your own data catalog, we recommend using the [ecgtools](https://ecgtools.readthedocs.io/en/latest/) package. The package provides a set of tools for harvesting metadata information from files and creating intake-esm compatible catalogs.

## Is there a list of existing catalogs?

The table below is an incomplete list of existing catalogs.
Please feel free to add to this list or raise an issue on [GitHub](https://github.com/intake/intake-esm/issues/new).

{% for catalog in catalogs %}
**{{ catalog.name }}**

- _Description_: **{{ catalog.description }}**
- _Platform_: **{{ catalog.platform }}**
- _Catalog path or url_: **{{ catalog.url }}**
- _Data Format_: **{{ catalog.data_format }}**
  {% if catalog.dataset_docs_link %}
- Documentation Page: [{{ catalog.dataset_docs_link }}]({{ catalog.dataset_docs_link }})
  {% endif %}

{% endfor %}

```{admonition} Note
:class: note
Some of these catalogs are also stored in intake-esm-datastore GitHub repository at https://github.com/NCAR/intake-esm-datastore/tree/master/catalogs
```

## Why do I get a segmentation fault when I try to open a dataset?

This is a known issue when trying to load datasets with xarray using the `netcdf4` backend. The issue is related to thread safety in the underlying `netcdf-c` library.

By default, Intake-ESM attempts to open multiple datasets by creating and executing delayed dask tasks, in order to maximise performance. However, this can lead to segmentations faults. If using a dask client, you may experience this issue as dask workers dying.

In order to avoid this issue, you can either pass `threaded=False` to `.to_dask()`, `.to_dataset_dict`, or `.to_datatree()`, in order to the use of delayed dask tasks when opening datasets on a per-function call basis, or set the environment variable `ITK_ESM_THREADING="False"` to set the default behaviour to eagerly execute dataset opening without using dask tasks. This should prevent the segmentation fault from occurring.

Note that if `ITK_ESM_THREADING="False"`, passing `threaded=True` to `.to_dask()`, `.to_dataset_dict`, or `.to_datatree()` will override the default behaviour and use dask tasks.

## My Dask Workers all die when I try to open a dataset - how can I fix this?

See [the segmentation fault section above](#im-getting-a-segmentation-fault-when-i-try-to-open-a-dataset).
