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
