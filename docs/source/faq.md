# Frequently Asked Questions

## How do I create my own catalog?

Intake-esm catalogs include two pieces:

1.  **An ESM-Collection file**: an ESM-Collection file is a simple json file that provides metadata about
    the catalog. The specification for this json file is found in the
    [esm-collection-spec](https://github.com/NCAR/esm-collection-spec/blob/master/collection-spec/collection-spec.md) repository.

2.  **A catalog file**: the catalog file is a CSV file that lists the catalog contents. This file
    includes one row per dataset granule (e.g. a NetCDF file or Zarr dataset).
    The columns in this CSV must match the attributes and assets listed in the
    ESM-Collection file. A short example of a catalog file is shown below::

        activity_id,institution_id,source_id,experiment_id,member_id,table_id,variable_id,grid_label,zstore,dcpp_init_year
        AerChemMIP,BCC,BCC-ESM1,piClim-CH4,r1i1p1f1,Amon,ch4,gn,gs://cmip6/AerChemMIP/BCC/BCC-ESM1/piClim-CH4/r1i1p1f1/Amon/ch4/gn/,
        AerChemMIP,BCC,BCC-ESM1,piClim-CH4,r1i1p1f1,Amon,clt,gn,gs://cmip6/AerChemMIP/BCC/BCC-ESM1/piClim-CH4/r1i1p1f1/Amon/clt/gn/,
        AerChemMIP,BCC,BCC-ESM1,piClim-CH4,r1i1p1f1,Amon,co2,gn,gs://cmip6/AerChemMIP/BCC/BCC-ESM1/piClim-CH4/r1i1p1f1/Amon/co2/gn/,
        AerChemMIP,BCC,BCC-ESM1,piClim-CH4,r1i1p1f1,Amon,evspsbl,gn,gs://cmip6/AerChemMIP/BCC/BCC-ESM1/piClim-CH4/r1i1p1f1/Amon/evspsbl/gn/,
        ...

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
