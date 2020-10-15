# NCAR CMIP Analysis Platform

NCAR's [CMIP Analysis Platform (CMIP AP)](https://www2.cisl.ucar.edu/resources/cmip-analysis-platform) includes a large collection of CMIP5 and CMIP6 data sets.

## Requesting data sets

Use this [form](https://www2.cisl.ucar.edu/resources/cmip-analysis-platform/request-cmip6-data-sets) to request new data be added to the CMIP AP. Typically requests are fulfilled within two weeks. Contact [CISL](https://www2.cisl.ucar.edu/user-support/getting-help) if you have further questions. Intake-ESM catalogs are regularly updated following the addition (or removal) of data from the platform.

## Available catalogs at NCAR

NCAR has created multiple Intake ESM catalogs that work on datasets stored on
GLADE. Those catalogs are listed below:

{% for catalog in catalogs %}

{% if 'ncar' in catalog.platform.lower() %}
**{{ catalog.name }}**

- _Description_: **{{ catalog.description }}**
- _Platform_: **{{ catalog.platform }}**
- _Catalog path or url_: **{{ catalog.url }}**
- _Data Format_: **{{ catalog.data_format }}**
  {% if catalog.dataset_docs_link %}
- Documentation Page: [{{ catalog.dataset_docs_link }}]({{ catalog.dataset_docs_link }})
  {% endif %}

{% endif %}
{% endfor %}
