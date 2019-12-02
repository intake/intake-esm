===========================
NCAR CMIP Analysis Platform
===========================

`NCAR's CMIP Analysis Platform (CMIP AP) <https://www2.cisl.ucar.edu/resources/cmip-analysis-platform>`_
includes a large collection of CMIP5 and CMIP6 data sets.

Requesting data sets
--------------------

`Use this form <https://www2.cisl.ucar.edu/resources/cmip-analysis-platform/request-cmip6-data-sets>`_
to request new data be added to the CMIP AP. Typically requests are fulfilled
within two weeks. Contact `CISL <https://www2.cisl.ucar.edu/user-support/getting-help>`_
if you have further questions. Intake-ESM catalogs are regularly updated
following the addition (or removal) of data from the platform.

.. _ncar-cats:

Available catalogs at NCAR
--------------------------

NCAR has created multiple Intake ESM catalogs that work on datasets stored on
GLADE. Those catalogs are listed below:


{% for catalog in catalogs %}

{% if 'ncar' in catalog.platform.lower() %}
{{ catalog.name }}
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
* *Description*: **{{ catalog.description }}**
* *Platform*: **{{ catalog.platform }}**
* *Catalog path or url*: **{{ catalog.url }}**
* *Data Format*: **{{ catalog.data_format }}**
{% if catalog.dataset_docs_link %}
* `Documentation Page <{{ catalog.dataset_docs_link }}>`_
{% endif %}

{% endif %}
{% endfor %}
