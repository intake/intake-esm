========
Tutorial
========

``intake-esm`` supports building user-defined collection catalogs for CMIP and CESM data holdings.


Collection Definition
---------------------

Aspects of the collection catalog are defined in ``intake-esm`` configuration file that is stored in ``~/.intake_esm/config.yaml``.
This configuration file is a YAML file with the following contents:

.. code-block:: JSON

    collections:
        cesm:
            collection_columns:
            - resource
            - resource_type
            - direct_access
            - experiment
            - case
            - component
            - stream
            - variable
            - date_range
            - ensemble
            - file_fullpath
            - file_basename
            - file_dirname
            - ctrl_branch_year
            - year_offset
            - sequence_order
            - has_ocean_bgc
            - grid
            order_by_columns:
            - sequence_order
            - file_fullpath
            required_columns:
            - sequence_order
            - file_fullpath
            component_streams:
            atm:
            - cam.h0
            - cam.h1
            - cam.h2
            - cam.h3
            glc:
            - cism.h
            - cism.h0
            - cism.h1
            - cism.h2
            ice:
            - cice.h2_06h
            - cice.h1
            - cice.h
            lnd:
            - clm2.h0
            - clm2.h1
            - clm2.h2
            - clm2.h3
            ocn:
            - pop.h.nday1
            - pop.h.nyear1
            - pop.h.ecosys.nday1
            - pop.h.ecosys.nyear1
            - pop.h
            - pop.h.sigma
            rof:
            - rtm.h0
            - rtm.h1
            - rtm.h2
            - rtm.h3
            - mosart.h0
            - mosart.h1
            - mosart.h2
            - mosart.h3
            replacements:
            freq:
                daily: day_1
                monthly: month_1
                yearly: year_1
        cmip:
            collection_columns:
            - ensemble
            - experiment
            - file_basename
            - file_fullpath
            - frequency
            - institution
            - model
            - realm
            - files_dirname
            - variable
            - version
            required_columns:
            - realm
            - frequency
            - ensemble
            - experiment
            - file_fullpath

    default_chunk_size: 128MiB
    data_cache_directory: ~/.intake_esm/data_cache
    database_directory: ~/.intake_esm/collections
    sources:
        cesm: intake_esm.cesm.CESMSource
        cmip: intake_esm.cmip.CMIPSource

``collection_columns`` consists of a list of columns to include in a collection
catalog database. This database is persisted on disk as an CSV file to the location specified in ``database_directory``.


Building a CMIP5 Collection Catalog
-----------------------------------

Collections are built from a ``YAML`` input file containing a nested dictionary of entries.
An example of such a file is provided below for a CMIP5 collection catalog:

.. ipython:: python

    !cat source/cmip_collection_input_test.yml



Build the Collection
~~~~~~~~~~~~~~~~~~~~~~

Let's begin by importing ``intake``.

.. ipython:: python

   import intake


The main entry point in ``intake-esm`` is ``esm_metadatastore`` class.
Since the class is in the top-level of the package i.e ``__init__.py``,
and the package name starts with ``intake_``, ``intake-esm`` package is scanned
when intake is imported. Now the plugin automatically appears in the set of known
plugins in the intake registry, and an associated ``intake.open_esm_metadatastore``
function is created at import time.

.. ipython:: python

   intake.registry

To build a collection catalog, we instatiate an ``esm_metadatastore`` class in ``intake-esm``
with a collection input YAML file.


.. ipython:: python

   collection_file = "source/cmip_collection_input_test.yml"
   col = intake.open_esm_metadatastore(collection_input_file=collection_file, overwrite_existing=True)
   col.df.head()
   col.df["model"].unique()
   col.df["model"].nunique()  # Find the total number of unique climate models
   col.df.groupby('model').nunique()

Search For Entries in the Built Catalog
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

One of the features supported in ``intake-esm`` is querying the collection catalog.
This is achieved through the ``search()`` method. The ``search`` method allows the user to
specify a query by using keyword arguments. This method returns a subset of the collection
with all the entries that match the query.

.. ipython:: python

   cat = col.search(variable=['hfls'], frequency='mon', modeling_realm='atmos')
   cat.query_results
   ds = cat.to_xarray(decode_times=False)
   ds
