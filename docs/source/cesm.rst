Building a CESM Collection Catalog
-----------------------------------

Building a CESM collection catalog follows the same steps that are used when building a CMIP collection catalog:

- Define a collection catalog in a YAML or nested dictionary
- Pass this collection definition to ``intake_open_esm_metadatastore()`` class.
- Use the built collection catalog

For demonstration purposes, we are going to use data from CESM-LE project.


.. ipython:: python

   import intake

   cdefinition = {'name': 'cesm1-le_test',
                'collection_type': 'cesm',
                'include_cache_dir': True,
                'data_sources': {'CTRL': {'locations': [{'name': 'SAMPLE-DATA',
                    'loc_type': 'posix',
                    'direct_access': True,
                    'urlpath': '../tests/sample_data/cesm-le'}],
                'component_attrs': {'ocn': {'grid': 'POP_gx1v6'}},
                'case_members': [{'case': 'b.e11.B1850C5CN.f09_g16.005',
                    'sequence_order': 0,
                    'ensemble': 0,
                    'has_ocean_bgc': True,
                    'year_offset': 1448}]},
                '20C': {'locations': [{'name': 'SAMPLE-DATA',
                    'loc_type': 'posix',
                    'direct_access': True,
                    'urlpath': '../tests/sample_data/cesm-le'}],
                'component_attrs': {'ocn': {'grid': 'POP_gx1v6'}},
                'case_members': [{'case': 'b.e11.B20TRC5CNBDRD.f09_g16.001',
                    'sequence_order': 0,
                    'ensemble': 1,
                    'has_ocean_bgc': True}]}}}

Building the Collection
~~~~~~~~~~~~~~~~~~~~~~~~~

The build method loops over all the experiments and each of the ensemble members therein.
It attempts to parse file name; it fails in some instances and skips these files with a warning.
If HPSS access is not available (such as from compute nodes on Cheyenne),
this resource is omitted from the catalog.

.. ipython:: python
   :okwarning:

    col = intake.open_esm_metadatastore(collection_input_definition=cdefinition,
                                       overwrite_existing=True)


Using the Built Collection
~~~~~~~~~~~~~~~~~~~~~~~~~~

``Intake-esm`` builds a ``pandas.DataFrame`` to store the collection.
The DataFrame is stored as an attribute on the collection object.

.. ipython:: python

   col.df.head()
   col.df['variable'].unique()
   col.df['stream'].unique()
   col.df['ensemble'].unique()


Now you can query the collection catalog and load data sets of interests into xarray objects:

.. ipython:: python

    cat = col.search(
                variable=['STF_O2', 'SHF'],
                ensemble=[1, 3, 9],
                experiment=['20C', 'RCP85'],
                direct_access=True,
            )
    ds = cat.to_xarray(decode_times=False, chunks={'time': 100})
    ds
