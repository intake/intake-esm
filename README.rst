===============================
Intake-cesm
===============================

.. image:: https://img.shields.io/circleci/project/github/NCAR/intake-cesm/master.svg?style=for-the-badge&logo=circleci
    :target: https://circleci.com/gh/NCAR/intake-cesm/tree/master

.. image:: https://img.shields.io/codecov/c/github/NCAR/intake-cesm.svg?style=for-the-badge
    :target: https://codecov.io/gh/NCAR/intake-cesm


.. image:: https://img.shields.io/readthedocs/intake-cesm/latest.svg?style=for-the-badge
    :target: https://intake-cesm.readthedocs.io/en/latest/?badge=latest
    :alt: Documentation Status

.. image:: https://img.shields.io/pypi/v/intake-cesm.svg?style=for-the-badge
    :target: https://pypi.org/project/intake-cesm
    :alt: Python Package Index
    
.. image:: https://img.shields.io/conda/vn/conda-forge/intake-cesm.svg?style=for-the-badge
    :target: https://anaconda.org/conda-forge/intake-cesm
    :alt: Conda Version


Intake-cesm provides a plug for reading CESM Large Ensemble data sets using intake.
See documentation_ for more information.

.. _documentation: https://intake-cesm.readthedocs.io/en/latest/


An example of using intake-cesm:

.. code-block:: python

    >>> import intake
    >>> 'cesm_metadatastore' in intake.registry
    True
    >>> from intake_cesm.config import set_options
    >>> set_options(database_directory="./tests/test_collections")
    <intake_cesm.config.set_options object at 0x10349efd0>

    >>> build_args = {"collection_input_file" : "./tests/collection_input_test.yml", "collection_type_def_file" : "intake_cesm/cesm_definitions.yml", 
    ... "overwrite_existing" : True}
    >>> col = intake.open_cesm_metadatastore(collection="cesm_dple_test_collection", build_args=build_args)
    INFO:root:Active collection : cesm_dple_test_collection
    INFO:root:Active database: /Users/abanihi/devel/ncar/intake-cesm/tests/test_collections/cesm_dple_test_collection.csv
    INFO:root:calling build
    INFO:root:working on experiment: g.e11_LENS.GECOIAF.T62_g16.009
    INFO:root:getting file listing: TEST-LIST:input-file:tests/intake-cesm-test-filelist
    INFO:root:building file database: TEST-LIST:input-file:tests/intake-cesm-test-filelist
    100%|██████████████████████████████████████████████████████████████████████████████████████████████████████████████| 3/3 [00:00<00:00, 12052.60it/s]
    1it [00:00, 112.32it/s]
    INFO:root:working on experiment: g.e11_LENS.GECOIAF.T62_g16.009-copy
    INFO:root:getting file listing: SAMPLE-DATA:posix:tests/sample_data
    INFO:root:building file database: SAMPLE-DATA:posix:tests/sample_data
    100%|██████████████████████████████████████████████████████████████████████████████████████████████████████████████| 3/3 [00:00<00:00, 23215.70it/s]
    1it [00:00, 71.65it/s]
    Active collection: cesm_dple_test_collection
    >>> col.df
                                                resource resource_type direct_access  ... sequence_order has_ocean_bgc       grid
    0                SAMPLE-DATA:posix:tests/sample_data         posix          True  ...              0           NaN  POP_gx1v6
    1                SAMPLE-DATA:posix:tests/sample_data         posix          True  ...              0           NaN  POP_gx1v6
    2                SAMPLE-DATA:posix:tests/sample_data         posix          True  ...              0           NaN  POP_gx1v6
    3  TEST-LIST:input-file:tests/intake-cesm-test-fi...    input-file         False  ...              0           NaN  POP_gx1v6
    4  TEST-LIST:input-file:tests/intake-cesm-test-fi...    input-file         False  ...              0           NaN  POP_gx1v6
    5  TEST-LIST:input-file:tests/intake-cesm-test-fi...    input-file         False  ...              0           NaN  POP_gx1v6

    [6 rows x 18 columns]
    >>> cat = col.search(variable='O2', direct_access=True)
    >>> print(cat.yaml(True))
    plugins:
    source:
    - module: intake_cesm.core
    sources:
    cesm_dple_test_collection-26a47333-f124-4b88-a235-020a731c2509:
        args:
        chunks:
            time: 1
        collection: cesm_dple_test_collection
        concat_dim: time
        decode_coords: false
        decode_times: false
        engine: netcdf4
        query:
            case: null
            component: null
            ctrl_branch_year: null
            date_range: null
            direct_access: true
            ensemble: null
            experiment: null
            files: null
            files_basename: null
            files_dirname: null
            grid: null
            has_ocean_bgc: null
            resource: null
            resource_type: null
            sequence_order: null
            stream: null
            variable: O2
            year_offset: null
        description: Catalog from cesm_dple_test_collection collection
        driver: cesm
        metadata:
        cache: {}
        catalog_dir: ''

    >>> ds = cat.to_xarray()
    >>> ds
    <xarray.Dataset>
    Dimensions:  (ens: 1, nlat: 5, nlon: 5, sigma: 5, time: 12)
    Coordinates:
    * sigma    (sigma) float64 23.4 23.45 23.5 23.55 23.6
    * time     (time) float64 9.092e+04 9.094e+04 ... 9.122e+04 9.125e+04
    Dimensions without coordinates: ens, nlat, nlon
    Data variables:
        TLAT     (nlat, nlon) float64 dask.array<shape=(5, 5), chunksize=(5, 5)>
        ULONG    (nlat, nlon) float64 dask.array<shape=(5, 5), chunksize=(5, 5)>
        ULAT     (nlat, nlon) float64 dask.array<shape=(5, 5), chunksize=(5, 5)>
        TLONG    (nlat, nlon) float64 dask.array<shape=(5, 5), chunksize=(5, 5)>
        O2       (ens, time, sigma, nlat, nlon) float32 dask.array<shape=(1, 12, 5, 5, 5), chunksize=(1, 1, 5, 5, 5)>
    >>> 