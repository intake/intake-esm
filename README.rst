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
    >>> collection = intake.open_cesm_metadatastore('cesm1_le')
    Active collection: cesm1_le
    >>> cat = collection.search(experiment=['20C', 'RCP85'], component='ocn', ensemble=1, variable='FG_CO2')
    >>> cat.results
                                        case component  ... year_offset  ctrl_branch_year
    100755   b.e11.B20TRC5CNBDRD.f09_g16.001       ocn  ...         NaN               NaN
    64401   b.e11.BRCP85C5CNBDRD.f09_g16.001       ocn  ...         NaN               NaN
    64402   b.e11.BRCP85C5CNBDRD.f09_g16.001       ocn  ...         NaN               NaN

    [3 rows x 14 columns]
    >>> print(cat.yaml(True))
    plugins:
    source:
    - module: intake_cesm.core
    sources:
    cesm1_le-bd481b86-b627-4f75-9608-235352846296:
        args:
        chunks:
            time: 1
        collection: cesm1_le
        concat_dim: time
        decode_coords: false
        decode_times: false
        engine: netcdf4
        query:
            case: null
            component: ocn
            ctrl_branch_year: null
            date_range: null
            ensemble: 1
            experiment:
            - 20C
            - RCP85
            has_ocean_bgc: null
            stream: null
            variable: FG_CO2
        description: Catalog from cesm1_le collection
        driver: cesm
        metadata:
        cache: {}
        catalog_dir: ''

    >>> ds = cat.to_xarray()
    >>> ds
    <xarray.Dataset>
    Dimensions:               (d2: 2, lat_aux_grid: 395, moc_comp: 3, moc_z: 61, nlat: 384, nlon: 320, time: 3012, transport_comp: 5, transport_reg: 2, z_t: 60, z_t_150m: 15, z_w: 60, z_w_bot: 60, z_w_top: 60)
    Coordinates:
    * lat_aux_grid          (lat_aux_grid) float32 -79.48815 -78.952896 ... 90.0
    * moc_z                 (moc_z) float32 0.0 1000.0 ... 525000.94 549999.06
    * z_t                   (z_t) float32 500.0 1500.0 ... 512502.8 537500.0
    * z_t_150m              (z_t_150m) float32 500.0 1500.0 ... 13500.0 14500.0
    * z_w                   (z_w) float32 0.0 1000.0 2000.0 ... 500004.7 525000.94
    * z_w_bot               (z_w_bot) float32 1000.0 2000.0 ... 549999.06
    * z_w_top               (z_w_top) float32 0.0 1000.0 ... 500004.7 525000.94
    * time                  (time) float64 6.753e+05 6.753e+05 ... 7.669e+05
    Dimensions without coordinates: d2, moc_comp, nlat, nlon, transport_comp, transport_reg
    Data variables:
        ANGLE                 (time, nlat, nlon) float64 dask.array<shape=(3012, 384, 320), chunksize=(1872, 384, 320)>
        ANGLET                (time, nlat, nlon) float64 dask.array<shape=(3012, 384, 320), chunksize=(1872, 384, 320)>
        DXT                   (time, nlat, nlon) float64 dask.array<shape=(3012, 384, 320), chunksize=(1872, 384, 320)>
        DXU                   (time, nlat, nlon) float64 dask.array<shape=(3012, 384, 320), chunksize=(1872, 384, 320)>
        DYT                   (time, nlat, nlon) float64 dask.array<shape=(3012, 384, 320), chunksize=(1872, 384, 320)>
        DYU                   (time, nlat, nlon) float64 dask.array<shape=(3012, 384, 320), chunksize=(1872, 384, 320)>
        FG_CO2                (time, nlat, nlon) float32 dask.array<shape=(3012, 384, 320), chunksize=(1, 384, 320)>
        HT                    (time, nlat, nlon) float64 dask.array<shape=(3012, 384, 320), chunksize=(1872, 384, 320)>
        HTE                   (time, nlat, nlon) float64 dask.array<shape=(3012, 384, 320), chunksize=(1872, 384, 320)>
        HTN                   (time, nlat, nlon) float64 dask.array<shape=(3012, 384, 320), chunksize=(1872, 384, 320)>
        HU                    (time, nlat, nlon) float64 dask.array<shape=(3012, 384, 320), chunksize=(1872, 384, 320)>
        HUS                   (time, nlat, nlon) float64 dask.array<shape=(3012, 384, 320), chunksize=(1872, 384, 320)>
        HUW                   (time, nlat, nlon) float64 dask.array<shape=(3012, 384, 320), chunksize=(1872, 384, 320)>
        KMT                   (time, nlat, nlon) float64 dask.array<shape=(3012, 384, 320), chunksize=(1872, 384, 320)>
        KMU                   (time, nlat, nlon) float64 dask.array<shape=(3012, 384, 320), chunksize=(1872, 384, 320)>
        REGION_MASK           (time, nlat, nlon) float64 dask.array<shape=(3012, 384, 320), chunksize=(1872, 384, 320)>
        T0_Kelvin             (time) float64 273.1 273.1 273.1 ... 273.1 273.1 273.1
        TAREA                 (time, nlat, nlon) float64 dask.array<shape=(3012, 384, 320), chunksize=(1872, 384, 320)>
        TLAT                  (time, nlat, nlon) float64 dask.array<shape=(3012, 384, 320), chunksize=(1872, 384, 320)>
        TLONG                 (time, nlat, nlon) float64 dask.array<shape=(3012, 384, 320), chunksize=(1872, 384, 320)>
        UAREA                 (time, nlat, nlon) float64 dask.array<shape=(3012, 384, 320), chunksize=(1872, 384, 320)>
        ULAT                  (time, nlat, nlon) float64 dask.array<shape=(3012, 384, 320), chunksize=(1872, 384, 320)>
        ULONG                 (time, nlat, nlon) float64 dask.array<shape=(3012, 384, 320), chunksize=(1872, 384, 320)>
        cp_air                (time) float64 1.005e+03 1.005e+03 ... 1.005e+03
        cp_sw                 (time) float64 3.996e+07 3.996e+07 ... 3.996e+07
        days_in_norm_year     (time) float64 365.0 365.0 365.0 ... 365.0 365.0 365.0
        dz                    (time, z_t) float32 dask.array<shape=(3012, 60), chunksize=(1872, 60)>
        dzw                   (time, z_w) float32 dask.array<shape=(3012, 60), chunksize=(1872, 60)>
        fwflux_factor         (time) float64 0.0001 0.0001 0.0001 ... 0.0001 0.0001
        grav                  (time) float64 980.6 980.6 980.6 ... 980.6 980.6 980.6
        heat_to_PW            (time) float64 4.186e-15 4.186e-15 ... 4.186e-15
        hflux_factor          (time) float64 2.439e-05 2.439e-05 ... 2.439e-05
        latent_heat_fusion    (time) float64 3.337e+09 3.337e+09 ... 3.337e+09
        latent_heat_vapor     (time) float64 2.501e+06 2.501e+06 ... 2.501e+06
        mass_to_Sv            (time) float64 1e-12 1e-12 1e-12 ... 1e-12 1e-12 1e-12
        moc_components        (time, moc_comp) |S256 dask.array<shape=(3012, 3), chunksize=(1872, 3)>
        momentum_factor       (time) float64 10.0 10.0 10.0 10.0 ... 10.0 10.0 10.0
        nsurface_t            (time) float64 8.621e+04 8.621e+04 ... 8.621e+04
        nsurface_u            (time) float64 8.305e+04 8.305e+04 ... 8.305e+04
        ocn_ref_salinity      (time) float64 34.7 34.7 34.7 34.7 ... 34.7 34.7 34.7
        omega                 (time) float64 7.292e-05 7.292e-05 ... 7.292e-05
        ppt_to_salt           (time) float64 0.001 0.001 0.001 ... 0.001 0.001 0.001
        radius                (time) float64 6.371e+08 6.371e+08 ... 6.371e+08
        rho_air               (time) float64 1.292 1.292 1.292 ... 1.292 1.292 1.292
        rho_fw                (time) float64 1.0 1.0 1.0 1.0 1.0 ... 1.0 1.0 1.0 1.0
        rho_sw                (time) float64 1.026 1.026 1.026 ... 1.026 1.026 1.026
        salinity_factor       (time) float64 -0.00347 -0.00347 ... -0.00347 -0.00347
        salt_to_Svppt         (time) float64 1e-09 1e-09 1e-09 ... 1e-09 1e-09 1e-09
        salt_to_mmday         (time) float64 3.154e+05 3.154e+05 ... 3.154e+05
        salt_to_ppt           (time) float64 1e+03 1e+03 1e+03 ... 1e+03 1e+03 1e+03
        sea_ice_salinity      (time) float64 4.0 4.0 4.0 4.0 4.0 ... 4.0 4.0 4.0 4.0
        sflux_factor          (time) float64 0.1 0.1 0.1 0.1 0.1 ... 0.1 0.1 0.1 0.1
        sound                 (time) float64 1.5e+05 1.5e+05 ... 1.5e+05 1.5e+05
        stefan_boltzmann      (time) float64 5.67e-08 5.67e-08 ... 5.67e-08 5.67e-08
        time_bound            (time, d2) float64 dask.array<shape=(3012, 2), chunksize=(1, 2)>
        transport_components  (time, transport_comp) |S256 dask.array<shape=(3012, 5), chunksize=(1872, 5)>
        transport_regions     (time, transport_reg) |S256 dask.array<shape=(3012, 2), chunksize=(1872, 2)>
        vonkar                (time) float64 0.4 0.4 0.4 0.4 0.4 ... 0.4 0.4 0.4 0.4
    Attributes:
        title:                     b.e11.B20TRC5CNBDRD.f09_g16.001
        history:                   Sat Aug 31 13:20:44 2013: /glade/apps/opt/nco/...
        Conventions:               CF-1.0; http://www.cgd.ucar.edu/cms/eaton/netc...
        contents:                  Diagnostic and Prognostic Variables
        source:                    CCSM POP2, the CCSM Ocean Component
        revision:                  $Id: tavg.F90 41939 2012-11-14 16:37:23Z mlevy...
        calendar:                  All years have exactly  365 days.
        start_time:                This dataset was created on 2013-05-24 at 14:5...
        cell_methods:              cell_methods = time: mean ==> the variable val...
        nsteps_total:              750
        tavg_sum:                  2592000.0
        tavg_sum_qflux:            2592000.0
        NCO:                       4.3.4
        nco_openmp_thread_number:  1