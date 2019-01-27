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
    >>> 'cesm_cat' in intake.registry
    True
    >>> cat = intake.open_cesm_cat('cesm1_le')
    Active collection: cesm1_le
    >>> cat.df.head()
                                   case       ...        ctrl_branch_year
    0  b.e11.BRCP85C5CNBDRD.f09_g16.105       ...                     NaN
    1  b.e11.BRCP85C5CNBDRD.f09_g16.105       ...                     NaN
    2  b.e11.BRCP85C5CNBDRD.f09_g16.105       ...                     NaN
    3  b.e11.BRCP85C5CNBDRD.f09_g16.105       ...                     NaN
    4  b.e11.BRCP85C5CNBDRD.f09_g16.105       ...                     NaN

    [5 rows x 14 columns]
    >>> len(cat.df)
    116275
    >>> results = cat.search(experiment=['20C', 'RCP85'], component='ocn', ensemble=1, variable='FG_CO2')
    >>> results
                                        case       ...        ctrl_branch_year
    64401   b.e11.BRCP85C5CNBDRD.f09_g16.001       ...                     NaN
    64402   b.e11.BRCP85C5CNBDRD.f09_g16.001       ...                     NaN
    100755   b.e11.B20TRC5CNBDRD.f09_g16.001       ...                     NaN

    [3 rows x 14 columns]
    >>> cat.set_collection('cesm2_runs')
    Active collection: cesm2_runs
    >>> cat.df.head()
                                                 case      ...      has_ocean_bgc
    0  g.e21a01d.G1850ECOIAF.T62_g17.extraterr-fe.001      ...                NaN
    1  g.e21a01d.G1850ECOIAF.T62_g17.extraterr-fe.001      ...                NaN
    2  g.e21a01d.G1850ECOIAF.T62_g17.extraterr-fe.001      ...                NaN
    3  g.e21a01d.G1850ECOIAF.T62_g17.extraterr-fe.001      ...                NaN
    4  g.e21a01d.G1850ECOIAF.T62_g17.extraterr-fe.001      ...                NaN

    [5 rows x 14 columns]
    >>> len(cat.df)
    45939
