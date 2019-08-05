=================
Changelog History
=================

Intake-esm v2019.8.5 (2019-08-05)
==================================


Features
--------

- Support building collections using inputs from intake-esm-datastore repository.
  (:pr:`79`) `Anderson Banihirwe`_

- Ensure that requested files are available locally before loading data into xarray datasets.
  (:pr:`82`) `Anderson Banihirwe`_ and `Matthew Long`_

- Split collection definitions out of config. (:pr:`83`) `Matthew Long`_

- Add ``intake-esm-builder``, a CLI tool for building collection from the command line. (:pr:`89`) `Anderson Banihirwe`_

- Add support for CESM-LENS data holdings residing in AWS S3. (:pr:`98`) `Anderson Banihirwe`_

- Sort collection upon creation according to order-by-columns, pass urlpath through stack for use in parsing collection filenames (:pr:`100`) `Paul Branson`_

Bug Fixes
----------

- Fix bug in ``_list_files_hsi()`` to return list instead of filter object.
  (:pr:`81`) `Matthew Long`_ and `Anderson Banihirwe`_

- ``cesm._get_file_attrs`` fixed to break loop when longest `stream` is matched. (:pr:`80`) `Matthew Long`_

- Restore ``non_dim_coords`` to data variables all the time. (:pr:`90`) `Anderson Banihirwe`_

- Fix bug in ``intake_esm/cesm.py`` that caused ``intake-esm`` to exclude hourly (1hr, 6hr, etc..) CESM-LE data.
  (:pr:`110`) `Anderson Banihirwe`_

- Fix bugs in ``intake_esm/cmip.py`` that caused improper regular expression matching for ``table_id`` and ``grid_label``.
  (:pr:`113`) & (:issue:`111`) `Naomi Henderson`_ and `Anderson Banihirwe`_


Internal Changes
----------------

- Refactor existing functionality to make intake-esm robust and extensible. (:pr:`77`) `Anderson Banihirwe`_

- Add ``aggregate._override_coords`` function to override dim coordinates except time
  in case there's floating point precision difference. (:pr:`108`) `Anderson Banihirwe`_

- Fix CESM-LE ice component peculiarities that caused intake-esm to load data improperly.
  The fix separates variables for `ice` component into two separate components:

  - ``ice_sh``: for southern hemisphere
  - ``ice_nh``: for northern hemisphere

  (:pr:`114`) `Anderson Banihirwe`_


Intake-esm v2019.5.11 (2019-05-11)
===================================


Features
---------

- Add implementation for The Gridded Meteorological Ensemble Tool (GMET) data holdings (:pr:`61`) `Anderson Banihirwe`_
- Allow users to specify exclude_dirs for CMIP collections (:pr:`63`) & (:issue:`62`) `Anderson Banihirwe`_
- Keep CMIP6 ``tracking_id`` in merge_keys (:pr:`67`) `Anderson Banihirwe`_
- Add implementation for ERA5 datasets (:pr:`68`) `Anderson Banihirwe`_


Intake-esm v2019.4.26 (2019-04-26)
===================================


Features
---------

- Add implementations for ``CMIPCollection`` and ``CMIPSource`` (:pr:`38`) `Anderson Banihirwe`_
- Add support for CMIP6 data (:pr:`46`) `Anderson Banihirwe`_
- Add implementation for The Max Planck Institute Grand Ensemble (MPI-GE) data holdings (:pr:`52`) & (:issue:`51`) `Aaron Spring`_ and `Anderson Banihirwe`_
- Return dictionary of datasets all the time for consistency (:pr:`56`) `Anderson Banihirwe`_

Bug Fixes
----------

- Include multiple netcdf files in same subdirectory (:pr:`55`) & (:issue:`54`) `Naomi Henderson`_ and `Anderson Banihirwe`_


Intake-esm v2019.2.28 (2019-02-28)
===================================


Features
---------

- Allow CMIP integration (:pr:`35`) `Anderson Banihirwe`_

Bug Fixes
----------

- Fix bug on build catalog and move `exclude_dirs` to `locations` (:pr:`33`) `Matthew Long`_


Trivial/Internal Changes
------------------------

- Change Logger, update dev-environment dependencies, and formatting fix in input.yml (:pr:`31`) `Matthew Long`_
- Update CircleCI workflow (:pr:`32`) `Anderson Banihirwe`_
- Rename package from `intake-cesm` to `intake-esm` (:pr:`34`) `Anderson Banihirwe`_


.. _`Aaron Spring`: https://github.com/aaronspring
.. _`Anderson Banihirwe`: https://github.com/andersy005
.. _`Matthew Long`: https://github.com/matt-long
.. _`Naomi Henderson`: https://github.com/naomi-henderson
.. _`Paul Branson`: https://github.com/pbranson
