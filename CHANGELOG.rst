=================
Changelog History
=================

Intake-esm v2019.6.xx (2019-06-xx)
==================================


Features
--------

- Support building collections using inputs from intake-esm-datastore repository.
  (:pr:`79`) `Anderson Banihirwe`_

- Ensure that requested files are available locally before loading data into xarray datasets.
  (:pr:`82`) `Anderson Banihirwe`_ and `Matthew Long`_

- Split collection definitions out of config. (:pr:`83`) `Matthew Long`_

- Add ``intake-esm-builder``, a CLI tool for building collection from the command line. (:pr:`89`) `Anderson Banihirwe`_


Bug Fixes
----------

- Fix bug in ``_list_files_hsi()`` to return list instead of filter object.
  (:pr:`81`) `Matthew Long`_ and `Anderson Banihirwe`_

- ``cesm._get_file_attrs`` fixed to break loop when longest `stream` is matched. (:pr:`80`) `Matthew Long`_


Internal Changes
----------------

- Refactor existing functionality to make intake-esm robust and extensible. (:pr:`77`) `Anderson Banihirwe`_



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
