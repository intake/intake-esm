API Reference
=============

This page provides an auto-generated summary of intake-esm’s API.
For more details and examples, refer to the relevant chapters in the main part of the documentation.


Entry Point
~~~~~~~~~~~

.. autosummary::

   intake_esm.core.ESMMetadataStoreCatalog


.. autoclass:: intake_esm.core.ESMMetadataStoreCatalog
   :members:


Collections
~~~~~~~~~~~~

.. autosummary::

   intake_esm.cordex.CORDEXCollection
   intake_esm.cmip.CMIP5Collection
   intake_esm.cmip.CMIP6Collection
   intake_esm.cesm.CESMCollection
   intake_esm.cesm_aws.CESMAWSCollection
   intake_esm.gmet.GMETCollection
   intake_esm.era5.ERA5Collection
   intake_esm.mpige.MPIGECollection


.. autoclass:: intake_esm.cordex.CORDEXCollection
   :members:

.. autoclass:: intake_esm.cmip.CMIP5Collection
   :members:

.. autoclass:: intake_esm.cmip.CMIP6Collection
   :members:

.. autoclass:: intake_esm.cesm.CESMCollection
   :members:

.. autoclass:: intake_esm.cesm_aws.CESMAWSCollection
   :members:

.. autoclass:: intake_esm.gmet.GMETCollection
   :members:

.. autoclass:: intake_esm.era5.ERA5Collection
   :members:

.. autoclass:: intake_esm.mpige.MPIGECollection
   :members:


Sources
~~~~~~~~~~~~

.. autosummary::

   intake_esm.cordex.CORDEXSource
   intake_esm.cmip.CMIP5Source
   intake_esm.cmip.CMIP6Source
   intake_esm.cesm.CESMSource
   intake_esm.cesm_aws.CESMAWSSource
   intake_esm.gmet.GMETSource
   intake_esm.era5.ERA5Source
   intake_esm.mpige.MPIGESource


.. autoclass:: intake_esm.cordex.CORDEXSource
   :members:

.. autoclass:: intake_esm.cmip.CMIP5Source
   :members:

.. autoclass:: intake_esm.cmip.CMIP6Source
   :members:

.. autoclass:: intake_esm.cesm.CESMSource
   :members:

.. autoclass:: intake_esm.cesm_aws.CESMAWSSource
   :members:

.. autoclass:: intake_esm.gmet.GMETSource
   :members:

.. autoclass:: intake_esm.era5.ERA5Source
   :members:

.. autoclass:: intake_esm.mpige.MPIGESource
   :members:


Storage Resource
~~~~~~~~~~~~~~~~

.. autosummary::

   intake_esm.storage.StorageResource

.. autoclass:: intake_esm.storage.StorageResource
   :members:


Utilities to support merge/concat of datasets.
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autosummary::

   intake_esm.aggregate.ensure_time_coord_name
   intake_esm.aggregate.dict_union
   intake_esm.aggregate.merge_vars_two_datasets
   intake_esm.aggregate.merge
   intake_esm.aggregate.concat_time_levels
   intake_esm.aggregate.concat_ensembles
   intake_esm.aggregate.set_coords
   intake_esm.aggregate.open_dataset
   intake_esm.aggregate.open_store


.. autofunction:: intake_esm.aggregate.ensure_time_coord_name
.. autofunction:: intake_esm.aggregate.dict_union
.. autofunction:: intake_esm.aggregate.merge_vars_two_datasets
.. autofunction:: intake_esm.aggregate.merge
.. autofunction:: intake_esm.aggregate.concat_time_levels
.. autofunction:: intake_esm.aggregate.concat_ensembles
.. autofunction:: intake_esm.aggregate.set_coords
.. autofunction:: intake_esm.aggregate.open_dataset
.. autofunction:: intake_esm.aggregate.open_store
