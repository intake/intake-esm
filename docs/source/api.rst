API reference
=============

This is a reference API class listing, and modules.

.. currentmodule:: intake_cesm

.. autosummary::
   manage_collections.StorageResource
   manage_collections.CESMCollections
   core.CesmMetadataStoreCatalog
   core.CesmSource


.. currentmodule:: intake_cesm.manage_collections

.. autoclass:: StorageResource
   :members: __init__

.. autoclass:: CESMCollections
   :members: __init__, get_built_collection

.. currentmodule:: intake_cesm.core

.. autoclass:: CesmMetadataStoreCatalog
   :members: __init__, search 

.. autoclass:: CesmSource
   :members: __init__, to_xarray


