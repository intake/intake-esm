API reference
=============

This is a reference API class listing, and modules.

.. autosummary::
   intake_esm.core.ESMMetadataStoreCatalog
   intake_esm.cmip.CMIPSource
   intake_esm.cmip.CMIPCollection
   intake_esm.cesm.CESMSource
   intake_esm.cesm.CESMCollection


.. autoclass:: intake_esm.core.ESMMetadataStoreCatalog
   :members: search, open_collection


.. autoclass:: intake_esm.cesm.CESMSource
   :members: to_xarray, results

.. autoclass:: intake_esm.cesm.CESMCollection
   :members:

.. autoclass:: intake_esm.cmip.CMIPSource
   :members: to_xarray, results

.. autoclass:: intake_esm.cmip.CMIPCollection
   :members:
