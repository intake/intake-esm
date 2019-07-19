Configuration
=============

Taking full advantage of ``intake-esm`` sometimes requires user configuration.
This might be to control location on which to store the built collection catalogues, etc...


Configuration is specified in one of the following ways:

1.  YAML files in ``~/.intake_esm/`` or ``./intake_esm/``
2.  Environment variables like ``INTAKE_ESM_DATABASE_DIRECTORY=/tmp/intake_esm``

This combination makes it easy to specify configuration in a variety of
settings.



Access Configuration
--------------------

.. currentmodule:: intake_esm

.. autosummary::
   intake_esm.config.get

Configuration is usually read by using the ``intake_esm.config`` module, either with
the ``config`` dictionary or the ``get`` function:

.. ipython:: python

   import intake_esm
   intake_esm.config.config
   intake_esm.config.get('collections.cmip5')
   intake_esm.config.get('sources')


You may wish to inspect the ``intake_esm.config.config`` dictionary to get a sense
for what configuration is being used by your current system.

Note that the ``get`` function treats underscores and hyphens identically.
For example, ``intake_esm.config.get('collections.cmip5.collection_columns')`` is equivalent to
``intake_esm.config.get('collections.cmip5.collection-columns')``.

.. ipython:: python

   intake_esm.config.get('collections.cmip5.collection_columns')
   intake_esm.config.get('collections.cmip5.collection-columns')


Specify Configuration
---------------------


Directly within Python
~~~~~~~~~~~~~~~~~~~~~~

.. autosummary::
   intake_esm.config.set

Configuration is stored within a normal Python dictionary in
``intake_esm.config.config`` and can be modified using normal Python operations.

Additionally, you can temporarily set a configuration value using the
``intake_esm.config.set`` function.  This function accepts a dictionary as an input
and interprets ``"."`` as nested access:


.. ipython:: python

   intake_esm.config.set({'database-directory': './tests/test_collections'})

This function can also be used as a context manager for consistent cleanup:


.. ipython:: python

   with intake_esm.config.set({'database-directory': './tests/test_collections'}):
       pass


Note that the ``set`` function treats underscores and hyphens identically.
For example, ``intake_esm.config.set({'database-directory': './tests/test_collections'})`` is
equivalent to ``intake_esm.config.set({'database_directory': './tests/test_collections'})``.


API
---

.. autofunction:: intake_esm.config.get
.. autofunction:: intake_esm.config.set
