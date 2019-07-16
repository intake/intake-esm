Command Line Tool
=================


``intake-esm`` provides a CLI tool for building collection catalogs from the command line:

.. code-block:: bash

    $ intake-esm-builder --help

    Usage: intake-esm-builder [OPTIONS]

    Options:
    -cdef, --collection-input-definition TEXT
                                    Path to a collection input YAML file or a
                                    name of supported collection input(see:
                                    https://github.com/NCAR/intake-esm-
                                    datastore) for list of supported collection
                                    inputs.
    --overwrite-existing            Whether or not to overwrite the existing
                                    database file.  [default: False]
    -db, --database-dir TEXT        Directory in which to persist the built
                                    collection database  [default: /glade/u/home
                                    /abanihi/.intake_esm/collections]
    --anon / --no-anon              Access the AWS-S3 filesystem anonymously or
                                    not
    --profile-name TEXT             Named profile to use when authenticating
    --help                          Show this message and exit.
