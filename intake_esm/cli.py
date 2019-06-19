#! /usr/bin/env python
import os

import click

from . import config
from .bld_collection_utils import load_collection_input_file
from .core import ESMMetadataStoreCatalog

# http://click.palletsprojects.com/en/5.x/python3/
# Enforce en_US.utf-8 as the encoding of choice

os.environ['LC_ALL'] = 'C.UTF-8'
os.environ['LANG'] = 'C.UTF-8'

_default_database_dir = config.get('database-directory')


def _builder(collection_input_definition, overwrite_existing, database_dir):
    if not collection_input_definition:
        load_collection_input_file()
        raise ValueError(
            f'\n\n*** Please specify collection input name from the list above '
            'or collection input YAML file. ***'
        )
    with config.set({'database-dir': database_dir}):
        ESMMetadataStoreCatalog(collection_input_definition, overwrite_existing=overwrite_existing)


@click.command()
@click.option(
    '--collection-input-definition',
    '-cdef',
    default=None,
    help='Path to a collection input YAML file '
    'or a name of supported collection input'
    '(see: https://github.com/NCAR/intake-esm-datastore) for list of supported collection inputs.',
)
@click.option(
    '--overwrite-existing',
    default=False,
    is_flag=True,
    help='Whether or not to overwrite the existing database file.',
    show_default=True,
)
@click.option(
    '--database-dir',
    '-db',
    type=str,
    default=_default_database_dir,
    help='Directory in which to persist the built collection database',
    show_default=True,
)
def main(collection_input_definition, overwrite_existing, database_dir):
    _builder(collection_input_definition, overwrite_existing, database_dir)


if __name__ == '__main__':
    main()
