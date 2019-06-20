import os
import subprocess

here = os.path.abspath(os.path.dirname(__file__))


def test_basic():

    collection_input_definition = os.path.join(here, 'copy-to-cache-collection-input.yml')
    p = subprocess.Popen(
        [
            'intake-esm-builder',
            '--collection-input-definition',
            collection_input_definition,
            '--overwrite-existing',
        ],
        stdin=subprocess.DEVNULL,
    )

    p.communicate()

    assert p.returncode == 0


def test_empty_input():
    p = subprocess.Popen(['intake-esm-builder'], stdin=subprocess.DEVNULL)

    p.communicate()

    assert p.returncode != 0
