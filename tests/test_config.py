import os
import stat
import sys
from collections import OrderedDict
from contextlib import contextmanager

import pytest
import yaml
from dask.utils import tmpfile

from intake_esm import config as _config
from intake_esm.config import (
    collect,
    collect_env,
    collect_yaml,
    ensure_file,
    expand_environment_variables,
    get,
    merge,
    normalize_key,
    normalize_nested_keys,
    refresh,
    rename,
    set,
    update,
    update_defaults,
)


def test_update():
    a = {'x': 1, 'y': {'a': 1}}
    b = {'x': 2, 'z': 3, 'y': OrderedDict({'b': 2})}
    update(b, a)
    assert b == {'x': 1, 'y': {'a': 1, 'b': 2}, 'z': 3}

    a = {'x': 1, 'y': {'a': 1}}
    b = {'x': 2, 'z': 3, 'y': {'a': 3, 'b': 2}}
    update(b, a, priority='old')
    assert b == {'x': 2, 'y': {'a': 3, 'b': 2}, 'z': 3}


def test_expand_environment_variables():

    expected_user = os.environ.get('USER', None)
    if expected_user is None:
        os.environ['USER'] = 'root'
        expected_user = 'root'
    expected_output = {'x': [1, 2, expected_user]}
    a = expand_environment_variables({'x': [1, 2, '$USER']})
    assert a == expected_output


def test_merge():
    a = {'x': 1, 'y': {'a': 1}}
    b = {'x': 2, 'z': 3, 'y': {'b': 2}}

    expected = {'x': 2, 'y': {'a': 1, 'b': 2}, 'z': 3}

    c = merge(a, b)
    assert c == expected


def test_collect_yaml_paths():
    a = {'x': 1, 'y': {'a': 1}}
    b = {'x': 2, 'z': 3, 'y': {'b': 2}}

    expected = {'x': 2, 'y': {'a': 1, 'b': 2}, 'z': 3}

    with tmpfile(extension='yaml') as fn1:
        with tmpfile(extension='yaml') as fn2:
            with open(fn1, 'w') as f:
                yaml.dump(a, f)
            with open(fn2, 'w') as f:
                yaml.dump(b, f)

            config = merge(*collect_yaml(paths=[fn1, fn2]))
            assert config == expected


def test_collect_yaml_dir():
    a = {'x': 1, 'y': {'a': 1}}
    b = {'x': 2, 'z': 3, 'y': {'b': 2}}

    expected = {'x': 2, 'y': {'a': 1, 'b': 2}, 'z': 3}

    with tmpfile() as dirname:
        os.mkdir(dirname)
        with open(os.path.join(dirname, 'a.yaml'), mode='w') as f:
            yaml.dump(a, f)
        with open(os.path.join(dirname, 'b.yaml'), mode='w') as f:
            yaml.dump(b, f)

        config = merge(*collect_yaml(paths=[dirname]))
        assert config == expected


@contextmanager
def no_read_permissions(path):
    perm_orig = stat.S_IMODE(os.stat(path).st_mode)
    perm_new = perm_orig ^ stat.S_IREAD
    try:
        os.chmod(path, perm_new)
        yield
    finally:
        os.chmod(path, perm_orig)


def test_default_config():
    assert isinstance(_config.get('collections')['cesm'], dict)


def test_set_options():
    _config.set({'database_directory': '/tmp/collections'})
    s1 = _config.get('database_directory')
    assert s1 == os.path.abspath(os.path.expanduser('/tmp/collections'))
    _config.set({'database_directory': '/tests/test_collections'})
    s2 = _config.get('database_directory')

    assert not s1 == s2


def test_collect_env():
    env = {}
    env['INTAKE_ESM_FOO__BAR_BAZ'] = 123

    results = collect_env(env)
    expected = {'foo': {'bar-baz': 123}}
    assert expected == results
