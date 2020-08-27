import re

import numpy as np
import pandas as pd
import pytest

from intake_esm.search import _is_pattern, _normalize_query, _unique, search


def test_unique():
    df = pd.DataFrame(
        {
            'path': ['file1', 'file2', 'file3', 'file4'],
            'variable': [['A', 'B'], ['A', 'B', 'C'], ['C', 'D', 'A'], 'C'],
            'attr': [1, 2, 3, np.nan],
            'random': [set(['bx', 'by']), set(['bx', 'bz']), set(['bx', 'by']), None],
        }
    )
    expected = {
        'path': {'count': 4, 'values': ['file1', 'file2', 'file3', 'file4']},
        'variable': {'count': 4, 'values': ['A', 'B', 'C', 'D']},
        'attr': {'count': 3, 'values': [1.0, 2.0, 3.0]},
        'random': {'count': 3, 'values': ['bx', 'by', 'bz']},
    }
    actual = _unique(df, df.columns.tolist())
    assert actual == expected

    actual = _unique(df)
    assert actual == expected

    actual = _unique(df, columns='random')
    expected = {'random': {'count': 3, 'values': ['bx', 'by', 'bz']}}
    assert actual == expected


params = [
    ({}, None, []),
    (
        {'C': ['control', 'hist']},
        ['B', 'D'],
        [
            {'A': 'NCAR', 'B': 'CESM', 'C': 'hist', 'D': 'O2'},
            {'A': 'NCAR', 'B': 'CESM', 'C': 'control', 'D': 'O2'},
            {'A': 'IPSL', 'B': 'FOO', 'C': 'control', 'D': 'O2'},
            {'A': 'IPSL', 'B': 'FOO', 'C': 'hist', 'D': 'O2'},
        ],
    ),
    ({'C': ['control', 'hist'], 'D': ['NO2']}, 'B', []),
    (
        {'C': ['control', 'hist'], 'D': ['O2']},
        'B',
        [
            {'A': 'NCAR', 'B': 'CESM', 'C': 'hist', 'D': 'O2'},
            {'A': 'NCAR', 'B': 'CESM', 'C': 'control', 'D': 'O2'},
            {'A': 'IPSL', 'B': 'FOO', 'C': 'control', 'D': 'O2'},
            {'A': 'IPSL', 'B': 'FOO', 'C': 'hist', 'D': 'O2'},
        ],
    ),
    (
        {'C': ['hist'], 'D': ['NO2', 'O2']},
        'B',
        [
            {'A': 'IPSL', 'B': 'FOO', 'C': 'hist', 'D': 'O2'},
            {'A': 'IPSL', 'B': 'FOO', 'C': 'hist', 'D': 'NO2'},
        ],
    ),
    (
        {'C': 'hist', 'D': ['NO2', 'O2']},
        'B',
        [
            {'A': 'IPSL', 'B': 'FOO', 'C': 'hist', 'D': 'O2'},
            {'A': 'IPSL', 'B': 'FOO', 'C': 'hist', 'D': 'NO2'},
        ],
    ),
    (
        {'C': 'hist', 'D': ['NO2', 'O2'], 'B': 'FOO'},
        ['B'],
        [
            {'A': 'IPSL', 'B': 'FOO', 'C': 'hist', 'D': 'O2'},
            {'A': 'IPSL', 'B': 'FOO', 'C': 'hist', 'D': 'NO2'},
        ],
    ),
    (
        {'C': ['control']},
        None,
        [
            {'A': 'IPSL', 'B': 'FOO', 'C': 'control', 'D': 'O2'},
            {'A': 'CSIRO', 'B': 'BAR', 'C': 'control', 'D': 'O2'},
            {'A': 'NCAR', 'B': 'CESM', 'C': 'control', 'D': 'O2'},
        ],
    ),
    (
        {'D': [re.compile(r'^O2$'), 'NO2'], 'B': ['CESM', 'BAR']},
        None,
        [
            {'A': 'NCAR', 'B': 'CESM', 'C': 'hist', 'D': 'O2'},
            {'A': 'CSIRO', 'B': 'BAR', 'C': 'control', 'D': 'O2'},
            {'A': 'NCAR', 'B': 'CESM', 'C': 'control', 'D': 'O2'},
        ],
    ),
    (
        {'C': ['^co.*ol$']},
        None,
        [
            {'A': 'IPSL', 'B': 'FOO', 'C': 'control', 'D': 'O2'},
            {'A': 'CSIRO', 'B': 'BAR', 'C': 'control', 'D': 'O2'},
            {'A': 'NCAR', 'B': 'CESM', 'C': 'control', 'D': 'O2'},
        ],
    ),
    (
        {'C': ['hist'], 'D': ['TA']},
        None,
        [{'A': 'NCAR', 'B': 'WACM', 'C': 'hist', 'D': 'TA'}],
    ),
    (
        {
            'C': [re.compile('hist.*', flags=re.IGNORECASE)],
            'D': [re.compile('TA.*', flags=re.IGNORECASE)],
        },
        None,
        [
            {'A': 'NCAR', 'B': 'WACM', 'C': 'hist', 'D': 'TA'},
            {'A': 'NASA', 'B': 'foo', 'C': 'HiSt', 'D': 'tAs'},
        ],
    ),
]


@pytest.mark.parametrize('query, require_all_on, expected', params)
def test_search(query, require_all_on, expected):
    df = pd.DataFrame(
        {
            'A': ['NCAR', 'IPSL', 'IPSL', 'CSIRO', 'IPSL', 'NCAR', 'NOAA', 'NCAR', 'NASA'],
            'B': ['CESM', 'FOO', 'FOO', 'BAR', 'FOO', 'CESM', 'GCM', 'WACM', 'foo'],
            'C': ['hist', 'control', 'hist', 'control', 'hist', 'control', 'hist', 'hist', 'HiSt'],
            'D': ['O2', 'O2', 'O2', 'O2', 'NO2', 'O2', 'O2', 'TA', 'tAs'],
        }
    )

    x = search(df, require_all_on=require_all_on, **query).to_dict(orient='records')
    assert x == expected


def test_normalize_query():
    query = {
        'experiment_id': ['historical', 'piControl'],
        'variable_id': 'tas',
        'table_id': 'Amon',
    }
    expected = {
        'experiment_id': ['historical', 'piControl'],
        'variable_id': ['tas'],
        'table_id': ['Amon'],
    }
    actual = _normalize_query(query)
    assert actual == expected


@pytest.mark.parametrize(
    'value, expected',
    [
        (2, False),
        ('foo', False),
        ('foo\\**bar', True),
        ('foo\\?*bar', True),
        ('foo\\?\\*bar', False),
        ('foo\\*bar', False),
        (r'foo\*bar*', True),
        ('^foo', True),
        ('^foo.*bar$', True),
        (re.compile('hist.*', flags=re.IGNORECASE), True),
    ],
)
def test_is_pattern(value, expected):
    assert _is_pattern(value) == expected
