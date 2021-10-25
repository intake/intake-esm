import re

import pandas as pd
import pytest

from intake_esm._search import is_pattern, search, search_apply_require_all_on
from intake_esm.cat import QueryModel


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
    assert is_pattern(value) == expected


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
    query_model = QueryModel(
        query=query, columns=df.columns.tolist(), require_all_on=require_all_on
    )
    results = search(df=df, query=query_model.query, columns_with_iterables=set())
    assert isinstance(results, pd.DataFrame)
    if require_all_on:
        results = search_apply_require_all_on(
            df=results, query=query_model.query, require_all_on=query_model.require_all_on
        )
    assert results.to_dict(orient='records') == expected


@pytest.mark.parametrize(
    'query,expected',
    [
        (
            dict(variable=['A', 'C'], random='bz'),
            [{'path': 'file2', 'variable': ['A', 'B', 'C'], 'attr': 2, 'random': {'bx', 'bz'}}],
        ),
        (
            dict(variable=['A', 'C'], attr=[1, 2]),
            [
                {'path': 'file1', 'variable': ['A', 'B'], 'attr': 1, 'random': {'bx', 'by'}},
                {'path': 'file2', 'variable': ['A', 'B', 'C'], 'attr': 2, 'random': {'bx', 'bz'}},
            ],
        ),
    ],
)
def test_search_columns_with_iterables(query, expected):
    df = pd.DataFrame(
        {
            'path': ['file1', 'file2', 'file3'],
            'variable': [['A', 'B'], ['A', 'B', 'C'], ['C', 'D', 'A']],
            'attr': [1, 2, 3],
            'random': [set(['bx', 'by']), set(['bx', 'bz']), set(['bx', 'by'])],
        }
    )
    query_model = QueryModel(query=query, columns=df.columns.tolist())
    results = search(
        df=df, query=query_model.query, columns_with_iterables={'variable', 'random'}
    ).to_dict(orient='records')
    assert results == expected
