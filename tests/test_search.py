import re

import numpy as np
import pandas as pd
import polars as pl
import pytest
from pandas.testing import assert_frame_equal

from intake_esm._search import (
    is_pattern,
    pl_search,
    search,
    search_apply_require_all_on,
)
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
    ({'A': None}, None, [{'A': None, 'B': None, 'C': 'exp', 'D': 'UA'}]),
    ({'A': np.nan}, None, [{'A': None, 'B': None, 'C': 'exp', 'D': 'UA'}]),
]


@pytest.mark.parametrize('query, require_all_on, expected', params)
def test_search(query, require_all_on, expected):
    df = pd.DataFrame(
        {
            'A': ['NCAR', 'IPSL', 'IPSL', 'CSIRO', 'IPSL', 'NCAR', 'NOAA', 'NCAR', 'NASA', None],
            'B': ['CESM', 'FOO', 'FOO', 'BAR', 'FOO', 'CESM', 'GCM', 'WACM', 'foo', None],
            'C': [
                'hist',
                'control',
                'hist',
                'control',
                'hist',
                'control',
                'hist',
                'hist',
                'HiSt',
                'exp',
            ],
            'D': ['O2', 'O2', 'O2', 'O2', 'NO2', 'O2', 'O2', 'TA', 'tAs', 'UA'],
        }
    )
    query_model = QueryModel(
        query=query, columns=df.columns.tolist(), require_all_on=require_all_on
    )
    results = search(df=df, query=query_model.query, columns_with_iterables=set())

    lf = pl.from_pandas(df).lazy()
    results_pl = pl_search(lf=lf, query=query_model.query, columns_with_iterables=set())
    assert_frame_equal(results_pl, results)

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
            'random': [{'bx', 'by'}, {'bx', 'bz'}, {'bx', 'by'}],
        }
    )

    query_model = QueryModel(query=query, columns=df.columns.tolist())

    lf = pl.from_pandas(df).lazy()

    # This mirrors a setup step in the esmcat.search function which preserves dtypes.
    # If altering this test, ensure that the dtypes are preserved here as well!
    iterable_dtypes = {colname: type(df[colname].iloc[0]) for colname in {'variable', 'random'}}

    results = search(df=df, query=query_model.query, columns_with_iterables={'variable', 'random'})

    results_pl = pl_search(
        lf=lf,
        query=query_model.query,
        columns_with_iterables={'variable', 'random'},
        iterable_dtypes=iterable_dtypes,
    )
    assert_frame_equal(results_pl, results)
    assert results.to_dict(orient='records') == expected


@pytest.mark.parametrize(
    'query,expected',
    [
        (
            dict(variable=['A', 'B'], random='bx'),
            [
                {'path': 'file1', 'variable': ['A', 'B'], 'attr': 1, 'random': {'bx', 'by'}},
                {'path': 'file3', 'variable': ['A'], 'attr': 2, 'random': {'bx', 'bz'}},
                {'path': 'file4', 'variable': ['B', 'C'], 'attr': 2, 'random': {'bx', 'bz'}},
            ],
        ),
    ],
)
def test_search_require_all_on_columns_with_iterables(query, expected):
    df = pd.DataFrame(
        {
            'path': ['file1', 'file2', 'file3', 'file4', 'file5'],
            'variable': [['A', 'B'], ['C', 'D'], ['A'], ['B', 'C'], ['C', 'D', 'A']],
            'attr': [1, 1, 2, 2, 3],
            'random': [
                {'bx', 'by'},
                {'bx', 'by'},
                {'bx', 'bz'},
                {'bx', 'bz'},
                {'bx', 'by'},
            ],
        }
    )
    query_model = QueryModel(query=query, columns=df.columns.tolist(), require_all_on=['attr'])

    results = search(df=df, query=query_model.query, columns_with_iterables={'variable', 'random'})

    lf = pl.from_pandas(df).lazy()

    # This mirrors a setup step in the esmcat.search function which preserves dtypes.
    # If altering this test, ensure that the dtypes are preserved here as well!
    iterable_dtypes = {colname: type(df[colname].iloc[0]) for colname in {'variable', 'random'}}

    results_pl = pl_search(
        lf=lf,
        query=query_model.query,
        columns_with_iterables={'variable', 'random'},
        iterable_dtypes=iterable_dtypes,
    )
    assert_frame_equal(results_pl, results)

    results_pl = search_apply_require_all_on(
        df=results_pl,
        query=query_model.query,
        require_all_on=query_model.require_all_on,
        columns_with_iterables={'variable', 'random'},
    )

    results = search_apply_require_all_on(
        df=results,
        query=query_model.query,
        require_all_on=query_model.require_all_on,
        columns_with_iterables={'variable', 'random'},
    )

    assert_frame_equal(results_pl, results)

    assert results.to_dict(orient='records') == expected
