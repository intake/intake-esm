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
    ({'A': None}, None, [{'A': np.nan, 'B': np.nan, 'C': 'exp', 'D': 'UA'}]),
    ({'A': np.nan}, None, [{'A': np.nan, 'B': np.nan, 'C': 'exp', 'D': 'UA'}]),
]


@pytest.mark.parametrize('query, require_all_on, expected', params)
def test_search(query, require_all_on, expected):
    df = pd.DataFrame(
        {
            'A': ['NCAR', 'IPSL', 'IPSL', 'CSIRO', 'IPSL', 'NCAR', 'NOAA', 'NCAR', 'NASA', np.nan],
            'B': ['CESM', 'FOO', 'FOO', 'BAR', 'FOO', 'CESM', 'GCM', 'WACM', 'foo', np.nan],
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
            dict(variable=['A', 'C'], random='.*z'),
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
            dict(variable=['A', 'C']),
            [
                {
                    'path': 'file1',
                    'variable': ['A', 'B'],
                    'attr': 1,
                },
                {
                    'path': 'file2',
                    'variable': ['A', 'B', 'C'],
                    'attr': 2,
                },
                {
                    'attr': 3,
                    'path': 'file3',
                    'variable': [
                        'C',
                        'D',
                        'A',
                    ],
                },
            ],
        ),
        (
            dict(variable=['A', 'C'], attr=[1, 2]),
            [
                {
                    'path': 'file1',
                    'variable': ['A', 'B'],
                    'attr': 1,
                },
                {
                    'path': 'file2',
                    'variable': ['A', 'B', 'C'],
                    'attr': 2,
                },
            ],
        ),
    ],
)
def test_search_columns_with_iterables_str_specified(query, expected):
    df = pd.DataFrame(
        {
            'path': ['file1', 'file2', 'file3'],
            'variable': [['A', 'B'], ['A', 'B', 'C'], ['C', 'D', 'A']],
            'attr': [1, 2, 3],
        }
    )

    query_model = QueryModel(query=query, columns=df.columns.tolist())

    lf = pl.from_pandas(df).lazy()

    # This mirrors a setup step in the esmcat.search function which preserves dtypes.
    # If altering this test, ensure that the dtypes are preserved here as well!
    iterable_dtypes = {colname: type(df[colname].iloc[0]) for colname in {'variable'}}

    results = search(
        df=df,
        query=query_model.query,
        columns_with_iterables={
            'variable',
        },
    )

    results_pl = pl_search(
        lf=lf,
        query=query_model.query,
        columns_with_iterables='variable',
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


def test_pattern_itercol_raises():
    """
    If we try to use pattern matching within iterable columns, we should raise a NotImplementedError.
    """

    df = pd.DataFrame(
        {
            'path': ['file1', 'file2', 'file3'],
            'variable': [['A', 'B'], ['A', 'B', 'C'], ['C', 'D', 'A']],
            'attr': [1, 2, 3],
        }
    )

    query_model = QueryModel(query=dict(variable=[re.compile('^A$')]), columns=df.columns.tolist())

    lf = pl.from_pandas(df).lazy()

    with pytest.raises(
        NotImplementedError,
        match='Pattern matching within iterable columns is not implemented yet.',
    ):
        pl_search(
            lf=lf,
            query=query_model.query,
            columns_with_iterables={'variable'},
            iterable_dtypes={'variable': list},
        )


def test_numpy_dtypes_coerced_to_tuples():
    """
    If we have a column with numpy array values, we should coerce those to tuples
    in the pandas dataframe. These numpy dtypes can arise from polars <=> pandas
    conversions & from parquet serialised catalogues.
    """

    lf = pl.LazyFrame(
        {
            'path': ['file1', 'file2', 'file3'],
            'variable': [
                np.array(['A', 'B'], dtype=object),
                np.array(['A', 'B', 'C'], dtype=object),
                np.array(['C', 'D', 'A'], dtype=object),
            ],
            'attr': [1, 2, 3],
        }
    )

    query_model = QueryModel(query=dict(variable=['A', 'C']), columns=['path', 'variable', 'attr'])

    results_pl = pl_search(
        lf=lf,
        query=query_model.query,
        columns_with_iterables={'variable'},
        iterable_dtypes={'variable': np.ndarray},
    )

    assert isinstance(results_pl['variable'][0], tuple)
    assert isinstance(results_pl['variable'][0][0], str)
