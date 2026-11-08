import re

import numpy as np
import pandas as pd
import polars as pl
import pytest
from pandas.testing import assert_frame_equal

import intake_esm._search
from intake_esm._search import (
    is_pattern,
    pl_search,
    search,
    search_apply_require_all_on,
)
from intake_esm.cat import QueryModel


def arrow_result(frame, iterable_columns=(), schema=None):
    """Expected Arrow scalar storage, retaining explicitly requested containers."""
    expected = pl.from_pandas(frame, schema_overrides=schema).to_pandas(
        use_pyarrow_extension_array=True
    )
    for column in iterable_columns:
        expected[column] = frame[column]
    return expected


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
    assert_frame_equal(
        results_pl,
        arrow_result(results, schema=lf.collect_schema()) if query_model.query else results,
        check_dtype=bool(query_model.query),
    )

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
    assert_frame_equal(
        results_pl, arrow_result(results, {'variable', 'random'} & set(results.columns))
    )
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
    assert_frame_equal(
        results_pl, arrow_result(results, {'variable', 'random'} & set(results.columns))
    )
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
    assert_frame_equal(
        results_pl, arrow_result(results, {'variable', 'random'} & set(results.columns))
    )

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

    assert_frame_equal(
        results_pl, arrow_result(results, {'variable', 'random'} & set(results.columns))
    )

    assert results.to_dict(orient='records') == expected


def test_pl_search_matches_literals_exactly_alongside_patterns():
    """
    A literal in a query containing a pattern must not become a substring regex.
    """
    lf = pl.LazyFrame({'D': ['O2', 'NO2', 'XNO2']})
    actual = pl_search(
        lf=lf, query={'D': [re.compile('^O2$'), 'NO2']}, columns_with_iterables=set()
    )
    assert actual['D'].tolist() == ['O2', 'NO2']


def test_pl_search_scopes_ignorecase_to_its_pattern():
    """
    A case insensitive pattern must not change how other patterns match.
    """
    lf = pl.LazyFrame({'C': ['HIST', 'ctl1', 'CTL1']})
    actual = pl_search(
        lf=lf,
        query={'C': [re.compile('hist', re.IGNORECASE), '^ctl']},
        columns_with_iterables=set(),
    )
    assert actual['C'].tolist() == ['HIST', 'ctl1']


def test_pl_search_runs_portable_iterable_regex_in_polars(monkeypatch):
    """
    Regex which Polars can run must not fall back to materialising in pandas.
    """

    def fail(**kwargs):
        raise AssertionError('search should not be used for a portable pattern')

    monkeypatch.setattr('intake_esm._search.search', fail)
    lf = pl.LazyFrame({'variable': [['aice_m', 'hi_m'], ['sst']]})
    actual = pl_search(lf=lf, query={'variable': ['^aice.*']}, columns_with_iterables={'variable'})
    assert len(actual) == 1


@pytest.mark.parametrize(
    'pattern',
    [
        '^(?!sst).*',
        re.compile('^(?!sst).*'),
        re.compile('^aice', re.MULTILINE),
    ],
)
def test_pl_search_uses_search_for_patterns_polars_cannot_run(pattern, mocker):
    """
    Look-around needs Python's re, and Polars doesn't apply flags beyond IGNORECASE.
    """
    spy = mocker.spy(intake_esm._search, 'search')
    lf = pl.LazyFrame({'variable': [['aice_m', 'hi_m'], ['sst']], 'name': ['aice_m', 'sst']})
    for column in ('variable', 'name'):
        actual = pl_search(lf=lf, query={column: [pattern]}, columns_with_iterables={'variable'})
        assert actual['name'].tolist() == ['aice_m']
    assert spy.call_count == 2


@pytest.mark.filterwarnings('error::FutureWarning')
def test_search_regex_over_nulls_in_object_column():
    """
    Regex over nulls in an object column shouldn't rely on deprecated downcasting.
    """
    df = pd.DataFrame({'C': ['hist', None, 'control']}, dtype=object)
    results = search(df=df, query={'C': ['^hi']}, columns_with_iterables=set())
    assert results['C'].tolist() == ['hist']


def test_pattern_itercol_matches_search():
    """
    Iterable regex searches on a lazy frame match the results of `search`.
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

    actual = pl_search(
        lf=lf,
        query=query_model.query,
        columns_with_iterables={'variable'},
        iterable_dtypes={'variable': list},
    )
    expected = search(df=df, query=query_model.query, columns_with_iterables={'variable'})
    assert_frame_equal(actual, arrow_result(expected, {'variable'}))
    assert len(actual) == 3


def test_pl_search_collects_with_streaming_engine(mocker):
    """
    Searches stream without changing the caller's global Polars configuration.
    """
    spy = mocker.spy(pl.LazyFrame, 'collect')
    lf = pl.LazyFrame({'A': ['x', 'y']})
    results = pl_search(lf=lf, query={'A': ['x']}, columns_with_iterables=set())
    assert results['A'].tolist() == ['x']
    assert spy.call_args.kwargs == {'engine': 'streaming'}


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
