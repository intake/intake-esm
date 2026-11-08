"""Property tests asserting that `pl_search` and `search` agree exactly."""

import re

import pandas as pd
import polars as pl
import pytest
from hypothesis import given, settings, strategies as st
from pandas.testing import assert_frame_equal

from intake_esm._search import pl_search, search, search_apply_require_all_on

# `search` warns about the capture groups our generated patterns can contain.
pytestmark = pytest.mark.filterwarnings('ignore:This pattern is interpreted as a regular')

ITERABLE_COLUMNS = {'variable', 'sizes'}

# A small alphabet makes matches and near-misses common.
words = st.text(alphabet='AaB2', min_size=0, max_size=4)
fragments = st.text(alphabet='AaB2', min_size=1, max_size=2)

# Regex-looking text without an `is_pattern` wildcard is an exact value.
literals = st.one_of(words, st.sampled_from(['A.B', 'A|B', '[AB]', 'A+', '(A)']))

# Syntax on which Python's re and Polars' Rust engine agree.
portable_patterns = st.one_of(
    fragments.map(lambda s: f'^{s}'),
    fragments.map(lambda s: f'{s}$'),
    fragments.map(lambda s: f'^{s}$'),
    fragments.map(lambda s: f'{s}.*'),
    fragments.map(lambda s: f'{s}.?'),
    st.tuples(fragments, fragments).map(lambda p: f'^{p[0]}.*{p[1]}$'),
    st.tuples(fragments, fragments).map(lambda p: f'^({p[0]}|{p[1]})$'),
)

# Syntax only Python's re can compile, which has to reach the fallback.
python_only_patterns = st.one_of(
    fragments.map(lambda s: f'^(?!{s}).*'),
    fragments.map(lambda s: f'({s})\\1*$'),
)

string_patterns = st.one_of(portable_patterns, python_only_patterns)

compiled_patterns = st.builds(
    re.compile,
    string_patterns,
    flags=st.sampled_from([0, re.IGNORECASE, re.MULTILINE]),
)

patterns = st.one_of(string_patterns, compiled_patterns)

# Any column can be queried with any value: mismatched types just don't match.
numbers = st.sampled_from([0, 1, 2, 1.0, 1.5, True, False, float('inf'), float('nan')])
values = st.lists(st.one_of(literals, patterns, numbers, st.none()), min_size=1, max_size=3)

small_ints = st.integers(min_value=0, max_value=2)


def _column(n, elements):
    return st.lists(st.one_of(elements, st.none()), min_size=n, max_size=n)


def _list_column(n, elements):
    return st.lists(
        st.one_of(st.lists(st.one_of(elements, st.none()), min_size=0, max_size=3), st.none()),
        min_size=n,
        max_size=n,
    )


frames = st.integers(min_value=1, max_value=8).flatmap(
    lambda n: st.fixed_dictionaries(
        {
            'name': _column(n, words),
            'group': st.lists(st.sampled_from(['x', 'y']), min_size=n, max_size=n),
            'count': _column(n, small_ints),
            'width': _column(n, small_ints),
            'ratio': _column(n, st.sampled_from([0.0, 1.0, 1.5])),
            'flag': _column(n, st.booleans()),
            'variable': _list_column(n, words),
            'sizes': _list_column(n, small_ints),
        }
    )
)

SCHEMA = {
    'name': pl.String,
    'group': pl.String,
    'count': pl.Int64,
    'width': pl.Int32,
    'ratio': pl.Float64,
    'flag': pl.Boolean,
    'variable': pl.List(pl.String),
    'sizes': pl.List(pl.Int64),
}

queries = st.fixed_dictionaries(
    {},
    optional={
        column: values
        for column in ('name', 'count', 'width', 'ratio', 'flag', 'variable', 'sizes')
    },
).filter(bool)


def build_lazyframe(data: dict) -> pl.LazyFrame:
    return pl.LazyFrame(data, schema=SCHEMA)


def to_arrow(lf: pl.LazyFrame) -> pd.DataFrame:
    return lf.collect().to_pandas(use_pyarrow_extension_array=True)


@settings(deadline=None, max_examples=300)
@given(data=frames, query=queries)
def test_pl_search_matches_search(data, query):
    lf = build_lazyframe(data)
    actual = pl_search(lf=lf, query=query, columns_with_iterables=ITERABLE_COLUMNS)
    expected = search(df=to_arrow(lf), query=query, columns_with_iterables=ITERABLE_COLUMNS)
    assert_frame_equal(actual, expected)


@settings(deadline=None, max_examples=100)
@given(data=frames, query=queries)
def test_pl_search_matches_search_with_require_all_on(data, query):
    lf = build_lazyframe(data)
    actual = pl_search(lf=lf, query=query, columns_with_iterables=ITERABLE_COLUMNS)
    expected = search(df=to_arrow(lf), query=query, columns_with_iterables=ITERABLE_COLUMNS)
    if actual.empty:
        assert expected.empty
        return

    kwargs = dict(query=query, require_all_on=['group'], columns_with_iterables=ITERABLE_COLUMNS)
    assert_frame_equal(
        search_apply_require_all_on(df=actual, **kwargs),
        search_apply_require_all_on(df=expected, **kwargs),
    )


@given(data=frames)
def test_empty_query_matches_search(data):
    lf = build_lazyframe(data)
    actual = pl_search(lf=lf, query={}, columns_with_iterables=ITERABLE_COLUMNS)
    expected = search(df=to_arrow(lf), query={}, columns_with_iterables=ITERABLE_COLUMNS)
    assert_frame_equal(actual, expected, check_dtype=False)
