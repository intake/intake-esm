import itertools
import re
import typing
from collections.abc import Collection

import numpy as np
import pandas as pd
import polars as pl


def unpack_iterable_column(df: pd.DataFrame, column: str) -> pd.DataFrame:
    """Return a DataFrame where elements of a given iterable column have been unpacked into multiple lines."""
    rows = []
    for _, row in df.iterrows():
        for val in row[column]:
            new_row = row.copy()
            new_row[column] = val
            rows.append(new_row)
    return pd.DataFrame(rows)


def is_pattern(value: str | typing.Pattern | Collection) -> bool:
    """
    Check whether the passed value is a pattern

    Parameters
    ----------
    value: str or Pattern
        The value to check
    """
    if isinstance(value, typing.Pattern):
        return True  # Obviously, it's a pattern
    if isinstance(value, Collection) and not isinstance(value, str):
        return any(is_pattern(item) for item in value)  # Recurse into the collection
    wildcard_chars = {'*', '?', '$', '^'}
    try:
        value_ = value
        for char in wildcard_chars:
            value_ = value_.replace(rf'\{char}', '')
        return any(char in value_ for char in wildcard_chars)
    except (TypeError, AttributeError):
        return False


def search(
    *, df: pd.DataFrame, query: dict[str, typing.Any], columns_with_iterables: set
) -> pd.DataFrame:
    """Search for entries in the catalog."""

    if not query:
        return pd.DataFrame(columns=df.columns)
    global_mask = np.ones(len(df), dtype=bool)
    for column, values in query.items():
        local_mask = np.zeros(len(df), dtype=bool)
        column_is_stringtype = isinstance(
            df[column].dtype, object | pd.core.arrays.string_.StringDtype
        )
        column_has_iterables = column in columns_with_iterables
        for value in values:
            if column_has_iterables:
                mask = df[column].str.contains(value, regex=False)
            elif column_is_stringtype and is_pattern(value):
                mask = df[column].str.contains(value, regex=True, case=True, flags=0)
            elif pd.isna(value):
                mask = df[column].isnull()
            else:
                mask = df[column] == value
            local_mask = local_mask | mask
        global_mask = global_mask & local_mask
    results = df.loc[global_mask]
    return results.reset_index(drop=True)


from memory_profiler import profile


@profile
def pl_search(*, lf: pl.LazyFrame, query: dict[str, typing.Any], columns_with_iterables: set):
    """
    Search for entries in the catalog.

    Parameters
    ----------
    df: :py:class:`~pandas.DataFrame`
        A dataframe to search
    query: dict
        A dictionary of query parameters to execute against the dataframe
    columns_with_iterables: list
        Columns in the dataframe that have iterables
    name_column: str
        The name column in the dataframe catalog
    require_all: bool
        If True, groupby name_column and return only entries that match
        for all elements in each group

    Returns
    -------
    dataframe: :py:class:`~pandas.DataFrame`
        A new dataframe with the entries satisfying the query criteria.

    """
    if not query:
        return lf.filter(pl.lit(False)).collect().to_pandas()

    if isinstance(columns_with_iterables, str):
        columns_with_iterables = [columns_with_iterables]

    schema = lf.collect_schema()
    all_cols = schema.names()

    str_cols = [col for col, dtype in schema.items() if dtype == pl.String]
    int_cols = [col for col, dtype in schema.items() if dtype == pl.Int64]
    float_cols = [col for col, dtype in schema.items() if dtype == pl.Float64]

    sentinel_map = {
        pl.String: '_NULL_SENTINEL_',
        pl.Int64: -999999,
        pl.Float64: -999999.0,
    }

    _sentinels = {
        col: sentinel_map[dtype] for col, dtype in schema.items() if dtype in sentinel_map
    }

    cols_to_deiter = set(all_cols).difference(columns_with_iterables)

    lf = lf.with_row_index(name='_index')
    for column in columns_with_iterables:
        # N.B: Cannot explode multiple columns together as we need a cartesian product
        lf = lf.explode(column)

    # Make sure we can keep pre-existing nulls distinct from non-matches in the
    # next steps
    lf = lf.with_columns(
        pl.col(colname).fill_null(sentinel) for colname, sentinel in _sentinels.items()
    )

    lf, tmp_cols = _match_and_filter(lf, query)

    lf = _group_and_filter_on_index(lf, all_cols, tmp_cols)

    lf = lf.select(*all_cols)

    lf = lf.explode(list(cols_to_deiter))

    # Change back sentinel to nulls and collect to the final pandas DataFrame
    df = (
        lf.with_columns(
            [
                *[pl.col(col).replace('_NULL_SENTINEL_', None) for col in str_cols],
                *[pl.col(col).replace(-999999, None) for col in int_cols],
                *[pl.col(col).replace(-999999.0, None) for col in float_cols],
            ]
        )
        .collect()
        .to_pandas()
    )

    for colname in columns_with_iterables:
        df[colname] = df[colname].apply(tuple)

    return df


def _group_and_filter_on_index(
    lf: pl.LazyFrame,
    all_cols: list[str],
    tmp_cols: list[str],
    /,
) -> pl.LazyFrame:
    return (
        lf.group_by('_index')
        .agg(
            [
                pl.col(col).flatten().unique(maintain_order=True).drop_nulls()
                for col in [*all_cols, *tmp_cols]
            ]
        )
        .drop('_index')
    )


def _match_and_filter(
    lf: pl.LazyFrame, query: dict[str, typing.Any], /
) -> tuple[pl.LazyFrame, list[str]]:
    """
    Take a lazyframe and a query dict, and add match columns and filter the lazyframe
    accordingly. Positional-only arguments - internal use only.
    """
    schema = lf.collect_schema()

    for colname, subquery in query.items():
        if schema[colname] == pl.Utf8 and is_pattern(subquery):
            case_insensitive = [
                bool(q.flags & re.IGNORECASE) if isinstance(q, re.Pattern) else False
                for q in subquery
            ]

            # Prepend (?i) to patterns for case insensitive matching if needed
            subquery = [q.pattern if isinstance(q, re.Pattern) else q for q in subquery]
            subquery = [f'(?i){q}' if ci else q for q, ci in zip(subquery, case_insensitive)]

            # Build match expressions
            match_exprs = [
                pl.when(pl.col(colname).str.contains(q)).then(pl.lit(q)).otherwise(None)
                for q in subquery
            ]
        else:
            subquery = ['_NULL_SENTINEL_' if pd.isna(q) else q for q in subquery]
            # Can't unify these branches with literal=True, because that assumes
            # non-pattern columns *must be strings*, which is not the case.
            match_exprs = [
                pl.when(pl.col(colname) == q)
                .then(pl.lit('_NULL_SENTINEL_' if q is None else q))
                .otherwise(None)
                for q in subquery
            ]

        _matchlist = pl.concat_list(match_exprs)

        lf = lf.with_columns(
            pl.when(_matchlist.list.drop_nulls().list.len() > 0)
            .then(_matchlist)
            .otherwise(None)  # This whole when-then-otherwise is to map empty lists to null
            .alias(f'{colname}_matches')
        )

        lf = lf.filter(pl.col(f'{colname}_matches').is_not_null())

    tmp_cols = [f'{colname}_matches' for colname in query.keys()]

    return lf, tmp_cols


def search_apply_require_all_on(
    *,
    df: pd.DataFrame,
    query: dict[str, typing.Any],
    require_all_on: str | list[typing.Any],
    columns_with_iterables: set | None = None,
) -> pd.DataFrame:
    _query = query.copy()
    # Make sure to remove columns that were already
    # specified in the query when specified in `require_all_on`. For example,
    # if query = dict(variable_id=["A", "B"], source_id=["FOO", "BAR"])
    # and require_all_on = ["source_id"], we need to make sure `source_id` key is
    # not present in _query for the logic below to work
    for column in require_all_on:
        _query.pop(column, None)

    keys = list(_query.keys())
    grouped = df.groupby(require_all_on)
    values = [tuple(v) for v in _query.values()]
    condition = set(itertools.product(*values))
    query_results = []
    for _, group in grouped:
        group_for_index = group
        # Unpack iterables to get testable index.
        for column in (columns_with_iterables or set()).intersection(keys):
            group_for_index = unpack_iterable_column(group_for_index, column)

        index = group_for_index.set_index(keys).index
        if not isinstance(index, pd.MultiIndex):
            index = {(element,) for element in index.to_list()}
        else:
            index = set(index.to_list())
        if condition.issubset(index):  # with iterables we could have more then requested
            query_results.append(group)

    if query_results:
        return pd.concat(query_results).reset_index(drop=True)

    return pd.DataFrame(columns=df.columns)
