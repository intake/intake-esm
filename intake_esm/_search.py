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


def pl_search(
    *,
    lf: pl.LazyFrame,
    query: dict[str, typing.Any],
    columns_with_iterables: set,
    iterable_dtypes: dict[str, type] | None = None,
) -> pd.DataFrame:
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
    iterable_dtypes: dict, optional
        A dictionary mapping column names to their iterable dtypes. If not provided,
        defaults to all tuple

    Returns
    -------
    dataframe: :py:class:`~pandas.DataFrame`
        A new dataframe with the entries satisfying the query criteria.

    """

    if not query:
        return lf.filter(pl.lit(False)).collect().to_pandas()

    full_schema = lf.head(1).collect().collect_schema()

    lf = lf.with_columns(
        [
            pl.col(colname).cast(lf.head(1).collect().collect_schema()[colname])
            for colname in full_schema.keys()
        ]
    )

    if isinstance(columns_with_iterables, str):
        columns_with_iterables = [columns_with_iterables]

    iterable_dtypes = iterable_dtypes or {colname: tuple for colname in columns_with_iterables}

    for colname, dtype in iterable_dtypes.items():
        if dtype == np.ndarray:
            iterable_dtypes[colname] = tuple

    query_non_iterable = {
        key: val for key, val in query.items() if key not in columns_with_iterables
    }

    for colname, subquery in query_non_iterable.items():
        subquery = [None if pd.isna(subq) else subq for subq in subquery]
        if is_pattern(subquery):
            case_insensitive = [
                bool(q.flags & re.IGNORECASE) if isinstance(q, re.Pattern) else False
                for q in subquery
            ]
            # Prepend (?i) to patterns for case insensitive matching if needed
            subquery = [q.pattern if isinstance(q, re.Pattern) else q for q in subquery]
            subquery = [f'(?i){q}' if ci else q for q, ci in zip(subquery, case_insensitive)]

            lf = lf.filter(pl.col(colname).str.contains('|'.join(subquery), literal=False))
        else:
            lf = lf.filter(pl.col(colname).is_in(subquery, nulls_equal=True))

    query_iterable = {key: val for key, val in query.items() if key in columns_with_iterables}
    for colname, subquery in query_iterable.items():
        if is_pattern(subquery):
            raise NotImplementedError(
                'Pattern matching within iterable columns is not implemented yet.'
            )
            case_insensitive = [
                bool(q.flags & re.IGNORECASE) if isinstance(q, re.Pattern) else False
                for q in subquery
            ]
            # Prepend (?i) to patterns for case insensitive matching if needed
            subquery = [q.pattern if isinstance(q, re.Pattern) else q for q in subquery]
            subquery = [f'(?i){q}' if ci else q for q, ci in zip(subquery, case_insensitive)]

            lf = lf.filter(
                pl.col(colname)
                .list.eval(pl.element().str.contains('|'.join(subquery), literal=False))
                .any()
            )
        else:
            lf = lf.filter(
                pl.col(colname)
                .list.eval(pl.element().is_in(subquery, nulls_equal=True).any())
                .explode()
            )

    df = lf.collect().to_pandas()

    for colname, dtype in iterable_dtypes.items():
        df[colname] = df[colname].apply(dtype)

    return df


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
