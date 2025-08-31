import itertools
import typing

import numpy as np
import pandas as pd


def unpack_iterable_column(df: pd.DataFrame, column: str) -> pd.DataFrame:
    """Return a DataFrame where elements of a given iterable column have been unpacked into multiple lines."""
    rows = []
    for _, row in df.iterrows():
        for val in row[column]:
            new_row = row.copy()
            new_row[column] = val
            rows.append(new_row)
    return pd.DataFrame(rows)


def is_pattern(value):
    if isinstance(value, typing.Pattern):
        return True
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
