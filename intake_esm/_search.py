import itertools
import typing

import numpy as np
import pandas as pd

if typing.TYPE_CHECKING:
    from ._types import QueryModel

import warnings


def is_pattern(value):
    if isinstance(value, typing.Pattern):
        return True
    wildcard_chars = {'*', '?', '$', '^'}
    try:
        value_ = value
        for char in wildcard_chars:
            value_ = value_.replace(fr'\{char}', '')
        return any(char in value_ for char in wildcard_chars)
    except (TypeError, AttributeError):
        return False


def search(
    df: pd.DataFrame, query_model: 'QueryModel', columns_with_iterables: set
) -> pd.DataFrame:
    """Search for entries in the catalog."""
    query = query_model.normalize_query()
    if not query:
        warnings.warn(f'Empty query: {query} returned zero results.', UserWarning, stacklevel=2)
        return pd.DataFrame(columns=df.columns)
    global_mask = np.ones(len(df), dtype=bool)
    for column, values in query.items():
        local_mask = np.zeros(len(df), dtype=bool)
        column_is_stringtype = isinstance(
            df[column].dtype, (object, pd.core.arrays.string_.StringDtype)
        )
        column_has_iterables = column in columns_with_iterables
        for value in values:
            if column_has_iterables:
                mask = df[column].str.contains(value, regex=False)
            elif column_is_stringtype and is_pattern(value):
                mask = df[column].str.contains(value, regex=True, case=True, flags=0)
            else:
                mask = df[column] == value
            local_mask = local_mask | mask
        global_mask = global_mask & local_mask
    results = df.loc[global_mask]
    if results.empty:
        warnings.warn(f'Query: {query} returned zero results.', UserWarning, stacklevel=2)
    return results


def search_apply_require_all_on(results: pd.DataFrame, query_model: 'QueryModel') -> pd.DataFrame:
    _query = query_model.normalize_query().copy()
    require_all_on = query_model.require_all_on
    # Make sure to remove columns that were already
    # specified in the query when specified in `require_all_on`. For example,
    # if query = dict(variable_id=["A", "B"], source_id=["FOO", "BAR"])
    # and require_all_on = ["source_id"], we need to make sure `source_id` key is
    # not present in _query for the logic below to work
    for column in require_all_on:
        _query.pop(column, None)

    keys = list(_query.keys())
    grouped = results.groupby(require_all_on)
    values = [tuple(v) for v in _query.values()]
    condition = set(itertools.product(*values))
    query_results = []
    for _, group in grouped:
        index = group.set_index(keys).index
        if not isinstance(index, pd.MultiIndex):
            index = {(element,) for element in index.to_list()}
        else:
            index = set(index.to_list())
        if index == condition:
            query_results.append(group)

    if query_results:
        return pd.concat(query_results)

    warnings.warn(
        f'Query: {query_model.normalize_query()} returned zero results.', UserWarning, stacklevel=2
    )
    return pd.DataFrame(columns=results.columns)
