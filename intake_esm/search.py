import itertools
from collections.abc import Iterable
from typing import Pattern
from warnings import warn

import numpy as np
import pandas as pd


def _unique(df, columns=None):
    if isinstance(columns, str):
        columns = [columns]
    if not columns:
        columns = df.columns.tolist()

    def _find_unique(series):
        values = series.dropna().values
        uniques = list(set(_flatten_list(values)))
        return uniques

    x = df[columns].apply(_find_unique, result_type='reduce').to_dict()
    info = {}
    for col in x.keys():
        info[col] = {'count': len(x[col]), 'values': x[col]}
    return info


def search(df, require_all_on=None, **query):
    """
    Search for entries in a pandas DataFrame.

    Parameters
    ----------
    df : pd.DataFrame
        Pandas DataFrame to run query against.
    require_all_on : list, str, optional
        A dataframe column or a list of dataframe columns across
        which all entries must satisfy the query criteria.
        If None, return entries that fulfill any of the criteria specified
        in the query, by default None.
    **query:
        keyword arguments corresponding to user's query to execute against the dataframe.

    Returns
    -------
    pd.DataFrame
    """
    columns_with_iterables = _get_columns_with_iterables(df)
    message = 'Query returned zero results.'
    if not query:
        warn(message)
        return pd.DataFrame(columns=df.columns)
    condition = np.ones(len(df), dtype=bool)
    query = _normalize_query(query)
    for key, val in query.items():
        condition_i = np.zeros(len(df), dtype=bool)
        column_is_stringtype = isinstance(
            df[key].dtype, (np.object, pd.core.arrays.string_.StringDtype)
        )
        column_has_iterables = key in columns_with_iterables
        for val_i in val:
            if column_has_iterables:
                cond = df[key].str.contains(val_i, regex=False)
            else:
                value_is_pattern = _is_pattern(val_i)
                if column_is_stringtype and value_is_pattern:
                    cond = df[key].str.contains(val_i, regex=True, case=True, flags=0)
                else:
                    cond = df[key] == val_i
            condition_i = condition_i | cond
        condition = condition & condition_i
    query_results = df.loc[condition]

    if require_all_on:
        if isinstance(require_all_on, str):
            require_all_on = [require_all_on]
        _query = query.copy()

        # Make sure to remove columns that were already
        # specified in the query when specified in `require_all_on`. For example,
        # if query = dict(variable_id=["A", "B"], source_id=["FOO", "BAR"])
        # and require_all_on = ["source_id"], we need to make sure `source_id` key is
        # not present in _query for the logic below to work
        for key in require_all_on:
            _query.pop(key, None)

        keys = list(_query.keys())
        grouped = query_results.groupby(require_all_on)
        values = [tuple(v) for v in _query.values()]
        condition = set(itertools.product(*values))
        results = []
        for key, group in grouped:
            index = group.set_index(keys).index
            if not isinstance(index, pd.MultiIndex):
                index = {(element,) for element in index.to_list()}
            else:
                index = set(index.to_list())
            if index == condition:
                results.append(group)

        if len(results) >= 1:
            return pd.concat(results).reset_index(drop=True)

        warn(message)
        return pd.DataFrame(columns=df.columns)
    if query_results.empty:
        warn(message)

    return query_results.reset_index(drop=True)


def _normalize_query(query):
    q = query.copy()
    for key, val in q.items():
        if isinstance(val, str) or not isinstance(val, Iterable):
            q[key] = [val]
    return q


def _is_pattern(value):
    if isinstance(value, Pattern):
        return True
    wildcard_chars = {'*', '?', '$', '^'}
    try:
        value_ = value
        for char in wildcard_chars:
            value_ = value_.replace(fr'\{char}', '')
        return any(char in value_ for char in wildcard_chars)
    except (TypeError, AttributeError):
        return False


def _flatten_list(data):
    for item in data:
        if isinstance(item, Iterable) and not isinstance(item, str):
            for x in _flatten_list(item):
                yield x
        else:
            yield item


def _get_columns_with_iterables(df):
    if not df.empty:
        has_iterables = (
            df.sample(20, replace=True).applymap(type).isin([list, tuple, set]).any().to_dict()
        )
        columns_with_iterables = [column for column, check in has_iterables.items() if check]
    else:
        columns_with_iterables = []
    return columns_with_iterables
