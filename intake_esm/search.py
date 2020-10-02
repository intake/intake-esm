import itertools
from collections.abc import Iterable
from typing import Pattern
from warnings import warn

import numpy as np
import pandas as pd
from datetime import datetime

def _search_time(df, needed_timerange):
#Some checks for the format:
    if len(needed_timerange) != 2 :
        message="Timerange must have start and end values."
        warn(message)
        return pd.DataFrame(columns=columns)
    if type(needed_timerange) == tuple:
        needed_timerange = list(needed_timerange)
    try:
        int(needed_timerange[0])
        int(needed_timerange[1])
    except:
        message="Timerange values must be convertable into integers."
        warn(message)
        return pd.DataFrame(columns=columns)

#Functions used in the actual code:
    def combine_alternately(S1, S2):
        i = 0
        while i < len(S2):
            yield S1[i]
            yield S2[i]
            i = i + 1
        yield S1[i]

    def limit_format(fmt, date):
        last_entry = int((len(date) - 2) / 2)
        return fmt[:last_entry]

    def select_fmt(date):
        fmt = ['%Y','%m','%d','%H','%M','%s']
        nondigits = [x for x in date if not x.isdigit()]
        fmt = combine_alternately(fmt, nondigits) if nondigits else limit_format(fmt, date)    
        fmt = ''.join(fmt)
        return fmt

    def strptime(date):
        return datetime.strptime(date, select_fmt(date))

    def within_timerange(needed_timerange, given_timerange):
        n_start = strptime(needed_timerange[0])
        n_stop  = strptime(needed_timerange[1])

        try:
            g_start = strptime(given_timerange[0])
            g_stop  = strptime(given_timerange[1])
        except:
            g_start = n_start
            g_stop = n_stop

        if g_start <= n_start and n_start <= g_stop:
            return True
        elif g_start <= n_stop and n_stop <= g_stop:
            return True
        elif n_start <= g_start and g_stop <= n_stop:
            return True
        else:
            return False        

    rows, columns = df.shape
    given_timeranges = df['time_range'].to_list()
    i = 0
    drop = []
    while i < rows:
        if isinstance(given_timeranges[i], str):
            within = within_timerange(needed_timerange, given_timeranges[i].split('-'))
            if not within: drop.append(i)
        i = i + 1
    drop = df.index[drop]
    return df.drop(drop)

def _unique(df, columns=None):
    if isinstance(columns, str):
        columns = [columns]
    if not columns:
        columns = df.columns.tolist()
    info = {}
    for col in columns:
        values = df[col].dropna().values
        uniques = np.unique(list(_flatten_list(values))).tolist()
        info[col] = {'count': len(uniques), 'values': uniques}
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
        for val_i in val:
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
