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


def is_stringtype(series):
    if isinstance(series.dtype, pd.ArrowDtype) and pd.api.types.is_string_dtype(series.dtype):
        return True
    if series.dtype == 'object' and pd.api.types.infer_dtype(series, skipna=True) == 'string':
        return True
    # scalar
    if isinstance(series.dtype, pd.core.arrays.string_.StringDtype) or series.dtype in [
        'string',
        'large_string',
    ]:
        return True
    # iterable (pandas has no list dtype, so it's pyarrow for sure)
    if series.dtype.type is list and series.dtype.pyarrow_dtype.value_type in [
        'string',
        'large_string',
    ]:
        return True
    # generic object, a bit more complicated, we only support sets, tuples and lists
    if series.dtype == 'object' and isinstance(series.iloc[0], (set, tuple, list)):
        return isinstance(next(iter(series.iloc[0])), str)
    return False


def iterable_contains(data, value, regex=False):
    if regex:
        return any([re.search(value, d) for d in data])
    return value in data


def search(
    *, df: pd.DataFrame, query: dict[str, typing.Any], columns_with_iterables: set
) -> pd.DataFrame:
    """Search for entries in the catalog."""

    if not query:
        return pd.DataFrame(columns=df.columns)
    global_mask = np.ones(len(df), dtype=bool)
    for column, values in query.items():
        local_mask = np.zeros(len(df), dtype=bool)
        column_is_stringtype = is_stringtype(df[column])
        column_has_iterables = column in columns_with_iterables
        for value in values:
            if column_has_iterables:
                mask = df[column].transform(
                    iterable_contains, value=value, regex=is_pattern(value) & column_is_stringtype
                )
            elif column_is_stringtype and is_pattern(value):
                # Arrow's regex engine can't handle look-around or compiled patterns.
                strings = df[column]
                if isinstance(strings.dtype, pd.ArrowDtype):
                    strings = strings.astype('string[python]')
                mask = strings.str.contains(value, regex=True, case=True, flags=0, na=False)
            elif pd.isna(value):
                mask = df[column].isnull()
            else:
                mask = df[column] == value
            local_mask = local_mask | mask
        global_mask = global_mask & local_mask
    results = df.loc[global_mask]
    return results.reset_index(drop=True)


def _pl_regex(pattern: str | typing.Pattern) -> str:
    """Return a pattern as Polars regex source, scoping its flags to the pattern."""
    if isinstance(pattern, re.Pattern):
        prefix = '(?i:' if pattern.flags & re.IGNORECASE else '(?:'
        return f'{prefix}{pattern.pattern})'
    return f'(?:{pattern})'


def _polars_can_run(pattern: str | typing.Pattern) -> bool:
    """Check whether Polars can match a pattern as Python's `re` would."""
    if isinstance(pattern, re.Pattern) and pattern.flags & ~(re.IGNORECASE | re.UNICODE):
        return False
    try:
        pl.select(pl.lit('').str.contains(_pl_regex(pattern)))
    except pl.exceptions.ComputeError:
        return False
    return True


def _pl_mask(element: pl.Expr, subquery: Collection, is_string: bool) -> pl.Expr:
    """Match exact values and regex patterns separately, as `search` does."""
    literals = [q for q in subquery if not is_pattern(q)]
    patterns = [q for q in subquery if is_pattern(q)]
    mask = element.is_in(literals, nulls_equal=True)
    if is_string and patterns:
        mask = mask | element.str.contains('|'.join(_pl_regex(q) for q in patterns))
    return mask


def pl_search(
    *,
    lf: pl.LazyFrame,
    query: dict[str, typing.Any],
    columns_with_iterables: set,
    iterable_dtypes: dict[str, type] | None = None,
) -> pd.DataFrame:
    """
    lf: :py:class:`~polars.LazyFrame`
        A lazy dataframe to search.
    query: dict
        A dictionary of query parameters to execute against the lazy dataframe.
    columns_with_iterables: set
        Names of columns in the lazy dataframe whose values contain iterables.
    iterable_dtypes: dict, optional
        Optional explicit Python container conversions for iterable columns.
        Without an override, retain Arrow-backed list storage in the result.
    Returns
    -------
    dataframe: :py:class:`~pandas.DataFrame`
        A pandas dataframe containing the entries satisfying the query criteria.

    """

    if not query:
        return lf.filter(pl.lit(False)).collect().to_pandas()

    full_schema = lf.head(1).collect().schema

    lf = lf.with_columns(
        [pl.col(colname).cast(full_schema[colname]) for colname in full_schema.keys()]
    )

    if isinstance(columns_with_iterables, str):
        columns_with_iterables = [columns_with_iterables]

    iterable_dtypes = iterable_dtypes or {}

    for colname, dtype in iterable_dtypes.items():
        if dtype == np.ndarray:
            iterable_dtypes[colname] = tuple

    # Patterns Polars can't run faithfully are the only ones matched in Python.
    use_python_regex = not all(
        _polars_can_run(value) for values in query.values() for value in values if is_pattern(value)
    )

    if not use_python_regex:
        for colname, subquery in query.items():
            dtype = full_schema[colname]
            if colname in columns_with_iterables:
                mask = _pl_mask(pl.element(), subquery, dtype.inner == pl.String)
                lf = lf.filter(pl.col(colname).list.eval(mask).list.any())
            else:
                subquery = [None if pd.isna(subq) else subq for subq in subquery]
                lf = lf.filter(_pl_mask(pl.col(colname), subquery, dtype == pl.String))

    df = lf.collect(engine='streaming').to_pandas(use_pyarrow_extension_array=True)

    if use_python_regex:
        df = search(df=df, query=query, columns_with_iterables=columns_with_iterables)

    for colname, dtype in iterable_dtypes.items():
        df[colname] = df[colname].astype(object).apply(dtype)

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
