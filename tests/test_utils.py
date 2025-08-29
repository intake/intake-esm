from unittest import mock

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from intake_esm.utils import MinimalExploder, _set_async_flag


@pytest.mark.parametrize(
    'data_format, xarray_open_kwargs, expected',
    [
        ('zarr', {}, {}),
        ('netcdf', {}, {}),
        ('reference', {}, {}),
        (
            'zarr2',
            {},
            {
                'backend_kwargs': {
                    'storage_options': {'remote_options': {'asynchronous': False}},
                }
            },
        ),
        (
            'zarr3',
            {},
            {
                'backend_kwargs': {
                    'storage_options': {'remote_options': {'asynchronous': True}},
                }
            },
        ),
        (
            'zarr2',
            {
                'engine': 'zarr',
                'chunks': {},
                'backend_kwargs': {},
                'decode_timedelta': False,
            },
            {
                'engine': 'zarr',
                'chunks': {},
                'backend_kwargs': {
                    'storage_options': {'remote_options': {'asynchronous': False}},
                },
                'decode_timedelta': False,
            },
        ),
        (
            'zarr3',
            {
                'engine': 'zarr',
                'chunks': {},
                'backend_kwargs': {},
                'decode_timedelta': False,
            },
            {
                'engine': 'zarr',
                'chunks': {},
                'backend_kwargs': {
                    'storage_options': {'remote_options': {'asynchronous': True}},
                },
                'decode_timedelta': False,
            },
        ),
        (
            'zarr2',
            {
                'engine': 'zarr',
                'chunks': {},
                'backend_kwargs': {'storage_options': {'remote_options': {'anon': True}}},
                'decode_timedelta': False,
            },
            {
                'engine': 'zarr',
                'chunks': {},
                'backend_kwargs': {
                    'storage_options': {'remote_options': {'anon': True, 'asynchronous': False}},
                },
                'decode_timedelta': False,
            },
        ),
        (
            'zarr3',
            {
                'engine': 'zarr',
                'chunks': {},
                'backend_kwargs': {'storage_options': {'remote_options': {'anon': True}}},
                'decode_timedelta': False,
            },
            {
                'engine': 'zarr',
                'chunks': {},
                'backend_kwargs': {
                    'storage_options': {'remote_options': {'anon': True, 'asynchronous': True}},
                },
                'decode_timedelta': False,
            },
        ),
        (
            'zarr3',
            {
                'engine': 'zarr',
                'chunks': {},
                'backend_kwargs': {
                    'storage_options': {
                        'remote_options': {'anon': True},
                        'remote_protocol': 's3',
                    }
                },
                'decode_timedelta': False,
            },
            {
                'engine': 'zarr',
                'chunks': {},
                'backend_kwargs': {
                    'storage_options': {
                        'remote_options': {'anon': True, 'asynchronous': True},
                        'remote_protocol': 's3',
                    }
                },
                'decode_timedelta': False,
            },
        ),
    ],
)
def test__set_async_flag(data_format, xarray_open_kwargs, expected):
    with mock.patch('intake_esm.utils._zarr_async', return_value=data_format == 'zarr3'):
        res = _set_async_flag(data_format, xarray_open_kwargs)
    assert res == expected


def test_MinimalExploder_length_patterns():
    df = pl.DataFrame(
        {
            'a': [['a', 'b'], ['c'], ['d', 'e', 'f']],
            'b': [['a'], ['b', 'c'], ['d', 'e', 'f']],
            'c': [['a', 'b', 'c'], ['d'], ['e', 'f']],
            'd': [[1, 2], [3], [4, 5, 6]],  # Same as a but all numbers
            'e': ['first', 'second', 'third'],  # not iterable``
        }
    )

    me = MinimalExploder(df)

    assert me.length_patterns == {
        'a': (2, 1, 3),
        'b': (1, 2, 3),
        'c': (3, 1, 2),
        'd': (2, 1, 3),
    }


def test_MinimalExploder_explodable_groups():
    df = pl.DataFrame(
        {
            'a': [['a', 'b'], ['c'], ['d', 'e', 'f']],
            'b': [['a'], ['b', 'c'], ['d', 'e', 'f']],
            'c': [['a', 'b', 'c'], ['d'], ['e', 'f']],
            'd': [[1, 2], [3], [4, 5, 6]],  # Same as a but all numbers
            'e': ['first', 'second', 'third'],  # not iterable``
        }
    )

    me = MinimalExploder(df)

    assert me.explodable_groups == [['a', 'd'], ['b'], ['c']]


def test_MinimalExploder_summary():
    df = pl.DataFrame(
        {
            'a': [['a', 'b'], ['c'], ['d', 'e', 'f']],
            'b': [['a'], ['b', 'c'], ['d', 'e', 'f']],
            'c': [['a', 'b', 'c'], ['d'], ['e', 'f']],
            'd': [[1, 2], [3], [4, 5, 6]],  # Same as a but all numbers
            'e': ['first', 'second', 'third'],  # not iterable``
        }
    )

    me = MinimalExploder(df)

    assert me.summary == {
        'explodable_groups': 3,
        'explosion_operations_needed': 3,
        'groups': [
            ['a', 'd'],
            ['b'],
            ['c'],
        ],
        'list_columns': 4,
        'total_columns': 5,
        'unique_patterns': 3,
    }


def test_MinimalExploder_single_explode():
    """Ensure two columns with the same list lengths are only exploded once"""
    df = pl.DataFrame(
        {
            'a': [['a', 'b'], ['c'], ['d', 'e', 'f']],
            'b': [[1, 2], [3], [4, 5, 6]],  # Same as a but all numbers
            'c': ['first', 'second', 'third'],
        }
    )

    exploded_df = MinimalExploder(df)()
    assert (len(exploded_df)) == 6

    expected_df_1 = pl.DataFrame(
        {
            'a': ['a', 'b', 'c', 'd', 'e', 'f'],
            'b': [1, 2, 3, 4, 5, 6],
            'c': ['first', 'first', 'second', 'third', 'third', 'third'],
        }
    )

    assert_frame_equal(exploded_df, expected_df_1)


def test_MinimalExploder_double_explode():
    """
    Make sure that if we have two columns which have different column lengths,
    we do explode both of them separately
    """
    df = pl.DataFrame(
        {
            'a': [[1, 2], [3, 4, 5]],
            'b': [['a', 'b', 'c'], ['d', 'e']],
            'c': ['first', 'second'],
        }
    )

    expected_df = pl.DataFrame(
        {
            'a': [1, 1, 1, 2, 2, 2, 3, 3, 4, 4, 5, 5],
            'b': ['a', 'b', 'c', 'a', 'b', 'c', 'd', 'e', 'd', 'e', 'd', 'e'],
            'c': [
                'first',
                'first',
                'first',
                'first',
                'first',
                'first',
                'second',
                'second',
                'second',
                'second',
                'second',
                'second',
            ],
        }
    )

    exploded_df = MinimalExploder(df)()

    assert_frame_equal(exploded_df, expected_df)
