import ast
import tempfile
from pathlib import Path

import pandas as pd
import polars as pl
import pydantic
import pytest
from polars import testing as pl_testing
from pydantic import ValidationError

from intake_esm.cat import ESMCatalogModel
from intake_esm.iodrivers import (
    FramesModel,
    PandasCsvReader,
    PandasParquetReader,
    PolarsCsvReader,
    PolarsCsvWriter,
    PolarsParquetReader,
    PolarsParquetWriter,
)

from .utils import (
    access_columns_with_lists_cat,
    here,
    sample_df,
    sample_esmcat_data,
    sample_lf,
)


@pytest.mark.parametrize(
    'pd_df, lf, err',
    [
        (sample_df, sample_lf, False),
        (sample_df, None, False),
        (None, None, True),
        (None, sample_lf, False),
    ],
)
def test_FramesModel_init(pd_df, lf, err):
    """
    Make sure FramesModel works with different input combos
    """
    if not err:
        FramesModel(df=pd_df, lf=lf)
        assert True
    else:
        with pytest.raises(pydantic.ValidationError):
            FramesModel(df=pd_df, lf=lf)


@pytest.mark.parametrize(
    'pd_df, lf',
    [
        (sample_df, sample_lf),
        (sample_df, None),
        (None, sample_lf),
    ],
)
@pytest.mark.parametrize('attr', ['polars', 'lazy', 'columns_with_iterables'])
def test_FramesModel_no_accidental_pd(pd_df, lf, attr):
    """
    Make sure that if we instantiate with a polars dataframe or a lazy frame, we
    don't accidentally trigger the creation of a pandas dataframe.
    """
    f = FramesModel(df=pd_df, lf=lf)

    if pd_df is not None:
        assert f.df is not None
    else:
        assert f.df is None

    # Now we just want to run through the properties to ensure they don't error
    # and that they don't trigger the creation of a pandas dataframe
    if pd_df is None:
        got_attr = getattr(f, attr)
        assert got_attr is not None
        assert f.df is None
    else:
        got_attr = getattr(f, attr)
        assert got_attr is not None
        assert f.df is not None


def test_FramesModel_polars_from_lf():
    pd_df = None
    lf = sample_lf
    f = FramesModel(df=pd_df, lf=lf)

    assert isinstance(f.polars, pl.DataFrame)


@pytest.mark.parametrize(
    'lf, pd_df',
    [
        (sample_lf.head(0), None),
        (None, sample_df.head(0)),
    ],
)
def test_FramesModel_columns_with_iterables(lf, pd_df):
    if lf is not None:
        lf = sample_lf.head(0)
    if pd_df is not None:
        pd_df = sample_df.head(0)

    f = FramesModel(df=pd_df, lf=lf)
    assert f.columns_with_iterables == set()


def test_FramesModel_set_manual_df():
    """
    Test that if we set esmcat._df, we don't cause an error or our state to get wonky.
    """
    cat = ESMCatalogModel.from_dict({'esmcat': sample_esmcat_data, 'df': sample_df})

    new_df = pd.DataFrame({'numeric_col': [1, 2, 3], 'str_col': ['a', 'b', 'c']})
    cat._df = new_df

    expected_pl_df = pl.DataFrame({'numeric_col': [1, 2, 3], 'str_col': ['a', 'b', 'c']})

    pl_testing.assert_frame_equal(cat.pl_df, expected_pl_df)


def test_FramesModel_applies_tuple():
    """
    Test that if we have a column with lists, we convert it to tuples in the pandas dataframe.
    """
    cat = ESMCatalogModel.load(
        access_columns_with_lists_cat,
        df_reader='polars',
        read_kwargs={'converters': {'variable': ast.literal_eval}},
    )

    pandas_df = cat._frames.pandas
    assert 'variable' in pandas_df.columns
    assert pandas_df['variable'].map(lambda value: isinstance(value, tuple)).all()


def test_frames_model_ok_with_pandas_only():
    df = pd.DataFrame({'a': [1, 2]})
    m = FramesModel(df=df, lf=None)
    assert m.df is not None
    assert m.lf is None


def test_frames_model_error_when_all_none():
    with pytest.raises(ValidationError):
        FramesModel(df=None, lf=None)


@pytest.mark.parametrize('read_kwargs', [{}, {'converters': {'variable': ast.literal_eval}}])
def test_polarsParquetReader(read_kwargs):
    """
    Also test that the parquet reader ignores columns_with_iterables since parquet
    natively supports them. These are passed as converters (see `esm_datastore.__init__`)
    """
    pl_pqr = PolarsParquetReader(
        Path(here) / 'sample-catalogs' / 'cmip5-netcdf.parquet', {}, **read_kwargs
    )
    assert pl_pqr._frames is None
    pl_pqr.frames  # Should trigger .read()
    assert pl_pqr._frames is not None


@pytest.mark.parametrize('read_kwargs', [{}, {'converters': {'variable': ast.literal_eval}}])
def test_pandasParquetReader(read_kwargs):
    """
    Also test that the parquet reader ignores columns_with_iterables since parquet
    natively supports them. These are passed as converters (see `esm_datastore.__init__`)
    """
    pd_pqr = PandasParquetReader(
        Path(here) / 'sample-catalogs' / 'cmip5-netcdf.parquet', {}, **read_kwargs
    )
    assert pd_pqr._frames is None
    with pytest.raises(NotImplementedError):
        pd_pqr.frames  # Should trigger .read()


def test_polarsParquetReader_file_none():
    with pytest.raises(
        AssertionError, match='catalog_file must be set to a valid file path or URL'
    ):
        PolarsParquetReader(None, {})


@pytest.mark.parametrize('csv_reader', [PandasCsvReader, PolarsCsvReader])
def test_csv_readers(csv_reader):
    reader = csv_reader(Path(here) / 'sample-catalogs' / 'cmip5-netcdf.csv', {})
    assert reader._frames is None
    reader.frames  # Should trigger .read()
    assert reader._frames is not None


@pytest.mark.parametrize('csv_reader', [PandasCsvReader, PolarsCsvReader])
@pytest.mark.parametrize(
    'csv_fname',
    [
        'access-columns-with-tuples',
        'access-columns-with-sets',
        'multi-variable-catalog',
    ],
)
def test_readers_columns_with_iterables(csv_reader, csv_fname):
    fname = Path(here) / 'sample-catalogs' / f'{csv_fname}.csv'
    reader = csv_reader(fname, storage_options={}, converters={'variable': ast.literal_eval})
    frames = reader.frames
    pl_df = frames.polars

    assert pl_df.get_column('variable').dtype == pl.List(pl.Utf8)

    # Then write it to a parquet file in a temporary directory and read it
    # back with the PolarsParquetReader to ensure round-trip works
    with tempfile.TemporaryDirectory() as tmpdir:
        out_parquet = Path(tmpdir) / 'out.parquet'
        pl_df.write_parquet(out_parquet)

        pqr = PolarsParquetReader(
            out_parquet, storage_options={}, converters={'variable': ast.literal_eval}
        )
        pl_df_roundtrip = pqr.frames.polars

        pl_testing.assert_frame_equal(pl_df, pl_df_roundtrip)


@pytest.mark.parametrize('writer', [PolarsParquetWriter, PolarsCsvWriter])
def test_unimplemented_writers(writer):
    test_args = {
        'data': {},
        'df': pd.DataFrame(),
        'dtype_map': {},
        'name': 'test_catalog',
        'write_kwargs': {},
    }
    with pytest.raises(NotImplementedError):
        writer.write(**test_args)


@pytest.mark.parametrize(
    'cls', [PolarsCsvReader, PandasCsvReader, PolarsParquetReader, PandasParquetReader]
)
def test_CatalogFileReader_no_catalog_file(cls):
    """
    Test that CatalogFileWriter raises a ValueError if catalog_type is not 'dict'
    """
    with pytest.raises(AssertionError, match='catalog_file must be set '):
        cls(catalog_file=None, storage_options={})
