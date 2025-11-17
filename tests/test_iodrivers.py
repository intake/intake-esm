import ast
import tempfile
from pathlib import Path

import pandas as pd
import polars as pl
import pydantic
import pytest
from polars import testing as pl_testing

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
    here,
    sample_df,
    sample_esmcat_data,
    sample_lf,
    sample_pl_df,
)


@pytest.mark.parametrize(
    'pd_df, pl_df, lf, err',
    [
        (sample_df, sample_pl_df, sample_lf, False),
        (sample_df, None, None, False),
        (None, None, None, True),
        (None, sample_pl_df, None, False),
        (None, None, sample_lf, False),
    ],
)
def test_FramesModel_init(pd_df, pl_df, lf, err):
    """
    Make sure FramesModel works with different input combos
    """
    if not err:
        FramesModel(df=pd_df, pl_df=pl_df, lf=lf)
        assert True
    else:
        with pytest.raises(pydantic.ValidationError):
            FramesModel(df=pd_df, pl_df=pl_df, lf=lf)


@pytest.mark.parametrize(
    'pd_df, pl_df, lf',
    [
        (sample_df, sample_pl_df, sample_lf),
        (sample_df, None, None),
        (None, sample_pl_df, None),
        (None, None, sample_lf),
    ],
)
@pytest.mark.parametrize('attr', ['polars', 'lazy', 'columns_with_iterables'])
def test_FramesModel_no_accidental_pd(pd_df, pl_df, lf, attr):
    """
    Make sure that if we instantiate with a polars dataframe or a lazy frame, we
    don't accidentally trigger the creation of a pandas dataframe.
    """
    f = FramesModel(df=pd_df, pl_df=pl_df, lf=lf)

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


def test_FramesModel_no_accidental_pl():
    """
    Make sure that if we instantiate with a pandas dataframe, we
    don't accidentally trigger the creation of a polars dataframe.
    """
    pd_df = sample_df
    pl_df = None
    lf = None
    f = FramesModel(df=pd_df, pl_df=pl_df, lf=lf)

    assert f.pl_df is None
    assert f.polars is not None
    assert f.pl_df is not None


def test_FramesModel_pandas_from_pldf():
    pd_df = None
    pl_df = sample_pl_df
    lf = None
    f = FramesModel(df=pd_df, pl_df=pl_df, lf=lf)

    assert isinstance(f.pandas, pd.DataFrame)


def test_FramesModel_polars_from_lf():
    pd_df = None
    pl_df = None
    lf = sample_lf
    f = FramesModel(df=pd_df, pl_df=pl_df, lf=lf)

    assert isinstance(f.polars, pl.DataFrame)


def test_FramesModel_columns_with_iterables():
    pd_df = None
    pl_df = None
    lf = sample_lf.head(0)
    f = FramesModel(df=pd_df, pl_df=pl_df, lf=lf)
    assert f.columns_with_iterables == set()


def test_FramesModel_set_manual_df():
    """
    Test that if we set esmcat._df, we don't cause an error. We also test that the
    creation of `cat._frames.pl_df` is deferred until we ask for it with the
    `cat.pl_df` property.
    """
    cat = ESMCatalogModel.from_dict({'esmcat': sample_esmcat_data, 'df': sample_df})

    assert cat._frames.pl_df is None

    new_df = pd.DataFrame({'numeric_col': [1, 2, 3], 'str_col': ['a', 'b', 'c']})
    cat._df = new_df

    assert getattr(cat, '_frames') is not None

    pd.testing.assert_frame_equal(cat.df, new_df)

    expected_pl_df = pl.DataFrame({'numeric_col': [1, 2, 3], 'str_col': ['a', 'b', 'c']})

    assert cat._frames.pl_df is None
    pl_testing.assert_frame_equal(cat.pl_df, expected_pl_df)


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
