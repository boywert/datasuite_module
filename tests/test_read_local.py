"""Tests for read_local.py — reading CSV, Excel, Parquet, TXT."""

import os
import tempfile

import pandas as pd
import polars as pl
import pytest

from datasuite.read_local import read_local


def _has_fastexcel():
    try:
        import fastexcel  # noqa: F401
        return True
    except ImportError:
        return False


@pytest.fixture
def sample_df():
    return pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"], "c": [1.1, 2.2, 3.3]})


@pytest.fixture
def csv_file(sample_df, tmp_path):
    p = tmp_path / "test.csv"
    sample_df.to_csv(p, index=False)
    return str(p)


@pytest.fixture
def tsv_file(sample_df, tmp_path):
    p = tmp_path / "test.txt"
    sample_df.to_csv(p, index=False, sep="\t")
    return str(p)


@pytest.fixture
def excel_file(sample_df, tmp_path):
    p = tmp_path / "test.xlsx"
    sample_df.to_excel(p, index=False)
    return str(p)


@pytest.fixture
def parquet_file(sample_df, tmp_path):
    p = tmp_path / "test.parquet"
    sample_df.to_parquet(p, index=False)
    return str(p)


class TestReadCSV:
    def test_pandas(self, csv_file, sample_df):
        result = read_local(csv_file, engine="pandas")
        pd.testing.assert_frame_equal(result, sample_df)

    def test_polars(self, csv_file):
        result = read_local(csv_file, engine="polars")
        assert isinstance(result, pl.LazyFrame)
        collected = result.collect()
        assert collected.shape == (3, 3)
        assert collected.columns == ["a", "b", "c"]

    def test_explicit_file_type(self, csv_file):
        result = read_local(csv_file, file_type="csv", engine="pandas")
        assert len(result) == 3

    def test_kwargs_forwarded(self, csv_file):
        result = read_local(csv_file, engine="pandas", usecols=["a", "b"])
        assert list(result.columns) == ["a", "b"]


class TestReadTXT:
    def test_tab_separated(self, tsv_file):
        result = read_local(tsv_file, engine="pandas", sep="\t")
        assert len(result) == 3
        assert list(result.columns) == ["a", "b", "c"]


class TestReadExcel:
    def test_pandas(self, excel_file, sample_df):
        result = read_local(excel_file, engine="pandas")
        pd.testing.assert_frame_equal(result, sample_df)

    @pytest.mark.skipif(
        not _has_fastexcel(),
        reason="fastexcel not installed (required by polars for Excel)",
    )
    def test_polars(self, excel_file):
        result = read_local(excel_file, engine="polars")
        assert isinstance(result, pl.LazyFrame)
        assert result.collect().shape == (3, 3)


class TestReadParquet:
    def test_pandas(self, parquet_file, sample_df):
        result = read_local(parquet_file, engine="pandas")
        pd.testing.assert_frame_equal(result, sample_df)

    def test_polars(self, parquet_file):
        result = read_local(parquet_file, engine="polars")
        assert isinstance(result, pl.LazyFrame)
        assert result.collect().shape == (3, 3)


class TestEdgeCases:
    def test_unsupported_extension_raises(self, tmp_path):
        p = tmp_path / "test.json"
        p.write_text("{}")
        with pytest.raises(ValueError, match="Unsupported file extension"):
            read_local(str(p), engine="pandas")

    def test_buffer_without_file_type_raises(self):
        import io
        buf = io.BytesIO(b"a,b\n1,2\n")
        with pytest.raises(ValueError, match="Cannot detect file type"):
            read_local(buf, engine="pandas")

    def test_buffer_with_file_type(self):
        import io
        buf = io.BytesIO(b"a,b\n1,2\n")
        result = read_local(buf, file_type="csv", engine="pandas")
        assert list(result.columns) == ["a", "b"]
        assert len(result) == 1

    def test_pathlib_path(self, csv_file):
        import pathlib
        result = read_local(pathlib.Path(csv_file), engine="pandas")
        assert len(result) == 3
